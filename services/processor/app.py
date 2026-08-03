"""Kafka stream processor for enrichment, fraud scoring and windowed metrics.

The processor uses a lightweight Python consumer as an alternative streaming
engine. It keeps event-time state in memory and persists derived Silver and
Gold tables in Delta format on MinIO.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from deltalake.writer import write_deltalake
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-processor-group")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "fraud")
MERCHANTS_PATH = os.getenv("MERCHANTS_PATH", "/data/merchants.csv")

HIGH_AMOUNT = float(os.getenv("HIGH_AMOUNT", "800"))
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.7"))
VELOCITY_THRESHOLD = int(os.getenv("VELOCITY_THRESHOLD", "10"))
WINDOW_DURATION = int(os.getenv("WINDOW_SECONDS", "60"))
ALLOWED_LATENESS = int(os.getenv("ALLOWED_LATENESS_SECONDS", "120"))
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "25"))
FLUSH_INTERVAL_MS = int(os.getenv("FLUSH_INTERVAL_MS", "1000"))

SILVER = f"s3://{LAKE_BUCKET}/silver/transactions"
GOLD_VELOCITY = f"s3://{LAKE_BUCKET}/gold/card_velocity"
GOLD_STATS = f"s3://{LAKE_BUCKET}/gold/fraud_stats"

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": S3_ENDPOINT,
    "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


def parse_event_time(value: str | None) -> tuple[datetime, bool]:
    if not value:
        return datetime.now(timezone.utc), True
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc), False
    except ValueError:
        return datetime.now(timezone.utc), True


def load_merchants(path: str) -> dict[str, dict[str, Any]]:
    df = pd.read_csv(path)
    return {
        str(row["merchant_id"]): {
            "merchant_category": row.get("category", "unknown"),
            "merchant_country": row.get("country", "unknown"),
            "merchant_risk": float(row.get("risk_score", 0.0)),
        }
        for _, row in df.iterrows()
    }


def write_delta(
    path: str,
    rows: list[dict[str, Any]],
    partition_by: list[str] | None = None,
) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    write_deltalake(
        path,
        df,
        mode="append",
        schema_mode="merge",
        partition_by=partition_by,
        storage_options=STORAGE_OPTIONS,
    )


def fraud_signals(amount: float, merchant_risk: float) -> tuple[int, int, float, int]:
    amount_flag = int(amount > HIGH_AMOUNT)
    merchant_flag = int(merchant_risk >= RISK_THRESHOLD)
    amount_component = 0.5 * min(max(amount / max(HIGH_AMOUNT, 1.0), 0.0), 2.0) / 2.0
    merchant_component = 0.5 * min(max(merchant_risk, 0.0), 1.0)
    fraud_score = round(amount_component + merchant_component, 4)
    is_fraud = int(amount_flag == 1 or merchant_flag == 1)
    return amount_flag, merchant_flag, fraud_score, is_fraud


def fixed_window(event_time: datetime) -> tuple[datetime, datetime]:
    start_epoch = int(event_time.timestamp()) // WINDOW_DURATION * WINDOW_DURATION
    start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    return start, start + timedelta(seconds=WINDOW_DURATION)


def flush_pending(
    consumer: KafkaConsumer,
    silver_rows: list[dict[str, Any]],
    velocity_rows: list[dict[str, Any]],
    stats_rows: dict[tuple[datetime, str], dict[str, Any]],
) -> None:
    if not silver_rows:
        return

    write_delta(SILVER, silver_rows, partition_by=["event_date"])
    write_delta(GOLD_VELOCITY, velocity_rows)
    write_delta(GOLD_STATS, list(stats_rows.values()))
    consumer.commit()

    print("Flushed batch to Delta tables")
    print(
        f"Batch details: {len(silver_rows)} Silver, {len(velocity_rows)} velocity, "
        f"{len(stats_rows)} stats rows"
    )
    silver_rows.clear()
    velocity_rows.clear()
    stats_rows.clear()


def main() -> None:
    merchants = load_merchants(MERCHANTS_PATH)

    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
            )
        except NoBrokersAvailable:
            print("Kafka not ready yet, retrying in 3s...")
            time.sleep(3)

    Path("/tmp/processor-ready").touch()

    # Each entry is (event_time, amount, fraud_score). Kafka keys keep all
    # events for one card in the same partition/consumer.
    card_windows: dict[str, deque[tuple[datetime, float, float]]] = defaultdict(deque)
    latest_card_time: dict[str, datetime] = {}
    stats_state: dict[tuple[datetime, str], dict[str, float | int]] = {}
    latest_global_time: datetime | None = None

    silver_rows: list[dict[str, Any]] = []
    velocity_rows: list[dict[str, Any]] = []
    stats_rows: dict[tuple[datetime, str], dict[str, Any]] = {}
    last_flush = time.monotonic()

    print("Processor started. Waiting for Kafka events...")

    while True:
        polled = consumer.poll(timeout_ms=250, max_records=FLUSH_EVERY)

        for messages in polled.values():
            for msg in messages:
                event = msg.value
                event_time, used_time_fallback = parse_event_time(event.get("event_time"))
                if used_time_fallback:
                    print(f"Invalid event_time for transaction {event.get('transaction_id')}; using UTC now")

                card_id = str(event.get("card_id", "unknown"))
                merchant_id = str(event.get("merchant_id", "unknown"))
                amount = float(event.get("amount", 0.0))

                merchant = merchants.get(
                    merchant_id,
                    {
                        "merchant_category": "unknown",
                        "merchant_country": "unknown",
                        "merchant_risk": 0.0,
                    },
                )

                merchant_risk = float(merchant["merchant_risk"])
                amount_flag, merchant_flag, fraud_score, is_fraud = fraud_signals(
                    amount, merchant_risk
                )

                previous_latest = latest_card_time.get(card_id, event_time)
                is_late = event_time < previous_latest
                is_too_late = event_time < previous_latest - timedelta(seconds=ALLOWED_LATENESS)
                latest_card_time[card_id] = max(previous_latest, event_time)

                card_state = card_windows[card_id]
                if not is_too_late:
                    card_state.append((event_time, amount, fraud_score))

                retention_start = latest_card_time[card_id] - timedelta(
                    seconds=WINDOW_DURATION + ALLOWED_LATENESS
                )
                card_state = deque(item for item in card_state if item[0] >= retention_start)
                card_windows[card_id] = card_state

                window_start = event_time - timedelta(seconds=WINDOW_DURATION)
                current_window = [
                    item for item in card_state if window_start <= item[0] <= event_time
                ]
                tx_count = len(current_window)
                amount_sum = round(sum(item[1] for item in current_window), 2)
                max_fraud_score = max((item[2] for item in current_window), default=fraud_score)
                velocity_alert = int(tx_count > VELOCITY_THRESHOLD and not is_too_late)

                silver_rows.append(
                    {
                        "transaction_id": event.get("transaction_id"),
                        "event_time": event_time.isoformat(),
                        "ingest_time": datetime.now(timezone.utc).isoformat(),
                        "event_date": event_time.date().isoformat(),
                        "card_id": card_id,
                        "user_id": event.get("user_id"),
                        "merchant_id": merchant_id,
                        "merchant_category": merchant["merchant_category"],
                        "merchant_country": merchant["merchant_country"],
                        "merchant_risk": merchant_risk,
                        "amount": amount,
                        "currency": event.get("currency", "EUR"),
                        "lat": event.get("lat"),
                        "lon": event.get("lon"),
                        "country": event.get("country"),
                        "amount_flag": amount_flag,
                        "merchant_flag": merchant_flag,
                        "fraud_score": fraud_score,
                        "is_fraud": is_fraud,
                        "is_late": int(is_late),
                        "event_time_fallback": int(used_time_fallback),
                        "velocity_excluded": int(is_too_late),
                    }
                )

                velocity_rows.append(
                    {
                        "window_start": window_start.isoformat(),
                        "window_end": event_time.isoformat(),
                        "card_id": card_id,
                        "tx_count": tx_count,
                        "amount_sum": amount_sum,
                        "max_fraud_score": max_fraud_score,
                        "velocity_alert": velocity_alert,
                        "late_event_excluded": int(is_too_late),
                    }
                )

                if not is_too_late:
                    stats_start, stats_end = fixed_window(event_time)
                    category = str(merchant["merchant_category"])
                    stats_key = (stats_start, category)
                    aggregate = stats_state.setdefault(
                        stats_key, {"total": 0, "flagged": 0, "score_sum": 0.0}
                    )
                    aggregate["total"] += 1
                    aggregate["flagged"] += is_fraud
                    aggregate["score_sum"] += fraud_score
                    total = int(aggregate["total"])
                    stats_rows[stats_key] = {
                        "window_start": stats_start.isoformat(),
                        "window_end": stats_end.isoformat(),
                        "merchant_category": category,
                        "total": total,
                        "flagged": int(aggregate["flagged"]),
                        "avg_fraud_score": round(float(aggregate["score_sum"]) / total, 4),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }

                latest_global_time = (
                    event_time
                    if latest_global_time is None
                    else max(latest_global_time, event_time)
                )
                stats_retention = latest_global_time - timedelta(
                    seconds=WINDOW_DURATION + ALLOWED_LATENESS
                )
                for key in list(stats_state):
                    if key[0] < stats_retention:
                        del stats_state[key]

        flush_due_to_size = len(silver_rows) >= FLUSH_EVERY
        flush_due_to_time = (
            silver_rows
            and (time.monotonic() - last_flush) * 1000 >= FLUSH_INTERVAL_MS
        )
        if flush_due_to_size or flush_due_to_time:
            flush_pending(consumer, silver_rows, velocity_rows, stats_rows)
            last_flush = time.monotonic()

        time.sleep(0.01)


if __name__ == "__main__":
    main()
