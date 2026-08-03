"""
Lightweight stream processor for local/offline-friendly execution.

Why this implementation:
- Keeps the required streaming pipeline behavior (ingestion -> processing -> storage -> serving)
- Avoids runtime Maven/JVM dependency downloads (blocked by corporate TLS in this environment)
- Still writes a Lakehouse-like storage using Delta tables on MinIO (S3)

Pipeline behavior:
- Consumes Kafka topic `transactions`
- Enriches with merchant risk metadata
- Computes non-trivial fraud features/scores
- Maintains per-card state for velocity detection in event-time windows
- Writes Silver + Gold Delta tables for the serving API
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
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
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "25"))

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


def parse_event_time(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


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
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
            )
        except NoBrokersAvailable:
            print("Kafka not ready yet, retrying in 3s...")
            time.sleep(3)

    # Stateful velocity tracking: per-card deque of event times (sliding window)
    card_windows: dict[str, deque[datetime]] = defaultdict(deque)

    silver_rows: list[dict[str, Any]] = []
    velocity_rows: list[dict[str, Any]] = []
    stats_rows: list[dict[str, Any]] = []

    print("Processor started. Waiting for Kafka events...")

    for msg in consumer:
        event = msg.value
        et = parse_event_time(event.get("event_time"))
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
        amount_flag = int(amount > HIGH_AMOUNT)
        merchant_flag = int(merchant_risk >= RISK_THRESHOLD)
        fraud_score = round(
            0.5 * min(amount / max(HIGH_AMOUNT, 1.0), 2.0) / 2.0 + 0.5 * merchant_risk,
            4,
        )
        is_fraud = int(amount_flag == 1 or merchant_flag == 1)

        # Update event-time state for velocity checks
        dq = card_windows[card_id]
        dq.append(et)
        threshold_time = et - timedelta(seconds=WINDOW_DURATION)
        while dq and dq[0] < threshold_time:
            dq.popleft()

        tx_count = len(dq)
        velocity_alert = int(tx_count > VELOCITY_THRESHOLD)
        window_end = et
        window_start = et - timedelta(seconds=WINDOW_DURATION)

        silver_rows.append(
            {
                "transaction_id": event.get("transaction_id"),
                "event_time": et.isoformat(),
                "ingest_time": datetime.now(timezone.utc).isoformat(),
                "ingest_date": et.date().isoformat(),
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
            }
        )

        velocity_rows.append(
            {
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "card_id": card_id,
                "tx_count": tx_count,
                "amount_sum": amount,
                "max_fraud_score": fraud_score,
                "velocity_alert": velocity_alert,
            }
        )

        stats_rows.append(
            {
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "merchant_category": merchant["merchant_category"],
                "total": 1,
                "flagged": is_fraud,
                "avg_fraud_score": fraud_score,
            }
        )

        if len(silver_rows) >= FLUSH_EVERY:
            write_delta(SILVER, silver_rows, partition_by=["ingest_date"])
            write_delta(GOLD_VELOCITY, velocity_rows)
            write_delta(GOLD_STATS, stats_rows)
            silver_rows.clear()
            velocity_rows.clear()
            stats_rows.clear()
            print("Flushed batch to Delta tables")

        # Small pacing avoids busy-loop on high frequency runs
        time.sleep(0.01)


if __name__ == "__main__":
    main()
