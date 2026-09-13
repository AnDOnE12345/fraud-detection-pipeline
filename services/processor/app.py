"""Atomic event facts and window features, recovered by Kafka partition."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from deltalake.writer import write_deltalake
from kafka import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.errors import CommitFailedError, NoBrokersAvailable
from kafka.structs import OffsetAndMetadata

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-processor-group")
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "fraud")
LAKE_PREFIX = os.getenv("LAKE_PREFIX", "v2").strip("/")
SILVER = f"s3://{LAKE_BUCKET}/{LAKE_PREFIX}/silver/transactions"
MERCHANTS_PATH = os.getenv("MERCHANTS_PATH", "/data/merchants.csv")
HIGH_AMOUNT = float(os.getenv("HIGH_AMOUNT", "800"))
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.7"))
VELOCITY_THRESHOLD = int(os.getenv("VELOCITY_THRESHOLD", "10"))
WINDOW_DURATION = int(os.getenv("WINDOW_SECONDS", "60"))
ALLOWED_LATENESS = int(os.getenv("ALLOWED_LATENESS_SECONDS", "120"))
MAX_FUTURE_SKEW = int(os.getenv("MAX_FUTURE_SKEW_SECONDS", "300"))
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "25"))
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "minioadmin"),
    "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
    "AWS_CONDITIONAL_PUT": "etag",
}

# UTC ISO-8601 strings are intentional for JSON/UI interoperability.
SCHEMA = pa.schema([
    pa.field(name, pa.string(), nullable=name in {"user_id", "country"})
    for name in ["transaction_id", "event_time", "ingest_time", "event_date",
                 "card_id", "user_id", "merchant_id", "merchant_category",
                 "merchant_country", "currency", "country", "window_start",
                 "window_end", "stats_window_start", "stats_window_end", "source_topic"]
] + [pa.field(name, pa.float64(), nullable=name in {"lat", "lon"})
     for name in ["merchant_risk", "amount", "lat", "lon", "fraud_score",
                  "amount_sum", "max_fraud_score"]]
  + [pa.field(name, pa.int64(), nullable=False)
     for name in ["amount_flag", "merchant_flag", "is_fraud", "is_late",
                  "event_time_fallback", "velocity_excluded", "tx_count",
                  "velocity_alert", "source_partition", "source_offset"]])


def parse_event_time(value: str | None, fallback: datetime | None = None) -> tuple[datetime, bool]:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc), False
    except (AttributeError, TypeError, ValueError):
        return fallback or datetime.now(timezone.utc), True


def resolve_event_time(value: str | None, kafka_time: datetime) -> tuple[datetime, bool]:
    """Use deterministic Kafka time for invalid or implausibly future event time."""
    parsed, used_fallback = parse_event_time(value, kafka_time)
    if parsed > kafka_time + timedelta(seconds=MAX_FUTURE_SKEW):
        return kafka_time, True
    return parsed, used_fallback


def load_merchants(path: str) -> dict:
    return {str(row["merchant_id"]): {
        "merchant_category": str(row["category"]),
        "merchant_country": str(row["country"]),
        "merchant_risk": float(row["risk_score"]),
    } for _, row in pd.read_csv(path).iterrows()}


def fraud_signals(amount: float, merchant_risk: float) -> tuple[int, int, float, int]:
    amount_flag = int(amount > HIGH_AMOUNT)
    merchant_flag = int(merchant_risk >= RISK_THRESHOLD)
    score = 0.5 * min(max(amount / max(HIGH_AMOUNT, 1.0), 0.0), 2.0) / 2.0
    score += 0.5 * min(max(merchant_risk, 0.0), 1.0)
    return amount_flag, merchant_flag, round(score, 4), int(amount_flag or merchant_flag)


def fixed_window(event_time: datetime) -> tuple[datetime, datetime]:
    start = datetime.fromtimestamp(
        int(event_time.timestamp()) // WINDOW_DURATION * WINDOW_DURATION, tz=timezone.utc)
    return start, start + timedelta(seconds=WINDOW_DURATION)


class WindowState:
    """A partition watermark bounds all keys, including inactive cards."""
    def __init__(self):
        self.latest: datetime | None = None
        self.cards: dict[str, list[tuple[datetime, float, float]]] = {}

    def prune(self):
        if self.latest is None:
            return
        cutoff = self.latest - timedelta(seconds=WINDOW_DURATION + ALLOWED_LATENESS)
        self.cards = {card: kept for card, items in self.cards.items()
                      if (kept := [item for item in items if item[0] >= cutoff])}

    def add(self, card: str, at: datetime, amount: float, score: float) -> dict:
        late = self.latest is not None and at < self.latest
        excluded = self.latest is not None and at < self.latest - timedelta(seconds=ALLOWED_LATENESS)
        self.latest = max(self.latest, at) if self.latest else at
        self.prune()
        if not excluded:
            self.cards.setdefault(card, []).append((at, amount, score))
        start = at - timedelta(seconds=WINDOW_DURATION)
        window = [item for item in self.cards.get(card, []) if start <= item[0] <= at]
        return {
            "is_late": int(late), "velocity_excluded": int(excluded),
            "window_start": start.isoformat(), "window_end": at.isoformat(),
            "tx_count": len(window), "amount_sum": round(sum(item[1] for item in window), 2),
            "max_fraud_score": max((item[2] for item in window), default=score),
            "velocity_alert": int(len(window) > VELOCITY_THRESHOLD and not excluded),
        }

    @classmethod
    def restore(cls, records: list[dict]):
        state = cls()
        if not records:
            return state
        state.latest = max(parse_event_time(row["event_time"])[0] for row in records)
        cutoff = state.latest - timedelta(seconds=WINDOW_DURATION + ALLOWED_LATENESS)
        for row in records:
            at = parse_event_time(row["event_time"])[0]
            if not row["velocity_excluded"] and at >= cutoff:
                state.cards.setdefault(row["card_id"], []).append((at, row["amount"], row["fraud_score"]))
        return state


def enrich(event: dict, partition: int, offset: int, timestamp: int,
           merchants: dict, state: WindowState) -> dict:
    fallback = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    at, used_fallback = resolve_event_time(event.get("event_time"), fallback)
    merchant_id = str(event.get("merchant_id", "unknown"))
    merchant = merchants.get(merchant_id, {
        "merchant_category": "unknown", "merchant_country": "unknown", "merchant_risk": 0.0})
    amount = float(event.get("amount", 0.0))
    amount_flag, merchant_flag, score, fraud = fraud_signals(amount, merchant["merchant_risk"])
    card = str(event.get("card_id", "unknown"))
    stats_start, stats_end = fixed_window(at)
    return {
        "transaction_id": str(event.get("transaction_id") or f"{KAFKA_TOPIC}:{partition}:{offset}"),
        "event_time": at.isoformat(), "ingest_time": datetime.now(timezone.utc).isoformat(),
        "event_date": at.date().isoformat(), "card_id": card,
        "user_id": event.get("user_id"), "merchant_id": merchant_id, **merchant,
        "amount": amount, "currency": event.get("currency", "EUR"),
        "lat": event.get("lat"), "lon": event.get("lon"), "country": event.get("country"),
        "amount_flag": amount_flag, "merchant_flag": merchant_flag,
        "fraud_score": score, "is_fraud": fraud, "event_time_fallback": int(used_fallback),
        "stats_window_start": stats_start.isoformat(), "stats_window_end": stats_end.isoformat(),
        "source_topic": KAFKA_TOPIC, "source_partition": partition, "source_offset": offset,
        **state.add(card, at, amount, score),
    }


def partition_path(partition: int) -> str:
    return f"{SILVER}/partition-{partition}"


def recover(partition: int) -> tuple[WindowState, int]:
    try:
        frame = DeltaTable(partition_path(partition), storage_options=STORAGE_OPTIONS).to_pandas()
    except TableNotFoundError:
        return WindowState(), -1
    frame = frame.drop_duplicates(["source_topic", "source_partition", "source_offset"])
    return WindowState.restore(frame.to_dict("records")), int(frame["source_offset"].max())


class Assignment(ConsumerRebalanceListener):
    def __init__(self):
        self.states = {}

    def on_partitions_revoked(self, revoked):
        self.states.clear()

    def on_partitions_assigned(self, assigned):
        self.states.clear()


def process_batch(messages, merchants, state, persisted_offset):
    rows = []
    for msg in messages:
        if msg.offset > persisted_offset:
            rows.append(enrich(msg.value, msg.partition, msg.offset, msg.timestamp, merchants, state))
    if rows:
        write_deltalake(partition_path(messages[0].partition),
                        pa.Table.from_pylist(rows, schema=SCHEMA), mode="append",
                        schema_mode="merge", partition_by=["event_date"], storage_options=STORAGE_OPTIONS)
        persisted_offset = rows[-1]["source_offset"]
        print(json.dumps({"written": len(rows), "last_offset": persisted_offset,
                          "sample": rows[-1]}, default=str), flush=True)
    return persisted_offset


def main():
    merchants = load_merchants(MERCHANTS_PATH)
    assignment = Assignment()
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","), group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest", enable_auto_commit=False,
                max_poll_interval_ms=300000,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")))
            break
        except NoBrokersAvailable:
            time.sleep(3)
    consumer.subscribe([KAFKA_TOPIC], listener=assignment)
    try:
        while True:
            batches = consumer.poll(timeout_ms=1000, max_records=FLUSH_EVERY)
            for tp, messages in batches.items():
                if tp not in assignment.states:
                    assignment.states[tp] = recover(tp.partition)
                state, persisted = assignment.states[tp]
                persisted = process_batch(messages, merchants, state, persisted)
                assignment.states[tp] = state, persisted
                try:
                    consumer.commit({tp: OffsetAndMetadata(messages[-1].offset + 1, "")})
                except CommitFailedError:
                    assignment.states.clear()
                    break
            Path("/tmp/processor-ready").touch()
    finally:
        consumer.close(autocommit=False)


if __name__ == "__main__":
    main()
