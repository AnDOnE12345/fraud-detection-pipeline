"""
Producer / Ingestion edge service for the fraud-detection pipeline.

Two responsibilities:
  1. Accept transactions submitted by the User-facing UI (data-provider role)
     and publish them to Kafka  -> POST /transactions
  2. Generate a synthetic transaction stream on demand (load / demo)
     -> POST /simulate

The Kafka topic is the single ingestion entry point of the Kappa pipeline.
All configuration comes from environment variables (12-factor / ConfigMap).
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Configuration (injected via ConfigMap / Secret in Kubernetes)
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

# Reference merchants (kept in sync with data/merchants.csv used for enrichment)
MERCHANTS = [
    ("M0001", "RU", 0.85),
    ("M0002", "DE", 0.35),
    ("M0003", "DE", 0.05),
    ("M0004", "CH", 0.70),
    ("M0005", "DE", 0.15),
    ("M0006", "MT", 0.90),
    ("M0007", "FR", 0.40),
    ("M0008", "US", 0.25),
    ("M0009", "MT", 0.88),
    ("M0010", "DE", 0.03),
    ("M0011", "NG", 0.92),
    ("M0012", "CN", 0.55),
    ("M0013", "DE", 0.08),
    ("M0014", "IT", 0.60),
    ("M0015", "DE", 0.20),
]

app = FastAPI(title="Fraud Pipeline - Producer", version="1.0.0")

# The UI is a separate origin (browser), so CORS must be permissive for the demo.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    """Lazily create a single KafkaProducer (retries until brokers are up)."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            key_serializer=lambda k: (k or "").encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",          # durability: wait for all in-sync replicas
            retries=5,           # producer-side retry -> no silent loss
            linger_ms=20,        # small batching for throughput
        )
    return _producer


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #
class Transaction(BaseModel):
    card_id: str = Field(..., examples=["card_00042"])
    user_id: str = Field(..., examples=["user_00042"])
    merchant_id: str = Field(..., examples=["M0006"])
    amount: float = Field(..., gt=0, examples=[1299.99])
    currency: str = Field(default="EUR")
    lat: float = Field(default=48.06)
    lon: float = Field(default=8.53)
    country: str = Field(default="DE")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_event(tx: dict) -> dict:
    """Attach identifiers / event-time that the stream processor relies on."""
    tx.setdefault("transaction_id", str(uuid.uuid4()))
    tx.setdefault("event_time", _now_iso())
    return tx


def _publish(event: dict) -> None:
    producer = get_producer()
    # Key by card_id so all events of one card land in the same partition
    # (preserves per-card ordering for stateful velocity checks).
    producer.send(KAFKA_TOPIC, key=event["card_id"], value=event)


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/healthz")
def healthz():
    return {"status": "ok", "topic": KAFKA_TOPIC, "bootstrap": KAFKA_BOOTSTRAP}


@app.post("/transactions")
def submit_transaction(tx: Transaction):
    """Ingest a single transaction coming from the UI (data-provider role)."""
    try:
        event = _to_event(tx.model_dump())
        _publish(event)
        get_producer().flush(timeout=5)
        return {"accepted": True, "transaction_id": event["transaction_id"]}
    except NoBrokersAvailable as exc:  # pragma: no cover - infra dependent
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {exc}")


def _synthetic_transaction(fraud: bool) -> dict:
    """Create one plausible transaction. Fraudulent ones bias toward
    high amounts and high-risk merchants so the processing rules can flag them."""
    card = f"card_{random.randint(0, 999):05d}"
    if fraud:
        merchant_id, country, _ = random.choice(
            [m for m in MERCHANTS if m[2] >= 0.7]
        )
        amount = round(random.uniform(800, 5000), 2)
    else:
        merchant_id, country, _ = random.choice(
            [m for m in MERCHANTS if m[2] < 0.5]
        )
        amount = round(random.uniform(1, 250), 2)
    return _to_event(
        {
            "card_id": card,
            "user_id": card.replace("card", "user"),
            "merchant_id": merchant_id,
            "amount": amount,
            "currency": "EUR",
            "lat": round(random.uniform(47.0, 55.0), 4),
            "lon": round(random.uniform(6.0, 15.0), 4),
            "country": country,
        }
    )


class SimulateRequest(BaseModel):
    count: int = Field(default=100, ge=1, le=100_000)
    fraud_ratio: float = Field(default=0.1, ge=0.0, le=1.0)
    burst_card: bool = Field(
        default=True,
        description="Emit a rapid burst from one card to trigger velocity rules.",
    )


@app.post("/simulate")
def simulate(req: SimulateRequest):
    """Generate a synthetic burst of transactions for load / demo purposes."""
    produced = 0
    for _ in range(req.count):
        is_fraud = random.random() < req.fraud_ratio
        _publish(_synthetic_transaction(is_fraud))
        produced += 1

    # Optional: a velocity attack -> many transactions from ONE card quickly.
    if req.burst_card:
        victim = f"card_{random.randint(0, 999):05d}"
        for _ in range(15):
            event = _synthetic_transaction(fraud=True)
            event["card_id"] = victim
            event["user_id"] = victim.replace("card", "user")
            event["event_time"] = _now_iso()
            _publish(event)
            produced += 1

    get_producer().flush(timeout=10)
    return {"produced": produced, "topic": KAFKA_TOPIC}


if __name__ == "__main__":  # local dev entrypoint
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
