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
from collections import Counter
from datetime import datetime, timezone
from typing import Literal

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Configuration (injected via ConfigMap / Secret in Kubernetes)
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
SIMULATE_DELAY_MS = int(os.getenv("SIMULATE_DELAY_MS", "60"))
HIGH_AMOUNT = float(os.getenv("HIGH_AMOUNT", "800"))
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.7"))

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
            max_in_flight_requests_per_connection=1,
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
    event_time: str | None = Field(default=None, description="Optional ISO-8601 event time for late-data demos.")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_event(tx: dict) -> dict:
    """Attach identifiers / event-time that the stream processor relies on."""
    tx.setdefault("transaction_id", str(uuid.uuid4()))
    if not tx.get("event_time"):
        tx["event_time"] = _now_iso()
    return tx


def _publish(event: dict) -> None:
    producer = get_producer()
    # Key by card_id so all events of one card land in the same partition
    # (preserves per-card ordering for stateful velocity checks).
    producer.send(KAFKA_TOPIC, key=event["card_id"], value=event).get(timeout=30)


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/healthz")
def healthz():
    return {"status": "ok", "topic": KAFKA_TOPIC, "bootstrap": KAFKA_BOOTSTRAP}


@app.get("/rules")
def rules():
    return {
        "high_amount": HIGH_AMOUNT,
        "risk_threshold": RISK_THRESHOLD,
    }


@app.get("/readyz")
def readyz():
    try:
        if not get_producer().bootstrap_connected():
            raise HTTPException(status_code=503, detail="Kafka disconnected")
    except KafkaError as exc:
        raise HTTPException(status_code=503, detail="Kafka unavailable") from exc
    return {"status": "ready"}


@app.post("/transactions")
def submit_transaction(tx: Transaction):
    """Ingest a single transaction coming from the UI (data-provider role)."""
    try:
        event = _to_event(tx.model_dump())
        _publish(event)
        get_producer().flush(timeout=5)
        return {"accepted": True, "transaction_id": event["transaction_id"]}
    except KafkaError as exc:  # delivery errors must not become accepted=True
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {exc}")


FraudScenario = Literal["normal", "amount", "merchant", "both"]


def _synthetic_transaction(scenario: FraudScenario) -> dict:
    """Create one transaction whose rule outcome matches the scenario."""
    card = f"card_{random.randint(0, 999):05d}"
    high_amount = scenario in {"amount", "both"}
    high_risk_merchant = scenario in {"merchant", "both"}
    merchant_pool = [
        merchant
        for merchant in MERCHANTS
        if (merchant[2] >= RISK_THRESHOLD) == high_risk_merchant
    ]
    merchant_id, country, _ = random.choice(merchant_pool)
    amount = (
        round(random.uniform(HIGH_AMOUNT + 100, max(5000, HIGH_AMOUNT + 100)), 2)
        if high_amount
        else round(random.uniform(1, min(250, HIGH_AMOUNT - 1)), 2)
    )
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


def _simulation_scenarios(count: int, fraud_ratio: float) -> list[FraudScenario]:
    """Return an exact flagged share, balanced across three explainable causes."""
    flagged_count = int(count * fraud_ratio + 0.5)
    reasons: tuple[FraudScenario, ...] = ("amount", "merchant", "both")
    offset = random.randrange(len(reasons))
    scenarios: list[FraudScenario] = ["normal"] * (count - flagged_count)
    scenarios.extend(reasons[(index + offset) % len(reasons)] for index in range(flagged_count))
    random.shuffle(scenarios)
    return scenarios


def _produce_simulation(
    scenarios: list[FraudScenario], pace_ms: int, burst_card: bool
) -> None:
    delay_s = pace_ms / 1000.0
    try:
        for scenario in scenarios:
            _publish(_synthetic_transaction(scenario))
            if delay_s > 0:
                time.sleep(delay_s)

        if burst_card:
            victim = f"card_velocity_{uuid.uuid4().hex[:8]}"
            for _ in range(15):
                event = _synthetic_transaction("normal")
                event["card_id"] = victim
                event["user_id"] = victim.replace("card", "user")
                event["event_time"] = _now_iso()
                _publish(event)
                if delay_s > 0:
                    time.sleep(delay_s)

        get_producer().flush(timeout=30)
        print(f"Simulation completed: {len(scenarios)} base events, burst={burst_card}")
    except Exception as exc:  # pragma: no cover - infrastructure dependent
        print(f"Simulation failed: {exc!r}")


class SimulateRequest(BaseModel):
    count: int = Field(default=100, ge=1, le=100_000)
    fraud_ratio: float = Field(
        default=0.1,
        ge=0.0,
        le=1.0,
        description="Share of base events that trigger amount and/or merchant rules.",
    )
    pace_ms: int = Field(
        default=SIMULATE_DELAY_MS,
        ge=0,
        le=2000,
        description="Delay between generated events in milliseconds.",
    )
    burst_card: bool = Field(
        default=True,
        description="Emit a rapid burst from one card to trigger velocity rules.",
    )


@app.post("/simulate", status_code=202)
def simulate(req: SimulateRequest, background_tasks: BackgroundTasks):
    """Schedule a paced synthetic stream and return before generation finishes."""
    try:
        get_producer()  # Fail the request immediately if Kafka cannot be reached.
    except NoBrokersAvailable as exc:  # pragma: no cover - infrastructure dependent
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {exc}")
    scenarios = _simulation_scenarios(req.count, req.fraud_ratio)
    breakdown = Counter(scenarios)
    burst_count = 15 if req.burst_card else 0
    scheduled = req.count + burst_count
    background_tasks.add_task(
        _produce_simulation, scenarios, req.pace_ms, req.burst_card
    )
    return {
        "accepted": True,
        "scheduled": scheduled,
        "topic": KAFKA_TOPIC,
        "pace_ms": req.pace_ms,
        "estimated_seconds": round(scheduled * req.pace_ms / 1000.0, 1),
        "planned": {
            "normal": breakdown["normal"],
            "high_amount": breakdown["amount"],
            "high_risk_merchant": breakdown["merchant"],
            "both": breakdown["both"],
            "velocity_burst": burst_count,
        },
    }


if __name__ == "__main__":  # local dev entrypoint
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
