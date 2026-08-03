"""
Serving layer for the fraud-detection pipeline.

Reads the Gold/Silver Delta tables directly from MinIO (S3) using delta-rs
(the `deltalake` package) -- no Spark needed on the query path. This is a
deliberate, justified deviation (bonus): the serving tier stays lightweight
and scales horizontally as a stateless FastAPI Deployment.

Endpoints consumed by the User-facing UI dashboard:
  GET /healthz       liveness/readiness
  GET /summary       totals: processed, flagged, fraud-rate
  GET /transactions  recent transactions (approved + flagged)
  GET /flagged       only flagged transactions
  GET /velocity      per-card velocity alerts (stateful windowed signal)
  GET /stats         flagged/approved aggregates per merchant category
"""

from __future__ import annotations

import os

import pandas as pd
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# --------------------------------------------------------------------------- #
# Configuration (ConfigMap / Secret)
# --------------------------------------------------------------------------- #
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "fraud")

SILVER = f"s3://{LAKE_BUCKET}/silver/transactions"
GOLD_VELOCITY = f"s3://{LAKE_BUCKET}/gold/card_velocity"
GOLD_STATS = f"s3://{LAKE_BUCKET}/gold/fraud_stats"

# delta-rs (object_store) credentials for MinIO
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": S3_ENDPOINT,
    "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",          # MinIO over plain HTTP in-cluster
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

app = FastAPI(title="Fraud Pipeline - Serving", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _read(path: str) -> pd.DataFrame:
    """Read a Delta table into pandas; return empty frame if it doesn't exist yet."""
    try:
        return DeltaTable(path, storage_options=STORAGE_OPTIONS).to_pandas()
    except (TableNotFoundError, FileNotFoundError, OSError):
        return pd.DataFrame()


@app.get("/healthz")
def healthz():
    return {"status": "ok", "bucket": LAKE_BUCKET, "endpoint": S3_ENDPOINT}


@app.get("/summary")
def summary():
    df = _read(SILVER)
    if df.empty:
        return {"processed": 0, "flagged": 0, "fraud_rate": 0.0}
    processed = int(len(df))
    flagged = int(df["is_fraud"].sum()) if "is_fraud" in df else 0
    rate = round(flagged / processed, 4) if processed else 0.0
    return {"processed": processed, "flagged": flagged, "fraud_rate": rate}


def _recent(df: pd.DataFrame, limit: int) -> list[dict]:
    if df.empty:
        return []
    cols = [
        c
        for c in [
            "transaction_id",
            "event_time",
            "card_id",
            "merchant_id",
            "merchant_category",
            "amount",
            "fraud_score",
            "is_fraud",
        ]
        if c in df.columns
    ]
    df = df[cols]
    if "event_time" in df.columns:
        df = df.sort_values("event_time", ascending=False)
    return df.head(limit).to_dict(orient="records")


@app.get("/transactions")
def transactions(limit: int = Query(50, ge=1, le=1000)):
    return {"items": _recent(_read(SILVER), limit)}


@app.get("/flagged")
def flagged(limit: int = Query(50, ge=1, le=1000)):
    df = _read(SILVER)
    if not df.empty and "is_fraud" in df.columns:
        df = df[df["is_fraud"] == 1]
    return {"items": _recent(df, limit)}


@app.get("/velocity")
def velocity(limit: int = Query(50, ge=1, le=1000)):
    df = _read(GOLD_VELOCITY)
    if df.empty:
        return {"items": []}
    if "velocity_alert" in df.columns:
        df = df[df["velocity_alert"] == 1]
    if "window_end" in df.columns:
        df = df.sort_values("window_end", ascending=False)
    return {"items": df.head(limit).to_dict(orient="records")}


@app.get("/stats")
def stats(limit: int = Query(100, ge=1, le=1000)):
    df = _read(GOLD_STATS)
    if df.empty:
        return {"items": []}
    if "window_end" in df.columns:
        df = df.sort_values("window_end", ascending=False)
    return {"items": df.head(limit).to_dict(orient="records")}


if __name__ == "__main__":  # local dev entrypoint
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
