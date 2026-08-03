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

import asyncio
import json
import os
import threading
import time
from typing import Any

import pandas as pd
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

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

STREAM_POLL_MS = int(os.getenv("STREAM_POLL_MS", "1000"))
SNAPSHOT_CACHE_MS = int(os.getenv("SNAPSHOT_CACHE_MS", "900"))
_snapshot_lock = threading.Lock()
_snapshot_cache: dict[
    int, tuple[float, tuple[int | None, int | None], dict[str, Any]]
] = {}


def _read(path: str) -> pd.DataFrame:
    """Read a Delta table into pandas; return empty frame if it doesn't exist yet."""
    try:
        return DeltaTable(path, storage_options=STORAGE_OPTIONS).to_pandas()
    except (TableNotFoundError, FileNotFoundError, OSError):
        return pd.DataFrame()


def _summary_from_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"processed": 0, "flagged": 0, "fraud_rate": 0.0}
    processed = int(len(df))
    flagged = int(df["is_fraud"].sum()) if "is_fraud" in df else 0
    rate = round(flagged / processed, 4) if processed else 0.0
    return {"processed": processed, "flagged": flagged, "fraud_rate": rate}


@app.get("/healthz")
def healthz():
    return {"status": "ok", "bucket": LAKE_BUCKET, "endpoint": S3_ENDPOINT}


@app.get("/summary")
def summary():
    return _summary_from_df(_read(SILVER))


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
            "amount_flag",
            "merchant_flag",
            "fraud_score",
            "is_fraud",
            "is_late",
            "event_time_fallback",
            "velocity_excluded",
        ]
        if c in df.columns
    ]
    df = df[cols]
    if "event_time" in df.columns:
        df = df.sort_values("event_time", ascending=False)
    return df.head(limit).to_dict(orient="records")


def _dashboard_snapshot(limit: int) -> dict[str, Any]:
    silver_df = _read(SILVER)
    flagged_df = silver_df
    if not flagged_df.empty and "is_fraud" in flagged_df.columns:
        flagged_df = flagged_df[flagged_df["is_fraud"] == 1]

    velocity_df = _read(GOLD_VELOCITY)
    if not velocity_df.empty and "velocity_alert" in velocity_df.columns:
        velocity_df = velocity_df[velocity_df["velocity_alert"] == 1]
    if not velocity_df.empty and "window_end" in velocity_df.columns:
        velocity_df = velocity_df.sort_values("window_end", ascending=False)

    return {
        "summary": _summary_from_df(silver_df),
        "flagged": {"items": _recent(flagged_df, limit)},
        "velocity": {"items": velocity_df.head(15).to_dict(orient="records") if not velocity_df.empty else []},
    }


def _table_version(path: str) -> int | None:
    try:
        return DeltaTable(path, storage_options=STORAGE_OPTIONS).version()
    except (TableNotFoundError, FileNotFoundError, OSError):
        return None


def _cached_dashboard_snapshot(limit: int) -> dict[str, Any]:
    now = time.monotonic()
    with _snapshot_lock:
        cached = _snapshot_cache.get(limit)
        if cached and (now - cached[0]) * 1000 < SNAPSHOT_CACHE_MS:
            return cached[2]

        versions = (_table_version(SILVER), _table_version(GOLD_VELOCITY))
        if cached and cached[1] == versions:
            _snapshot_cache[limit] = (now, versions, cached[2])
            return cached[2]

        snapshot = _dashboard_snapshot(limit)
        _snapshot_cache[limit] = (time.monotonic(), versions, snapshot)
        return snapshot


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
    if all(column in df.columns for column in ["window_start", "merchant_category", "updated_at"]):
        df = (
            df.sort_values("updated_at")
            .drop_duplicates(["window_start", "merchant_category"], keep="last")
        )
    if "window_end" in df.columns:
        df = df.sort_values("window_end", ascending=False)
    return {"items": df.head(limit).to_dict(orient="records")}


@app.get("/stream")
async def stream(request: Request, limit: int = Query(15, ge=1, le=1000)):
    async def event_generator():
        last_payload = ""
        sleep_s = max(STREAM_POLL_MS, 100) / 1000.0

        while True:
            if await request.is_disconnected():
                break

            payload = await asyncio.to_thread(_cached_dashboard_snapshot, limit)
            payload_json = json.dumps(payload, default=str, sort_keys=True)

            if payload_json != last_payload:
                last_payload = payload_json
                yield f"event: dashboard\ndata: {payload_json}\n\n"

            await asyncio.sleep(sleep_s)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_generator(), media_type="text/event-stream", headers=headers)


if __name__ == "__main__":  # local dev entrypoint
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
