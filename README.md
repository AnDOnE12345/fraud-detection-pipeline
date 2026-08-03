# Real-time Fraud Detection — Cloud & Big Data Prototype

> DHBW · Cloud Computing und Big Data · Prüfungsleistung 2026
> Datengetriebener Prototyp auf Kubernetes-Basis

A streaming-first (**Kappa**) big-data pipeline that ingests payment
transactions, detects fraud in real time and serves the results to a web UI.

## Architecture (short)

```
UI ──► Producer (Kafka) ──► Spark Structured Streaming ──► Delta Lakehouse (MinIO)
                                                              │
                                                     Serving API ──► UI dashboard
```

- **Ingestion:** Apache Kafka (Strimzi)
- **Processing:** Spark Structured Streaming (Bronze/Silver/Gold medallion)
- **Storage:** Delta Lake on MinIO (S3) — object storage instead of HDFS
- **Serving:** FastAPI query API (delta-rs) + web UI
- **Runtime:** Kubernetes, deployed declaratively via Helm

## Status

Work in progress — this README will grow into the full report
(12 sections per the grading scheme) as the components are implemented.

## Repository layout

- `services/` — producer, processor, serving, ui
- `data/` — merchant reference data for enrichment
- `deploy/` — Helm chart and Kubernetes manifests *(added incrementally)*
- `docs/` — architecture notes and screenshots
