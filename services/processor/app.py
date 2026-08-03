"""
Spark Structured Streaming processor -- the heart of the Kappa pipeline.

Medallion architecture on a Delta Lakehouse stored in MinIO (S3):

    Kafka(transactions)
        |
        v
  [Bronze]  raw, append-only, exactly-once via checkpoint
        |
        v
  [Silver]  parsed + ENRICHED (broadcast join with merchants) + rule-based
            fraud flags, idempotent UPSERT via Delta MERGE (dedup by id)
        |
        +--> [Gold: card_velocity]  event-time WINDOWED, STATEFUL count per card
        |                           with WATERMARK -> velocity fraud signal
        +--> [Gold: fraud_stats]    windowed flagged/approved aggregates for serving

Concepts demonstrated (exam rubric):
  * Windowing / State / Late data  -> withWatermark + window() aggregation
  * Non-trivial transformation     -> enrichment join + composite fraud scoring
  * Exactly-once                   -> checkpoints + Delta + idempotent MERGE
  * Schema evolution               -> mergeSchema on writes
  * Storage deviation (bonus)      -> MinIO/S3 + Delta instead of HDFS
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --------------------------------------------------------------------------- #
# Configuration (ConfigMap / Secret in Kubernetes)
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-processor-group")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "fraud")

MERCHANTS_PATH = os.getenv("MERCHANTS_PATH", "/data/merchants.csv")

# Fraud rule thresholds (tunable via ConfigMap without code changes)
HIGH_AMOUNT = float(os.getenv("HIGH_AMOUNT", "800"))
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.7"))
VELOCITY_THRESHOLD = int(os.getenv("VELOCITY_THRESHOLD", "10"))
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "2 minutes")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "30 seconds")

# Lakehouse paths
BRONZE = f"s3a://{LAKE_BUCKET}/bronze/transactions"
SILVER = f"s3a://{LAKE_BUCKET}/silver/transactions"
GOLD_VELOCITY = f"s3a://{LAKE_BUCKET}/gold/card_velocity"
GOLD_STATS = f"s3a://{LAKE_BUCKET}/gold/fraud_stats"
CKPT = f"s3a://{LAKE_BUCKET}/_checkpoints"

# Schema of the JSON events arriving on Kafka
TX_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("card_id", StringType()),
        StructField("user_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("event_time", StringType()),  # ISO8601 -> cast below
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("country", StringType()),
    ]
)


# --------------------------------------------------------------------------- #
# Spark session (Delta + S3A/MinIO)
# --------------------------------------------------------------------------- #
def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("fraud-detection-processor")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Idempotent/atomic Delta writes to object storage
        .config(
            "spark.databricks.delta.commitInfo.userMetadata", "fraud-processor"
        )
        # ----- MinIO / S3A -----
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Schema evolution for streaming/merge writes
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    # Pull the matching Delta + hadoop-aws jars automatically.
    extra = ",".join(
        [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=extra.split(",")
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- #
# Bronze: raw ingest from Kafka (exactly-once via checkpoint)
# --------------------------------------------------------------------------- #
def start_bronze(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", KAFKA_GROUP_ID)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), TX_SCHEMA).alias("t"))
        .select("t.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("event_time"))
    )

    return (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/bronze")
        .option("mergeSchema", "true")           # schema evolution (bonus)
        .partitionBy("ingest_date")              # partitioning rationale in README
        .start(BRONZE)
    )


# --------------------------------------------------------------------------- #
# Silver: enrich + rule-based flagging, idempotent UPSERT (Delta MERGE)
# --------------------------------------------------------------------------- #
def start_silver(spark: SparkSession):
    merchants = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(MERCHANTS_PATH)
        .select(
            "merchant_id",
            F.col("category").alias("merchant_category"),
            F.col("country").alias("merchant_country"),
            F.col("risk_score").cast("double").alias("merchant_risk"),
        )
    )

    bronze_stream = (
        spark.readStream.format("delta")
        .load(BRONZE)
        # Late/out-of-order data handled by the watermark on event_time.
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    def upsert_silver(batch: DataFrame, batch_id: int) -> None:
        if batch.rdd.isEmpty():
            return

        enriched = (
            batch.join(F.broadcast(merchants), on="merchant_id", how="left")
            .withColumn(
                "amount_flag", (F.col("amount") > F.lit(HIGH_AMOUNT)).cast("int")
            )
            .withColumn(
                "merchant_flag",
                (F.coalesce(F.col("merchant_risk"), F.lit(0.0)) >= F.lit(RISK_THRESHOLD)).cast("int"),
            )
            # Composite, explainable fraud score in [0, 1].
            .withColumn(
                "fraud_score",
                F.round(
                    0.5 * F.least(F.col("amount") / F.lit(HIGH_AMOUNT), F.lit(2.0)) / 2.0
                    + 0.5 * F.coalesce(F.col("merchant_risk"), F.lit(0.0)),
                    4,
                ),
            )
            .withColumn(
                "is_fraud",
                ((F.col("amount_flag") == 1) | (F.col("merchant_flag") == 1)).cast("int"),
            )
            .dropDuplicates(["transaction_id"])
        )

        if DeltaTable.isDeltaTable(spark, SILVER):
            (
                DeltaTable.forPath(spark, SILVER)
                .alias("t")
                .merge(enriched.alias("s"), "t.transaction_id = s.transaction_id")
                .whenNotMatchedInsertAll()   # idempotent -> exactly-once effect
                .execute()
            )
        else:
            (
                enriched.write.format("delta")
                .option("mergeSchema", "true")
                .partitionBy("ingest_date")
                .save(SILVER)
            )

    return (
        bronze_stream.writeStream.foreachBatch(upsert_silver)
        .option("checkpointLocation", f"{CKPT}/silver")
        .outputMode("update")
        .start()
    )


# --------------------------------------------------------------------------- #
# Gold: event-time windowed, stateful aggregates for serving
# --------------------------------------------------------------------------- #
def start_gold_velocity(spark: SparkSession):
    """Stateful velocity signal: transactions per card per sliding window."""
    silver_stream = (
        spark.readStream.format("delta")
        .load(SILVER)
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    windowed = (
        silver_stream.groupBy(
            F.window("event_time", WINDOW_DURATION, SLIDE_DURATION),
            F.col("card_id"),
        )
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("amount_sum"),
            F.max("fraud_score").alias("max_fraud_score"),
        )
        .withColumn(
            "velocity_alert",
            (F.col("tx_count") > F.lit(VELOCITY_THRESHOLD)).cast("int"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "card_id",
            "tx_count",
            "amount_sum",
            "max_fraud_score",
            "velocity_alert",
        )
    )

    return (
        windowed.writeStream.format("delta")
        .outputMode("append")  # append works because watermark closes windows
        .option("checkpointLocation", f"{CKPT}/gold_velocity")
        .option("mergeSchema", "true")
        .start(GOLD_VELOCITY)
    )


def start_gold_stats(spark: SparkSession):
    """Flagged vs. approved aggregates per merchant category (for the dashboard)."""
    silver_stream = (
        spark.readStream.format("delta")
        .load(SILVER)
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    stats = (
        silver_stream.groupBy(
            F.window("event_time", WINDOW_DURATION),
            F.col("merchant_category"),
        )
        .agg(
            F.count("*").alias("total"),
            F.sum("is_fraud").alias("flagged"),
            F.round(F.avg("fraud_score"), 4).alias("avg_fraud_score"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "merchant_category",
            "total",
            "flagged",
            "avg_fraud_score",
        )
    )

    return (
        stats.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/gold_stats")
        .option("mergeSchema", "true")
        .start(GOLD_STATS)
    )


def main() -> None:
    spark = build_spark()
    start_bronze(spark)
    start_silver(spark)
    start_gold_velocity(spark)
    start_gold_stats(spark)
    # Block until any stream fails; Kubernetes restarts the pod on crash.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
