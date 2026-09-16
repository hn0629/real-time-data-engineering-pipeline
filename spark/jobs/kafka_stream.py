import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    trim,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker:29092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "pipeline-events",
)

DATA_BASE_PATH = os.getenv(
    "DATA_BASE_PATH",
    "/opt/spark-data",
)

RAW_PATH = "{0}/raw/stock_prices".format(DATA_BASE_PATH)
CLEAN_PATH = "{0}/clean/stock_prices".format(DATA_BASE_PATH)
QUARANTINE_PATH = "{0}/quarantine/stock_prices".format(DATA_BASE_PATH)
METRICS_PATH = "{0}/metrics/stream_batches".format(DATA_BASE_PATH)
CHECKPOINT_PATH = "{0}/checkpoints/kafka_to_parquet".format(
    DATA_BASE_PATH
)

TRIGGER_INTERVAL = os.getenv(
    "SPARK_TRIGGER_INTERVAL",
    "10 seconds",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger("kafka_stream")

event_schema = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)

metrics_schema = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("raw_count", StringType(), False),
        StructField("clean_count", StringType(), False),
        StructField("quarantine_count", StringType(), False),
        StructField("processed_at_utc", StringType(), False),
        StructField("kafka_topic", StringType(), False),
    ]
)


def write_parquet_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Write one Kafka micro-batch to raw, clean, quarantine, and metrics."""
    if batch_df.rdd.isEmpty():
        logger.info("Batch %s is empty; skipping writes.", batch_id)
        return

    raw_batch_df = (
        batch_df
        .withColumn("ingested_at", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
    )

    (
        raw_batch_df
        .write
        .mode("append")
        .parquet(RAW_PATH)
    )

    parsed_df = (
        raw_batch_df
        .withColumn(
            "parsed_event",
            from_json(
                col("value").cast("string"),
                event_schema,
            ),
        )
        .select(
            col("parsed_event.symbol").alias("symbol"),
            col("parsed_event.price").alias("price"),
            col("parsed_event.source").alias("source"),
            col("parsed_event.event_time").alias("event_time"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("ingested_at"),
            col("batch_id"),
            col("value").cast("string").alias("raw_payload"),
        )
    )

    validated_df = (
        parsed_df
        .withColumn("symbol", trim(col("symbol")))
        .withColumn(
            "validation_error",
            when(
                col("symbol").isNull() | (col("symbol") == ""),
                "missing_symbol",
            )
            .when(col("price").isNull(), "missing_price")
            .when(col("price") <= 0, "non_positive_price")
            .when(
                col("event_time").isNull()
                | (trim(col("event_time")) == ""),
                "missing_event_time",
            )
            .otherwise(lit(None)),
        )
    )

    clean_batch_df = (
        validated_df
        .filter(col("validation_error").isNull())
        .drop("validation_error")
        .dropDuplicates(
            [
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            ]
        )
    )

    quarantine_batch_df = (
        validated_df
        .filter(col("validation_error").isNotNull())
    )

    raw_count = raw_batch_df.count()
    clean_count = clean_batch_df.count()
    quarantine_count = quarantine_batch_df.count()

    if clean_count > 0:
        (
            clean_batch_df
            .withColumn(
                "event_date",
                col("event_time").substr(1, 10),
            )
            .write
            .mode("append")
            .partitionBy("event_date")
            .parquet(CLEAN_PATH)
        )

    if quarantine_count > 0:
        (
            quarantine_batch_df
            .withColumn(
                "quarantined_at",
                current_timestamp(),
            )
            .write
            .mode("append")
            .parquet(QUARANTINE_PATH)
        )

    metrics_row = [
        (
            str(batch_id),
            str(raw_count),
            str(clean_count),
            str(quarantine_count),
            datetime.now(timezone.utc).isoformat(),
            KAFKA_TOPIC,
        )
    ]

    metrics_df = batch_df.sparkSession.createDataFrame(
        metrics_row,
        schema=metrics_schema,
    )

    (
        metrics_df
        .write
        .mode("append")
        .parquet(METRICS_PATH)
    )

    logger.info(
        "Completed batch %s | raw=%s | clean=%s | quarantine=%s",
        batch_id,
        raw_count,
        clean_count,
        quarantine_count,
    )


def main() -> None:
    """Start the Kafka-to-Parquet Structured Streaming job."""
    spark = (
        SparkSession.builder
        .appName("kafka-to-parquet-stream")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Starting Kafka stream | broker=%s | topic=%s | trigger=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        TRIGGER_INTERVAL,
    )

    kafka_events_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        kafka_events_df
        .writeStream
        .foreachBatch(write_parquet_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(
        "Streaming query started | checkpoint=%s",
        CHECKPOINT_PATH,
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()