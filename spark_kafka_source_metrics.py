import config
from utils.schema import EVENT_SCHEMA
from utils.validation import (
    prepare_events_df,
    get_valid_metric_events,
    get_invalid_metric_events,
)
from utils.logging_config import setup_logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum as spark_sum

logger = setup_logger("source-metrics-job", config.METRICS_LOG_FILE)
logger.info("Starting source metrics Spark job")
logger.info(f"Kafka topic: {config.KAFKA_TOPIC}")
logger.info(f"Cassandra keyspace: {config.CASSANDRA_KEYSPACE}")


spark = (
    SparkSession.builder.appName("KafkaToCassandraSourceMetrics")
    .config("spark.cassandra.connection.host", config.CASSANDRA_HOST)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", config.KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), EVENT_SCHEMA).alias("data"))
    .select("data.*")
)

prepared_df = prepare_events_df(parsed_df)
valid_df = get_valid_metric_events(prepared_df)
invalid_df = get_invalid_metric_events(prepared_df)

aggregated_df = valid_df.groupBy("source").agg(
    count("*").alias("event_count"),
    spark_sum("price").alias("total_revenue"),
)


def write_metrics_batch(batch_df, batch_id):
    row_count = batch_df.count()
    logger.info(f"Batch {batch_id}: writing {row_count} metric rows")

    if row_count > 0:
        (
            batch_df.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(
                table=config.CASSANDRA_METRICS_TABLE,
                keyspace=config.CASSANDRA_KEYSPACE,
            )
            .save()
        )


def log_invalid_batch(batch_df, batch_id):
    invalid_count = batch_df.count()
    if invalid_count > 0:
        logger.warning(f"Batch {batch_id}: found {invalid_count} invalid input rows")


metrics_query = (
    aggregated_df.writeStream.foreachBatch(write_metrics_batch)
    .outputMode("complete")
    .option("checkpointLocation", config.METRICS_CHECKPOINT_PATH)
    .start()
)

invalid_query = (
    invalid_df.writeStream.foreachBatch(log_invalid_batch)
    .outputMode("append")
    .option("checkpointLocation", config.METRICS_INVALID_CHECKPOINT_PATH)
    .start()
)

spark.streams.awaitAnyTermination()
