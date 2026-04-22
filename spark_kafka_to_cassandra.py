import config
from utils.schema import EVENT_SCHEMA
from utils.validation import (
    prepare_events_df,
    get_valid_raw_events,
    get_invalid_raw_events,
)
from utils.logging_config import setup_logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

logger = setup_logger("raw-events-job", config.RAW_LOG_FILE)
logger.info("Starting raw events Spark job")
logger.info(f"Kafka topic: {config.KAFKA_TOPIC}")
logger.info(f"Cassandra keyspace: {config.CASSANDRA_KEYSPACE}")


spark = (
    SparkSession.builder.appName("KafkaToCassandraRawEvents")
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
valid_df = get_valid_raw_events(prepared_df)
invalid_df = get_invalid_raw_events(prepared_df)


def write_batch(batch_df, batch_id):
    count = batch_df.count()
    logger.info(f"Batch {batch_id}: writing {count} valid records")

    if count > 0:
        (
            batch_df.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(
                table=config.CASSANDRA_EVENTS_TABLE,
                keyspace=config.CASSANDRA_KEYSPACE,
            )
            .save()
        )


def log_invalid_batch(batch_df, batch_id):
    invalid_count = batch_df.count()
    if invalid_count > 0:
        logger.warning(f"Batch {batch_id}: found {invalid_count} invalid records")


valid_query = (
    valid_df.writeStream.foreachBatch(write_batch)
    .outputMode("append")
    .option("checkpointLocation", config.RAW_CHECKPOINT_PATH)
    .start()
)

invalid_query = (
    invalid_df.writeStream.foreachBatch(log_invalid_batch)
    .outputMode("append")
    .option("checkpointLocation", config.RAW_INVALID_CHECKPOINT_PATH)
    .start()
)

spark.streams.awaitAnyTermination()
