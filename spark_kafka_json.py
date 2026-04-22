from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("source", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("KafkaJsonStream")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "test-topic")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    df.selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), schema).alias("data"))
      .select("data.*")
      .filter(col("event_id").isNotNull())
      .withColumn("price", col("price").cast("double"))
      .withColumn("event_time", to_timestamp("event_time"))
)

query = (
    parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/spark-checkpoints/json-events")
    .start()
)

query.awaitTermination()