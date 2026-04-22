from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSparkTest") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "test-topic") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")

query = df.writeStream \
    .format("console") \
    .start()

query.awaitTermination()