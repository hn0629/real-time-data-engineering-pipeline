from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

spark = (
    SparkSession.builder
    .appName("pipeline-smoke-test")
    .getOrCreate()
)

data = [
    ("orders", 125.50),
    ("orders", 99.99),
    ("payments", 250.00),
    ("payments", 75.25),
    ("events", 10.00),
]

df = spark.createDataFrame(data, ["source", "amount"])

result = (
    df.groupBy("source")
    .agg(
        count("*").alias("record_count"),
        spark_sum(col("amount")).alias("total_amount"),
    )
    .orderBy("source")
)

result.show()

spark.stop()