from pyspark.sql.types import StructType, StructField, StringType

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("source", StringType(), True),
    ]
)
