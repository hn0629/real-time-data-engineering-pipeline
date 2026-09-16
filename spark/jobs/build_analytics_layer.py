from pathlib import Path
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


CLEAN_PATH = "/opt/spark-data/clean/stock_prices"
ANALYTICS_PATH = "/opt/spark-data/analytics/stock_price_summary"
PARTITION_PREFIX = "event_date="


def build_spark_session() -> SparkSession:
    """Create the Spark session for a small local analytics job."""
    return (
        SparkSession.builder
        .appName("build-analytics-layer")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )
        .getOrCreate()
    )


def find_latest_clean_partition(
    clean_root_path: str,
) -> Tuple[Path, List[Path]]:
    """Return the latest event_date partition and its Parquet files."""
    clean_root = Path(clean_root_path)

    if not clean_root.exists():
        raise RuntimeError(
            "Clean data path does not exist: {0}".format(
                clean_root_path
            )
        )

    partitions = sorted(
        directory
        for directory in clean_root.iterdir()
        if directory.is_dir()
        and directory.name.startswith(PARTITION_PREFIX)
    )

    if not partitions:
        raise RuntimeError(
            "No event_date partitions found under: {0}".format(
                clean_root_path
            )
        )

    latest_partition = partitions[-1]

    parquet_files = sorted(
        parquet_file
        for parquet_file in latest_partition.glob("*.parquet")
        if parquet_file.is_file()
    )

    if not parquet_files:
        raise RuntimeError(
            "No Parquet files found in latest partition: {0}".format(
                latest_partition
            )
        )

    return latest_partition, parquet_files


def get_latest_price_by_symbol(
    typed_df: DataFrame,
) -> DataFrame:
    """Return the most recent price for every date/symbol/source group."""
    latest_price_window = (
        Window
        .partitionBy(
            "event_date",
            "symbol",
            "source",
        )
        .orderBy(
            F.col("event_timestamp").desc(),
            F.col("kafka_offset").desc(),
        )
    )

    return (
        typed_df
        .withColumn(
            "row_number",
            F.row_number().over(latest_price_window),
        )
        .filter(F.col("row_number") == 1)
        .select(
            "event_date",
            "symbol",
            "source",
            F.round(
                F.col("price"),
                2,
            ).alias("latest_price"),
        )
    )


def main() -> None:
    """Create query-ready analytics summaries from the newest clean partition."""
    spark: Optional[SparkSession] = None

    try:
        spark = build_spark_session()

        latest_partition, parquet_files = find_latest_clean_partition(
            CLEAN_PATH
        )

        print(
            "Reading latest clean partition: {0}".format(
                latest_partition
            )
        )
        print(
            "Found {0} Parquet file(s) in latest partition.".format(
                len(parquet_files)
            )
        )

        clean_df = spark.read.parquet(str(latest_partition))

        typed_df = (
            clean_df
            .withColumn(
                "event_timestamp",
                F.to_timestamp("event_time"),
            )
            .withColumn(
                "event_date",
                F.to_date("event_timestamp"),
            )
            .withColumn(
                "price",
                F.col("price").cast("double"),
            )
            .filter(F.col("symbol").isNotNull())
            .filter(F.trim(F.col("symbol")) != "")
            .filter(F.col("source").isNotNull())
            .filter(F.trim(F.col("source")) != "")
            .filter(F.col("price").isNotNull())
            .filter(F.col("price") > 0)
            .filter(F.col("event_timestamp").isNotNull())
        )

        summary_df = (
            typed_df
            .groupBy(
                "event_date",
                "symbol",
                "source",
            )
            .agg(
                F.count("*").alias("tick_count"),
                F.round(
                    F.min("price"),
                    2,
                ).alias("min_price"),
                F.round(
                    F.max("price"),
                    2,
                ).alias("max_price"),
                F.round(
                    F.avg("price"),
                    2,
                ).alias("avg_price"),
                F.min("event_timestamp").alias(
                    "first_event_time"
                ),
                F.max("event_timestamp").alias(
                    "last_event_time"
                ),
            )
        )

        latest_price_df = get_latest_price_by_symbol(typed_df)

        analytics_df = (
            summary_df
            .join(
                latest_price_df,
                on=[
                    "event_date",
                    "symbol",
                    "source",
                ],
                how="left",
            )
            .withColumn(
                "processed_at",
                F.current_timestamp(),
            )
            .select(
                "event_date",
                "symbol",
                "source",
                "tick_count",
                "min_price",
                "max_price",
                "avg_price",
                "latest_price",
                "first_event_time",
                "last_event_time",
                "processed_at",
            )
        )

        analytics_row_count = analytics_df.count()

        if analytics_row_count == 0:
            raise RuntimeError(
                "Analytics output is empty. "
                "No valid clean events were available."
            )

        (
            analytics_df
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(ANALYTICS_PATH)
        )

        print(
            "Analytics layer written successfully: {0}".format(
                ANALYTICS_PATH
            )
        )
        print(
            "Analytics rows written: {0}".format(
                analytics_row_count
            )
        )

        (
            analytics_df
            .orderBy(
                F.col("symbol").asc(),
                F.col("source").asc(),
            )
            .show(
                50,
                truncate=False,
            )
        )

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()