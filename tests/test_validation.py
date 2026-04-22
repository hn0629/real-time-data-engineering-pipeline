from pyspark.sql.types import StructField, StructType, StringType

from utils.validation import (
    get_invalid_metric_events,
    get_invalid_raw_events,
    get_valid_metric_events,
    get_valid_raw_events,
    prepare_events_df,
)

SCHEMA = StructType(
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


def test_prepare_events_df_casts_price_to_double(spark):
    data = [
        ("1", "u1", "view", "p1", "19.99", "2026-04-15T18:00:00", "web")
    ]

    df = spark.createDataFrame(data, SCHEMA)
    result_df = prepare_events_df(df)

    assert dict(result_df.dtypes)["price"] == "double"


def test_get_valid_raw_events_returns_only_valid_rows(spark):
    data = [
        ("1", "u1", "view", "p1", "19.99", "2026-04-15T18:00:00", "web"),
        (
            None,
            "u2",
            "purchase",
            "p2",
            "59.50",
            "2026-04-15T18:05:00",
            "mobile",
        ),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    prepared_df = prepare_events_df(df)
    valid_df = get_valid_raw_events(prepared_df)

    assert valid_df.count() == 1


def test_get_invalid_raw_events_returns_only_invalid_rows(spark):
    data = [
        ("1", "u1", "view", "p1", "19.99", "2026-04-15T18:00:00", "web"),
        (
            "2",
            "u2",
            "bad_event",
            "p2",
            "59.50",
            "2026-04-15T18:05:00",
            "mobile",
        ),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    prepared_df = prepare_events_df(df)
    invalid_df = get_invalid_raw_events(prepared_df)

    assert invalid_df.count() == 1


def test_get_valid_metric_events_returns_only_valid_rows(spark):
    data = [
        ("1", "u1", "view", "p1", "19.99", "2026-04-15T18:00:00", "web"),
        (
            "2",
            "u2",
            "bad_event",
            "p2",
            "59.50",
            "2026-04-15T18:05:00",
            "mobile",
        ),
        (
            None,
            "u3",
            "purchase",
            "p3",
            "9.99",
            "2026-04-15T18:10:00",
            "web",
        ),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    prepared_df = prepare_events_df(df)
    valid_df = get_valid_metric_events(prepared_df)

    assert valid_df.count() == 1


def test_get_invalid_metric_events_returns_only_invalid_rows(spark):
    data = [
        ("1", "u1", "view", "p1", "19.99", "2026-04-15T18:00:00", "web"),
        (
            "2",
            "u2",
            "bad_event",
            "p2",
            "59.50",
            "2026-04-15T18:05:00",
            "mobile",
        ),
        (
            None,
            "u3",
            "purchase",
            "p3",
            "9.99",
            "2026-04-15T18:10:00",
            "web",
        ),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    prepared_df = prepare_events_df(df)
    invalid_df = get_invalid_metric_events(prepared_df)

    assert invalid_df.count() == 2