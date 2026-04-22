import config
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType


def prepare_events_df(df: DataFrame) -> DataFrame:
    """Prepare raw event records for downstream validation.

    This function casts the `price` column to a numeric type so the data can be
    validated and aggregated safely.

    Args:
        df: Input Spark DataFrame containing parsed event records.

    Returns:
        A Spark DataFrame with transformed columns ready for validation.
    """
    return df.withColumn("price", col("price").cast(DoubleType()))


def _is_valid_raw_event() -> Column:
    """Build the validation rule for raw event records.

    Returns:
        A Spark column expression that evaluates to True for valid raw events.
    """
    return (
        col("event_id").isNotNull()
        & col("user_id").isNotNull()
        & col("event_type").isin(*config.ALLOWED_EVENT_TYPES)
        & col("event_time").isNotNull()
        & col("source").isNotNull()
        & col("price").isNotNull()
    )


def _is_valid_metric_event() -> Column:
    """Build the validation rule for source metric records.

    Returns:
        A Spark column expression that evaluates to True for valid metric rows.
    """
    return (
        col("event_id").isNotNull()
        & col("event_type").isin(*config.ALLOWED_EVENT_TYPES)
        & col("source").isNotNull()
        & col("price").isNotNull()
    )


def get_valid_raw_events(df: DataFrame) -> DataFrame:
    """Return only valid raw event rows.

    Raw events must include the required identifiers, a valid event type, a
    source value, an event timestamp, and a numeric price.

    Args:
        df: Input Spark DataFrame of prepared events.

    Returns:
        A Spark DataFrame containing only valid raw event records.
    """
    return df.filter(_is_valid_raw_event())


def get_invalid_raw_events(df: DataFrame) -> DataFrame:
    """Return raw event rows that fail validation checks.

    Args:
        df: Input Spark DataFrame of prepared events.

    Returns:
        A Spark DataFrame containing invalid raw event records.
    """
    return df.filter(~_is_valid_raw_event())


def get_valid_metric_events(df: DataFrame) -> DataFrame:
    """Return only rows suitable for source-level metric aggregation.

    Args:
        df: Input Spark DataFrame of prepared events.

    Returns:
        A Spark DataFrame containing valid metric input rows.
    """
    return df.filter(_is_valid_metric_event())


def get_invalid_metric_events(df: DataFrame) -> DataFrame:
    """Return rows that should be excluded from metric aggregation.

    Args:
        df: Input Spark DataFrame of prepared events.

    Returns:
        A Spark DataFrame containing invalid metric input rows.
    """
    return df.filter(~_is_valid_metric_event())
