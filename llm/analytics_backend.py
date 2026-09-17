import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


LOCAL_PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_BASE_PATH = Path(
    os.getenv(
        "DATA_BASE_PATH",
        str(LOCAL_PROJECT_ROOT / "data"),
    )
)

ANALYTICS_ROOT = (
    DATA_BASE_PATH
    / "analytics"
    / "stock_price_summary"
)

STATUS_PATH = (
    DATA_BASE_PATH
    / "monitoring"
    / "pipeline_status.json"
)

REQUIRED_ANALYTICS_COLUMNS = {
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
}


def load_pipeline_status() -> Dict[str, Any]:
    if not STATUS_PATH.exists():
        return {
            "available": False,
            "message": "No pipeline status artifact was found.",
        }

    try:
        payload = json.loads(
            STATUS_PATH.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as error:
        return {
            "available": False,
            "message": "Pipeline status artifact could not be read.",
            "error": str(error),
        }

    if not isinstance(payload, dict):
        return {
            "available": False,
            "message": "Pipeline status artifact has an invalid structure.",
        }

    return {
        "available": True,
        "status": payload,
    }


def _available_partitions() -> List[Path]:
    if not ANALYTICS_ROOT.exists():
        return []

    return sorted(
        path
        for path in ANALYTICS_ROOT.iterdir()
        if path.is_dir()
        and path.name.startswith("event_date=")
    )


def _partition_for_date(
    event_date: Optional[str],
) -> Optional[Path]:
    partitions = _available_partitions()

    if not partitions:
        return None

    if event_date:
        expected_partition = (
            ANALYTICS_ROOT / f"event_date={event_date}"
        )

        if expected_partition.is_dir():
            return expected_partition

        return None

    return partitions[-1]


def _partition_date(partition: Path) -> Optional[str]:
    prefix = "event_date="

    if not partition.name.startswith(prefix):
        return None

    return partition.name.split("=", 1)[1]


def _validate_analytics_schema(
    dataframe: pd.DataFrame,
) -> bool:
    return REQUIRED_ANALYTICS_COLUMNS.issubset(
        dataframe.columns
    )


def load_analytics(
    event_date: Optional[str] = None,
) -> pd.DataFrame:
    partition = _partition_for_date(event_date)

    if partition is None:
        return pd.DataFrame()

    try:
        dataframe = pd.read_parquet(
            partition,
            engine="pyarrow",
        )
    except (
        OSError,
        ValueError,
        ImportError,
    ):
        return pd.DataFrame()

    if not _validate_analytics_schema(dataframe):
        return pd.DataFrame()

    partition_date = _partition_date(partition)

    if "event_date" not in dataframe.columns:
        if partition_date is None:
            return pd.DataFrame()

        dataframe = dataframe.copy()
        dataframe["event_date"] = partition_date
    else:
        dataframe["event_date"] = (
            dataframe["event_date"].astype(str)
        )

    dataframe["symbol"] = (
        dataframe["symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    dataframe["source"] = (
        dataframe["source"]
        .astype(str)
        .str.strip()
    )

    return dataframe


def latest_prices(
    symbols: List[str],
) -> List[Dict[str, Any]]:
    dataframe = load_analytics()

    if dataframe.empty:
        return []

    requested_symbols = {
        symbol.strip().upper()
        for symbol in symbols
        if symbol.strip()
    }

    filtered = dataframe[
        dataframe["symbol"].isin(requested_symbols)
    ].copy()

    if filtered.empty:
        return []

    filtered = (
        filtered.sort_values(
            ["symbol", "last_event_time"],
            ascending=[True, False],
        )
        .drop_duplicates(
            subset=["symbol"],
            keep="first",
        )
    )

    output_columns = [
        "event_date",
        "symbol",
        "source",
        "latest_price",
        "last_event_time",
        "tick_count",
    ]

    return filtered[
        output_columns
    ].to_dict(orient="records")


def daily_summary(
    symbol: str,
    event_date: str,
) -> List[Dict[str, Any]]:
    dataframe = load_analytics(event_date)

    if dataframe.empty:
        return []

    normalized_symbol = symbol.strip().upper()

    filtered = dataframe[
        dataframe["symbol"] == normalized_symbol
    ].copy()

    if filtered.empty:
        return []

    output_columns = [
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
    ]

    return filtered[
        output_columns
    ].to_dict(orient="records")