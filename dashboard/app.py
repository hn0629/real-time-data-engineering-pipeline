import json
from pathlib import Path
import sys

import pandas as pd
import streamlit as st


APP_ROOT = Path("/app")

if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from llm.analytics_backend import (
    load_analytics,
    load_pipeline_status,
)


st.set_page_config(
    page_title="Real-Time Market Data Pipeline",
    page_icon="📈",
    layout="wide",
)


ANALYTICS_ROOT = Path(
    "/opt/spark-data/analytics/stock_price_summary"
)

STATUS_PATH = Path(
    "/opt/spark-data/monitoring/pipeline_status.json"
)

REFRESH_TTL_SECONDS = 60

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


def available_partitions() -> list[Path]:
    if not ANALYTICS_ROOT.exists():
        return []

    return sorted(
        path
        for path in ANALYTICS_ROOT.iterdir()
        if path.is_dir()
        and path.name.startswith("event_date=")
    )


def latest_partition() -> Path | None:
    partitions = available_partitions()

    if not partitions:
        return None

    return partitions[-1]


@st.cache_data(
    ttl=REFRESH_TTL_SECONDS,
    show_spinner="Loading latest Analytics summary...",
)
def load_analytics() -> pd.DataFrame:
    partition = latest_partition()

    if partition is None:
        return pd.DataFrame()

    try:
        dataframe = pd.read_parquet(
            partition,
            engine="pyarrow",
        )
    except Exception as error:
        st.error(
            f"Could not read Analytics data from {partition}: "
            f"{error}"
        )
        return pd.DataFrame()

    if not REQUIRED_ANALYTICS_COLUMNS.issubset(
        dataframe.columns
    ):
        return pd.DataFrame()

    if "event_date" not in dataframe.columns:
        dataframe = dataframe.copy()
        dataframe["event_date"] = partition.name.split(
            "=",
            1,
        )[1]

    return dataframe


@st.cache_data(
    ttl=REFRESH_TTL_SECONDS,
    show_spinner="Loading pipeline health status...",
)
def load_pipeline_status() -> dict:
    if not STATUS_PATH.exists():
        return {
            "available": False,
            "message": (
                "No pipeline health artifact was found at "
                f"{STATUS_PATH}."
            ),
        }

    try:
        payload = json.loads(
            STATUS_PATH.read_text(encoding="utf-8")
        )
    except (
        OSError,
        json.JSONDecodeError,
    ) as error:
        return {
            "available": False,
            "message": (
                "Pipeline health artifact could not be read: "
                f"{error}"
            ),
        }

    if not isinstance(payload, dict):
        return {
            "available": False,
            "message": (
                "Pipeline health artifact has an invalid format."
            ),
        }

    return {
        "available": True,
        "status": payload,
    }


def value_from(
    payload: dict,
    *keys: str,
) -> object:
    for key in keys:
        if key in payload:
            return payload[key]

    return None


def format_price(value: object) -> str:
    if value is None or pd.isna(value):
        return "N/A"

    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def format_timestamp(value: object) -> str:
    if value is None or pd.isna(value):
        return "N/A"

    timestamp = pd.to_datetime(
        value,
        errors="coerce",
        utc=True,
    )

    if pd.isna(timestamp):
        return str(value)

    return timestamp.strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def render_pipeline_health(
    status_response: dict,
) -> None:
    st.subheader("Pipeline health")

    if not status_response.get("available"):
        st.warning(
            status_response.get(
                "message",
                "Pipeline health status is unavailable.",
            )
        )
        return

    status = status_response.get("status")

    if not isinstance(status, dict):
        st.warning(
            "Pipeline health status has an invalid format."
        )
        return

    overall_status = value_from(
        status,
        "status",
        "state",
    )

    checked_at = value_from(
        status,
        "checked_at_utc",
        "checked_at",
        "generated_at",
        "updated_at",
    )

    services = status.get("services", {})

    errors_seen = value_from(
        status,
        "errors_seen",
        "errors",
    )

    health_left, health_right = st.columns((1, 2))

    with health_left:
        if str(overall_status).lower() == "healthy":
            st.success(
                f"Overall status: {overall_status}"
            )
        elif overall_status:
            st.warning(
                f"Overall status: {overall_status}"
            )
        else:
            st.info("Overall status: unknown")

        if checked_at:
            st.caption(
                "Checked at: "
                + format_timestamp(checked_at)
            )

        if isinstance(errors_seen, list):
            if errors_seen:
                st.error(
                    "Errors seen: "
                    + "; ".join(map(str, errors_seen))
                )
            else:
                st.caption("Errors seen: none")

    with health_right:
        if not isinstance(services, dict) or not services:
            st.info(
                "No individual service checks are available."
            )
            return

        rows = []

        for name, service in services.items():
            if isinstance(service, dict):
                rows.append(
                    {
                        "Service": name,
                        "Status": value_from(
                            service,
                            "status",
                            "state",
                        )
                        or "unknown",
                        "Message": value_from(
                            service,
                            "message",
                            "detail",
                            "reason",
                        )
                        or "",
                    }
                )
            else:
                rows.append(
                    {
                        "Service": name,
                        "Status": str(service),
                        "Message": "",
                    }
                )

        st.dataframe(
            pd.DataFrame(rows),
            use_container_width=True,
            hide_index=True,
        )


with st.sidebar:
    st.header("Dashboard controls")

    st.caption(
        "Read-only dashboard using the newest local "
        "Analytics Parquet partition."
    )

    if st.button(
        "Refresh now",
        use_container_width=True,
    ):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.subheader("Analytics source")
    st.code(
        "/opt/spark-data/analytics/"
        "stock_price_summary/"
        "event_date=YYYY-MM-DD"
    )

    st.subheader("Refresh cache")
    st.write(f"{REFRESH_TTL_SECONDS} seconds")


st.title("📈 Real-Time Market Data Pipeline")
st.caption(
    "Kafka → Spark Structured Streaming → Raw / Clean / "
    "Quarantine → Analytics Parquet → Streamlit"
)

analytics_summary = load_analytics()
pipeline_status = load_pipeline_status()

render_pipeline_health(pipeline_status)

st.divider()

if analytics_summary.empty:
    st.warning(
        "No readable Analytics summary was found in the "
        "latest event_date partition. Confirm that the "
        "Spark analytics job has produced output under "
        "/opt/spark-data/analytics/stock_price_summary."
    )
    st.stop()

for column in [
    "tick_count",
    "min_price",
    "max_price",
    "avg_price",
    "latest_price",
]:
    analytics_summary[column] = pd.to_numeric(
        analytics_summary[column],
        errors="coerce",
    )

for column in [
    "first_event_time",
    "last_event_time",
    "processed_at",
]:
    analytics_summary[column] = pd.to_datetime(
        analytics_summary[column],
        errors="coerce",
        utc=True,
    )

analytics_summary["symbol"] = (
    analytics_summary["symbol"]
    .astype(str)
    .str.strip()
    .str.upper()
)

analytics_summary["source"] = (
    analytics_summary["source"]
    .astype(str)
    .str.strip()
)

latest_event_time = analytics_summary[
    "last_event_time"
].max()

if pd.notna(latest_event_time):
    now_utc = pd.Timestamp.now(tz="UTC")

    freshness_seconds = max(
        0,
        int(
            (
                now_utc - latest_event_time
            ).total_seconds()
        ),
    )

    latest_event_display = latest_event_time.strftime(
        "%H:%M:%S"
    )

    freshness_display = f"{freshness_seconds}s"
else:
    latest_event_display = "Unavailable"
    freshness_display = "Unavailable"

total_ticks = int(
    analytics_summary["tick_count"].fillna(0).sum()
)

unique_symbols = analytics_summary["symbol"].nunique()

latest_event_date = analytics_summary[
    "event_date"
].astype(str).max()

metric_1, metric_2, metric_3, metric_4 = st.columns(4)

metric_1.metric(
    "Analytics ticks",
    f"{total_ticks:,}",
)

metric_2.metric(
    "Unique symbols",
    unique_symbols,
)

metric_3.metric(
    "Latest event (UTC)",
    latest_event_display,
)

metric_4.metric(
    "Freshness",
    freshness_display,
)

st.caption(
    f"Latest Analytics partition: {latest_event_date}"
)

st.divider()

left_column, right_column = st.columns((2, 1))

with left_column:
    st.subheader("Latest price by symbol")

    latest_prices = (
        analytics_summary[
            [
                "symbol",
                "source",
                "latest_price",
                "last_event_time",
                "tick_count",
            ]
        ]
        .sort_values(["symbol", "source"])
        .rename(
            columns={
                "symbol": "Symbol",
                "source": "Source",
                "latest_price": "Latest price",
                "last_event_time": "Last event (UTC)",
                "tick_count": "Tick count",
            }
        )
    )

    latest_prices["Latest price"] = (
        latest_prices["Latest price"].map(format_price)
    )

    latest_prices["Last event (UTC)"] = (
        latest_prices["Last event (UTC)"].map(
            format_timestamp
        )
    )

    st.dataframe(
        latest_prices,
        use_container_width=True,
        hide_index=True,
    )

with right_column:
    st.subheader("Analytics coverage")

    coverage = pd.DataFrame(
        {
            "Metric": [
                "Analytics rows",
                "Analytics ticks",
                "Symbols",
                "Sources",
            ],
            "Value": [
                len(analytics_summary),
                total_ticks,
                unique_symbols,
                analytics_summary["source"].nunique(),
            ],
        }
    )

    st.dataframe(
        coverage,
        use_container_width=True,
        hide_index=True,
    )

st.divider()

chart_left, chart_right = st.columns(2)

with chart_left:
    st.subheader("Price range by symbol")

    price_range = (
        analytics_summary[
            [
                "symbol",
                "min_price",
                "avg_price",
                "max_price",
            ]
        ]
        .groupby(
            "symbol",
            as_index=True,
        )
        .max()
        .sort_index()
    )

    st.bar_chart(
        price_range,
        use_container_width=True,
    )

with chart_right:
    st.subheader("Analytics summary")

    display_summary = (
        analytics_summary[
            [
                "symbol",
                "source",
                "tick_count",
                "min_price",
                "avg_price",
                "max_price",
                "latest_price",
                "first_event_time",
                "last_event_time",
                "processed_at",
            ]
        ]
        .sort_values(["symbol", "source"])
        .copy()
    )

    for column in [
        "min_price",
        "avg_price",
        "max_price",
        "latest_price",
    ]:
        display_summary[column] = (
            display_summary[column].map(format_price)
        )

    for column in [
        "first_event_time",
        "last_event_time",
        "processed_at",
    ]:
        display_summary[column] = (
            display_summary[column].map(
                format_timestamp
            )
        )

    st.dataframe(
        display_summary,
        use_container_width=True,
        hide_index=True,
    )

st.divider()

st.subheader("Data contract")

st.markdown(
    """
- **Reader:** The dashboard reads the newest Analytics Parquet partition only.
- **Grain:** One derived summary row per `event_date`, `symbol`, and `source`.
- **Measures:** Tick count, minimum, maximum, average, and latest observed price.
- **Boundaries:** Streamlit is read-only; Spark owns the Raw, Clean, Quarantine, Metrics, and Analytics writes.
- **Assistant:** The local command-line assistant provides the same read-only, allowlisted pipeline and stock-summary lookups.
    """
)