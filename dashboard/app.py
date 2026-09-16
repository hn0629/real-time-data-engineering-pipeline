from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Real-Time Market Data Pipeline",
    page_icon="📈",
    layout="wide",
)


CLEAN_PATH = Path("/opt/spark-data/clean/stock_prices")
METRICS_PATH = Path("/opt/spark-data/metrics/stream_batches")
QUARANTINE_PATH = Path("/opt/spark-data/quarantine/stock_prices")

REFRESH_TTL_SECONDS = 60


def parquet_files(path: Path) -> list[Path]:
    if not path.exists():
        return []

    return sorted(
        path.rglob("*.parquet"),
        key=lambda file: file.stat().st_mtime,
        reverse=True,
    )


@st.cache_data(ttl=REFRESH_TTL_SECONDS)
def load_parquet_dataset(path_as_string: str) -> pd.DataFrame:
    path = Path(path_as_string)
    files = parquet_files(path)

    if not files:
        return pd.DataFrame()

    try:
        return pd.read_parquet(files, engine="pyarrow")
    except Exception as exc:
        st.error(f"Could not read Parquet files from {path}: {exc}")
        return pd.DataFrame()


def prepare_clean_events(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return events_df

    events_df = events_df.copy()

    if "event_time" in events_df.columns:
        events_df["event_time"] = pd.to_datetime(
            events_df["event_time"],
            errors="coerce",
            utc=True,
        )

    if "ingested_at" in events_df.columns:
        events_df["ingested_at"] = pd.to_datetime(
            events_df["ingested_at"],
            errors="coerce",
            utc=True,
        )

    if "price" in events_df.columns:
        events_df["price"] = pd.to_numeric(
            events_df["price"],
            errors="coerce",
        )

    return events_df.dropna(
        subset=[
            column
            for column in ["symbol", "price", "event_time"]
            if column in events_df.columns
        ]
    )


def prepare_metrics(metrics_df: pd.DataFrame) -> pd.DataFrame:
    if metrics_df.empty:
        return metrics_df

    metrics_df = metrics_df.copy()

    for column in [
        "raw_count",
        "clean_count",
        "quarantine_count",
        "batch_id",
    ]:
        if column in metrics_df.columns:
            metrics_df[column] = pd.to_numeric(
                metrics_df[column],
                errors="coerce",
            )

    if "processed_at_utc" in metrics_df.columns:
        metrics_df["processed_at_utc"] = pd.to_datetime(
            metrics_df["processed_at_utc"],
            errors="coerce",
            utc=True,
        )

    return metrics_df


with st.sidebar:
    st.header("Dashboard controls")
    st.caption(
        "Metrics are read from local Spark Parquet outputs. "
        "The Spark stream writes new micro-batches continuously."
    )

    if st.button("Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.subheader("Data source")
    st.code("/opt/spark-data/clean/stock_prices")

    st.subheader("Refresh cache")
    st.write(f"{REFRESH_TTL_SECONDS} seconds")


st.title("📈 Real-Time Market Data Pipeline")
st.caption(
    "Kafka → Spark Structured Streaming → Local Parquet → Streamlit"
)

clean_events = prepare_clean_events(
    load_parquet_dataset(str(CLEAN_PATH))
)

batch_metrics = prepare_metrics(
    load_parquet_dataset(str(METRICS_PATH))
)

quarantine_events = load_parquet_dataset(str(QUARANTINE_PATH))

if clean_events.empty:
    st.warning(
        "No clean events are available yet. "
        "Confirm the Spark streaming job is running and writing Parquet files."
    )
    st.stop()

latest_event_time = clean_events["event_time"].max()
now_utc = pd.Timestamp.now(tz="UTC")
freshness_seconds = max(
    0,
    int((now_utc - latest_event_time).total_seconds()),
)

latest_by_symbol = (
    clean_events
    .sort_values("event_time")
    .groupby("symbol", as_index=False)
    .tail(1)
    .sort_values("symbol")
)

total_clean_events = len(clean_events)
unique_symbols = clean_events["symbol"].nunique()
quarantine_count = len(quarantine_events)

if not batch_metrics.empty and "raw_count" in batch_metrics.columns:
    total_raw_events = int(batch_metrics["raw_count"].fillna(0).sum())
else:
    total_raw_events = total_clean_events + quarantine_count

metric_1, metric_2, metric_3, metric_4 = st.columns(4)

metric_1.metric("Clean events", f"{total_clean_events:,}")
metric_2.metric("Unique symbols", unique_symbols)
metric_3.metric("Latest event (UTC)", latest_event_time.strftime("%H:%M:%S"))
metric_4.metric("Freshness", f"{freshness_seconds}s")

st.divider()

left_column, right_column = st.columns((2, 1))

with left_column:
    st.subheader("Latest price by symbol")

    latest_prices = latest_by_symbol[
        ["symbol", "price", "event_time"]
    ].rename(
        columns={
            "symbol": "Symbol",
            "price": "Latest price",
            "event_time": "Event time (UTC)",
        }
    )

    st.dataframe(
        latest_prices,
        use_container_width=True,
        hide_index=True,
    )

with right_column:
    st.subheader("Pipeline quality")

    quality_data = pd.DataFrame(
        {
            "Metric": [
                "Raw events",
                "Clean events",
                "Quarantined events",
            ],
            "Count": [
                total_raw_events,
                total_clean_events,
                quarantine_count,
            ],
        }
    )

    st.dataframe(
        quality_data,
        use_container_width=True,
        hide_index=True,
    )

st.divider()

chart_left, chart_right = st.columns(2)

with chart_left:
    st.subheader("Price trend")

    selected_symbols = st.multiselect(
        "Symbols",
        options=sorted(clean_events["symbol"].unique()),
        default=sorted(clean_events["symbol"].unique()),
    )

    chart_events = clean_events[
        clean_events["symbol"].isin(selected_symbols)
    ].copy()

    if not chart_events.empty:
        chart_data = (
            chart_events
            .sort_values("event_time")
            .pivot_table(
                index="event_time",
                columns="symbol",
                values="price",
                aggfunc="last",
            )
        )

        st.line_chart(chart_data, use_container_width=True)

with chart_right:
    st.subheader("Price summary")

    price_summary = (
        clean_events
        .groupby("symbol", as_index=False)["price"]
        .agg(
            min_price="min",
            average_price="mean",
            max_price="max",
            event_count="count",
        )
        .sort_values("symbol")
    )

    price_summary["min_price"] = price_summary["min_price"].round(2)
    price_summary["average_price"] = price_summary[
        "average_price"
    ].round(2)
    price_summary["max_price"] = price_summary["max_price"].round(2)

    st.dataframe(
        price_summary,
        use_container_width=True,
        hide_index=True,
    )

st.divider()

st.subheader("Most recent clean events")

recent_events = (
    clean_events
    .sort_values("event_time", ascending=False)
    .head(50)
)

display_columns = [
    column
    for column in [
        "symbol",
        "price",
        "source",
        "event_time",
        "ingested_at",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ]
    if column in recent_events.columns
]

st.dataframe(
    recent_events[display_columns],
    use_container_width=True,
    hide_index=True,
)

if not batch_metrics.empty:
    st.divider()
    st.subheader("Recent Spark micro-batches")

    metrics_columns = [
        column
        for column in [
            "batch_id",
            "raw_count",
            "clean_count",
            "quarantine_count",
            "processed_at_utc",
            "kafka_topic",
        ]
        if column in batch_metrics.columns
    ]

    st.dataframe(
        batch_metrics
        .sort_values(
            "processed_at_utc",
            ascending=False,
        )
        .head(25)[metrics_columns],
        use_container_width=True,
        hide_index=True,
    )