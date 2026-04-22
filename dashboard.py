from datetime import datetime

import pandas as pd
import streamlit as st
from cassandra.cluster import Cluster, Session

st.set_page_config(
    page_title="Real-Time Pipeline Dashboard",
    layout="wide",
)

CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "realtime_pipeline"

EXPECTED_RAW_ROWS = 3
EXPECTED_METRIC_ROWS = 2
EXPECTED_WEB_EVENTS = 2
EXPECTED_WEB_REVENUE = 39.98
EXPECTED_MOBILE_EVENTS = 1
EXPECTED_MOBILE_REVENUE = 59.50


@st.cache_resource
def get_cassandra_session() -> tuple[Cluster, Session]:
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    return cluster, session


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    cluster, session = get_cassandra_session()

    events_rows = session.execute("SELECT * FROM events;")
    metrics_rows = session.execute("SELECT * FROM source_metrics;")

    events_df = pd.DataFrame(list(events_rows))
    metrics_df = pd.DataFrame(list(metrics_rows))

    if not events_df.empty:
        if "price" in events_df.columns:
            events_df["price"] = pd.to_numeric(events_df["price"], errors="coerce")
        if "event_time" in events_df.columns:
            events_df["event_time"] = pd.to_datetime(
                events_df["event_time"], errors="coerce"
            )

    if not metrics_df.empty:
        if "event_count" in metrics_df.columns:
            metrics_df["event_count"] = pd.to_numeric(
                metrics_df["event_count"], errors="coerce"
            )
        if "total_revenue" in metrics_df.columns:
            metrics_df["total_revenue"] = pd.to_numeric(
                metrics_df["total_revenue"], errors="coerce"
            )

    return events_df, metrics_df


def main():
    if st.button("Refresh data"):
        st.rerun()

    st.title("Real-Time Data Pipeline Dashboard")
    st.caption("Kafka → Spark Structured Streaming → Cassandra → Streamlit")

    try:
        events_df, metrics_df = load_data()
    except Exception as e:
        st.error(f"Failed to connect to Cassandra or fetch data: {e}")
        st.stop()

    raw_count = len(events_df)
    metrics_count = len(metrics_df)

    st.subheader("Pipeline Health")

    health_col1, health_col2, health_col3 = st.columns(3)
    health_col1.metric("Raw rows", raw_count, delta=raw_count - EXPECTED_RAW_ROWS)
    health_col2.metric(
        "Metric rows",
        metrics_count,
        delta=metrics_count - EXPECTED_METRIC_ROWS,
    )
    health_col3.metric("Last refresh", datetime.now().strftime("%H:%M:%S"))

    if raw_count == EXPECTED_RAW_ROWS and metrics_count == EXPECTED_METRIC_ROWS:
        st.success("Validated replay matches expected output")
    elif raw_count > 0 or metrics_count > 0:
        st.warning(
            "Pipeline is active, but current counts do not match "
            "the expected clean replay."
        )
    else:
        st.error("No pipeline output found yet.")

    st.markdown("""
- Expected clean replay: **3 raw rows** and **2 metric rows**
- Check Cassandra first if the dashboard looks wrong
- If `events` is correct but `source_metrics` is wrong, inspect the metrics Spark job
""")

    st.subheader("Source Metrics")

    if not metrics_df.empty:
        total_events = (
            int(metrics_df["event_count"].fillna(0).sum())
            if "event_count" in metrics_df.columns
            else 0
        )
        total_revenue = (
            float(metrics_df["total_revenue"].fillna(0).sum())
            if "total_revenue" in metrics_df.columns
            else 0.0
        )
        total_sources = (
            int(metrics_df["source"].nunique())
            if "source" in metrics_df.columns
            else 0
        )

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", total_events)
        col2.metric("Total Revenue", f"${total_revenue:.2f}")
        col3.metric("Sources", total_sources)

        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            st.markdown("### Event Count by Source")
            if {"source", "event_count"}.issubset(metrics_df.columns):
                count_chart_df = metrics_df.set_index("source")[["event_count"]]
                st.bar_chart(count_chart_df)
            else:
                st.warning("Missing columns needed for event count chart.")

        with chart_col2:
            st.markdown("### Revenue by Source")
            if {"source", "total_revenue"}.issubset(metrics_df.columns):
                revenue_chart_df = metrics_df.set_index("source")[["total_revenue"]]
                st.bar_chart(revenue_chart_df)
            else:
                st.warning("Missing columns needed for revenue chart.")

        st.markdown("### Metrics Table")
        st.dataframe(metrics_df, use_container_width=True)
    else:
        st.warning("No source metrics found.")

    st.subheader("Raw Events")

    if not events_df.empty:
        if "source" in events_df.columns:
            source_options = ["All"] + sorted(
                events_df["source"].dropna().unique().tolist()
            )
            selected_source = st.selectbox(
                "Filter raw events by source",
                source_options,
            )

            filtered_events_df = events_df.copy()

            if selected_source != "All":
                filtered_events_df = filtered_events_df[
                    filtered_events_df["source"] == selected_source
                ]
        else:
            filtered_events_df = events_df.copy()
            st.info("No source column found for filtering.")

        st.dataframe(filtered_events_df, use_container_width=True)
    else:
        st.warning("No raw events found.")

    st.subheader("Validation Snapshot")

    validation_messages = []

    if raw_count == EXPECTED_RAW_ROWS:
        validation_messages.append("✅ Raw events row count matches expected clean replay")
    else:
        validation_messages.append(
            f"⚠️ Raw events row count is {raw_count}, expected {EXPECTED_RAW_ROWS}"
        )

    if metrics_count == EXPECTED_METRIC_ROWS:
        validation_messages.append(
            "✅ Source metrics row count matches expected clean replay"
        )
    else:
        validation_messages.append(
            f"⚠️ Source metrics row count is {metrics_count}, expected {EXPECTED_METRIC_ROWS}"
        )

    required_metric_columns = {"source", "event_count", "total_revenue"}

    if not metrics_df.empty and required_metric_columns.issubset(metrics_df.columns):
        web_row = metrics_df[metrics_df["source"] == "web"]
        mobile_row = metrics_df[metrics_df["source"] == "mobile"]

        if not web_row.empty:
            web_events = int(web_row["event_count"].iloc[0])
            web_revenue = float(web_row["total_revenue"].iloc[0])

            if (
                web_events == EXPECTED_WEB_EVENTS
                and round(web_revenue, 2) == EXPECTED_WEB_REVENUE
            ):
                validation_messages.append("✅ Web metrics match expected values")
            else:
                validation_messages.append(
                    f"⚠️ Web metrics mismatch: event_count={web_events}, total_revenue={web_revenue}"
                )
        else:
            validation_messages.append("⚠️ No web row found in source_metrics")

        if not mobile_row.empty:
            mobile_events = int(mobile_row["event_count"].iloc[0])
            mobile_revenue = float(mobile_row["total_revenue"].iloc[0])

            if (
                mobile_events == EXPECTED_MOBILE_EVENTS
                and round(mobile_revenue, 2) == EXPECTED_MOBILE_REVENUE
            ):
                validation_messages.append("✅ Mobile metrics match expected values")
            else:
                validation_messages.append(
                    f"⚠️ Mobile metrics mismatch: event_count={mobile_events}, total_revenue={mobile_revenue}"
                )
        else:
            validation_messages.append("⚠️ No mobile row found in source_metrics")

    for message in validation_messages:
        st.markdown(f"- {message}")

    st.caption(
        "Refresh the page after replaying Kafka events to see updated Cassandra results."
    )


if __name__ == "__main__":
    main()