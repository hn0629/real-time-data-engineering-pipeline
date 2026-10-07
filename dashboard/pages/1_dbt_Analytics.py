import os
import re
import time

import boto3
from botocore.config import Config
import pandas as pd
import streamlit as st


st.set_page_config(page_title="dbt Analytics", layout="wide")
st.title("dbt Analytics")
st.caption(
    "Amazon Athena | dbt latest available daily metrics | Simulated stock data"
)


@st.cache_data(ttl=300, show_spinner="Querying the dbt mart in Athena...")
def load_dbt_mart():
    database = os.environ["ATHENA_DATABASE"]
    output = os.environ["ATHENA_OUTPUT"]
    region = os.environ["AWS_REGION"]

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError("ATHENA_DATABASE must be a simple SQL identifier.")

    client = boto3.client(
        "athena",
        region_name=region,
        config=Config(
            connect_timeout=10,
            read_timeout=20,
            retries={"mode": "standard", "total_max_attempts": 3},
        ),
    )

    sql = f"""
        SELECT
            event_date,
            symbol,
            source,
            event_count,
            average_price,
            minimum_price,
            maximum_price,
            latest_event_time
        FROM "{database}"."mart_latest_symbol_metrics"
        ORDER BY symbol, source
        LIMIT 1000
    """

    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": database,
            "Catalog": "AwsDataCatalog",
        },
        ResultConfiguration={"OutputLocation": output},
    )
    query_id = response["QueryExecutionId"]
    deadline = time.monotonic() + 90

    while True:
        status = client.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]

        state = status["State"]

        if state == "SUCCEEDED":
            break

        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "No reason returned.")
            raise RuntimeError(f"Athena query {query_id}: {reason}")

        if time.monotonic() >= deadline:
            try:
                client.stop_query_execution(QueryExecutionId=query_id)
            except Exception:
                pass
            raise TimeoutError(
                f"Athena polling timed out after 90 seconds. Query ID: {query_id}"
            )

        time.sleep(1)

    columns = None
    rows = []
    first_row = True

    paginator = client.get_paginator("get_query_results")

    for result_page in paginator.paginate(QueryExecutionId=query_id):
        result_set = result_page["ResultSet"]

        if columns is None:
            columns = [
                column["Name"]
                for column in result_set["ResultSetMetadata"]["ColumnInfo"]
            ]

        for row in result_set["Rows"]:
            if first_row:
                first_row = False
                continue

            rows.append([
                value.get("VarCharValue")
                for value in row["Data"]
            ])

    dataframe = pd.DataFrame(rows, columns=columns)

    for column in [
        "event_count",
        "average_price",
        "minimum_price",
        "maximum_price",
    ]:
        dataframe[column] = pd.to_numeric(
            dataframe[column], errors="coerce"
        )

    dataframe["latest_event_time"] = pd.to_datetime(
        dataframe["latest_event_time"],
        errors="coerce",
        utc=True,
    )

    return dataframe, query_id, pd.Timestamp.now(tz="UTC").isoformat()


if st.button("Refresh Athena results"):
    load_dbt_mart.clear()

try:
    dataframe, query_id, queried_at = load_dbt_mart()
except Exception as error:
    st.error(f"Could not load the dbt mart: {error}")
    st.stop()

if dataframe.empty:
    st.warning("The dbt mart returned no rows.")
    st.stop()

st.info(
    "Each symbol/source pair uses its latest available event date. "
    "Dates can differ between pairs. These results are not a live-market feed."
)

left, middle, right = st.columns(3)
left.metric("Summary rows", len(dataframe))
middle.metric("Symbols", dataframe["symbol"].nunique())
right.metric(
    "Captured events in selected daily summaries",
    f"{int(dataframe['event_count'].fillna(0).sum()):,}",
)

st.dataframe(dataframe, use_container_width=True, hide_index=True)

latest_event = dataframe["latest_event_time"].max()

if pd.notna(latest_event):
    age = pd.Timestamp.now(tz="UTC") - latest_event
    st.caption(
        f"Newest underlying event: {latest_event.isoformat()} | "
        f"Age: {max(0, age.total_seconds()) / 86400:.1f} days"
    )

st.caption(f"Results fetched at: {queried_at}")
st.caption(f"Athena query ID: {query_id}")
st.caption(
    "Results are cached for five minutes and limited to 1,000 summary rows. "
    "Refreshing submits a new Athena query and may incur AWS charges."
)