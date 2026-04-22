# Real-Time Data Pipeline Runbook

This runbook provides the operating steps for starting, validating, resetting, and troubleshooting the local real-time data pipeline.

## Scope

This runbook covers the local portfolio version of the pipeline using Docker, Kafka, Spark Structured Streaming, Cassandra, and Streamlit.

Use this document when you need to:
- start the environment,
- run the Spark jobs,
- replay clean test events,
- validate Cassandra output,
- troubleshoot duplicate or missing results.

## Services

| Service | Purpose |
|---|---|
| Kafka | Receives JSON event messages before Spark consumes them |
| Spark job 1 | Parses Kafka events and writes raw rows into Cassandra `events` |
| Spark job 2 | Aggregates events by `source` and writes metrics into Cassandra `source_metrics` |
| Cassandra | Stores raw event data and source-level metrics |
| Streamlit | Displays KPIs, charts, and validation tables from Cassandra |

## Preconditions

Before starting, confirm the following:
- Docker Desktop is running
- The repository files are present locally
- `docker-compose.yml` is in the project root
- The Spark scripts and dashboard file are available
- Kafka topic and checkpoint paths match your current config values

## Startup

### 1. Start the platform

```bash
docker-compose up -d
```

### 2. Check container health

```bash
docker ps
```

Expected result:
- broker is running,
- cassandra is running,
- spark-master is running,
- any required worker or dashboard containers are running.

### 3. Start the raw-events Spark job

```bash
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.cassandra.connection.host=cassandra \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /opt/spark_kafka_to_cassandra.py
```

### 4. Start the metrics Spark job

```bash
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.cassandra.connection.host=cassandra \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /opt/spark_kafka_source_metrics.py
```

### 5. Start the dashboard

```bash
python -m streamlit run dashboard.py
```

## Test event replay

### 1. Open the Kafka producer

```bash
docker exec -it broker bash
kafka-console-producer --topic test-topic-clean --bootstrap-server localhost:9092
```

### 2. Send sample events

Paste one JSON event per line:

```json
{"event_id":"1","user_id":"u101","event_type":"view","product_id":"p10","price":"19.99","event_time":"2026-04-15T18:00:00","source":"web"}
{"event_id":"2","user_id":"u101","event_type":"add_to_cart","product_id":"p10","price":"19.99","event_time":"2026-04-15T18:00:05","source":"web"}
{"event_id":"3","user_id":"u205","event_type":"purchase","product_id":"p22","price":"59.50","event_time":"2026-04-15T18:00:15","source":"mobile"}
```

Expected outcome after a clean replay:
- raw events appear in Cassandra `events`,
- aggregated results appear in Cassandra `source_metrics`,
- the dashboard shows total events, total revenue, and per-source charts.

## Validation

### 1. Check Cassandra directly

```bash
docker exec -it cassandra cqlsh
```

Then run:

```sql
USE realtime_pipeline;
SELECT * FROM events;
SELECT * FROM source_metrics;
```

A prior clean validation for this project showed:
- 3 rows in `events`,
- 2 rows in `source_metrics`,
- `web` with 2 events and 39.98 revenue,
- `mobile` with 1 event and 59.5 revenue.

### 2. Check dashboard output

The dashboard should show:
- KPI totals,
- source-level event count and revenue charts,
- raw event table,
- source metrics table.

If Cassandra is correct but Streamlit is wrong, investigate the dashboard environment or query logic before restarting Kafka or Spark.

## Clean reset

A clean replay requires more than truncating Cassandra because Kafka topic history and Spark checkpoints can preserve prior state.

### Reset procedure
1. Stop both Spark jobs.
2. Stop the Streamlit dashboard if needed.
3. Remove Spark checkpoint directories.
4. Truncate Cassandra tables.
5. Use a clean Kafka topic such as `test-topic-clean` if necessary.
6. Restart the Spark jobs.
7. Replay test events.
8. Re-run Cassandra validation queries.

### Example reset commands

Delete old checkpoints:

```bash
rm -rf /tmp/checkpoints/raw_events
rm -rf /tmp/checkpoints/raw_events_invalid
rm -rf /tmp/checkpoints/source_metrics
rm -rf /tmp/checkpoints/source_metrics_invalid
```

Truncate Cassandra tables:

```sql
USE realtime_pipeline;
TRUNCATE events;
TRUNCATE source_metrics;
```

`TRUNCATE` removes all rows from a Cassandra table, which makes it useful for clean local reruns.

## Common problems

### Duplicate results after replay

Possible causes:
- old Spark checkpoints still exist,
- old Kafka topic data is still being consumed,
- the metrics job replayed historical data into Cassandra.

Actions:
1. stop both Spark jobs,
2. delete checkpoints,
3. truncate both Cassandra tables,
4. restart the jobs,
5. replay only the intended events.

### Events table updates but source_metrics stays empty

Checks:
- confirm the metrics Spark job is actually running,
- inspect metrics job logs for schema or write errors,
- verify the `source` and `price` fields are valid in the input events,
- confirm the metrics Cassandra table name and keyspace are correct.

### Dashboard shows no data

Possible causes:
- Streamlit is connected to the wrong Cassandra environment,
- dashboard Python version has Cassandra-driver compatibility issues,
- Spark jobs have not yet written data.

Checks:
1. validate Cassandra directly with `cqlsh`,
2. restart the dashboard environment,
3. verify Python 3.11 is being used for the dashboard if the driver issue reappears.

### Invalid input rows

If validation is enabled in the Spark jobs, invalid records should be logged instead of written into the target table.

Likely causes:
- missing `event_id`,
- invalid `event_type`,
- null `source`,
- nonnumeric `price`.

Actions:
- review the producer payload,
- resend corrected events,
- confirm the validation rules match the expected schema.

## Verification checklist

Use this quick checklist after each clean run:

- [ ] Docker containers are running
- [ ] Raw-events Spark job is running
- [ ] Metrics Spark job is running
- [ ] Kafka test events were replayed
- [ ] Cassandra `events` contains expected rows
- [ ] Cassandra `source_metrics` contains expected aggregates
- [ ] Dashboard loads and shows KPIs/charts

## Escalation logic

For this local portfolio project, escalate from dashboard troubleshooting to streaming troubleshooting only after Cassandra validation fails. Cassandra should be treated as the source of truth for whether the Spark jobs processed data correctly.

If `events` is correct but `source_metrics` is wrong, focus on the metrics job. If both tables are correct but the UI is wrong, focus on Streamlit and the Python environment.