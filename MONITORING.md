# Monitoring Guide

This document defines the basic monitoring and health checks for the local real-time data pipeline.

## Monitoring goals

The purpose of monitoring in this project is to answer these questions:
- Is the pipeline running
- Is data arriving
- Are Spark jobs processing records
- Are Cassandra tables being updated
- Is the dashboard reflecting the latest validated data

## Core health signals

For this project, monitor these five areas:

### 1. Volume
Check whether expected rows are reaching Cassandra.

Healthy signs:
- `events` row count increases after replay
- `source_metrics` contains expected grouped results

Problem signs:
- No new rows after sending Kafka events
- Raw rows appear but metrics stay empty

### 2. Freshness
Check whether recent test events show up shortly after being produced.

Healthy signs:
- New Kafka test messages appear in Cassandra within a short time
- Dashboard updates after refresh

Problem signs:
- Long delay between event replay and Cassandra updates
- Dashboard remains stale after confirmed Cassandra writes

### 3. Error rate
Check whether Spark jobs are logging failures or invalid records.

Healthy signs:
- No repeated exceptions in Spark logs
- Invalid row count is zero or expected

Problem signs:
- Repeated batch failures
- Schema parsing errors
- Cassandra write errors
- Many invalid rows

### 4. Data quality
Check whether the actual values look correct.

Healthy signs:
- `event_type` values are valid
- `source` is populated
- `price` is numeric
- Aggregate totals match the replayed test data

Problem signs:
- Null source values
- Bad price casts
- Unexpected event types
- Revenue totals that do not match input data

### 5. System status
Check whether the required services are running.

Healthy signs:
- Docker containers are up
- Spark jobs stay active
- Cassandra accepts queries
- Streamlit loads successfully

Problem signs:
- Containers restart repeatedly
- Spark jobs exit early
- Cassandra connection fails
- Dashboard cannot connect

## Healthy run definition

Treat a run as healthy if all of the following are true:

- Docker containers are running
- Kafka accepts new test events
- Raw-events Spark job stays active
- Metrics Spark job stays active
- Cassandra `events` contains expected rows
- Cassandra `source_metrics` contains expected aggregates
- Dashboard shows KPI totals and charts
- No major validation or write errors appear in logs

## Quick monitoring commands

### Check containers

```bash
docker ps
```

### Check Cassandra

```bash
docker exec -it cassandra cqlsh
```

Then run:

```sql
USE realtime_pipeline;
SELECT * FROM events;
SELECT * FROM source_metrics;
```

### Check dashboard

```bash
python -m streamlit run dashboard.py
```

### Check for Spark job output

Review the terminal where each Spark job is running and look for:
- batch start messages
- row count logs
- invalid row warnings
- Cassandra write success or failure messages

## Expected demo validation

For the standard clean replay, expect:

- `events` to contain 3 rows
- `source_metrics` to contain 2 rows
- `web` to show 2 events and 39.98 revenue
- `mobile` to show 1 event and 59.5 revenue

If these values are present, the replay is considered valid.

## Monitoring checklist

Use this after each replay:

- [ ] Containers are running
- [ ] Kafka producer accepted events
- [ ] Raw-events Spark job processed the batch
- [ ] Metrics Spark job processed the batch
- [ ] No major Spark errors appeared
- [ ] No unexpected invalid rows appeared
- [ ] Cassandra raw table looks correct
- [ ] Cassandra metrics table looks correct
- [ ] Dashboard matches Cassandra output

## Alert-style conditions

Even in a local project, define simple “alert” rules for yourself.

Treat these as failures that need investigation:
- No Cassandra updates after replay
- `events` updates but `source_metrics` does not
- Dashboard does not match Cassandra
- Invalid rows appear unexpectedly
- Duplicate totals appear after reset
- Spark job crashes or stops streaming

## Troubleshooting order

Always check in this order:
1. Docker container status
2. Spark job logs
3. Cassandra table contents
4. Dashboard output

This order reduces wasted debugging time because Cassandra is the best checkpoint for whether processing actually happened.

## Future improvements

Later, this project could add:
- a simple status dashboard
- log file rotation
- row-count trend tracking
- latency timing by batch
- automated alerts through email or Slack