-- Synthetic replay fixture only.
-- Target: isolated Databricks replay schema.
-- Safe to rerun: replaces only its own replay fixture table.

SET TIME ZONE 'UTC';

CREATE SCHEMA IF NOT EXISTS workspace.stock_pipeline_clean_replay_20261014;

CREATE OR REPLACE TABLE workspace.stock_pipeline_clean_replay_20261014.clean_stock_prices_v2 (
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    symbol STRING,
    price DECIMAL(18, 4),
    source STRING,
    event_time STRING,
    ingested_at TIMESTAMP,
    event_date DATE
)
USING DELTA
COMMENT 'Isolated synthetic replay fixture for dbt warehouse portability validation; not live market data.';

INSERT INTO workspace.stock_pipeline_clean_replay_20261014.clean_stock_prices_v2 (
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
    symbol, price, source, event_time, ingested_at, event_date
)
VALUES
('stock_prices_fixture', 0, 100, '2026-10-08 20:00:01',
 ' aapl ', 100.00, 'synthetic_fixture', '2026-10-08T20:00:00Z',
 '2026-10-08 20:00:02', '2026-10-08'),
('stock_prices_fixture', 0, 101, '2026-10-08 22:30:01',
 'AAPL', 102.00, 'synthetic_fixture', '2026-10-09T00:30:00+02:00',
 '2026-10-08 22:30:02', '2026-10-08'),
('stock_prices_fixture', 0, 102, '2026-10-09 14:00:01',
 'aapl', 104.00, 'synthetic_fixture', '2026-10-09T14:00:00Z',
 '2026-10-09 14:00:02', '2026-10-09'),
('stock_prices_fixture', 0, 103, '2026-10-09 15:00:01',
 ' AAPL ', 106.00, 'synthetic_fixture', '2026-10-09T15:00:00Z',
 '2026-10-09 15:00:02', '2026-10-09'),
('stock_prices_fixture', 0, 104, '2026-10-08 16:00:01',
 ' msft ', 300.00, 'synthetic_fixture', '2026-10-08T16:00:00Z',
 '2026-10-08 16:00:02', '2026-10-08'),
('stock_prices_fixture', 0, 105, '2026-10-09 16:00:01',
 'goog', 150.00, 'synthetic_fixture', '2026-10-09T16:00:00Z',
 '2026-10-09 16:00:02', '2026-10-09');

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT kafka_offset) AS distinct_offsets
FROM workspace.stock_pipeline_clean_replay_20261014.clean_stock_prices_v2;
-- Expected: row_count = 6, distinct_offsets = 6.