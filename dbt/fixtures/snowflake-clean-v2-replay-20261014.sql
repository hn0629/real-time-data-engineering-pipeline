-- Synthetic replay fixture only.
-- Target: isolated Snowflake replay schema.
-- Safe to rerun: replaces only its own replay fixture table.

USE ROLE SYSADMIN;
USE WAREHOUSE STOCK_DBT_DEV_WH;
ALTER SESSION SET TIMEZONE = 'UTC';

CREATE SCHEMA IF NOT EXISTS STOCK_PIPELINE_DEV.CLEAN_REPLAY_20261014;

CREATE OR REPLACE TABLE STOCK_PIPELINE_DEV.CLEAN_REPLAY_20261014.CLEAN_STOCK_PRICES_V2 (
    kafka_topic VARCHAR,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP_NTZ,
    symbol VARCHAR,
    price NUMBER(18, 4),
    source VARCHAR,
    event_time VARCHAR,
    ingested_at TIMESTAMP_NTZ,
    event_date DATE
)
COMMENT = 'Isolated synthetic replay fixture for dbt warehouse portability validation; not live market data.';

INSERT INTO STOCK_PIPELINE_DEV.CLEAN_REPLAY_20261014.CLEAN_STOCK_PRICES_V2 (
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
FROM STOCK_PIPELINE_DEV.CLEAN_REPLAY_20261014.CLEAN_STOCK_PRICES_V2;
-- Expected: row_count = 6, distinct_offsets = 6.