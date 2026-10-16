# Clean v2 manual cross-engine final-mart comparison

Comparison recorded: 2026-10-16  
Event date compared: 2026-10-05  
Engines: Athena, Snowflake, and Databricks

## Purpose

Record a manual comparison of five supplied final-mart rows from the
Clean v2 replays in Athena, Snowflake, and Databricks.

This comparison uses displayed or copied query results. It is not an
automated parity test and does not establish continuous ingestion,
scheduled execution, or full-dataset equivalence.

## Compared columns

- event_date
- symbol
- source
- event_count
- average_price
- minimum_price
- maximum_price
- latest_event_time

All supplied rows have event date `2026-10-05` and source
`simulated-producer`.

The comparison aligns rows by event date, symbol, and source.

## Event counts and price ranges

The following values matched numerically across all three engines
for the five supplied rows.

| Symbol | Event count | Minimum price | Maximum price |
| --- | ---: | ---: | ---: |
| AAPL | 1094 | 224.41 | 300.90 |
| AMZN | 1079 | 44.26 | 84.76 |
| MSFT | 1076 | 317.32 | 349.99 |
| NVDA | 1058 | 158.62 | 187.62 |
| TSLA | 1122 | 140.98 | 197.57 |

The five supplied mart rows represent 5,429 events per engine,
calculated by summing `event_count`.

## Average-price comparison

Values below preserve the digits supplied for each engine.
The Databricks values are the displayed values, not a confirmed
full-precision export.

| Symbol | Athena | Snowflake | Databricks displayed |
| --- | ---: | ---: | ---: |
| AAPL | 257.7801645338206 | 257.7801645338 | 257.7801645 |
| AMZN | 65.67534754402226 | 65.6753475440 | 65.67534754 |
| MSFT | 336.0375557620821 | 336.0375557621 | 336.0375558 |
| NVDA | 174.2340548204157 | 174.2340548204 | 174.2340548 |
| TSLA | 174.44333333333338 | 174.4433333333 | 174.4433333 |

Each supplied Athena average rounds to the corresponding Snowflake
value and to the displayed Databricks value.

This supports agreement at the displayed comparison precision.
It does not establish exact equality of underlying stored values.

## Latest-event timestamp comparison

The following date and clock values matched through milliseconds
across all three supplied outputs.

| Symbol | Latest event date and clock time |
| --- | --- |
| AAPL | 2026-10-05 16:23:52.431 |
| AMZN | 2026-10-05 16:23:48.428 |
| MSFT | 2026-10-05 16:23:30.409 |
| NVDA | 2026-10-05 16:23:40.420 |
| TSLA | 2026-10-05 16:23:46.426 |

Databricks displayed an explicit `Z` suffix. The supplied Athena
and Snowflake outputs did not display a timezone suffix.

Timestamp types and timezone interpretation were not independently
verified. Matching clock values alone do not establish that all
engines represent the same instant.

## Findings

For the five supplied rows:

- Event date, symbol, and source matched.
- Event counts matched exactly.
- Minimum and maximum prices matched numerically.
- Average prices agreed at the displayed comparison precision.
- Latest-event date and clock values matched through milliseconds,
  subject to the timezone limitation above.

## Evidence basis and limitations

This record was transcribed from user-supplied query outputs,
including a Databricks result screenshot and copied Athena and
Snowflake results.

- The comparison was manual, not automated.
- Only the five supplied rows were compared.
- Full result-set completeness was not independently verified.
- Original query text and model definitions were not reviewed as
  part of this comparison.
- Full-precision numeric equality was not established.
- Timestamp timezone semantics were not independently verified.
- Source snapshot identity was not independently verified.
- The original result exports are not attached to this document.

## Follow-up validation

To strengthen this evidence:

1. Record the exact model names and SQL used in each engine.
2. Confirm that all engines used the same source snapshot and filters.
3. Preserve full-precision result exports.
4. Verify timestamp types and normalize timezone interpretation.
5. Define numeric comparison tolerances.
6. Automate key-based comparisons and report missing or differing rows.
