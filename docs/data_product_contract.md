# University Chapters Gold Data Product Contract

## Product

**Name:** University Chapters Gold v1

**Owner:** Data Engineering

**Purpose:** Provide a clean, stable and quality-labelled dataset of active university chapters in California, Oregon and Washington.

## Intended Use Cases

The product is intended for analytics, reporting, geographic analysis and downstream data consumption requiring a trusted list of active university chapters.

## Interface

Local development path:

`data/gold/university_chapters/v1/`

Format: Apache Parquet.

The local path intentionally mirrors an ADLS-style medallion layout. In Azure, the same logical product can be published to an ADLS Gen2 Gold container and consumed through Databricks, Synapse, Fabric or another Spark-compatible engine.

## Grain

One row represents one university chapter identified by `chapter_id`.

## Schema

| Column | Type | Description |
|---|---|---|
| chapter_id | string | University chapter business identifier |
| chapter_name | string | University/chapter name |
| city | string | Chapter city |
| state | string | US state code |
| longitude | double | WGS84 longitude |
| latitude | double | WGS84 latitude |
| dq_status | string | `OK` or `WARNING` |
| dq_warnings | array<string> | Data-quality warning codes |

## Freshness SLA

The Gold product is expected to be refreshed daily when operated on a production schedule.

A successful refresh should publish the current source snapshot to the stable v1 Gold interface.

Scheduling infrastructure is intentionally outside the scope of this assignment.

## Data Quality Contract

### DQ-Q1 — Invalid Coordinates

A record is quarantined when longitude or latitude is:

- missing or null;
- non-numeric;
- longitude outside `[-180, 180]`; or
- latitude outside `[-90, 90]`.

Quarantine reason:

`INVALID_COORDINATES`

Quarantined records do not enter Gold.

### DQ-W1 — Missing or Unknown City

A record receives a warning when city is:

- null;
- blank; or
- `UNKNOWN`, case-insensitive.

The record remains publishable.

Gold values:

- `dq_status = WARNING`
- `dq_warnings = [MISSING_OR_UNKNOWN_CITY]`

Rows without warnings have:

- `dq_status = OK`
- an empty `dq_warnings` array.

## Batch-Level Expectations

Oregon and Washington may legitimately return zero rows.

California is expected to return at least one row. A zero-row California population fails the pipeline.

A completely empty source batch also fails the pipeline rather than publishing an empty Gold product.

## Deduplication

Silver retains one record per `chapter_id`.

If duplicate chapter IDs occur, the row with the highest source `OBJECTID` is retained as a deterministic technical tie-breaker.

## Versioning

This contract defines **v1**.

The stable interface is:

`gold/university_chapters/v1/`

Backward-compatible implementation changes can remain within v1.

Breaking schema or semantic changes require a new product version such as `v2`.

## Classification

**Public / No PII**

The product contains public university chapter and geographic information and is not intended to contain personal information.

## Publish Semantics

Gold is a small current-state snapshot and is published using overwrite semantics.

This makes repeated successful executions idempotent at the Gold interface while Bronze retains run-level source history.

For a larger incremental production workload, Delta Lake with `MERGE` keyed by `chapter_id` would be a reasonable alternative.
