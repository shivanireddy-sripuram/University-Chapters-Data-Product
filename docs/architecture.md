# Architecture and Engineering Decisions



## Overview



The solution implements a thin medallion data pipeline:



ArcGIS FeatureServer -> Bronze -> Spark Transformation and DQ -> Silver -> Gold v1



Records that fail hard data-quality rules are physically separated into Quarantine and never enter Gold.



## Bronze



Bronze preserves the raw or near-raw ArcGIS API response and ingestion metadata for every pipeline execution.



Each execution receives a unique run ID. This retains source history and provides traceability.



Business transformations are intentionally excluded from Bronze.



## Silver



Spark flattens the nested ArcGIS feature structure into one row per university chapter.



Source fields are mapped as follows:



- ChapterID -> chapter_id

- University_Chapter -> chapter_name

- City -> city

- State -> state

- geometry.x -> longitude

- geometry.y -> latitude



Silver performs deterministic deduplication by chapter_id.



If duplicate chapter IDs occur, the row with the highest source OBJECTID is retained as a deterministic technical tie-breaker.



Parquet is used for curated outputs because it is typed, compressed, columnar, and Spark-native.



## Data Quality and Quarantine



Coordinate validation is a hard data-quality rule.



A record is quarantined when longitude or latitude is:



- missing or null;

- non-numeric;

- longitude outside \[-180, 180]; or

- latitude outside \[-90, 90].



These records receive quarantine reason INVALID_COORDINATES.



Quarantine is physically separated from Silver and Gold. It retains the ingestion run ID and raw source context for investigation.



City quality is a warning-level rule.



A null, blank, or case-insensitive UNKNOWN city receives warning MISSING_OR_UNKNOWN_CITY.



Warning records remain eligible for Gold and are labelled with dq_status WARNING.



Clean records receive dq_status OK.



## Batch-Level Quality



The complete source batch must not be empty.



California is expected to contain at least one record. An unexpected zero-row California population fails the pipeline.



Oregon and Washington are explicitly allowed to contain zero records.



This prevents legitimate zero counts for OR or WA from being incorrectly treated as pipeline failures.



## Gold as a Data Product



Gold is treated as a deliberate data product rather than simply another transformation layer.



Gold v1 has:



- a stable interface;

- an explicit schema and grain;

- ownership;

- quality semantics;

- freshness expectations;

- classification; and

- versioning rules.



Only records that pass hard DQ rules are eligible for Gold.



Warning-level records remain visible through dq_status and dq_warnings.



The complete contract is documented in docs/data_product_contract.md.



## Failure Behaviour



The pipeline fails loudly when:



- the HTTP source request fails;

- the complete source batch is empty; or

- California unexpectedly contains zero records.



Individual records with invalid coordinates are quarantined rather than failing the complete batch.



## Idempotency



Bronze retains historical executions using unique run IDs.



Silver and Quarantine outputs are isolated by run ID.



Gold represents the current v1 snapshot and uses overwrite semantics.



Repeated successful publication therefore replaces the current Gold snapshot rather than accumulating duplicate Gold records.



For a larger incremental workload, Delta Lake MERGE keyed by chapter_id would be a reasonable alternative.



Snapshot overwrite was selected because this source is a small current-state dataset and introducing Delta Lake solely for this assignment would add unnecessary complexity.



## Azure Mapping



The implementation runs locally so it can be reproduced without Azure credentials.



The local architecture maps naturally to Azure:



- ADLS Gen2 for Bronze, Silver, Quarantine, and Gold storage;

- Azure Databricks or Synapse Spark for Spark processing; and

- Azure Data Factory for production scheduling and orchestration.



The Spark transformation logic would require minimal change when moved to a managed Azure Spark environment.



## Scope and Trade-offs



The solution intentionally does not introduce:



- infrastructure-as-code;

- CI/CD pipelines;

- production scheduling;

- external catalogs;

- secret-management infrastructure; or

- SCD Type 2 processing.



These capabilities could be appropriate in a larger production platform, but they would add complexity beyond the requested thin medallion data product.



The implementation instead prioritizes reproducibility, clear medallion boundaries, explicit DQ behaviour, failure visibility, and a well-defined Gold contract.

