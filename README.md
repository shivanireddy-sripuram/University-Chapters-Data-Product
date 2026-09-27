# University Chapters Azure Medallion Data Product



A Spark-based medallion pipeline that retrieves active university chapter data for California, Oregon, and Washington from a public ArcGIS FeatureServer and publishes a versioned Gold data product.



## Architecture



The pipeline follows a medallion architecture:



ArcGIS FeatureServer -> Bronze -> Spark transformation and data quality -> Silver -> Gold v1



Records that fail hard data-quality rules are routed separately to Quarantine and never reach Gold.



Detailed engineering decisions are documented in docs/architecture.md.



The Gold data product contract is documented in docs/data_product_contract.md.



## Source



The pipeline queries the public ArcGIS UniversityChapters_Public FeatureServer.



The request filters upstream to CA, OR, and WA and requests geometry.



Source mappings:



- ChapterID -> chapter_id

- University_Chapter -> chapter_name

- City -> city

- State -> state

- geometry.x -> longitude

- geometry.y -> latitude



The public API requires no credentials or secrets.



## Repository Structure



src/ contains ingestion, Bronze, Silver, quality, Gold, storage, and pipeline code.



tests/ contains synthetic data-quality fixtures and automated tests.



docs/ contains the architecture decisions and Gold data product contract.



## Prerequisites



Tested with:



- Python 3.14.4

- Java 21

- PySpark 4.2.0



A Linux environment is recommended. WSL is recommended when running on Windows.



## Setup



From the repository root:



1\. Create a virtual environment with: python3 -m venv .venv

2\. Activate it with: source .venv/bin/activate

3\. Upgrade pip with: python -m pip install --upgrade pip

4\. Install dependencies with: python -m pip install -r requirements.txt

5\. Confirm Java is available with: java -version



## Run Tests



Run:



python -m pytest -v



The automated tests cover clean records, city warnings, invalid coordinates, non-numeric coordinates, quarantine routing, deterministic deduplication, state-level batch expectations, and exclusion of quarantined records from Gold.



Synthetic bad records are included so DQ behaviour remains reproducible even when the live API contains only clean records.



## Run the Pipeline



Run:



python src/pipeline.py



A successful execution:



1\. retrieves the ArcGIS source;

2\. preserves the raw response in Bronze;

3\. flattens and deduplicates records using Spark;

4\. applies row-level and batch-level DQ rules;

5\. writes valid records to Silver;

6\. writes hard DQ failures to Quarantine;

7\. publishes Gold v1; and

8\. logs pipeline metrics.



## Generated Data Layout



Bronze:

data/bronze/university_chapters/<run_id>/



Silver:

data/silver/university_chapters/run_id=<run_id>/



Quarantine:

data/quarantine/university_chapters/run_id=<run_id>/



Gold:

data/gold/university_chapters/v1/



The data/ directory is excluded from Git because it contains generated pipeline outputs.



## Data Quality



Invalid, missing, non-numeric, or out-of-range coordinates are quarantined with reason INVALID_COORDINATES.



A null, blank, or case-insensitive UNKNOWN city remains publishable with dq_status WARNING and warning MISSING_OR_UNKNOWN_CITY.



Clean records receive dq_status OK.



The pipeline fails if the complete source batch is empty or if California unexpectedly contains zero records. Oregon and Washington may legitimately contain zero records.



## Metrics



Each execution logs:



- rows_in

- rows_after_dedup

- rows_quarantined

- rows_warned

- rows_ok



## Gold Publishing Strategy



Gold v1 is published to the stable path data/gold/university_chapters/v1/.



Only records that pass hard DQ rules enter Gold. Warning-level records remain available with their DQ metadata.



Because the source is a small current-state dataset, Gold uses snapshot overwrite semantics. This prevents duplicate accumulation across repeated publications.



For a larger incremental production workload, Delta Lake MERGE keyed by chapter_id would be a natural alternative.



## Azure Production Mapping



The local layout intentionally mirrors an Azure medallion architecture.



A production implementation could use ADLS Gen2 for storage, Azure Databricks or Synapse Spark for processing, and Azure Data Factory for orchestration.



Those Azure services are deliberately not required to reproduce this assignment locally.

