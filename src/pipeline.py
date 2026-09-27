from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

from bronze import write_bronze
from gold import build_gold
from ingest import API_URL, fetch_university_chapters
from quality import apply_quality_rules, validate_batch
from silver import (
    create_spark_session,
    deduplicate_chapters,
    flatten_bronze,
    inspect_bronze,
)
from storage import (
    write_gold,
    write_quarantine,
    write_silver,
)


def generate_run_id():
    """Generate a unique identifier for each pipeline execution."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    unique_suffix = uuid4().hex[:8]
    return f"{timestamp}_{unique_suffix}"


def main():
    run_id = generate_run_id()
    print(f"Starting pipeline run: {run_id}")

    payload = fetch_university_chapters()
    features = payload.get("features", [])

    if not features:
        raise RuntimeError(
            "Source API returned zero university chapter records."
        )

    print(f"Rows received from source: {len(features)}")

    bronze_file = write_bronze(
        payload,
        run_id,
        API_URL,
    )

    print(f"Bronze payload written to: {bronze_file}")
    print("Bronze ingestion completed successfully.")

    spark = create_spark_session()

    try:
        bronze_df = inspect_bronze(
            spark,
            bronze_file,
        )

        flattened_df = flatten_bronze(
            bronze_df,
            run_id,
        )

        validate_batch(flattened_df)

        deduplicated_df = deduplicate_chapters(
            flattened_df
        )

        valid_df, quarantine_df = apply_quality_rules(
            deduplicated_df
        )

        rows_in = flattened_df.count()
        rows_after_dedup = deduplicated_df.count()
        rows_quarantined = quarantine_df.count()

        rows_warned = (
            valid_df
            .filter(F.col("dq_status") == "WARNING")
            .count()
        )

        rows_ok = (
            valid_df
            .filter(F.col("dq_status") == "OK")
            .count()
        )

        silver_path = write_silver(
            valid_df,
            run_id,
        )

        quarantine_path = write_quarantine(
            quarantine_df,
            run_id,
        )

        gold_df = build_gold(valid_df)

        gold_path = write_gold(gold_df)

        print(f"Silver data written to: {silver_path}")
        print(f"Quarantine data written to: {quarantine_path}")
        print(f"Gold data product written to: {gold_path}")

        print("Pipeline metrics:")
        print(f"  rows_in={rows_in}")
        print(f"  rows_after_dedup={rows_after_dedup}")
        print(f"  rows_quarantined={rows_quarantined}")
        print(f"  rows_warned={rows_warned}")
        print(f"  rows_ok={rows_ok}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
