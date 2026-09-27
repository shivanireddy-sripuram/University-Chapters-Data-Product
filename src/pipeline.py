from datetime import datetime, timezone
from uuid import uuid4

from bronze import write_bronze
from ingest import API_URL, fetch_university_chapters
from quality import apply_quality_rules
from silver import (
    create_spark_session,
    flatten_bronze,
    inspect_bronze,
)
from storage import (
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

        valid_df, quarantine_df = apply_quality_rules(
            flattened_df
        )

        silver_path = write_silver(
            valid_df,
            run_id,
        )

        quarantine_path = write_quarantine(
            quarantine_df,
            run_id,
        )

        print(f"Silver data written to: {silver_path}")
        print(f"Quarantine data written to: {quarantine_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()