from datetime import datetime, timezone
from uuid import uuid4

from ingest import API_URL, fetch_university_chapters
from bronze import write_bronze


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


if __name__ == "__main__":
    main()