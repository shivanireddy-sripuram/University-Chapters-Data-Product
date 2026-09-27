import json
from datetime import datetime, timezone
from pathlib import Path


def write_bronze(payload, run_id, source_url):
    """Write the raw API payload and ingestion metadata to Bronze."""

    bronze_path = (
        Path("data")
        / "bronze"
        / "university_chapters"
        / run_id
    )

    bronze_path.mkdir(parents=True, exist_ok=True)

    payload_file = bronze_path / "payload.json"
    metadata_file = bronze_path / "metadata.json"

    with payload_file.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)

    metadata = {
        "ingest_run_id": run_id,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_url": source_url,
        "rows_received": len(payload.get("features", [])),
    }

    with metadata_file.open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)

    return payload_file