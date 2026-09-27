import json
from pathlib import Path


def write_bronze(payload, run_id):
    """Write the raw API payload to the Bronze layer."""

    bronze_path = (
        Path("data")
        / "bronze"
        / "university_chapters"
        / run_id
    )

    bronze_path.mkdir(parents=True, exist_ok=True)

    output_file = bronze_path / "payload.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)

    return output_file