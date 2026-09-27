import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"

sys.path.insert(0, str(SRC_PATH))

from quality import apply_quality_rules
from silver import flatten_bronze


@pytest.fixture(scope="session")
def spark():
    """Create one Spark session for the test suite."""

    spark_session = (
        SparkSession.builder
        .appName("UniversityChaptersTests")
        .master("local[*]")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_quality_rules_route_records_correctly(spark):
    """Verify OK, WARNING, and quarantine DQ paths."""

    fixture_file = (
        PROJECT_ROOT
        / "tests"
        / "fixtures"
        / "university_chapters_dq_fixture.json"
    )

    bronze_df = (
        spark.read
        .option("multiline", "true")
        .json(str(fixture_file))
    )

    flattened_df = flatten_bronze(
        bronze_df,
        "synthetic-test-run",
    )

    valid_df, quarantine_df = apply_quality_rules(
        flattened_df
    )

    valid_rows = {
        row["chapter_id"]: row
        for row in valid_df.collect()
    }

    quarantine_rows = {
        row["chapter_id"]: row
        for row in quarantine_df.collect()
    }

    assert len(valid_rows) == 2
    assert len(quarantine_rows) == 1

    assert valid_rows["TEST-001"]["dq_status"] == "OK"
    assert valid_rows["TEST-001"]["dq_warnings"] == []

    assert valid_rows["TEST-002"]["dq_status"] == "WARNING"
    assert valid_rows["TEST-002"]["dq_warnings"] == [
        "MISSING_OR_UNKNOWN_CITY"
    ]

    assert "TEST-003" not in valid_rows

    assert (
        quarantine_rows["TEST-003"]["quarantine_reason"]
        == "INVALID_COORDINATES"
    )

    assert (
        quarantine_rows["TEST-003"]["ingest_run_id"]
        == "synthetic-test-run"
    )

    assert quarantine_rows["TEST-003"]["raw_payload"] is not None