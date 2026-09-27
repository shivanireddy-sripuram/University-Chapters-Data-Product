import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

from quality import apply_quality_rules, validate_batch
from silver import deduplicate_chapters, flatten_bronze


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("UniversityChaptersTests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def load_fixture(spark):
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

    return flatten_bronze(
        bronze_df,
        "synthetic-test-run",
    )


def test_quality_rules_route_records_correctly(spark):
    flattened_df = load_fixture(spark)

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
    assert len(quarantine_rows) == 2

    assert valid_rows["TEST-001"]["dq_status"] == "OK"
    assert valid_rows["TEST-001"]["dq_warnings"] == []

    assert valid_rows["TEST-002"]["dq_status"] == "WARNING"
    assert valid_rows["TEST-002"]["dq_warnings"] == [
        "MISSING_OR_UNKNOWN_CITY"
    ]

    assert "TEST-003" not in valid_rows
    assert "TEST-004" not in valid_rows

    assert (
        quarantine_rows["TEST-003"]["quarantine_reason"]
        == "INVALID_COORDINATES"
    )

    assert (
        quarantine_rows["TEST-004"]["quarantine_reason"]
        == "INVALID_COORDINATES"
    )

    assert quarantine_rows["TEST-003"]["raw_payload"] is not None
    assert quarantine_rows["TEST-004"]["raw_payload"] is not None


def test_deduplication_keeps_highest_source_object_id(spark):
    rows = [
        (
            "DUP-001", "Older University", "Seattle", "WA",
            -122.33, 47.60, 100, "test-run", "{}",
        ),
        (
            "DUP-001", "Newer University", "Seattle", "WA",
            -122.33, 47.60, 200, "test-run", "{}",
        ),
    ]

    columns = [
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "source_object_id",
        "ingest_run_id",
        "raw_payload",
    ]

    df = spark.createDataFrame(rows, columns)
    result = deduplicate_chapters(df).collect()

    assert len(result) == 1
    assert result[0]["source_object_id"] == 200


def test_batch_validation_allows_zero_or_and_wa(spark):
    rows = [
        (
            "CA-001", "California University", "Los Angeles", "CA",
            -118.2437, 34.0522, 1, "test-run", "{}",
        ),
    ]

    columns = [
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "source_object_id",
        "ingest_run_id",
        "raw_payload",
    ]

    df = spark.createDataFrame(rows, columns)

    validate_batch(df)


def test_batch_validation_fails_when_ca_is_zero(spark):
    rows = [
        (
            "OR-001", "Oregon University", "Portland", "OR",
            -122.6765, 45.5231, 1, "test-run", "{}",
        ),
    ]

    columns = [
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "source_object_id",
        "ingest_run_id",
        "raw_payload",
    ]

    df = spark.createDataFrame(rows, columns)

    with pytest.raises(
        RuntimeError,
        match="California returned zero records",
    ):
        validate_batch(df)
