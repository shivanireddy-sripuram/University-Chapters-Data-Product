import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

from gold import build_gold
from quality import apply_quality_rules
from silver import flatten_bronze


@pytest.fixture(scope="module")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("UniversityChaptersGoldTests")
        .master("local[*]")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_gold_contains_ok_and_warning_but_not_quarantine(spark):
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
        "gold-test-run",
    )

    valid_df, quarantine_df = apply_quality_rules(
        flattened_df
    )

    gold_df = build_gold(valid_df)

    gold_rows = {
        row["chapter_id"]: row
        for row in gold_df.collect()
    }

    quarantined_ids = {
        row["chapter_id"]
        for row in quarantine_df.collect()
    }

    assert "TEST-001" in gold_rows
    assert gold_rows["TEST-001"]["dq_status"] == "OK"

    assert "TEST-002" in gold_rows
    assert gold_rows["TEST-002"]["dq_status"] == "WARNING"

    assert "TEST-003" in quarantined_ids
    assert "TEST-004" in quarantined_ids

    assert "TEST-003" not in gold_rows
    assert "TEST-004" not in gold_rows
