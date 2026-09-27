from pathlib import Path


def write_silver(valid_df, run_id):
    """Write validated Silver records for the current pipeline run."""

    silver_path = (
        Path("data")
        / "silver"
        / "university_chapters"
        / f"run_id={run_id}"
    )

    (
        valid_df.write
        .mode("overwrite")
        .parquet(str(silver_path))
    )

    return silver_path


def write_quarantine(quarantine_df, run_id):
    """Write quarantined records for the current pipeline run."""

    quarantine_path = (
        Path("data")
        / "quarantine"
        / "university_chapters"
        / f"run_id={run_id}"
    )

    (
        quarantine_df.write
        .mode("overwrite")
        .parquet(str(quarantine_path))
    )

    return quarantine_path


def write_gold(gold_df):
    """
    Publish the current Gold v1 snapshot.

    Snapshot overwrite is intentional: the source is a small current-state
    dataset and this keeps repeated pipeline executions idempotent.
    """

    gold_path = (
        Path("data")
        / "gold"
        / "university_chapters"
        / "v1"
    )

    (
        gold_df.write
        .mode("overwrite")
        .parquet(str(gold_path))
    )

    return gold_path
