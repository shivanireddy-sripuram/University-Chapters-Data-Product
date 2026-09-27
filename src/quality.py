from pyspark.sql import functions as F


INVALID_COORDINATES = "INVALID_COORDINATES"
MISSING_OR_UNKNOWN_CITY = "MISSING_OR_UNKNOWN_CITY"


def apply_quality_rules(df):
    """Apply row-level DQ rules."""

    longitude_as_double = F.expr("try_cast(longitude as double)")
    latitude_as_double = F.expr("try_cast(latitude as double)")

    invalid_coordinates = (
        longitude_as_double.isNull()
        | latitude_as_double.isNull()
        | ~longitude_as_double.between(-180.0, 180.0)
        | ~latitude_as_double.between(-90.0, 90.0)
    )

    missing_or_unknown_city = (
        F.col("city").isNull()
        | (F.trim(F.col("city")) == "")
        | (F.upper(F.trim(F.col("city"))) == "UNKNOWN")
    )

    evaluated_df = (
        df
        .withColumn("_longitude_as_double", longitude_as_double)
        .withColumn("_latitude_as_double", latitude_as_double)
        .withColumn("_invalid_coordinates", invalid_coordinates)
        .withColumn("_missing_or_unknown_city", missing_or_unknown_city)
    )

    quarantine_df = (
        evaluated_df
        .filter(F.col("_invalid_coordinates"))
        .withColumn(
            "quarantine_reason",
            F.lit(INVALID_COORDINATES),
        )
        .select(
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "source_object_id",
            "ingest_run_id",
            "quarantine_reason",
            "raw_payload",
        )
    )

    valid_df = (
        evaluated_df
        .filter(~F.col("_invalid_coordinates"))
        .withColumn("longitude", F.col("_longitude_as_double"))
        .withColumn("latitude", F.col("_latitude_as_double"))
        .withColumn(
            "dq_status",
            F.when(
                F.col("_missing_or_unknown_city"),
                F.lit("WARNING"),
            ).otherwise(F.lit("OK")),
        )
        .withColumn(
            "dq_warnings",
            F.when(
                F.col("_missing_or_unknown_city"),
                F.array(F.lit(MISSING_OR_UNKNOWN_CITY)),
            ).otherwise(F.array().cast("array<string>")),
        )
        .select(
            "chapter_id",
            "chapter_name",
            "city",
            "state",
            "longitude",
            "latitude",
            "source_object_id",
            "ingest_run_id",
            "dq_status",
            "dq_warnings",
        )
    )

    return valid_df, quarantine_df


def validate_batch(df):
    """Validate batch-level source expectations."""

    rows_in = df.count()

    if rows_in == 0:
        raise RuntimeError(
            "Batch quality check failed: source batch is empty."
        )

    ca_rows = df.filter(F.col("state") == "CA").count()

    if ca_rows == 0:
        raise RuntimeError(
            "Batch quality check failed: California returned zero records."
        )
