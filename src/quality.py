from pyspark.sql import functions as F


INVALID_COORDINATES = "INVALID_COORDINATES"
MISSING_OR_UNKNOWN_CITY = "MISSING_OR_UNKNOWN_CITY"


def apply_quality_rules(df):
    """
    Apply data-quality rules and separate valid records
    from records that must be quarantined.
    """

    longitude_as_double = F.col("longitude").cast("double")
    latitude_as_double = F.col("latitude").cast("double")

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
        .withColumn(
            "_invalid_coordinates",
            invalid_coordinates,
        )
        .withColumn(
            "_missing_or_unknown_city",
            missing_or_unknown_city,
        )
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
        .withColumn(
            "longitude",
            longitude_as_double,
        )
        .withColumn(
            "latitude",
            latitude_as_double,
        )
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