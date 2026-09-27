from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    """Create a local Spark session for the pipeline."""

    return (
        SparkSession.builder
        .appName("UniversityChaptersDataProduct")
        .master("local[*]")
        .getOrCreate()
    )


def inspect_bronze(spark, bronze_file):
    """Read a Bronze payload and display its Spark schema."""

    bronze_df = (
        spark.read
        .option("multiline", "true")
        .json(str(bronze_file))
    )

    bronze_df.printSchema()

    return bronze_df


def flatten_bronze(bronze_df):
    """Flatten ArcGIS features into one row per university chapter."""

    features_df = bronze_df.select(
        F.explode("features").alias("feature")
    )

    flattened_df = features_df.select(
        F.col("feature.attributes.ChapterID").alias("chapter_id"),
        F.col("feature.attributes.University_Chapter").alias("chapter_name"),
        F.col("feature.attributes.City").alias("city"),
        F.col("feature.attributes.State").alias("state"),
        F.col("feature.geometry.x").alias("longitude"),
        F.col("feature.geometry.y").alias("latitude"),
        F.col("feature.attributes.OBJECTID").alias("source_object_id"),
    )

    return flattened_df