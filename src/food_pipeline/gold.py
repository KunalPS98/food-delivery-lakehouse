"""Gold layer: a small star schema (dim_location + fact_survey_response) and business aggregates.

Gold is rebuilt in full from Silver on every run (overwrite), which keeps it simple and idempotent.
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# (label, min_age, max_age); None = no upper bound
AGE_BANDS = [("18-22", 18, 22), ("23-26", 23, 26), ("27-30", 27, 30), ("31+", 31, None)]

# segments for agg_order_rate_by_segment: (segment_type column, optional column that gives the sort order)
SEGMENTS = [
    ("occupation", None),
    ("monthly_income", "income_band_order"),
    ("customer_type", None),
    ("age_band", None),
    ("gender", None),
    ("marital_status", None),
    ("educational_qualifications", None),
]

SMALL_SAMPLE = 20  # rates from fewer respondents than this are flagged as unreliable


def build_dim_location(silver: DataFrame) -> DataFrame:
    """One row per pin code. location_key is a hash of pin_code, so it is stable across rebuilds."""
    return (
        silver.select("pin_code", "latitude", "longitude").distinct()
              .withColumn("location_key", F.xxhash64("pin_code"))
              .select("location_key", "pin_code", "latitude", "longitude")
    )


def add_age_band(df: DataFrame) -> DataFrame:
    """Group age into the bands in AGE_BANDS."""
    band = F.lit(None).cast("string")
    for label, low, high in reversed(AGE_BANDS):
        in_band = F.col("age") >= low if high is None else F.col("age").between(low, high)
        band = F.when(in_band, label).otherwise(band)
    return df.withColumn("age_band", band)


def build_fact_survey_response(silver: DataFrame, dim_location: DataFrame) -> DataFrame:
    """One row per respondent (record_hash). Location attributes live in dim_location, joined by location_key."""
    return (
        silver.transform(add_age_band)
              .join(dim_location.select("location_key", "pin_code"), on="pin_code", how="left")
              .select(
                  "record_hash", "location_key",
                  "age", "age_band", "gender", "marital_status", "occupation",
                  "monthly_income", "income_band_order", "income_min_inr", "income_max_inr",
                  "educational_qualifications", "family_size", "customer_type",
                  "feedback", "ordered_again", "_batch_id",
              )
    )


def _metrics() -> list:
    return [
        F.count("*").alias("respondents"),
        F.sum(F.col("ordered_again").cast("int")).alias("ordered_again_yes"),
        F.round(F.avg(F.col("ordered_again").cast("int")), 3).alias("order_rate"),
        F.round(F.avg((F.col("feedback") == "Positive").cast("int")), 3).alias("positive_feedback_rate"),
    ]


def _rate_by(fact: DataFrame, column: str, order_column: str | None) -> DataFrame:
    sort_key = F.col(order_column) if order_column else F.lit(None).cast("int")
    return (
        fact.groupBy(F.col(column).cast("string").alias("segment_value"), sort_key.alias("segment_order"))
            .agg(*_metrics())
            .withColumn("segment_type", F.lit(column))
    )


def build_agg_order_rate_by_segment(fact: DataFrame) -> DataFrame:
    """Long format: one row per (segment_type, segment_value), so one table serves many charts."""
    per_segment = [_rate_by(fact, column, order_column) for column, order_column in SEGMENTS]
    return (
        reduce(DataFrame.unionByName, per_segment)
        .withColumn("is_small_sample", F.col("respondents") < SMALL_SAMPLE)
        .select("segment_type", "segment_value", "segment_order", "respondents", "ordered_again_yes",
                "order_rate", "positive_feedback_rate", "is_small_sample")
    )


def build_agg_area_summary(fact: DataFrame, dim_location: DataFrame) -> DataFrame:
    """The same metrics per pin code, with coordinates for mapping."""
    return (
        fact.groupBy("location_key").agg(*_metrics())
            .join(dim_location, on="location_key", how="inner")
            .withColumn("is_small_sample", F.col("respondents") < SMALL_SAMPLE)
            .select("location_key", "pin_code", "latitude", "longitude", "respondents", "ordered_again_yes",
                    "order_rate", "positive_feedback_rate", "is_small_sample")
    )


def write_gold(df: DataFrame, target: str) -> None:
    """Full rebuild: replace the table's contents (and schema) with the latest result."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)
