"""Data quality rules for Silver, and the split of rows into valid vs quarantine."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Bengaluru bounding box (the data sits at 12.87–13.10 N, 77.48–77.76 E)
LAT_BOUNDS = (12.7, 13.3)
LON_BOUNDS = (77.3, 77.9)

ALLOWED = {
    "gender": ["Male", "Female"],
    "marital_status": ["Single", "Married", "Prefer not to say"],
    "occupation": ["Student", "Employee", "Self Employed", "Homemaker"],
    "customer_type": ["New", "Regular", "Frequent"],
    "feedback": ["Positive", "Negative"],
}


def dq_rules() -> list[tuple[str, Column]]:
    """(rule_name, condition that must be TRUE for a valid row).

    Built in a function, not at import time, because Spark columns need an active SparkSession.
    """
    rules = [
        ("age_18_to_80", F.col("age").between(18, 80)),
        ("family_size_1_to_15", F.col("family_size").between(1, 15)),
        ("pin_code_bengaluru", F.col("pin_code").rlike(r"^560\d{3}$")),
        ("latitude_in_bengaluru", F.col("latitude").between(*LAT_BOUNDS)),
        ("longitude_in_bengaluru", F.col("longitude").between(*LON_BOUNDS)),
        ("income_band_known", F.col("income_band_order").isNotNull()),
        ("ordered_again_known", F.col("ordered_again").isNotNull()),
        ("raw_extra_matches_output", (F.col("raw_extra") == "Yes") == F.col("ordered_again")),
    ]
    rules += [(f"{column}_allowed", F.col(column).isin(values)) for column, values in ALLOWED.items()]
    return rules


def add_failed_rules(df: DataFrame) -> DataFrame:
    """Add `failed_rules`: an array with the name of every rule the row breaks (empty = valid).

    A condition that evaluates to NULL (e.g. age is null) counts as a failure, not a pass.
    """
    failures = [F.when(~F.coalesce(condition, F.lit(False)), F.lit(name)) for name, condition in dq_rules()]
    return df.withColumn("failed_rules", F.array_compact(F.array(*failures)))


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows without DQ columns, quarantined rows with failed_rules and a check timestamp)."""
    checked = add_failed_rules(df)
    valid = checked.filter(F.size("failed_rules") == 0).drop("failed_rules", "raw_extra")
    quarantine = (checked.filter(F.size("failed_rules") > 0)
                         .withColumn("_dq_checked_ts", F.current_timestamp()))
    return valid, quarantine


def failure_counts(df: DataFrame) -> dict[str, int]:
    """Number of rows failing each rule (a row can fail several), including rules with 0 failures."""
    checked = add_failed_rules(df)
    names = [name for name, _ in dq_rules()]
    row = checked.select(
        *[F.sum(F.array_contains("failed_rules", name).cast("int")).alias(name) for name in names]
    ).first()
    return {name: row[name] or 0 for name in names}


def write_quarantine(quarantine: DataFrame, target: str, batch_ids: list[str]) -> None:
    """Replace the quarantine rows for these batches, so reprocessing a batch never duplicates them."""
    batch_list = ", ".join(f"'{b}'" for b in batch_ids)
    (quarantine.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_id IN ({batch_list})")
        .save(target))
