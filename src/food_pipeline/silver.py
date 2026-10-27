"""Silver layer.

Cleaning and hashing are pure DataFrame -> DataFrame functions, one per data issue found in profiling.
`merge_into_silver` is the only function that writes.
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

OCCUPATION_FIXES = {
    "Self Employeed": "Self Employed",
    "House wife": "Homemaker",
}

# band label -> (income_band_order, income_min_inr, income_max_inr); None = no upper bound
INCOME_BANDS = {
    "No Income": (0, 0, 0),
    "Below Rs.10000": (1, 1, 10000),
    "10001 to 25000": (2, 10001, 25000),
    "25001 to 50000": (3, 25001, 50000),
    "More than 50000": (4, 50001, None),
}

COLUMN_TYPES = {
    "age": "int",
    "family_size": "int",
    "latitude": "double",
    "longitude": "double",
}

# The respondent's answers after cleaning. Lineage (_*) and derived income columns are excluded,
# so the same answers always give the same hash whichever batch or file they arrived in.
HASH_COLUMNS = [
    "age", "gender", "marital_status", "occupation", "monthly_income",
    "educational_qualifications", "family_size", "customer_type",
    "latitude", "longitude", "pin_code", "feedback", "ordered_again",
]
NULL_TOKEN = "<null>"


def trim_strings(df: DataFrame) -> DataFrame:
    """Remove leading/trailing whitespace from every string column."""
    return df.select(
        *[F.trim(F.col(f.name)).alias(f.name) if isinstance(f.dataType, StringType) else F.col(f.name)
          for f in df.schema.fields]
    )


def standardise_categories(df: DataFrame) -> DataFrame:
    """Fix misspelt or inconsistent occupation labels."""
    return df.replace(OCCUPATION_FIXES, subset=["occupation"])


def cast_types(df: DataFrame) -> DataFrame:
    """Cast numeric columns. pin_code stays a string because it is an identifier, not a quantity."""
    for column, dtype in COLUMN_TYPES.items():
        df = df.withColumn(column, F.col(column).cast(dtype))
    return df


def _income_lookup(position: int) -> F.Column:
    pairs = [x for band, values in INCOME_BANDS.items()
             for x in (F.lit(band), F.lit(values[position]).cast("int"))]
    return F.create_map(*pairs)[F.col("monthly_income")]


def map_income(df: DataFrame) -> DataFrame:
    """Add a sortable band number and numeric bounds for the text income bands (unknown band -> null)."""
    return (
        df.withColumn("income_band_order", _income_lookup(0))
          .withColumn("income_min_inr", _income_lookup(1))
          .withColumn("income_max_inr", _income_lookup(2))
    )


def clean_outcome(df: DataFrame) -> DataFrame:
    """Rename the ambiguous `output` column to `ordered_again` as a boolean (unknown value -> null)."""
    ordered_again = (F.when(F.col("output") == "Yes", True)
                      .when(F.col("output") == "No", False))
    return df.withColumn("ordered_again", ordered_again).drop("output")


def clean(df: DataFrame) -> DataFrame:
    """Apply every Silver cleaning step in order."""
    return (
        df.transform(trim_strings)
          .transform(standardise_categories)
          .transform(cast_types)
          .transform(map_income)
          .transform(clean_outcome)
    )


def add_record_hash(df: DataFrame) -> DataFrame:
    """Add `record_hash`: a SHA-256 fingerprint of the respondent's answers, used as the Silver key.

    Nulls become an explicit token so (null, "a") and ("a", null) can't produce the same hash.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_TOKEN)) for c in HASH_COLUMNS]
    return df.withColumn("record_hash", F.sha2(F.concat_ws("||", *parts), 256))


def dedupe(df: DataFrame) -> DataFrame:
    """Keep one row per record_hash. Run per batch, so the dropped copies have identical lineage too."""
    return df.dropDuplicates(["record_hash"])


def merge_into_silver(spark: SparkSession, df: DataFrame, target: str) -> None:
    """Insert rows whose record_hash isn't in Silver yet; existing hashes are left untouched (idempotent)."""
    if not DeltaTable.isDeltaTable(spark, target):
        df.write.format("delta").save(target)
        return
    (DeltaTable.forPath(spark, target).alias("t")
        .merge(df.alias("s"), "t.record_hash = s.record_hash")
        .whenNotMatchedInsertAll()
        .execute())
