"""Silver cleaning: pure DataFrame -> DataFrame functions, one per data issue found in profiling."""
from pyspark.sql import DataFrame
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
