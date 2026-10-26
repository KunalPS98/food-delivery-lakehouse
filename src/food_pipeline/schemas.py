from pyspark.sql.types import StringType, StructField, StructType

# Positional names for the 14 raw CSV columns. Values stay raw; only the names are
# made Delta-safe (no spaces). The 14th column has no header in the source file.
RAW_COLUMNS = [
    "age", "gender", "marital_status", "occupation", "monthly_income",
    "educational_qualifications", "family_size", "customer_type",
    "latitude", "longitude", "pin_code", "output", "feedback", "raw_extra",
]

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])
