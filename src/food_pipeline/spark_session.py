import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark(app_name: str = "food-delivery-lakehouse") -> SparkSession:
    # Windows fix: make Spark's Python workers use this same venv Python
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    # Adds the Delta Lake JARs (downloaded from Maven the first time)
    return configure_spark_with_delta_pip(builder).getOrCreate()