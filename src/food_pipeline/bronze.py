from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from food_pipeline.schemas import RAW_SCHEMA


def read_batch(spark: SparkSession, path: str, batch_id: str) -> DataFrame:
    """Read one landing CSV with the raw schema and add lineage columns."""
    return (
        spark.read.csv(path, header=True, schema=RAW_SCHEMA)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_batch(spark: SparkSession, path: str, batch_id: str, target: str) -> int:
    """Append one batch to the Bronze Delta table and return the number of rows written."""
    df = read_batch(spark, path, batch_id)
    row_count = df.count()
    df.write.format("delta").mode("append").save(target)
    return row_count
