import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from food_pipeline.schemas import RAW_SCHEMA

logger = logging.getLogger(__name__)


def read_batch(spark: SparkSession, path: str, batch_id: str) -> DataFrame:
    """Read one landing CSV with the raw schema and add lineage columns."""
    return (
        spark.read.csv(path, header=True, schema=RAW_SCHEMA)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def batch_already_loaded(spark: SparkSession, target: str, batch_id: str, source_file: str) -> bool:
    """True if Bronze already holds rows from this batch id or from this source file."""
    if not DeltaTable.isDeltaTable(spark, target):
        return False
    existing = spark.read.format("delta").load(target)
    match = (F.col("_batch_id") == batch_id) | (F.col("_source_file") == source_file)
    return existing.filter(match).limit(1).count() > 0


def ingest_batch(spark: SparkSession, path: str, batch_id: str, target: str) -> int:
    """Append one batch to the Bronze Delta table and return the number of rows written.

    Skips (and returns 0) if the batch id or source file was already ingested, so reruns
    and retries never duplicate data.
    """
    df = read_batch(spark, path, batch_id)
    first = df.select("_source_file").first()
    if first is None:
        logger.warning("%s: %s has no data rows, skipping", batch_id, path)
        return 0

    source_file = first["_source_file"]
    if batch_already_loaded(spark, target, batch_id, source_file):
        logger.warning("%s: already loaded (same batch id or source file), skipping", batch_id)
        return 0

    row_count = df.count()
    df.write.format("delta").mode("append").save(target)
    logger.info("%s: appended %d rows to %s", batch_id, row_count, target)
    return row_count
