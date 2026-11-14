"""Run the whole pipeline: landing CSVs -> Bronze -> Silver (+ quarantine) -> Gold.

    python -m food_pipeline.run_pipeline [--config path/to/pipeline.yaml]

Every stage is idempotent, so running it again changes nothing unless new landing files arrived.
"""
import argparse
import logging
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from food_pipeline import bronze, gold, quality, silver
from food_pipeline.config import DEFAULT_CONFIG, PipelineConfig, load_config
from food_pipeline.spark_session import get_spark

logger = logging.getLogger("food_pipeline.run_pipeline")


class DataQualityError(RuntimeError):
    """A batch quarantined more rows than the configured threshold allows."""


def discover_batches(landing: str) -> list[tuple[str, str]]:
    """(batch_id, path) for every CSV in the landing folder, in file-name order. batch_id = file name stem."""
    return [(p.stem, p.as_posix()) for p in sorted(Path(landing).glob("*.csv"))]


def run_bronze(spark: SparkSession, cfg: PipelineConfig) -> None:
    batches = discover_batches(cfg.landing)
    if not batches:
        raise FileNotFoundError(f"no CSV files in landing folder {cfg.landing}")
    for batch_id, path in batches:
        bronze.ingest_batch(spark, path, batch_id, cfg.bronze)


def _count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count() if DeltaTable.isDeltaTable(spark, path) else 0


def load_batch_to_silver(spark: SparkSession, bronze_df: DataFrame, batch_id: str, cfg: PipelineConfig) -> dict:
    """Clean one Bronze batch, quarantine rows failing DQ rules, dedupe and MERGE the rest into Silver."""
    batch = bronze_df.filter(F.col("_batch_id") == batch_id)
    valid, quarantined = quality.split_valid(silver.clean(batch))
    unique = silver.dedupe(silver.add_record_hash(valid))

    n_batch, n_quarantined = batch.count(), quarantined.count()
    if n_batch and n_quarantined / n_batch > cfg.max_quarantine_ratio:
        raise DataQualityError(
            f"{batch_id}: {n_quarantined} of {n_batch} rows failed DQ rules "
            f"(limit {cfg.max_quarantine_ratio:.0%}); Silver was not updated")
    quality.write_quarantine(quarantined, cfg.quarantine, [batch_id])

    before = _count(spark, cfg.silver)
    silver.merge_into_silver(spark, unique, cfg.silver)
    after = _count(spark, cfg.silver)

    n_valid, n_unique = valid.count(), unique.count()
    stats = {"batch": batch_id, "bronze_rows": n_batch, "quarantined": n_quarantined,
             "dupes_in_batch": n_valid - n_unique, "already_in_silver": n_unique - (after - before),
             "inserted": after - before, "silver_total": after}
    logger.info("silver %s", stats)
    return stats


def run_silver(spark: SparkSession, cfg: PipelineConfig) -> list[dict]:
    bronze_df = spark.read.format("delta").load(cfg.bronze)
    batch_ids = sorted(r["_batch_id"] for r in bronze_df.select("_batch_id").distinct().collect())
    return [load_batch_to_silver(spark, bronze_df, b, cfg) for b in batch_ids]


def run_gold(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int]:
    silver_df = spark.read.format("delta").load(cfg.silver)
    dim_location = gold.build_dim_location(silver_df)
    fact = gold.build_fact_survey_response(silver_df, dim_location)
    tables = {
        "dim_location": dim_location,
        "fact_survey_response": fact,
        "agg_order_rate_by_segment": gold.build_agg_order_rate_by_segment(fact),
        "agg_area_summary": gold.build_agg_area_summary(fact, dim_location),
    }
    counts = {}
    for name, df in tables.items():
        path = f"{cfg.gold}/{name}"
        gold.write_gold(df, path)
        counts[name] = _count(spark, path)
        logger.info("gold %s: %d rows", name, counts[name])
    return counts


def run(cfg: PipelineConfig) -> None:
    spark = get_spark(cfg.app_name, cfg.master, cfg.shuffle_partitions)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        logger.info("bronze: loading new files from %s", cfg.landing)
        run_bronze(spark, cfg)
        logger.info("bronze: %d rows", _count(spark, cfg.bronze))
        run_silver(spark, cfg)
        run_gold(spark, cfg)
        logger.info("done: bronze %d, quarantine %d, silver %d rows",
                    _count(spark, cfg.bronze), _count(spark, cfg.quarantine), _count(spark, cfg.silver))
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the food delivery lakehouse pipeline.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="path to pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run(load_config(args.config))
    except DataQualityError as e:
        logger.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
