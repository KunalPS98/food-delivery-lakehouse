from dataclasses import replace

import pytest

from food_pipeline import bronze
from food_pipeline.config import PROJECT_ROOT, load_config
from food_pipeline.run_pipeline import DataQualityError, discover_batches, load_batch_to_silver

HEADER = ("Age,Gender,Marital Status,Occupation,Monthly Income,Educational Qualifications,Family size,"
          "Customer Type,latitude,longitude,Pin code,Output,Feedback,\n")
GOOD = "20,Female,Single,Student,No Income,Post Graduate,4,Frequent,12.9766,77.5993,560001,Yes,Positive,Yes\n"
BAD_AGE = "150,Female,Single,Student,No Income,Post Graduate,4,Frequent,12.9766,77.5993,560001,Yes,Positive,Yes\n"


def test_discover_batches_finds_csvs_in_name_order(tmp_path):
    for name in ["batch_02.csv", "batch_01.csv", "notes.txt"]:
        (tmp_path / name).write_text("x", encoding="utf-8")
    assert [b for b, _ in discover_batches(tmp_path.as_posix())] == ["batch_01", "batch_02"]


def test_load_config_resolves_relative_paths_from_project_root(tmp_path):
    cfg_file = tmp_path / "pipeline.yaml"
    absolute = (tmp_path / "silver").as_posix()
    cfg_file.write_text(
        "app_name: test\nspark: {master: 'local[1]', shuffle_partitions: 1}\n"
        f"paths: {{landing: data/raw/landing, bronze: b, quarantine: q, silver: '{absolute}', gold: g}}\n"
        "quality: {max_quarantine_ratio: 0.05}\n", encoding="utf-8")
    cfg = load_config(cfg_file)
    assert cfg.landing == (PROJECT_ROOT / "data" / "raw" / "landing").as_posix()
    assert cfg.silver == absolute
    assert cfg.max_quarantine_ratio == 0.05


@pytest.fixture
def tmp_cfg(tmp_path):
    cfg = load_config()
    return replace(cfg, **{k: (tmp_path / k).as_posix() for k in ["bronze", "quarantine", "silver", "gold"]})


def test_batch_with_too_many_bad_rows_stops_before_silver(spark, tmp_path, tmp_cfg):
    csv = tmp_path / "batch_01.csv"
    csv.write_text(HEADER + GOOD + BAD_AGE, encoding="utf-8")   # 50% bad, limit is 5%
    df = bronze.read_batch(spark, csv.as_posix(), "batch_01")

    with pytest.raises(DataQualityError):
        load_batch_to_silver(spark, df, "batch_01", tmp_cfg)
    assert not (tmp_path / "silver").exists()


def test_batch_under_threshold_quarantines_and_loads_the_rest(spark, tmp_path, tmp_cfg):
    csv = tmp_path / "batch_01.csv"
    csv.write_text(HEADER + GOOD + BAD_AGE, encoding="utf-8")
    df = bronze.read_batch(spark, csv.as_posix(), "batch_01")

    stats = load_batch_to_silver(spark, df, "batch_01", replace(tmp_cfg, max_quarantine_ratio=0.6))
    assert (stats["quarantined"], stats["inserted"]) == (1, 1)
    q = spark.read.format("delta").load(tmp_cfg.quarantine).first()
    assert list(q["failed_rules"]) == ["age_18_to_80"]
