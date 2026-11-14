"""Load config/pipeline.yaml into a typed, read-only object."""
from dataclasses import dataclass
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class PipelineConfig:
    app_name: str
    master: str
    shuffle_partitions: int
    landing: str
    bronze: str
    quarantine: str
    silver: str
    gold: str
    max_quarantine_ratio: float


def _resolve(path: str) -> str:
    p = Path(path)
    return (p if p.is_absolute() else PROJECT_ROOT / p).as_posix()


def load_config(path: str | Path = DEFAULT_CONFIG) -> PipelineConfig:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    paths = raw["paths"]
    return PipelineConfig(
        app_name=raw["app_name"],
        master=raw["spark"]["master"],
        shuffle_partitions=int(raw["spark"]["shuffle_partitions"]),
        landing=_resolve(paths["landing"]),
        bronze=_resolve(paths["bronze"]),
        quarantine=_resolve(paths["quarantine"]),
        silver=_resolve(paths["silver"]),
        gold=_resolve(paths["gold"]),
        max_quarantine_ratio=float(raw["quality"]["max_quarantine_ratio"]),
    )
