from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def data_root() -> Path:
    """Where raw downloads and Parquet partitions live.

    Overridable so a test can point the whole pipeline at a temporary directory
    instead of the real 57 MB of downloads.
    """
    return Path(os.environ.get("STORMFACTORY_DATA", REPO_ROOT / "data"))


def raw_dir() -> Path:
    return data_root() / "raw"


def parquet_dir() -> Path:
    return data_root() / "parquet" / "storm_events"


def partition_path(year: int) -> Path:
    return parquet_dir() / f"year={year}" / "part.parquet"


def release_root() -> Path:
    """Where the two committed release files live.

    Separately overridable from the data root so a check can tamper with a copy of
    the manifest rather than with the one the repo publishes.
    """
    return Path(os.environ.get("STORMFACTORY_RELEASE", REPO_ROOT))


def lock_path() -> Path:
    return release_root() / "source.lock.json"


def manifest_path() -> Path:
    return release_root() / "measurements" / "day1-ingest.json"
