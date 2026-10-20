"""The two asset checks, driven against a tampered copy of the real manifest.

A check that has never refused anything is not evidence that it would. These point
`STORMFACTORY_RELEASE` and `STORMFACTORY_DATA` at a temporary tree, edit one number,
and assert the check notices.
"""

import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

import polars as pl

from stormfactory import assets, paths

REPO_ROOT = Path(__file__).resolve().parent.parent


@contextmanager
def sandbox(rows=3):
    """A release tree holding one small Parquet partition and a matching manifest."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        before = {k: os.environ.get(k) for k in ("STORMFACTORY_RELEASE", "STORMFACTORY_DATA")}
        os.environ["STORMFACTORY_RELEASE"] = str(tmp / "release")
        os.environ["STORMFACTORY_DATA"] = str(tmp / "data")
        try:
            real = pl.read_parquet(REPO_ROOT / "data" / "parquet" / "storm_events" / "year=2021" / "part.parquet")
            out = paths.partition_path(2021)
            out.parent.mkdir(parents=True, exist_ok=True)
            real.head(rows).write_parquet(out)
            # Only 2021 exists here, so the manifest the asset writes covers one year.
            original_years = assets.YEARS
            assets.YEARS = [2021]
            try:
                assets.ingest_manifest()
            finally:
                assets.YEARS = original_years
            yield tmp
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            for k, v in before.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


def _edit_manifest(fn):
    m = json.loads(paths.manifest_path().read_text())
    fn(m)
    paths.manifest_path().write_text(json.dumps(m, indent=2) + "\n")


def check_the_count_check_passes_on_an_untouched_manifest():
    with sandbox():
        assert assets.manifest_counts_match_parquet().passed


def check_the_count_check_refuses_a_bumped_per_year_count():
    with sandbox():
        _edit_manifest(lambda m: m["per_year"]["2021"].update(rows=m["per_year"]["2021"]["rows"] + 1))
        result = assets.manifest_counts_match_parquet()
        assert not result.passed
        assert "2021.rows" in result.metadata["detail"].text


def check_the_count_check_refuses_a_bumped_total():
    with sandbox():
        _edit_manifest(lambda m: m["total"].update(episodes=999))
        result = assets.manifest_counts_match_parquet()
        assert not result.passed
        assert "total.episodes" in result.metadata["detail"].text


def check_the_wall_time_check_passes_on_the_real_manifest():
    with sandbox():
        assert assets.manifest_holds_no_wall_time().passed


def check_the_wall_time_check_refuses_a_duration():
    with sandbox():
        _edit_manifest(lambda m: m.update(elapsed_seconds=4.1))
        result = assets.manifest_holds_no_wall_time()
        assert not result.passed
        assert "seconds" in result.metadata["found"].text


def check_the_manifest_asset_refuses_a_short_release():
    # Every declared year must have a partition. A manifest covering four of five
    # years with no complaint is how a release ships a quarter of itself.
    with sandbox():
        original = assets.YEARS
        assets.YEARS = [2021, 2022]
        try:
            assets.ingest_manifest()
        except RuntimeError as e:
            assert "2022" in str(e), str(e)
        else:
            raise AssertionError("a release missing a year was accepted")
        finally:
            assets.YEARS = original


def check_totals_are_the_sum_of_the_years():
    m = json.loads((REPO_ROOT / "measurements" / "day1-ingest.json").read_text())
    for key, total in m["total"].items():
        assert total == sum(v[key] for v in m["per_year"].values()), key
