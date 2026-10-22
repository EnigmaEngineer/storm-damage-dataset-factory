"""The two asset checks, driven against a tampered copy of a manifest.

A check that has never refused anything is not evidence that it would. These point
`STORMFACTORY_RELEASE` and `STORMFACTORY_DATA` at a temporary tree, edit one number,
and assert the check notices.

The partition they build that tree from is a committed fixture and not the release.
Every one of these six read `data/parquet` until now, so on a clean clone all six
failed with FileNotFoundError rather than grading anything. They were gates only on a
machine that happened to be holding 82 MB of downloads. Measured on the device VM,
where the whole suite came back 136 of 142 and every one of the six was this.

The fixture is three real 2021 rows carrying the full 54 column contract. They were
picked by running the scrub over the candidates and keeping only rows where both
detectors find nothing, because committing a narrative for a repo whose whole job is
redacting narratives is the mistake the withheld labels exist to avoid.
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
SAMPLE_PARTITION = Path(__file__).resolve().parent / "fixtures" / "storm_events_2021_sample.parquet"


@contextmanager
def sandbox(rows=3):
    """A release tree holding one small Parquet partition and a matching manifest.

    `rows` cannot exceed the fixture, so it is clamped and the caller is told rather
    than quietly handed fewer rows than it asked for.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        before = {k: os.environ.get(k) for k in ("STORMFACTORY_RELEASE", "STORMFACTORY_DATA")}
        os.environ["STORMFACTORY_RELEASE"] = str(tmp / "release")
        os.environ["STORMFACTORY_DATA"] = str(tmp / "data")
        try:
            real = pl.read_parquet(SAMPLE_PARTITION)
            out = paths.partition_path(2021)
            out.parent.mkdir(parents=True, exist_ok=True)
            if rows > real.height:
                raise AssertionError(
                    f"the sandbox was asked for {rows} rows and the fixture holds {real.height}")
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


def check_the_sample_partition_carries_the_whole_column_contract():
    """Otherwise the sandbox grades a manifest built from a narrower frame than the
    release has, and a contract change would not show up here at all.

    The expected column set is built from the contract rather than typed, so adding a
    raw column or a derived one fails here until the fixture is recaptured.
    """
    from stormfactory import schema

    sample = pl.read_parquet(SAMPLE_PARTITION)
    expected = set(schema.RAW_COLUMNS) | set(schema.DERIVED)
    assert set(sample.columns) == expected, sorted(expected ^ set(sample.columns))
    assert sample.height == 3, sample.height


def check_the_sample_partition_carries_nothing_the_scrub_would_redact():
    """The fixture is committed to a public repo, so it has to be clean by measurement
    rather than by having been read once."""
    from stormfactory import scrub

    sample = pl.read_parquet(SAMPLE_PARTITION)
    for column in ("EVENT_NARRATIVE", "EPISODE_NARRATIVE"):
        for text in sample[column].to_list():
            _, spans = scrub.scrub(text or "")
            assert not spans, (column, text, [s.kind for s in spans])


def check_nothing_in_this_module_reads_the_release_directory():
    """The defect this module carried, pinned so it cannot come back.

    Reads its own source, because a check that needs the release present in order to
    prove nothing needs the release present is circular. The expression it refuses is
    the exact one that was here, and the temporary tree the sandbox builds under
    `tmp / "data"` is a different thing and stays allowed.
    """
    source = Path(__file__).read_text()
    # Built from pieces rather than written out. A scanner that reads its own file and
    # looks for a literal finds the literal in its own assertion and fails on a healthy
    # module, which is what the first version of this did.
    needle = "REPO_ROOT / " + chr(34) + "data" + chr(34)
    assert needle not in source, "a check here is reading the release again"
