"""Checks on the release registry, the integrity routes and the om form fixture.

Nothing here reads `data/`, because a check that needs the 82 MB of downloads does
not run in a clean clone and is therefore not a gate.
"""

import json
from pathlib import Path

import polars as pl

from stormfactory import assets, digest, integrity, paths, release

FIXTURES = Path(__file__).resolve().parent / "fixtures"
OM_FORMS = FIXTURES / assets.OM_FORMS_FIXTURE


def check_every_registry_sort_key_is_a_list_of_names():
    for name, spec in release.ARTIFACTS.items():
        assert spec["sort_by"], name
        assert all(isinstance(c, str) for c in spec["sort_by"]), name


def check_every_storm_year_has_a_registry_entry():
    for year in assets.YEARS:
        assert f"storm_events_{year}" in release.ARTIFACTS, year
    registered = {n for n in release.ARTIFACTS if n.startswith("storm_events_")}
    assert len(registered) == len(assets.YEARS), registered


def check_the_year_list_is_one_object_and_not_two_that_agree():
    """Identity rather than equality.

    Two lists holding the same five years pass an equality check on the day somebody
    edits one of them and fail on no day at all. The registry is built from this list
    and so is the partition definition, so there has to be exactly one.
    """
    assert assets.YEARS is release.YEARS


def check_the_registry_path_and_the_partition_path_agree():
    """Two routes to the same file, so a change to either is caught here."""
    for year in assets.YEARS:
        assert release.artifact_path(f"storm_events_{year}") == paths.partition_path(year)


def check_write_artifact_refuses_a_frame_missing_its_sort_key(tmp=None):
    frame = pl.DataFrame({"not_the_key": [1, 2]})
    try:
        release.write_artifact("event_splits", frame)
    except release.ReleaseError as exc:
        assert "EVENT_ID" in str(exc), str(exc)
        return
    raise AssertionError("writing an artifact without its sort key should be refused")


def check_describing_an_artifact_that_is_not_there_is_refused():
    """Driven by a registry entry pointing nowhere, rather than by the data being absent.

    The first version of this check only did anything when the release had not been
    built, so on a machine holding the data it asserted nothing at all.
    """
    release.ARTIFACTS["a_name_nothing_writes"] = {
        "path": ("parquet", "nothing_here", "part.parquet"), "sort_by": ["id"]}
    try:
        release.describe("a_name_nothing_writes")
    except release.ReleaseError as exc:
        assert "does not exist" in str(exc), str(exc)
        return
    finally:
        del release.ARTIFACTS["a_name_nothing_writes"]
    raise AssertionError("describing an artifact that is not on disk should be refused")


def check_the_om_fixture_carries_both_forms_the_real_files_carry():
    """A derivation for the om split that travels. Six rows instead of 82 MB."""
    df = pl.read_csv(OM_FORMS, infer_schema_length=0)
    values = df["om"].to_list()
    as_int = pl.Series(values).cast(pl.Int64, strict=False)
    assert int(as_int.is_not_null().sum()) == 3, values
    assert int(as_int.is_null().sum()) == 3, values
    assert "617025" in values and "2001040929-01" in values, values


def check_a_strict_integer_cast_refuses_the_composite_om_form():
    """The refusal that made spc_id text, reproducible from the fixture."""
    df = pl.read_csv(OM_FORMS, infer_schema_length=0)
    try:
        df.with_columns(pl.col("om").cast(pl.Int64, strict=True))
    except pl.exceptions.InvalidOperationError as exc:
        assert "om" in str(exc), str(exc)
        return
    raise AssertionError("a strict integer cast should refuse a date-time-sequence identifier")


def check_the_composite_om_form_is_a_date_then_a_time_then_a_sequence():
    """Asserting the shape rather than the refusal, so the comment has a check behind it."""
    df = pl.read_csv(OM_FORMS, infer_schema_length=0)
    composite = [v for v in df["om"].to_list() if "-" in v]
    assert composite, df["om"].to_list()
    for value in composite:
        stamp, sequence = value.split("-")
        assert len(stamp) == 10 and stamp.isdigit(), value
        assert len(sequence) == 2 and sequence.isdigit(), value


def check_the_superseded_om_pair_is_named_rather_than_deleted():
    """A figure that moved says so. This pins the old pair in the source.

    If the manifest is ever rewritten to drop the history, this fails.
    """
    source = (paths.REPO_ROOT / "stormfactory" / "assets.py").read_text()
    assert "29097" in source and "108730" in source, "the superseded om pair is gone"


def check_a_gzip_file_that_has_been_truncated_fails_its_own_checksum(tmp=None):
    """A truncated download, built rather than argued about.

    A truncated download is the failure the lock could not see, so a truncated file
    has to actually fail here.
    """
    import gzip
    import tempfile

    with tempfile.TemporaryDirectory() as box:
        good = Path(box) / "good.gz"
        with gzip.open(good, "wb") as fh:
            fh.write(b"a line of source data\n" * 500)
        assert integrity.gzip_is_intact(good)["uncompressed_bytes"] == 22 * 500

        bad = Path(box) / "bad.gz"
        bad.write_bytes(good.read_bytes()[:-40])
        try:
            integrity.gzip_is_intact(bad)
        except integrity.IntegrityError as exc:
            assert "gzip checksum" in str(exc), str(exc)
            return
    raise AssertionError("a truncated gzip file should fail its own checksum")


def check_a_corrupted_gzip_byte_fails_its_own_checksum():
    """Truncation is one failure mode. A flipped byte in the middle is the other."""
    import gzip
    import tempfile

    with tempfile.TemporaryDirectory() as box:
        good = Path(box) / "good.gz"
        with gzip.open(good, "wb") as fh:
            fh.write(b"another line of source data\n" * 500)
        raw = bytearray(good.read_bytes())
        raw[len(raw) // 2] ^= 0xFF
        bad = Path(box) / "bad.gz"
        bad.write_bytes(bytes(raw))
        try:
            integrity.gzip_is_intact(bad)
        except integrity.IntegrityError:
            return
    raise AssertionError("a flipped byte should fail the gzip checksum")


def check_the_local_tail_hash_sees_a_change_in_the_tail():
    import tempfile

    with tempfile.TemporaryDirectory() as box:
        a = Path(box) / "a.txt"
        a.write_bytes(b"x" * 1000)
        before = integrity.local_tail_sha256(a, window=100)
        a.write_bytes(b"x" * 999 + b"y")
        assert integrity.local_tail_sha256(a, window=100) != before


def check_the_local_tail_hash_cannot_see_a_change_outside_its_window():
    """Stating the limit as a check, so nobody reads the tail route as a full one."""
    import tempfile

    with tempfile.TemporaryDirectory() as box:
        a = Path(box) / "a.txt"
        a.write_bytes(b"x" * 1000)
        before = integrity.local_tail_sha256(a, window=100)
        a.write_bytes(b"z" + b"x" * 999)
        assert integrity.local_tail_sha256(a, window=100) == before


def check_the_byte_hash_and_the_content_digest_are_different_functions():
    """Both are sha256 and they must not be computed over the same thing."""
    import tempfile

    with tempfile.TemporaryDirectory() as box:
        p = Path(box) / "x.parquet"
        frame = pl.DataFrame({"id": [1, 2], "v": ["a", "b"]})
        frame.write_parquet(p, compression="zstd")
        assert digest.byte_sha256(p) != digest.parquet_digest(p, ["id"])


def check_the_release_manifest_names_what_each_hash_proves():
    """A reader meeting two hashes has to be told which one answers which question.

    Reads the committed manifest rather than rebuilding one. The first version called
    build_manifest, which needs every artifact on disk, so it raised in a clean clone
    instead of grading the file that is actually in git.
    """
    if not release.manifest_path().exists():
        print("skipped, no release manifest committed to read the two claims out of")
        return
    manifest = release.read_manifest()
    assert "writer" in manifest["what_the_byte_hash_proves"]
    assert "any Parquet writer" in manifest["what_the_content_hash_proves"]
