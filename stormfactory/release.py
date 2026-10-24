"""What a release publishes about itself, and the rebuild claim it can support.

A release here is a set of Parquet artifacts, none of them in git. What git carries is this
manifest and the DVC pointer files, and between them they have to be enough to say
whether a rebuild produced the same dataset.

Each artifact gets two hashes and they answer different questions.

`sha256` is over the file's bytes. It answers "is this the file this release
 published" and it
is only reproducible under the same Parquet writer, because polars stamps its version
and build id into the footer.

`content_sha256` is the digest from `digest.py`. It answers "is this the same data",
and it survives a change of writer, of compression and of row group layout. It is the
one a rebuild on another machine should be judged by.

Keeping both is the point. A rebuild where the content digests all agree and the byte
hashes all differ is a successful rebuild under a different polars build, and a
release that only published byte hashes would have called that a failure.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from . import digest, paths

# v0.2 rather than v0.1. The imagery layer adds a tenth artifact, so the dataset a reader
# gets is not the one v0.1's manifest describes, and leaving the string alone would make
# two different datasets answer to one name. The artifacts v0.1 published are unchanged
# and their hashes are the same, which is the part worth saying out loud.
RELEASE = "v0.2"
SUPERSEDES = "v0.1"

# One registry, read by the manifest asset and by the check that re-derives it. The
# sort key is part of the artifact's definition rather than a detail, because the
# content digest is only well defined once the row order is pinned.
# Bounded on purpose. Five years is enough to block splits by storm and still small
# enough that a full rebuild is one call. The range widens on a later day once the
# release machinery is proven, not before.
#
# This list lives here rather than in assets.py because the registry below needs it and
# assets.py imports this module. One owner, imported by the partition definition, so
# there is no second copy to fall out of step. A check asserts they are the same object
# and not two lists that happen to agree.
YEARS = [2019, 2020, 2021, 2022, 2023]

ARTIFACTS: dict[str, dict] = {
    **{
        f"storm_events_{year}": {
            "path": ("parquet", "storm_events", f"year={year}", "part.parquet"),
            "sort_by": ["EVENT_ID"],
        }
        for year in YEARS
    },
    "event_counties": {"path": ("parquet", "event_counties", "part.parquet"), "sort_by": ["EVENT_ID", "county_fips"]},
    # spc_id alone does not identify a report. SPC reuses an `om` number between
    # report types, so the type is part of the key and not decoration.
    "spc_reports": {"path": ("parquet", "spc_reports", "part.parquet"), "sort_by": ["spc_id", "report_type", "county_fips"]},
    "fema_declarations": {"path": ("parquet", "fema_declarations", "part.parquet"), "sort_by": ["disaster_number", "county_fips"]},
    "event_splits": {"path": ("parquet", "event_splits", "part.parquet"), "sort_by": ["EVENT_ID"]},
    # One row per imagery tile over one event's damage track. The sort key is the tile id
    # because that is what identifies a row, and `z/x/y` sorts as text in the order a
    # reader expects only because every tile here shares a zoom and a magnitude.
    "imagery_tiles": {"path": ("parquet", "imagery_tiles", "part.parquet"), "sort_by": ["tile_id"]},
}


class ReleaseError(Exception):
    pass


def artifact_path(name: str) -> Path:
    return paths.data_root().joinpath(*ARTIFACTS[name]["path"])


def manifest_path() -> Path:
    return paths.release_root() / "measurements" / "release.json"


def write_artifact(name: str, frame: pl.DataFrame) -> Path:
    """Write a release artifact sorted by its declared key.

    Every writer goes through here so the row order of a published artifact is a
    property of the data and of nothing else. Two things made that necessary on the
    first rebuild. polars `unique` reorders its output unless it is told not to, and
    it does so differently between two calls in the same process. And the OpenFEMA
    API pages its records in whatever order it likes, so a second download arrives
    shuffled. Both produced artifacts holding identical rows and different bytes.

    Sorting here rather than in each asset also means the digest and the file cannot
    disagree about the order, because they read the same key out of the same registry.
    """
    key = ARTIFACTS[name]["sort_by"]
    missing = [c for c in key if c not in frame.columns]
    if missing:
        raise ReleaseError(f"{name} is written without {missing}, which its sort key names")
    path = artifact_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.sort(key, nulls_last=True).write_parquet(path, compression="zstd")
    return path


def describe(name: str) -> dict:
    """Both hashes, the row count, and the writer that produced the bytes."""
    path = artifact_path(name)
    if not path.exists():
        raise ReleaseError(f"{name} is in the registry and {path} does not exist")
    frame = pl.read_parquet(path)
    return {
        "path": str(path.relative_to(paths.data_root())).replace("\\", "/"),
        "rows": frame.height,
        "columns": frame.width,
        "bytes": path.stat().st_size,
        "sha256": digest.byte_sha256(path),
        "content_sha256": digest.frame_digest(frame, ARTIFACTS[name]["sort_by"]),
        "sort_by": ARTIFACTS[name]["sort_by"],
        "written_by": digest.writer_of(path),
    }


def build_manifest() -> dict:
    artifacts = {name: describe(name) for name in sorted(ARTIFACTS)}
    writers = sorted({a["written_by"] for a in artifacts.values()})
    return {
        "release": RELEASE,
        "supersedes": SUPERSEDES,
        "what_changed_since_the_version_it_supersedes": (
            "imagery_tiles was added. Every artifact the previous release published is "
            "byte identical and content identical to what it published"
        ),
        "digest_version": digest.DIGEST_VERSION,
        "artifacts": artifacts,
        "writers": writers,
        "what_the_byte_hash_proves": (
            "the artifact is the file this release published, under the writer named beside it"
        ),
        "what_the_content_hash_proves": (
            "the artifact holds the same rows, under any Parquet writer"
        ),
    }


def write_manifest() -> dict:
    manifest = build_manifest()
    p = manifest_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def read_manifest() -> dict:
    return json.loads(manifest_path().read_text())


def disagreements() -> list[str]:
    """Where the manifest and the data on disk differ, as sentences.

    Byte hash and content hash are reported apart, because a content hash that holds
    while a byte hash moves is a different event from data that changed.
    """
    manifest = read_manifest()
    out = []
    for name, stated in sorted(manifest["artifacts"].items()):
        if name not in ARTIFACTS:
            out.append(f"{name} is in the manifest and not in the registry")
            continue
        actual = describe(name)
        for key in ("rows", "columns", "bytes", "content_sha256", "sha256"):
            if actual[key] != stated.get(key):
                out.append(f"{name}.{key} states {stated.get(key)} and the data holds {actual[key]}")
    for name in sorted(ARTIFACTS):
        if name not in manifest["artifacts"]:
            out.append(f"{name} is in the registry and not in the manifest")
    return out
