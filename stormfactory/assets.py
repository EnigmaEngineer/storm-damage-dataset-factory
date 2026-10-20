"""Dagster assets for the Storm Events layer.

Three assets and two checks. `source_lock` decides which files a release is built
from, `storm_events` turns one storm year into one validated Parquet partition, and
`ingest_manifest` publishes the counts. The counts are then re-derived by a check,
because a count a repo states about its own data is the cheapest kind of stale number
to prevent.
"""

import json
import urllib.request
from pathlib import Path

import polars as pl
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    Output,
    StaticPartitionsDefinition,
    asset,
    asset_check,
)

from . import paths, sources
from .schema import cast_and_derive, read_raw, validate_or_raise

# Bounded on purpose. Five years is enough to block splits by storm and still small
# enough that a full rebuild is one call. The range widens on a later day once the
# release machinery is proven, not before.
YEARS = [2019, 2020, 2021, 2022, 2023]

year_partitions = StaticPartitionsDefinition([str(y) for y in YEARS])

USER_AGENT = "storm-damage-dataset-factory (portfolio project, contact via GitHub EnigmaEngineer)"


def _fetch(url: str, dest: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(req, timeout=180) as resp, tmp.open("wb") as fh:
        while chunk := resp.read(1 << 20):
            fh.write(chunk)
    tmp.replace(dest)


@asset(description="Which NOAA details files this release is built from, with content hashes.")
def source_lock(context: AssetExecutionContext) -> Output[dict]:
    index_html = urllib.request.urlopen(
        urllib.request.Request(sources.INDEX_URL, headers={"User-Agent": USER_AGENT}), timeout=120
    ).read().decode("utf-8", "replace")

    chosen = sources.newest_per_year(sources.parse_index(index_html), YEARS)

    entries = []
    for f in chosen:
        local = paths.raw_dir() / f.filename
        if not local.exists():
            context.log.info(f"downloading {f.filename}")
            _fetch(f.url, local)
        entries.append(sources.lock_entry(f, local))

    lock = {"index_url": sources.INDEX_URL, "years": YEARS, "files": entries}
    paths.lock_path().write_text(json.dumps(lock, indent=2) + "\n")
    return Output(
        lock,
        metadata={
            "files": len(entries),
            "revisions": MetadataValue.text(", ".join(f"{e['year']}:{e['revision']}" for e in entries)),
        },
    )


@asset(partitions_def=year_partitions, deps=[source_lock],
       description="One storm year as Parquet, validated against the contract before it is written.")
def storm_events(context: AssetExecutionContext) -> Output[None]:
    year = int(context.partition_key)
    lock = json.loads(paths.lock_path().read_text())
    entry = next(e for e in lock["files"] if e["year"] == year)
    csv_path = paths.raw_dir() / entry["filename"]

    df = cast_and_derive(read_raw(csv_path))
    validate_or_raise(df, year)

    out = paths.partition_path(year)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out, compression="zstd")

    located = int(df["has_point"].sum())
    return Output(
        None,
        metadata={
            "rows": len(df),
            "rows_with_a_point": located,
            "rows_without_a_point": len(df) - located,
            "source_sha256": MetadataValue.text(entry["sha256"]),
            "parquet_bytes": out.stat().st_size,
        },
    )


def partition_counts() -> dict[str, dict[str, int]]:
    """Counts read back off the Parquet, which is what the manifest has to agree with."""
    counts = {}
    for year in YEARS:
        p = paths.partition_path(year)
        if not p.exists():
            continue
        df = pl.read_parquet(p, columns=["EVENT_ID", "EPISODE_ID", "has_point", "damage_property_usd"])
        counts[str(year)] = {
            "rows": len(df),
            "events": df["EVENT_ID"].n_unique(),
            "episodes": df["EPISODE_ID"].n_unique(),
            "rows_with_a_point": int(df["has_point"].sum()),
            "rows_with_property_damage": int(df["damage_property_usd"].is_not_null().sum()),
        }
    return counts


@asset(deps=[storm_events], description="Counts this release publishes about itself.")
def ingest_manifest() -> Output[dict]:
    per_year = partition_counts()
    if len(per_year) != len(YEARS):
        missing = sorted(set(str(y) for y in YEARS) - set(per_year))
        raise RuntimeError(f"no Parquet partition for {missing}, so the manifest would be short")

    totals = {k: sum(v[k] for v in per_year.values()) for k in next(iter(per_year.values()))}
    manifest = {
        "dataset": "noaa_storm_events",
        "years": YEARS,
        "per_year": per_year,
        "total": totals,
    }
    paths.manifest_path().parent.mkdir(parents=True, exist_ok=True)
    paths.manifest_path().write_text(json.dumps(manifest, indent=2) + "\n")
    return Output(manifest, metadata={"total_rows": totals["rows"]})


@asset_check(asset=AssetKey("ingest_manifest"), description="Every count in the manifest re-derived from the Parquet.")
def manifest_counts_match_parquet() -> AssetCheckResult:
    manifest = json.loads(paths.manifest_path().read_text())
    live = partition_counts()
    wrong = []
    for year, counts in manifest["per_year"].items():
        for key, stated in counts.items():
            actual = live.get(year, {}).get(key)
            if actual != stated:
                wrong.append(f"{year}.{key} states {stated} and the Parquet holds {actual}")
    for key, stated in manifest["total"].items():
        actual = sum(v[key] for v in live.values())
        if actual != stated:
            wrong.append(f"total.{key} states {stated} and the Parquet holds {actual}")
    return AssetCheckResult(
        passed=not wrong,
        metadata={"disagreements": len(wrong), "detail": MetadataValue.text("; ".join(wrong) or "none")},
    )


@asset_check(asset=AssetKey("ingest_manifest"), description="Refuse a wall time in the manifest.")
def manifest_holds_no_wall_time() -> AssetCheckResult:
    text = paths.manifest_path().read_text().lower()
    banned = [w for w in ("seconds", "elapsed", "duration", "_ms", "millis", "runtime", "took") if w in text]
    return AssetCheckResult(
        passed=not banned,
        metadata={"found": MetadataValue.text(", ".join(banned) or "none")},
    )
