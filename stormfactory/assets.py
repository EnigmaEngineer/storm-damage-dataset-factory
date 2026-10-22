"""Dagster assets for the Storm Events layer.

Three assets and two checks. `source_lock` decides which files a release is built
from, `storm_events` turns one storm year into one validated Parquet partition, and
`ingest_manifest` publishes the counts. The counts are then re-derived by a check,
because a count a repo states about its own data is the cheapest kind of stale number
to prevent.
"""

import json
import urllib.request
from datetime import date
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

from . import digest, fema, geo, grading, integrity, joins, paths, release, scrub, sources, spc, splits
from .schema import cast_and_derive, read_raw, validate_or_raise

# Owned by release.py, because the artifact registry there is built from it and this
# module imports that one. Re-exported under the name every caller already uses.
YEARS = release.YEARS

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
        # Checked against the gzip stream's own CRC32 and against the byte count the
        # server reports, BEFORE the hash goes into the lock. Otherwise a truncated
        # download is hashed as it stands and the lock records that as correct.
        checked = integrity.check_gzip_source(local, f.url)
        entries.append({**sources.lock_entry(f, local), "integrity": checked})

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

    out = release.write_artifact(f"storm_events_{year}", df)

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


REFERENCE_FILES = "reference.lock.json"


def _reference_paths() -> dict[str, Path]:
    root = paths.raw_dir() / "reference"
    return {"zones": root / "zone_county.dbx", "counties": root / "national_county.txt"}


@asset(description="The two reference files the county join needs, with content hashes.")
def reference_lock(context: AssetExecutionContext) -> Output[dict]:
    """The zone crosswalk and the Census county list, pinned the same way the storm
    sources are.

    The crosswalk's file name carries the date it was issued, so the page is read
    rather than a name guessed. That is the `source_lock` lesson arriving on a second
    publisher, which is the reason it is worth having twice.
    """
    index = urllib.request.urlopen(
        urllib.request.Request(geo.ZONE_INDEX_URL, headers={"User-Agent": USER_AGENT}), timeout=120
    ).read().decode("utf-8", "replace")
    zone_file = geo.newest_zone_file(index)

    local = _reference_paths()
    if not local["zones"].exists():
        context.log.info(f"downloading {zone_file.name}")
        _fetch(zone_file.url, local["zones"])
    if not local["counties"].exists():
        context.log.info("downloading the Census county list")
        _fetch(geo.COUNTY_URL, local["counties"])

    lock = {
        "zone_county": {
            "url": zone_file.url, "issued": zone_file.stamp,
            "bytes": local["zones"].stat().st_size,
            "sha256": sources.sha256_of(local["zones"]),
            "integrity": integrity.check_plain_source(local["zones"], zone_file.url),
        },
        "national_county": {
            "url": geo.COUNTY_URL,
            "bytes": local["counties"].stat().st_size,
            "sha256": sources.sha256_of(local["counties"]),
            "integrity": integrity.check_plain_source(local["counties"], geo.COUNTY_URL),
        },
    }
    (paths.release_root() / REFERENCE_FILES).write_text(json.dumps(lock, indent=2) + "\n")
    return Output(lock, metadata={"zone_file": MetadataValue.text(zone_file.name)})


@asset(deps=[reference_lock, storm_events],
       description="Every storm event resolved to the counties it happened in, one row per pair.")
def event_counties() -> Output[dict]:
    """The join key, built rather than assumed.

    A county row resolves to itself after a check against the Census list. A zone row
    fans out through the crosswalk. The output is longer than the input and that is
    the part a reader has to know before joining anything to it.
    """
    ref = _reference_paths()
    counties = geo.read_counties(ref["counties"])
    crosswalk = geo.read_zone_crosswalk(ref["zones"])

    frames = []
    stats = {"events": 0, "resolved": 0, "unresolved": 0, "pairs": 0,
             "county_rows_resolved": 0, "county_rows_not_a_county": 0,
             "zone_rows_resolved": 0, "zone_rows_unresolved": 0}
    for year in YEARS:
        df = pl.read_parquet(
            paths.partition_path(year),
            columns=["EVENT_ID", "STATE_FIPS", "CZ_TYPE", "CZ_FIPS", "EVENT_TYPE",
                     "BEGIN_YEARMONTH", "BEGIN_DAY", "has_point", "damage_property_usd",
                     "DEATHS_DIRECT", "INJURIES_DIRECT"],
        )
        pairs = geo.resolve_frame(df, counties, crosswalk)

        stats["events"] += len(df)
        stats["pairs"] += len(pairs)
        for cz, hit_key, miss_key in (("C", "county_rows_resolved", "county_rows_not_a_county"),
                                      ("Z", "zone_rows_resolved", "zone_rows_unresolved")):
            of_type = df.filter(pl.col("CZ_TYPE") == cz)
            hit = pairs.filter(pl.col("CZ_TYPE") == cz)["EVENT_ID"].n_unique()
            stats[hit_key] += hit
            stats[miss_key] += len(of_type) - hit

        frames.append(pairs.select(
            "EVENT_ID",
            pl.lit(year, dtype=pl.Int64).alias("year"),
            "county_fips", "county_name",
            pl.col("CZ_TYPE").alias("cz_type"),
            pl.col("EVENT_TYPE").alias("event_type"),
            "has_point",
            pl.date(pl.col("BEGIN_YEARMONTH") // 100, pl.col("BEGIN_YEARMONTH") % 100,
                    pl.col("BEGIN_DAY")).alias("event_date"),
            "damage_property_usd",
            pl.col("DEATHS_DIRECT").alias("deaths_direct"),
            pl.col("INJURIES_DIRECT").alias("injuries_direct"),
        ))

    pairs = pl.concat(frames)
    stats["resolved"] = pairs["EVENT_ID"].n_unique()
    stats["unresolved"] = stats["events"] - stats["resolved"]
    release.write_artifact("event_counties", pairs)
    return Output(stats, metadata={k: v for k, v in stats.items()})


@asset(deps=[reference_lock], description="SPC severe weather reports, one row per report and county.")
def spc_reports(context: AssetExecutionContext) -> Output[dict]:
    frames = []
    per_type = {}
    for f in spc.files_for(YEARS):
        local = paths.raw_dir() / "spc" / f.filename
        if not local.exists():
            context.log.info(f"downloading {f.filename}")
            _fetch(f.url, local)
        df = spc.read_reports(local, f.report_type)
        per_type[f.report_type] = per_type.get(f.report_type, 0) + len(df)
        frames.append(df)
    allr = pl.concat(frames)
    release.write_artifact("spc_reports", allr)
    stats = {"report_county_rows": len(allr),
             "distinct_om_numbers": allr["spc_id"].n_unique(),
             "distinct_reports": allr.select(["spc_id", "report_type"]).unique().height,
             **{f"rows_{k}": v for k, v in sorted(per_type.items())}}
    return Output(stats, metadata=stats)


@asset(description="OpenFEMA county level declaration designations for the release years.")
def fema_declarations(context: AssetExecutionContext) -> Output[dict]:
    local = paths.raw_dir() / "fema" / "declarations_2019_2023.json"
    if not local.exists():
        context.log.info("downloading FEMA declarations")
        records: list[dict] = []
        while True:
            url = f"{fema.API_URL}?{fema.page_query(YEARS, 1000, len(records))}"
            batch = json.loads(
                urllib.request.urlopen(
                    urllib.request.Request(url, headers={"User-Agent": USER_AGENT}), timeout=180
                ).read()
            )["DisasterDeclarationsSummaries"]
            records += batch
            if len(batch) < 1000:
                break
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(json.dumps(records, indent=1) + "\n")

    df = fema.read_declarations(local)
    # Sorted on the way out, like every release artifact. The API pages its records
    # in no stated order and a second download came back shuffled, which made the
    # Parquet's bytes a property of the download rather than of the data.
    release.write_artifact("fema_declarations", df)
    stats = {"county_designations": len(df), "disasters": df["disaster_number"].n_unique(),
             "statewide_dropped": fema.statewide_count(local),
             "open_incidents": int(df["incident_end"].is_null().sum())}
    return Output(stats, metadata=stats)


@asset(deps=[event_counties, spc_reports, fema_declarations],
       description="Counts the join publishes about itself.")
def join_manifest() -> Output[dict]:
    root = paths.data_root() / "parquet"
    ev = pl.read_parquet(root / "event_counties" / "part.parquet")
    sp = pl.read_parquet(root / "spc_reports" / "part.parquet")
    fm = pl.read_parquet(root / "fema_declarations" / "part.parquet")

    # SPC joins on county and the same calendar date. An SPC report and a NOAA event
    # for one storm are the same observation filed twice by two offices, so an equality
    # is the right test and a mismatch is a disagreement rather than a miss.
    spc_hit = joins.same_day_reports(ev, sp)

    # A declaration is a decision made later that covers a window, so the test is
    # containment and not equality. A null end means the incident is still open.
    fema_hit = joins.declarations_covering(ev, fm)
    weather_hit = fema_hit.filter(pl.col("incident_type").is_in(list(fema.WEATHER_INCIDENTS)))
    pandemic_hit = fema_hit.filter(pl.col("incident_type") == fema.PANDEMIC_INCIDENT)

    manifest = {
        "dataset": "storm_event_county_joins",
        "years": YEARS,
        "event_county_pairs": len(ev),
        "distinct_events_with_a_county": ev["EVENT_ID"].n_unique(),
        "distinct_counties": ev["county_fips"].n_unique(),
        "pairs_from_a_county_coded_event": ev.filter(pl.col("cz_type") == "C").height,
        "pairs_from_a_zone_coded_event": ev.filter(pl.col("cz_type") == "Z").height,
        # `distinct_reports` was published here as 134,007 on the day this manifest
        # was written and that figure counts SPC `om` numbers rather than reports.
        # SPC reuses an om between report types, 6,526 of them across these five
        # years, so the key is om plus type and the report count is 140,621. The old
        # name is kept beside the new one rather than overwritten, because the
        # figure moved and a reader of the release deserves to see why.
        "spc": {
            "report_county_rows": len(sp),
            "distinct_om_numbers": sp["spc_id"].n_unique(),
            "distinct_reports": sp.select(["spc_id", "report_type"]).unique().height,
            "om_numbers_reused_between_report_types": sp.group_by("spc_id").agg(
                pl.col("report_type").n_unique().alias("types")
            ).filter(pl.col("types") > 1).height,
            "distinct_events_with_an_spc_report_the_same_day": spc_hit["EVENT_ID"].n_unique(),
            "matched_pairs": len(spc_hit),
        },
        # Reported split, because the unsplit figure is a pandemic. The COVID-19
        # declaration covers 3,238 counties for 1,207 days, so every storm event in
        # three of these five years sits inside a declared window and the join says
        # nothing. The weather figure is the one worth reading.
        "fema": {
            "county_designations": len(fm),
            "incident_types_neither_list_claims": fema.unknown_incident_types(fm),
            "matched_pairs_all_incident_types": len(fema_hit),
            "distinct_events_all_incident_types": fema_hit["EVENT_ID"].n_unique(),
            "matched_pairs_weather_incidents": len(weather_hit),
            "distinct_events_weather_incidents": weather_hit["EVENT_ID"].n_unique(),
            "matched_pairs_pandemic": len(pandemic_hit),
            "pandemic_share_of_all_matches_pct": round(100 * len(pandemic_hit) / len(fema_hit), 1) if len(fema_hit) else None,
        },
    }
    p = paths.release_root() / "measurements" / "day2-join.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2) + "\n")
    return Output(manifest, metadata={"event_county_pairs": len(ev)})


@asset(deps=[storm_events], description="Narratives with personal detail redacted, graded on the labelled sample.")
def narrative_scrub() -> Output[dict]:
    """The scrub, plus what it scores against labels written by hand.

    Precision and recall are reported per detector rather than together, because the
    two have nothing in common. One has almost nothing to find in this corpus and the
    other has a few thousand. One number over both would hide that.
    """
    sample = json.loads((paths.REPO_ROOT / "tests" / "fixtures" / "labelled_narratives.json").read_text())
    graded = grading.grade(sample["rows"], paths.parquet_dir())

    applied = {"narratives": 0, "narratives_changed": 0, "name_spans": 0, "casualty_spans": 0}
    for year in YEARS:
        df = pl.read_parquet(paths.partition_path(year), columns=["EVENT_ID", "EVENT_NARRATIVE"])
        for text in df["EVENT_NARRATIVE"].to_list():
            applied["narratives"] += 1
            _, spans = scrub.scrub(text or "")
            if spans:
                applied["narratives_changed"] += 1
            applied["name_spans"] += sum(1 for s in spans if s.kind == "name")
            applied["casualty_spans"] += sum(1 for s in spans if s.kind == "casualty")

    manifest = {"dataset": "narrative_scrub", "years": YEARS,
                "applied_to_the_release": applied, "graded_on_the_labelled_sample": graded}
    p = paths.release_root() / "measurements" / "day2-scrub.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2) + "\n")
    return Output(manifest, metadata={"narratives_changed": applied["narratives_changed"]})


@asset_check(asset=AssetKey("join_manifest"), description="The join manifest re-derived from the Parquet.")
def join_manifest_matches_parquet() -> AssetCheckResult:
    manifest = json.loads((paths.release_root() / "measurements" / "day2-join.json").read_text())
    root = paths.data_root() / "parquet"
    ev = pl.read_parquet(root / "event_counties" / "part.parquet")
    wrong = []
    for key, actual in (
        ("event_county_pairs", len(ev)),
        ("distinct_events_with_a_county", ev["EVENT_ID"].n_unique()),
        ("distinct_counties", ev["county_fips"].n_unique()),
        ("pairs_from_a_county_coded_event", ev.filter(pl.col("cz_type") == "C").height),
        ("pairs_from_a_zone_coded_event", ev.filter(pl.col("cz_type") == "Z").height),
    ):
        if manifest[key] != actual:
            wrong.append(f"{key} states {manifest[key]} and the Parquet holds {actual}")
    return AssetCheckResult(passed=not wrong, metadata={
        "disagreements": len(wrong), "detail": MetadataValue.text("; ".join(wrong) or "none")})


@asset_check(asset=AssetKey("event_counties"), description="No event-county pair repeats, and every county code is real.")
def event_counties_are_unique_and_real() -> AssetCheckResult:
    ev = pl.read_parquet(paths.data_root() / "parquet" / "event_counties" / "part.parquet")
    counties = geo.read_counties(_reference_paths()["counties"])
    dupes = len(ev) - ev.select("EVENT_ID", "county_fips").unique().height
    unknown = ev.filter(~pl.col("county_fips").is_in(list(counties))).height
    return AssetCheckResult(passed=not (dupes or unknown), metadata={
        "repeated_pairs": dupes, "pairs_naming_an_unknown_county": unknown})


@asset_check(asset=AssetKey("narrative_scrub"), description="Refuse a wall time in the join or the scrub manifest.")
def day2_manifests_hold_no_wall_time() -> AssetCheckResult:
    banned = []
    for name in ("day2-join.json", "day2-scrub.json"):
        p = paths.release_root() / "measurements" / name
        if not p.exists():
            continue
        text = p.read_text().lower()
        banned += [f"{name}:{w}" for w in ("seconds", "elapsed", "duration", "_ms", "millis", "runtime", "took") if w in text]
    return AssetCheckResult(passed=not banned, metadata={"found": MetadataValue.text(", ".join(banned) or "none")})


@asset(deps=[storm_events], description="Every event assigned to train, val and test under three schemes.")
def event_splits() -> Output[dict]:
    """The three schemes side by side, so one model can be trained twice.

    One table rather than three, because the comparison the project is for is between
    columns of one row and a reader should not have to join three files to make it.

    The 26,320 events the county resolution could not place are kept and flagged. They
    carry a state and an episode, so both grouped schemes can assign them, and nothing
    county keyed can use them. Dropping them here would hide a quarter of the marine
    events from a reader who only ever looks at the split table.
    """
    frames = []
    for year in YEARS:
        frames.append(pl.read_parquet(
            paths.partition_path(year),
            columns=["EVENT_ID", "EPISODE_ID", "STATE_FIPS", "STATE", "CZ_TYPE", "EVENT_TYPE"],
        ))
    events = pl.concat(frames)

    placed = set(
        pl.read_parquet(paths.data_root() / "parquet" / "event_counties" / "part.parquet",
                        columns=["EVENT_ID"])["EVENT_ID"].to_list()
    )
    events = events.with_columns(pl.col("EVENT_ID").is_in(list(placed)).alias("has_a_county"))

    events = splits.random_split(events)
    events = splits.storm_split(events)
    events = splits.blocked_split(events)

    release.write_artifact("event_splits", events)

    per_scheme = {}
    for col in ("random_split", "storm_split", "blocked_split"):
        counts = dict(events.group_by(col).agg(pl.len().alias("n")).iter_rows())
        per_scheme[col] = {
            "events": {name: counts.get(name, 0) for name in splits.SPLITS},
            "episodes_in_more_than_one_split": splits.units_spanning_two_splits(
                events, "EPISODE_ID", col).height,
            "states_in_more_than_one_split": splits.units_spanning_two_splits(
                events, "STATE_FIPS", col).height,
            "leakage": splits.shared_storm_rate(events, col),
        }

    manifest = {
        "dataset": "event_splits",
        "years": YEARS,
        "events": events.height,
        "episodes": events["EPISODE_ID"].n_unique(),
        "states": events["STATE_FIPS"].n_unique(),
        "events_without_a_county": int((~events["has_a_county"]).sum()),
        "targets": splits.TARGETS,
        "schemes": per_scheme,
    }
    p = paths.release_root() / "measurements" / "splits.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2) + "\n")
    return Output(manifest, metadata={
        "events": events.height,
        "random_leakage_pct": per_scheme["random_split"]["leakage"]["share_pct"],
        "storm_leakage_pct": per_scheme["storm_split"]["leakage"]["share_pct"],
        "blocked_leakage_pct": per_scheme["blocked_split"]["leakage"]["share_pct"],
    })


@asset_check(asset=AssetKey("event_splits"),
             description="No storm spans two splits under either grouped scheme.")
def no_storm_spans_two_splits() -> AssetCheckResult:
    """The check the whole day is for.

    The random column is expected to fail this and is reported rather than asserted,
    because it is the control. If it ever came back at zero the scheme would not be
    random and that is worth seeing.
    """
    events = pl.read_parquet(release.artifact_path("event_splits"))
    broken = {}
    for col in ("storm_split", "blocked_split"):
        broken[col] = splits.units_spanning_two_splits(events, "EPISODE_ID", col).height
    control = splits.units_spanning_two_splits(events, "EPISODE_ID", "random_split").height
    return AssetCheckResult(
        passed=not any(broken.values()),
        metadata={
            "storm_split_episodes_split": broken["storm_split"],
            "blocked_split_episodes_split": broken["blocked_split"],
            "random_split_episodes_split_for_reference": control,
        },
    )


@asset_check(asset=AssetKey("event_splits"),
             description="The blocked scheme keeps every state whole, which is what blocks it.")
def blocked_split_keeps_each_state_whole() -> AssetCheckResult:
    events = pl.read_parquet(release.artifact_path("event_splits"))
    broken = splits.units_spanning_two_splits(events, "STATE_FIPS", "blocked_split")
    loose = splits.units_spanning_two_splits(events, "STATE_FIPS", "storm_split").height
    return AssetCheckResult(
        passed=broken.height == 0,
        metadata={
            "states_split_by_the_blocked_scheme": broken.height,
            "states_split_by_the_storm_scheme_for_reference": loose,
        },
    )


@asset_check(asset=AssetKey("event_splits"),
             description="Every event appears once under every scheme, and in a named split.")
def every_event_is_assigned_once() -> AssetCheckResult:
    events = pl.read_parquet(release.artifact_path("event_splits"))
    problems = []
    if events["EVENT_ID"].n_unique() != events.height:
        problems.append("EVENT_ID repeats in the split table")
    for col in ("random_split", "storm_split", "blocked_split"):
        if events[col].null_count():
            problems.append(f"{col} has {events[col].null_count()} unassigned events")
        stray = sorted(set(events[col].unique().to_list()) - set(splits.SPLITS))
        if stray:
            problems.append(f"{col} names splits nobody declared: {stray}")
    return AssetCheckResult(passed=not problems, metadata={
        "problems": len(problems), "detail": MetadataValue.text("; ".join(problems) or "none")})


@asset(deps=[event_counties, spc_reports, fema_declarations, event_splits],
       description="Release v0.1, with a byte hash and a writer independent content hash per artifact.")
def release_manifest() -> Output[dict]:
    manifest = release.write_manifest()
    return Output(manifest, metadata={
        "release": MetadataValue.text(manifest["release"]),
        "artifacts": len(manifest["artifacts"]),
        "writers": MetadataValue.text(", ".join(manifest["writers"])),
    })


@asset_check(asset=AssetKey("release_manifest"),
             description="Every hash and count in the release manifest re-derived from the data.")
def release_manifest_matches_the_data() -> AssetCheckResult:
    wrong = release.disagreements()
    return AssetCheckResult(passed=not wrong, metadata={
        "disagreements": len(wrong), "detail": MetadataValue.text("; ".join(wrong) or "none")})


@asset_check(asset=AssetKey("release_manifest"),
             description="Refuse a wall time in the release or the split manifest.")
def day3_manifests_hold_no_wall_time() -> AssetCheckResult:
    banned = []
    for name in ("release.json", "splits.json"):
        p = paths.release_root() / "measurements" / name
        if not p.exists():
            continue
        text = p.read_text().lower()
        banned += [f"{name}:{w}" for w in ("seconds", "elapsed", "duration", "_ms", "millis", "runtime", "took") if w in text]
    return AssetCheckResult(passed=not banned, metadata={"found": MetadataValue.text(", ".join(banned) or "none")})


OM_FORMS_FIXTURE = "spc_om_forms.csv"


def om_form_counts() -> dict:
    """How the SPC report identifier splits between its two forms.

    Derived off the released artifact rather than off `data/raw`, so it re-derives in
    a clean clone that has built or pulled the release. The raw files are not in git
    and a figure that needs them is a figure nobody can check.

    Counted at both grains on purpose. A report touching three counties is three rows
    here, so the row count and the report count are different questions and the pair
    published before answered neither.
    """
    sp = pl.read_parquet(release.artifact_path("spc_reports"), columns=["spc_id", "report_type"])
    rows = sp.with_columns(pl.col("spc_id").cast(pl.Int64, strict=False).alias("as_int"))
    reports = sp.unique().with_columns(pl.col("spc_id").cast(pl.Int64, strict=False).alias("as_int"))
    return {
        "report_county_rows": rows.height,
        "rows_whose_om_is_an_integer": int(rows["as_int"].is_not_null().sum()),
        "rows_whose_om_is_composite": int(rows["as_int"].is_null().sum()),
        "distinct_reports": reports.height,
        "reports_whose_om_is_an_integer": int(reports["as_int"].is_not_null().sum()),
        "reports_whose_om_is_composite": int(reports["as_int"].is_null().sum()),
    }


@asset(deps=[spc_reports], description="Which form the SPC report identifier takes, and how often.")
def spc_om_forms() -> Output[dict]:
    counts = om_form_counts()
    fixture = (paths.REPO_ROOT / "tests" / "fixtures" / OM_FORMS_FIXTURE)
    manifest = {
        "dataset": "spc_om_forms",
        "years": YEARS,
        "counts": counts,
        "fixture": f"tests/fixtures/{OM_FORMS_FIXTURE}",
        "superseded": {
            "stated_before": {"integer": 29097, "composite": 108730},
            "why_it_is_gone": (
                "neither figure re-derives from the committed sources and the pair does "
                "not add up to any row count this release holds"
            ),
        },
    }
    p = paths.release_root() / "measurements" / "spc-om.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, indent=2) + "\n")
    return Output(manifest, metadata={**counts, "fixture_rows": len(fixture.read_text().splitlines()) - 1})


@asset_check(asset=AssetKey("spc_om_forms"),
             description="Both om form counts re-derived from the released artifact.")
def spc_om_form_counts_re_derive() -> AssetCheckResult:
    stated = json.loads((paths.release_root() / "measurements" / "spc-om.json").read_text())["counts"]
    live = om_form_counts()
    wrong = [f"{k} states {v} and the artifact holds {live[k]}" for k, v in stated.items() if live[k] != v]
    return AssetCheckResult(passed=not wrong, metadata={
        "disagreements": len(wrong), "detail": MetadataValue.text("; ".join(wrong) or "none")})
