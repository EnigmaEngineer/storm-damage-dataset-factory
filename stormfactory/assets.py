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

from . import (
    baseline, digest, fema, geo, grading, imagery, integrity, joins, labelling, paths,
    release, scrub, sources, spc, splits,
)
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


@asset(deps=[event_counties, spc_reports, fema_declarations, event_splits, AssetKey("imagery_tiles")],
       description="The release manifest, with a byte hash and a writer independent content hash per artifact.")
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


def baseline_results() -> dict:
    """Train the baseline under each cut, then run the matched experiment.

    One function rather than one per arm, because the asset and the check that grades it
    must not be able to disagree about which measurement was taken.
    """
    events = pl.concat([pl.read_parquet(paths.partition_path(year)) for year in YEARS])
    split_table = pl.read_parquet(release.artifact_path("event_splits"))
    frame = baseline.feature_frame(events, split_table)

    per_scheme = {name: baseline.score_one_scheme(frame, name) for name in baseline.SCHEMES}
    matched = baseline.matched_experiment(frame)

    # How much of each cut's test split has no damage value at all. This is the second of
    # the three things that move between the random arm and the blocked one, and it is
    # measured off the split table rather than argued about. It goes in the manifest
    # because the README states it and the data it comes from is not in git.
    damage = pl.concat(
        [pl.read_parquet(paths.partition_path(year), columns=["EVENT_ID", baseline.TARGET_SOURCE])
         for year in YEARS]
    )
    with_damage = split_table.join(damage, on="EVENT_ID", how="inner")
    absent_share = {}
    for name in baseline.SCHEMES:
        test = with_damage.filter(pl.col(name) == "test")
        absent = int(test[baseline.TARGET_SOURCE].null_count())
        absent_share[name] = {
            "test_rows": int(test.height),
            "test_rows_with_no_damage_value": absent,
            "pct": round(100 * absent / test.height, 1) if test.height else None,
        }

    stated = per_scheme["random_split"]["roc_auc"] - per_scheme["blocked_split"]["roc_auc"]
    episode_term = per_scheme["random_split"]["roc_auc"] - per_scheme["storm_split"]["roc_auc"]
    spatial_term = per_scheme["storm_split"]["roc_auc"] - per_scheme["blocked_split"]["roc_auc"]

    return {
        "frame": frame,
        "population": {
            "events_in_the_release": int(split_table.height),
            "events_carrying_a_damage_value": int(frame.height),
            "events_dropped_for_an_absent_damage_value": int(split_table.height - frame.height),
            "positives": int(frame["label"].sum()),
            "events_without_a_county_kept": int(frame["has_a_county"].not_().sum()),
        },
        "schemes": per_scheme,
        "random_against_blocked": {
            "roc_auc_gap": round(stated, 4),
            "average_precision_gap": round(
                per_scheme["random_split"]["average_precision"]
                - per_scheme["blocked_split"]["average_precision"],
                4,
            ),
            "what_moves_between_these_two_arms": [
                "the test rows are different events",
                "the share of rows whose damage field is absent differs between the cuts",
                "the blocked training set holds none of the states the blocked test set holds",
            ],
            "test_rows_with_no_damage_value_by_scheme": absent_share,
        },
        "decomposition": {
            "episode_term_random_to_storm": round(episode_term, 4),
            "spatial_term_storm_to_blocked": round(spatial_term, 4),
            "note": (
                "the storm cut keeps the state mix and removes sibling events, so the "
                "first term is the episode half and the second is the cost of "
                "extrapolating into unseen states"
            ),
        },
        "matched": matched,
    }


@asset(deps=[event_splits],
       description="One damage baseline under each cut, and the matched experiment that isolates the leak.")
def baseline_report() -> Output[dict]:
    """The day's measurement. Counts and ranking metrics, and no wall time anywhere.

    The headline the plan asked for is the random against blocked gap and it is
    published with the three things that move between those arms listed beside it. The
    number that survives a question about what it measures is the matched one.
    """
    results = baseline_results()
    frame = results.pop("frame")

    manifest = {
        "dataset": "baseline_report",
        "years": YEARS,
        "target": {
            "name": "recorded property damage above zero",
            "read_from": baseline.TARGET_SOURCE,
            "absent_values_are_dropped_rather_than_called_zero": True,
            "why": (
                "NOAA leaves DAMAGE_PROPERTY empty on 72,920 events and does not say that "
                "empty means none, so calling them negatives would invent that many labels"
            ),
        },
        "model": {
            "estimator": "HistGradientBoostingClassifier",
            "max_iter": 100,
            "seed": baseline.SEED,
            "one_model_class_fitted_once_per_arm": True,
        },
        "features": {
            "used": list(baseline.FEATURES),
            "excluded_with_the_reason": baseline.EXCLUDED,
        },
        **results,
        "what_the_metric_values_rest_on": (
            "every count here re-derives from the committed artifacts and a check does "
            "it. the ranking metrics are a deterministic function of those artifacts, "
            "the feature allowlist and the seed, and two fits in one session agreed to "
            "four places. they are not verified across machines and a check cannot "
            "re-derive them without refitting, which is the measurement itself"
        ),
    }

    path = paths.release_root() / "measurements" / "baseline.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2) + "\n")

    return Output(manifest, metadata={
        "modelled_rows": manifest["population"]["events_carrying_a_damage_value"],
        "random_auc": manifest["schemes"]["random_split"]["roc_auc"],
        "storm_auc": manifest["schemes"]["storm_split"]["roc_auc"],
        "blocked_auc": manifest["schemes"]["blocked_split"]["roc_auc"],
        "random_against_blocked_gap": manifest["random_against_blocked"]["roc_auc_gap"],
        "matched_gap": manifest["matched"]["roc_auc_gap"],
    })


@asset_check(asset=AssetKey("baseline_report"),
             description="Every count in the baseline manifest re-derived from the artifacts.")
def baseline_counts_re_derive() -> AssetCheckResult:
    """Counts only, and the gaps as arithmetic over the published metrics.

    Refitting five models to grade the file would be taking the measurement again rather
    than checking it, so the metric values are not re-derived here and the manifest says
    so in its own words.
    """
    stated = json.loads((paths.release_root() / "measurements" / "baseline.json").read_text())

    events = pl.concat([pl.read_parquet(paths.partition_path(year)) for year in YEARS])
    split_table = pl.read_parquet(release.artifact_path("event_splits"))
    frame = baseline.feature_frame(events, split_table)

    wrong = []

    live_population = {
        "events_in_the_release": int(split_table.height),
        "events_carrying_a_damage_value": int(frame.height),
        "events_dropped_for_an_absent_damage_value": int(split_table.height - frame.height),
        "positives": int(frame["label"].sum()),
        "events_without_a_county_kept": int(frame["has_a_county"].not_().sum()),
    }
    for key, value in stated["population"].items():
        if live_population[key] != value:
            wrong.append(f"population.{key} states {value} and the data holds {live_population[key]}")

    for name in baseline.SCHEMES:
        train = frame.filter(pl.col(name) == "train")
        test = frame.filter(pl.col(name) == "test")
        live = {
            "train_rows": train.height,
            "train_positives": int(train["label"].sum()),
            "score_rows": test.height,
            "score_positives": int(test["label"].sum()),
            "test_rows_in_a_storm_train_has_seen": baseline.episodes_shared(train, test),
        }
        for key, value in live.items():
            if stated["schemes"][name][key] != value:
                wrong.append(f"{name}.{key} states {stated['schemes'][name][key]} and the data holds {value}")

    damage = pl.concat(
        [pl.read_parquet(paths.partition_path(year), columns=["EVENT_ID", baseline.TARGET_SOURCE])
         for year in YEARS]
    )
    with_damage = split_table.join(damage, on="EVENT_ID", how="inner")
    for name in baseline.SCHEMES:
        test = with_damage.filter(pl.col(name) == "test")
        absent = int(test[baseline.TARGET_SOURCE].null_count())
        live = {
            "test_rows": int(test.height),
            "test_rows_with_no_damage_value": absent,
            "pct": round(100 * absent / test.height, 1) if test.height else None,
        }
        said = stated["random_against_blocked"]["test_rows_with_no_damage_value_by_scheme"][name]
        for key, value in live.items():
            if said[key] != value:
                wrong.append(f"{name} absent damage {key} states {said[key]} and the data holds {value}")

    for label, stated_gap, left, right in (
        ("random_against_blocked.roc_auc_gap",
         stated["random_against_blocked"]["roc_auc_gap"],
         stated["schemes"]["random_split"]["roc_auc"],
         stated["schemes"]["blocked_split"]["roc_auc"]),
        ("decomposition.episode_term_random_to_storm",
         stated["decomposition"]["episode_term_random_to_storm"],
         stated["schemes"]["random_split"]["roc_auc"],
         stated["schemes"]["storm_split"]["roc_auc"]),
        ("decomposition.spatial_term_storm_to_blocked",
         stated["decomposition"]["spatial_term_storm_to_blocked"],
         stated["schemes"]["storm_split"]["roc_auc"],
         stated["schemes"]["blocked_split"]["roc_auc"]),
        ("matched.roc_auc_gap",
         stated["matched"]["roc_auc_gap"],
         stated["matched"]["with_siblings"]["roc_auc"],
         stated["matched"]["without_siblings"]["roc_auc"]),
    ):
        if stated_gap != round(left - right, 4):
            wrong.append(f"{label} states {stated_gap} and the published metrics give {round(left - right, 4)}")

    return AssetCheckResult(passed=not wrong, metadata={
        "disagreements": len(wrong), "detail": MetadataValue.text("; ".join(wrong) or "none")})


@asset_check(asset=AssetKey("baseline_report"),
             description="The grouped arms trained on no sibling of their own test rows, and the random arm did.")
def baseline_arms_share_no_storm_except_the_control() -> AssetCheckResult:
    """The random arm is reported rather than asserted, because it is the control.

    If it ever came back at zero the cut would not be random, which is the same reasoning
    the split check uses.
    """
    stated = json.loads((paths.release_root() / "measurements" / "baseline.json").read_text())
    grouped = {
        name: stated["schemes"][name]["test_rows_in_a_storm_train_has_seen"]
        for name in ("storm_split", "blocked_split")
    }
    control = stated["schemes"]["random_split"]["test_rows_in_a_storm_train_has_seen"]
    matched_honest = stated["matched"]["without_siblings"]["episodes_shared_with_evaluation"]
    matched_leaky = stated["matched"]["with_siblings"]["episodes_shared_with_evaluation"]

    problems = [f"{k} trained on {v} sibling rows" for k, v in grouped.items() if v]
    if matched_honest:
        problems.append(f"the matched honest arm shares {matched_honest} evaluation rows")
    if not matched_leaky:
        problems.append("the matched leaky arm shares nothing, so it is not the leaky arm")
    if not control:
        problems.append("the random control shares nothing, so it is not a random cut")

    return AssetCheckResult(passed=not problems, metadata={
        "random_control_rows_sharing_a_storm": control,
        "storm_split": grouped["storm_split"],
        "blocked_split": grouped["blocked_split"],
        "matched_leaky_arm_rows_sharing_a_storm": matched_leaky,
        "problems": MetadataValue.text("; ".join(problems) or "none"),
    })


@asset_check(asset=AssetKey("baseline_report"),
             description="The matched arms hold one evaluation set and one training size.")
def matched_arms_hold_one_evaluation_set_and_one_training_size() -> AssetCheckResult:
    """The whole claim of the matched experiment is that nothing else moved.

    Equal training size and an identical evaluation set are the two halves of that, so
    they are asserted rather than described.
    """
    matched = json.loads(
        (paths.release_root() / "measurements" / "baseline.json").read_text()
    )["matched"]
    honest, leaky = matched["without_siblings"], matched["with_siblings"]

    problems = []
    if honest["train_rows"] != leaky["train_rows"]:
        problems.append(f"training sizes differ, {honest['train_rows']} against {leaky['train_rows']}")
    if honest["train_rows"] != matched["training_rows"]:
        problems.append("the stated training budget is not the size either arm trained on")
    for key in ("score_rows", "score_positives"):
        if honest[key] != leaky[key]:
            problems.append(f"the two arms were scored on different rows, {key} differs")
    if honest["score_rows"] != matched["evaluation_rows"]:
        problems.append("the stated evaluation size is not the size either arm was scored on")

    return AssetCheckResult(passed=not problems, metadata={
        "training_rows_each_arm": honest["train_rows"],
        "evaluation_rows_each_arm": honest["score_rows"],
        "problems": MetadataValue.text("; ".join(problems) or "none"),
    })


@asset_check(asset=AssetKey("baseline_report"),
             description="The feature allowlist carries no identifier, no place label and no part of the target.")
def baseline_features_hold_nothing_excluded() -> AssetCheckResult:
    """Derived from the two lists in the module rather than from a copy kept here.

    An allowlist is the shape this wants, because a blocklist only refuses what its
    author thought of. The excluded names are published alongside so a reader can argue
    with the choice rather than guess at it.
    """
    overlap = sorted(set(baseline.FEATURES) & set(baseline.EXCLUDED))
    stated = json.loads(
        (paths.release_root() / "measurements" / "baseline.json").read_text()
    )["features"]

    problems = [f"{name} is on both lists" for name in overlap]
    if sorted(stated["used"]) != sorted(baseline.FEATURES):
        problems.append("the manifest's feature list is not the module's")
    if sorted(stated["excluded_with_the_reason"]) != sorted(baseline.EXCLUDED):
        problems.append("the manifest's excluded list is not the module's")
    missing_reason = [k for k, v in baseline.EXCLUDED.items() if not v.strip()]
    if missing_reason:
        problems.append(f"excluded with no reason given: {missing_reason}")

    return AssetCheckResult(passed=not problems, metadata={
        "features": len(baseline.FEATURES),
        "excluded": len(baseline.EXCLUDED),
        "problems": MetadataValue.text("; ".join(problems) or "none"),
    })


@asset_check(asset=AssetKey("baseline_report"),
             description="Refuse a wall time in the baseline manifest.")
def day4_manifests_hold_no_wall_time() -> AssetCheckResult:
    path = paths.release_root() / "measurements" / "baseline.json"
    text = path.read_text().lower() if path.exists() else ""
    banned = [w for w in ("seconds", "elapsed", "duration", "_ms", "millis", "runtime", "took") if w in text]
    return AssetCheckResult(passed=not banned, metadata={
        "found": MetadataValue.text(", ".join(banned) or "none")})


def _release_events() -> pl.DataFrame:
    frames = [pl.read_parquet(paths.partition_path(y)) for y in YEARS]
    return pl.concat(frames)


def _catalogue_and_lock():
    """The catalogue, refetched every time this is called.

    It is not read back from `imagery.lock.json`, and the lock is a record of what was
    read rather than a gate on reading it again. That costs 28 fetches per call and it
    means a build is only as reproducible as NOAA's catalogue is stable. The hashes in
    the lock are what would let a later run notice the difference, and nothing compares
    them yet.
    """
    return imagery.build_catalogue()


@asset(description="Every NOAA emergency response imagery collection, from both catalogue shapes.")
def eri_catalogue(context: AssetExecutionContext) -> Output[dict]:
    collections, storms = _catalogue_and_lock()
    placeholder = imagery.placeholder_sha256()
    by_shape = {}
    for c in collections:
        by_shape[c.catalogue_shape] = by_shape.get(c.catalogue_shape, 0) + 1

    payload = {
        "placeholder_sha256": placeholder,
        "collections": len(collections),
        "collections_by_catalogue_shape": by_shape,
        "collections_with_a_declared_footprint": sum(1 for c in collections if c.bounds),
        "storms": storms,
        "what_a_footprint_is_not": (
            "the declared bounds are the hull of a sortie's flight lines and not its "
            "coverage, so a tile set is validated by probing and never by the box"
        ),
    }
    imagery.lock_path().write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    context.log.info(f"catalogue holds {len(collections)} collections over {len(storms)} storms")
    return Output(payload, metadata={
        "collections": len(collections),
        "with_a_footprint": payload["collections_with_a_declared_footprint"],
        "shapes": MetadataValue.text(json.dumps(by_shape)),
    })


# The zoom is a labelling budget and not a resolution choice, and saying it the other way
# round would be flattering. Both sorties publish to zoom 19. Over this event's track the
# corridor holds 37 tiles at 17 and 117 at 18, and 117 pairs is more than one annotator
# labels in one pass, so the grid is cut at 17 and a tile pixel covers about 1.03 m of
# ground instead of 0.52. The manifest publishes both counts so the cost is visible.
TILE_ZOOM = 17
DEEPER_ZOOM_FOR_REFERENCE = 18

# A tile is kept if its centre is within half the surveyed path width plus half a tile of
# the track, which is the condition for the tile to overlap the damage swath at all. The
# width comes from the event's own TOR_WIDTH rather than from a chosen number.
YARD_M = 0.9144


def _event_candidates(events: pl.DataFrame, collections) -> pl.DataFrame:
    """Events the catalogue can supply both a before and an after frame for.

    Prefiltered on the declared footprint, which is all a cheap pass can use. The box is
    not coverage, so this is a candidate list and not an answer, and the coverage probe
    below is what decides.
    """
    boxed = [c for c in collections if c.bounds]
    rows = []
    for r in events.filter(pl.col("has_point") & pl.col("damage_property_usd").is_not_null()).iter_rows(named=True):
        begin = imagery.begin_utc(r["BEGIN_DATE_TIME"], r["CZ_TIMEZONE"]).date()
        lat, lon = r["BEGIN_LAT"], r["BEGIN_LON"]
        here = [c for c in boxed if c.box_holds(lat, lon)]
        post = [c for c in here if 0 <= (c.acquired - begin).days <= imagery.POST_WINDOW_DAYS]
        pre = [c for c in here if c.acquired < begin]
        if post and pre:
            rows.append({
                "EVENT_ID": r["EVENT_ID"],
                "damage_property_usd": r["damage_property_usd"],
                "post_candidates": len(post),
                "pre_candidates": len(pre),
            })
    if not rows:
        raise imagery.ImageryError("no release event has both a before and an after candidate")
    return pl.DataFrame(rows).sort(["damage_property_usd", "EVENT_ID"], descending=[True, False])


def _pick_by_coverage(candidates, tiles, zoom, placeholder, nearest: str):
    """The collection covering the most of this tile set, probed rather than assumed.

    Coverage comes first and the acquisition date only breaks a tie. A sortie two days
    after the event that holds none of the track is worth less than one four days after
    that holds all of it, and ordering these the other way round is how a project ends up
    with a dozen answerable tasks out of forty.
    """
    scored = []
    for c in candidates:
        held = imagery.probe_coverage(c, tiles, zoom, placeholder)
        rank = c.acquired.toordinal() if nearest == "earliest" else -c.acquired.toordinal()
        scored.append({"collection": c, "tiles_with_imagery": len(held), "rank": rank, "probed": held})
    scored.sort(key=lambda s: (-s["tiles_with_imagery"], s["rank"]))
    return scored


@asset(deps=[eri_catalogue, storm_events],
       description="Imagery tile pairs over one event's damage track, before and after.")
def imagery_tiles(context: AssetExecutionContext) -> Output[dict]:
    events = _release_events()
    collections, _ = _catalogue_and_lock()
    placeholder = imagery.placeholder_sha256()

    candidates = _event_candidates(events, collections)
    chosen_id = int(candidates["EVENT_ID"][0])
    event = events.filter(pl.col("EVENT_ID") == chosen_id).to_dicts()[0]
    begin = imagery.begin_utc(event["BEGIN_DATE_TIME"], event["CZ_TIMEZONE"])

    start = (event["BEGIN_LAT"], event["BEGIN_LON"])
    end = (event["END_LAT"], event["END_LON"])
    mid_lat = (start[0] + end[0]) / 2
    tile_edge_m = 256 * imagery.metres_per_pixel(mid_lat, TILE_ZOOM)
    buffer_m = event["TOR_WIDTH"] * YARD_M / 2 + tile_edge_m / 2
    tiles = imagery.corridor_tiles(start, end, TILE_ZOOM, buffer_m)

    boxed = [c for c in collections if c.bounds and c.box_holds(*start)]
    post_candidates = [c for c in boxed if 0 <= (c.acquired - begin.date()).days <= imagery.POST_WINDOW_DAYS]
    pre_candidates = [c for c in boxed if c.acquired < begin.date()]
    post_scored = _pick_by_coverage(post_candidates, tiles, TILE_ZOOM, placeholder, "earliest")
    pre_scored = _pick_by_coverage(pre_candidates, tiles, TILE_ZOOM, placeholder, "latest")
    post, pre = post_scored[0]["collection"], pre_scored[0]["collection"]

    post_fetch = imagery.fetch_tiles(post, tiles, TILE_ZOOM, imagery.tile_dir(post.name), placeholder)
    pre_fetch = imagery.fetch_tiles(pre, tiles, TILE_ZOOM, imagery.tile_dir(pre.name), placeholder)

    # Selection asks with a HEAD and the fetch asks with a GET and hashes what comes back.
    # Two different questions, so the chosen pair is the one place they can be compared,
    # and a disagreement would mean the cheap question is answering the wrong thing.
    head_against_body = {
        side: sorted(
            f"{t.x},{t.y}" for t in fetched
            if t.is_imagery != ((t.x, t.y) in scored[0]["probed"])
        )
        for side, fetched, scored in (("post", post_fetch, post_scored), ("pre", pre_fetch, pre_scored))
    }
    frame = imagery.tiles_frame(event, TILE_ZOOM, post, pre, post_fetch, pre_fetch)
    path = release.write_artifact("imagery_tiles", frame)

    both = frame.filter(pl.col("post_has_imagery") & pl.col("pre_has_imagery")).height
    tasks = labelling.tasks_for_tiles(frame, post.tile_template, pre.tile_template)
    labelling.tasks_path().parent.mkdir(parents=True, exist_ok=True)
    labelling.tasks_path().write_text(json.dumps(tasks, indent=1) + "\n")

    payload = {
        "event": {
            "EVENT_ID": chosen_id,
            "event_type": event["EVENT_TYPE"],
            "state": event["STATE"],
            "county": event["CZ_NAME"],
            "begin_local": event["BEGIN_DATE_TIME"],
            "stated_zone": event["CZ_TIMEZONE"],
            "begin_utc_under_the_stated_offset": begin.isoformat(),
            "damage_property_usd": event["damage_property_usd"],
            "tor_f_scale": event["TOR_F_SCALE"],
            "tor_length_miles": event["TOR_LENGTH"],
            "tor_width_yards": event["TOR_WIDTH"],
            "track_length_m": round(imagery.haversine_m(start, end), 1),
        },
        "selection": {
            "events_with_a_before_and_an_after_candidate": candidates.height,
            "rule": (
                "highest recorded property damage among release events holding a point, a "
                "footprint-containing sortie within seven days after the begin date, and "
                "one before it"
            ),
        },
        "grid": {
            "zoom": TILE_ZOOM,
            "tile_edge_m": round(tile_edge_m, 1),
            "metres_per_pixel": round(imagery.metres_per_pixel(mid_lat, TILE_ZOOM), 4),
            "buffer_m": round(buffer_m, 1),
            "buffer_rule": "half the surveyed path width plus half a tile",
            "tiles_in_corridor": len(tiles),
            "tiles_in_corridor_at_the_deeper_zoom": len(imagery.corridor_tiles(
                start, end, DEEPER_ZOOM_FOR_REFERENCE,
                event["TOR_WIDTH"] * YARD_M / 2
                + 256 * imagery.metres_per_pixel(mid_lat, DEEPER_ZOOM_FOR_REFERENCE) / 2)),
            "deeper_zoom_for_reference": DEEPER_ZOOM_FOR_REFERENCE,
            "metres_per_pixel_at_the_deeper_zoom": round(
                imagery.metres_per_pixel(mid_lat, DEEPER_ZOOM_FOR_REFERENCE), 4),
            "why_not_the_deeper_zoom": (
                "both sorties publish to zoom 19 and the deeper grid is sharper. The grid "
                "is cut shallower because one annotator labelled this pass and the deeper "
                "corridor holds more pairs than one pass covers"
            ),
        },
        "chosen": {
            "post": {"collection": post.name, "storm": post.storm, "acquired": str(post.acquired),
                     "days_after_begin": (post.acquired - begin.date()).days,
                     **imagery.coverage(post_fetch)},
            "pre": {"collection": pre.name, "storm": pre.storm, "acquired": str(pre.acquired),
                    "days_before_begin": (begin.date() - pre.acquired).days,
                    **imagery.coverage(pre_fetch)},
            "tiles_with_both_panels": both,
            "tiles_where_the_head_probe_and_the_fetch_disagree": head_against_body,
        },
        "coverage_against_the_declared_footprint": {
            "post_candidates": [
                {"collection": s["collection"].name, "acquired": str(s["collection"].acquired),
                 "tiles_with_imagery": s["tiles_with_imagery"]} for s in post_scored
            ],
            "pre_candidates": [
                {"collection": s["collection"].name, "acquired": str(s["collection"].acquired),
                 "tiles_with_imagery": s["tiles_with_imagery"]} for s in pre_scored
            ],
            "candidates_whose_footprint_holds_every_tile_centre": sum(
                1 for s in post_scored + pre_scored
                if all(s["collection"].box_holds(*imagery.tile_centre(TILE_ZOOM, x, y))
                       for x, y in tiles)
            ),
            "candidates": len(post_scored) + len(pre_scored),
        },
        "timezone_field": imagery.timezone_field_audit(events),
        "labelstudio_tasks": len(tasks),
        "artifact": str(path.relative_to(paths.data_root())).replace("\\", "/"),
    }
    imagery_manifest_path().parent.mkdir(parents=True, exist_ok=True)
    imagery_manifest_path().write_text(json.dumps(payload, indent=2) + "\n")
    context.log.info(f"event {chosen_id}: {both} of {len(tiles)} tiles hold both panels")
    return Output(payload, metadata={
        "event": chosen_id,
        "tiles_in_corridor": len(tiles),
        "tiles_with_both_panels": both,
        "post": MetadataValue.text(post.name),
        "pre": MetadataValue.text(pre.name),
    })


def imagery_manifest_path() -> Path:
    return paths.release_root() / "measurements" / "imagery.json"


def labels_manifest_path() -> Path:
    return paths.release_root() / "measurements" / "labels.json"


@asset(deps=[imagery_tiles], description="The labelling pass over the tile pairs, and its agreement.")
def tile_labels() -> Output[dict]:
    labels = labelling.read_labels()
    frame = pl.read_parquet(release.artifact_path("imagery_tiles"))
    labelable = set(
        frame.filter(pl.col("post_has_imagery") & pl.col("pre_has_imagery"))["tile_id"].to_list()
    )
    labelled = set(labels["tile_id"].to_list())

    payload = {
        "annotations": labels.height,
        "tiles_labelled": len(labelled),
        "tiles_with_both_panels": len(labelable),
        "tiles_with_both_panels_left_unlabelled": sorted(labelable - labelled),
        "labelled_tiles_outside_the_release": sorted(labelled - labelable),
        "label_counts": labelling.label_counts(labels),
        "label_set": list(labelling.LABELS),
        "agreement": labelling.agreement(labels),
        "annotator_note": (
            "one annotator and it is an automated run reading each tile pair, recorded in "
            "the export as the annotator id. These labels are a worked example of the "
            "path from sortie to project to graded sample. They are not an expert ground "
            "truth and nothing in this release treats them as one"
        ),
    }
    labels_manifest_path().write_text(json.dumps(payload, indent=2) + "\n")
    return Output(payload, metadata={
        "annotations": labels.height,
        "agreement": MetadataValue.text(payload["agreement"]["status"]),
        "counts": MetadataValue.text(json.dumps(payload["label_counts"])),
    })


@asset_check(asset=AssetKey("imagery_tiles"),
             description="Every count in the imagery manifest re-derived from the Parquet.")
def imagery_counts_re_derive() -> AssetCheckResult:
    stated = json.loads(imagery_manifest_path().read_text())
    frame = pl.read_parquet(release.artifact_path("imagery_tiles"))
    problems = []
    if frame.height != stated["grid"]["tiles_in_corridor"]:
        problems.append(f"manifest says {stated['grid']['tiles_in_corridor']} tiles and the artifact holds {frame.height}")
    both = frame.filter(pl.col("post_has_imagery") & pl.col("pre_has_imagery")).height
    if both != stated["chosen"]["tiles_with_both_panels"]:
        problems.append(f"manifest says {stated['chosen']['tiles_with_both_panels']} tiles with both panels and the artifact holds {both}")
    for side in ("post", "pre"):
        held = frame.filter(pl.col(f"{side}_has_imagery")).height
        if held != stated["chosen"][side]["tiles_with_imagery"]:
            problems.append(f"{side} coverage states {stated['chosen'][side]['tiles_with_imagery']} and the artifact holds {held}")
        if frame[f"{side}_collection"].n_unique() != 1:
            problems.append(f"{side}_collection is not one collection")
    return AssetCheckResult(passed=not problems, metadata={
        "tiles": frame.height,
        "tiles_with_both_panels": both,
        "problems": MetadataValue.text("; ".join(problems) or "none"),
    })


@asset_check(asset=AssetKey("imagery_tiles"),
             description="No tile carries the placeholder hash, and none sits outside the buffer.")
def tiles_hold_imagery_inside_the_declared_buffer() -> AssetCheckResult:
    stated = json.loads(imagery_manifest_path().read_text())
    placeholder = json.loads(imagery.lock_path().read_text())["placeholder_sha256"]
    frame = pl.read_parquet(release.artifact_path("imagery_tiles"))
    problems = []
    for side in ("post", "pre"):
        if frame.filter(pl.col(f"{side}_sha256") == placeholder).height:
            problems.append(f"{side} keeps a tile whose content is the placeholder")
        held = frame.filter(pl.col(f"{side}_has_imagery"))
        if held.filter(pl.col(f"{side}_sha256").is_null()).height:
            problems.append(f"{side} claims imagery with no hash")
    disagreements = stated["chosen"]["tiles_where_the_head_probe_and_the_fetch_disagree"]
    for side, tiles in disagreements.items():
        if tiles:
            problems.append(f"the {side} probe and the {side} fetch disagree on {tiles}")
    outside = frame.filter(pl.col("metres_from_track") > stated["grid"]["buffer_m"] + 1e-6)
    if outside.height:
        problems.append(f"{outside.height} tiles sit beyond the declared buffer")
    if frame["tile_id"].n_unique() != frame.height:
        problems.append("tile_id repeats")
    return AssetCheckResult(passed=not problems, metadata={
        "placeholder_sha256": MetadataValue.text(placeholder[:16]),
        "problems": MetadataValue.text("; ".join(problems) or "none"),
    })


@asset_check(asset=AssetKey("tile_labels"),
             description="Agreement stays unmeasured while there is one annotator.")
def agreement_is_unmeasured_with_one_annotator() -> AssetCheckResult:
    stated = json.loads(labels_manifest_path().read_text())
    agreement = stated["agreement"]
    problems = []
    if agreement["annotators"] < 2 and agreement["status"] != "unmeasured":
        problems.append("one annotator and the manifest reports a measured agreement")
    if agreement["status"] == "unmeasured" and any(
        isinstance(v, float) for k, v in agreement.items()
    ):
        problems.append("an unmeasured agreement carries a number")
    if stated["labelled_tiles_outside_the_release"]:
        problems.append(f"labels name tiles the release does not hold: {stated['labelled_tiles_outside_the_release']}")
    if stated["tiles_with_both_panels_left_unlabelled"]:
        problems.append(f"{len(stated['tiles_with_both_panels_left_unlabelled'])} answerable tiles were not labelled")
    return AssetCheckResult(passed=not problems, metadata={
        "status": MetadataValue.text(agreement["status"]),
        "annotators": agreement["annotators"],
        "problems": MetadataValue.text("; ".join(problems) or "none"),
    })


@asset_check(asset=AssetKey("tile_labels"),
             description="Refuse a wall time in the imagery or the label manifest.")
def imagery_manifests_hold_no_wall_time() -> AssetCheckResult:
    banned = []
    for path in (imagery_manifest_path(), labels_manifest_path()):
        text = path.read_text().lower() if path.exists() else ""
        banned += [f"{path.name}:{w}" for w in ("seconds", "elapsed", "duration", "_ms", "millis", "runtime", "took") if w in text]
    return AssetCheckResult(passed=not banned, metadata={
        "found": MetadataValue.text(", ".join(banned) or "none")})
