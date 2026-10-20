# Storm-Damage Research Dataset Factory

Builds traceable research datasets from public storm and damage records, so that an
experiment can say exactly which rows it trained on. One command materialises every
partition, and a partition that fails its contract is never written.

```
python scripts/build_release.py
```

## What is real and what is synthetic

Every row of data in this repo is real and public. It comes from NOAA's Storm Events
Database, served as gzipped CSV by the National Centers for Environmental Information.
Nothing is generated or simulated.

The only invented rows live in `tests/test_schema.py`. They exist to drive the
contract's refusals and they never reach a Parquet file.

## What is here so far

The Storm Events layer for five storm years, 2019 through 2023. Later work adds the
Storm Prediction Center reports, OpenFEMA declarations and the imagery tiles.

```
NOAA csvfiles index page
          |
          v
   source_lock  ------------->  source.lock.json
          |                     year, revision, sha256, bytes
          v
 storm_events[year]  ------->   data/parquet/storm_events/year=YYYY/part.parquet
          |                     contract validated before the write
          v
 ingest_manifest  ---------->   measurements/day1-ingest.json
          |
          +--> manifest_counts_match_parquet
          +--> manifest_holds_no_wall_time
```

Orchestration is Dagster. Each partition records the source revision and sha256 it read
as materialisation metadata, so the lineage is a side effect of running the asset rather
than a graph somebody maintains. That metadata lives in the Dagster instance. The
headless driver in `scripts/build_release.py` uses an ephemeral one and discards it on
exit, so `source.lock.json` is the durable copy and `dagster dev` is where the history
is browsable.
Processing is Polars reading the CSV with every column as text, then casting under a
declared contract. The cast is strict on purpose. A column NOAA changed the type of
should stop the build rather than arrive quietly coerced.

## Why there is a lock file

NOAA republishes a storm year whenever records are corrected, and the revision date is
part of the file name as the `_cYYYYMMDD` stamp. So "Storm Events 2019 to 2023" does
not identify a dataset. These five file names do.

Two different revisions are already in play across the five years. 2022 was reissued on
2026-06-25 and the other four carry 2026-03-23. The lock file records four things about each
source file. The name and the revision, then the byte count and the sha256. A later
release has to match all four to claim it rebuilt the same thing.

## Measured on 2026-10-20

All of these come from the run above on this machine. Every one is re-derived by a
check rather than typed into this file.

| Figure | Value |
|---|---|
| Storm years | 5 |
| Event rows | 336,014 |
| Distinct storm episodes | 50,411 |
| Rows carrying a point location | 199,698 |
| Rows with no point location | 136,316 |
| Rows with a property damage figure | 263,094 |
| Source bytes downloaded | 57,317,148 |
| Checks in the suite | 34 |

`EVENT_ID` is unique across all 336,014 rows and not only within a year, which was
measured rather than assumed.

## What I got wrong today

The first version of the contract declared `BEGIN_RANGE` and `END_RANGE` as integers.
They read like counts and NOAA documents them as a distance in miles. The strict cast
refused 168,041 values on the first run, with decimals like `.53` and `117.53` in the
error. Both columns are floats now.

A permissive reader would have accepted the same data and silently produced nulls or
truncated miles. The refusal is the feature, and it cost one run to find out.

## Known limitations

- 136,316 of 336,014 rows have no latitude or longitude at all. Those events are
  recorded against a county or a forecast zone instead of a point. Spatial blocking is
  the whole reason this dataset exists, so a plan that blocks on point location would
  silently drop 40 percent of the rows. The blocking key has to cope with zone-only
  events and that decision is not made yet.
- Only the details files are ingested. The Storm Events fatalities and locations files
  are published alongside them and are not read yet.
- Damage figures are NOAA's own estimates and carry no uncertainty. A blank means not
  reported rather than zero, and that distinction survives as a null.
- Nothing is in Cloud Storage. Terraform for the bucket and the service account is
  not written yet, so a rebuild is local only. TODO: the GCS remote waits on the
  OIDC trust for CI.
- The release has no content-addressed identity beyond the source lock. A rebuild is
  not yet proven bit for bit.

## Layout

```
stormfactory/sources.py    resolving the index page to one file per year
stormfactory/schema.py     the column contract and the damage parser
stormfactory/assets.py     three Dagster assets and two asset checks
stormfactory/paths.py      where data and release files live
scripts/build_release.py   build every partition headlessly
tests/run_all.py           the whole suite, no test framework needed
```

Run the suite with `python tests/run_all.py`. Browse the asset graph with
`dagster dev -m stormfactory.definitions`.
