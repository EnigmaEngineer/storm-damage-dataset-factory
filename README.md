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

The Storm Events layer for five storm years, 2019 through 2023. Storm Prediction
Center reports and OpenFEMA declarations joined to it by county and date. A scrub of
the personal detail in the narratives. Imagery tiles come later.

```
NOAA csvfiles index page          NWS zone-county page       Census county codes
          |                              |                          |
          v                              v                          v
   source_lock                      reference_lock  ------>  reference.lock.json
          |                              |                   issued stamp, sha256
          v                              |
 storm_events[year]  ------->   data/parquet/storm_events/year=YYYY/part.parquet
          |                              |
          |                              v
          |                      event_counties  ------->  one row per event per county
          |                              |
          |                              |      spc_reports  <--- 15 SPC files
          |                              |           |
          |                              |      fema_declarations  <--- OpenFEMA API
          |                              v           v
          |                        join_manifest  ------>  measurements/day2-join.json
          v
 ingest_manifest  ---------->   measurements/day1-ingest.json
          |                     narrative_scrub  ----->  measurements/day2-scrub.json
          |
          +--> manifest_counts_match_parquet       +--> join_manifest_matches_parquet
          +--> manifest_holds_no_wall_time         +--> event_counties_are_unique_and_real
                                                   +--> day2_manifests_hold_no_wall_time
```

## The county key, which was not the one the plan assumed

NOAA codes an event against either a county or a National Weather Service forecast
zone. `CZ_TYPE` says which. On a zone row `CZ_FIPS` is a zone number and that is a
different namespace from a county code.

The two collide. Arkansas zone 3 is BOONE and Arkansas county 3 is Ashley. Over these
five years 55,933 zone rows carry a number that is a real county code in the same
state and names a different place. So joining on `STATE_FIPS` and `CZ_FIPS` does not
miss the zone rows. It mis-joins them, and nothing in the output says so.

The fix is the NWS zone-county correlation file, which maps a zone to every county it
covers. That mapping is one to many, up to ten counties for one zone, so the join
output is longer than its input. 336,014 events become 341,869 event-county pairs.

Two smaller things fell out of the same work. `STATE_FIPS` is not a FIPS code for four
jurisdictions. NOAA writes its own code and the real one is different, so 2,806 rows
need remapping before any county lookup.

| Jurisdiction | NOAA writes | FIPS is |
|---|---|---|
| Puerto Rico | 99 | 72 |
| Guam | 98 | 66 |
| American Samoa | 97 | 60 |
| Virgin Islands | 96 | 78 |

And 26,320 events still resolve to no county at all. Most of those are marine zones
like the Gulf of Mexico and Lake Erie, which correctly have no county and never will.

## What the FEMA join says, and what it does not

A declaration is not a storm. It is a decision made days or weeks later covering a
county for an incident window, so the test is containment rather than equality.

Run that way across all incident types, 207,706 of the 309,694 events with a county
fall inside a declared window. That reads like a result and it is a pandemic. The
COVID-19 declaration is filed as Biological, covers 3,238 counties and runs 1,207
days, so almost every storm event in three of these five years sits inside it. It is
91.5 percent of all 482,338 matched pairs.

Restricted to weather incident types the figure is 25,745 events. The median weather
declaration window is 6 days against COVID-19's 1,207. The manifest publishes both
numbers and names which one is the pandemic.

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

## Measured on this machine

All of these come from the run above. Every one is re-derived by a check rather than
typed into this file.

| Figure | Value |
|---|---|
| Storm years | 5 |
| Event rows | 336,014 |
| Distinct storm episodes | 50,411 |
| Rows carrying a point location | 199,698 |
| Rows with no point location | 136,316 |
| Rows with a property damage figure | 263,094 |
| Source bytes downloaded | 57,317,148 |
| Event county pairs | 341,869 |
| Events resolved to a county | 309,694 |
| Events resolved to no county | 26,320 |
| Distinct counties reached | 3,212 |
| SPC report county rows | 141,518 |
| FEMA county designations | 14,039 |
| Events inside a weather declaration window | 25,745 |
| Narratives the scrub changes | 708 |
| Checks in the suite | 90 |

`EVENT_ID` is unique across all 336,014 rows and not only within a year, which was
measured rather than assumed.

## The narrative scrub, and the base rate nobody checked first

The plan for this layer said scrub person names from the narratives. The narratives
barely have any.

The honorific shape matches three of the 336,014 event narratives and two of those
three are shop signs. `Mr. Clean Car Wash` is one. `Dr.` matches 272 narratives and 269
of them are Drive in a street name, so reading it as Doctor redacts roads.

What a capitalised pair before a reporting verb catches is places and agencies. `Rock
River measured`. `Highway Patrol reported`. `Cooperative Observer reported`. On the 97
row labelled sample that pattern fires 33 times and carries a person name zero times.
Precision 0 percent. A scrubber built on it would redact the geography this dataset
exists to carry.

What the narratives do hold is the age and sex of people who were killed or injured.
603 event narratives and 1,741 episode narratives carry one, and next to a county and
a date that is the part that identifies somebody. So there are two detectors and they
are reported separately. Collapsing them into one rate would hide that one of them has
almost nothing to find.

The scrub changes 708 of 336,014 narratives. 15 name spans and 848 casualty spans.

`tests/fixtures/labelled_narratives.json` carries the sample and the labels. Two rows
are withheld from it, because they carry the real names of private individuals and
committing the labelled sample for a name scrubber with the names in it would publish
exactly what the scrubber is for. Their labels are offsets into the NOAA text, so they
grade wherever the raw release is present, and the grader reports how many rows it
could not grade rather than quietly scoring a smaller sample.

## What I got wrong

The first version of the storm events contract declared `BEGIN_RANGE` and `END_RANGE`
as integers. They read like counts and NOAA documents them as a distance in miles. The
strict cast refused 168,041 values on the first run, with decimals like `.53` and
`117.53` in the error. Both columns are floats now.

Then the same mistake, one day later, on a different publisher. SPC's `om` column is a
report identifier and I cast it to an integer because 2019 is full of values like
617025. From 2020 onward it is a composite like `2001040929-01`, which is a date and a
time and a sequence. 29,097 rows parse and 108,730 do not. It is text now.

Twice in two days I typed a contract from what a column looked like rather than from
what it is. A permissive reader would have accepted both and produced nulls. The
refusal is the feature and it cost one run each time.

## Known limitations

- The blocking key is settled and it is the county, not the point. 136,316 of 336,014
  rows have no latitude or longitude, and a point key would drop all of them. The
  crosswalk reaches 309,694 events instead of 199,698. What it cannot reach is 26,320
  events, and a split built on county blocks will exclude them rather than guess.
- A zone covering several counties means one event appears in several counties, so a
  storm-level split has to group on the event and never on the pair. 341,869 pairs over
  309,694 events is the fan-out a split has to respect.
- The scrub is graded on the sample it was tuned against. Both detectors score 100
  percent precision and recall there, and the guards that produce that score were added
  because of cases in that same sample. So those two figures measure the tuning. The
  number that was measured before any guard existed is the naive baseline's 0 percent.
- The scrub covers names and casualty descriptors and not addresses. Narratives carry
  street addresses and home towns, and `722 Ferry Street` next to an age and a county is
  identifying. That is unmeasured and not implemented.
- The FEMA join says a declaration covered this county on this date. It does not say
  this event caused that declaration, and the manifest does not claim it.
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
stormfactory/geo.py        zone to county, and the STATE_FIPS that is not FIPS
stormfactory/spc.py        the three SPC report types, normalised
stormfactory/fema.py       OpenFEMA declarations, and the pandemic in them
stormfactory/joins.py      the two join conditions, equality and containment
stormfactory/scrub.py      the two detectors and the redaction
stormfactory/grading.py    scoring the scrub against the labelled sample
stormfactory/assets.py     nine Dagster assets and five asset checks
tests/fixtures/            the labelled narrative sample and the refusal rows
stormfactory/paths.py      where data and release files live
scripts/build_release.py   build every partition headlessly
tests/run_all.py           the whole suite, no test framework needed
```

Run the suite with `python tests/run_all.py`. Browse the asset graph with
`dagster dev -m stormfactory.definitions`.
