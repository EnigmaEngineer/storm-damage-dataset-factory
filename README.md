# Storm-Damage Research Dataset Factory

Builds traceable research datasets from public storm and damage records, so that an
experiment can say exactly which rows it trained on. One command materialises every
partition, and a partition that fails its contract is never written.

```
python scripts/build_release.py
```

Release v0.1 is nine Parquet artifacts. None of them are in git. What git carries is
`measurements/release.json` and one DVC pointer, and between them they are enough to
say whether a rebuild produced the same dataset.

## What is real and what is synthetic

Every row of data in this repo is real and public. It comes from NOAA's Storm Events
Database, served as gzipped CSV by the National Centers for Environmental Information.
Nothing is generated or simulated.

The only invented rows live in `tests/test_schema.py`. They exist to drive the
contract's refusals and they never reach a Parquet file.

## What is here so far

The Storm Events layer for five storm years, 2019 through 2023. Storm Prediction
Center reports and OpenFEMA declarations joined to it by county and date. A scrub of
the personal detail in the narratives. Release v0.1 under DVC, with three train and
test splits beside it. Imagery tiles come later.

The asset graph. This is a drawing and no command prints it, so it is indented
rather than fenced.

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

     storm_events[year] ----> event_splits  ------->  data/parquet/event_splits/part.parquet
                                    |                 measurements/splits.json
                                    |
                                    +--> no_storm_spans_two_splits
                                    +--> blocked_split_keeps_each_state_whole
                                    +--> every_event_is_assigned_once

     every artifact ---------> release_manifest  --->  measurements/release.json
                                    |                  byte hash and content digest each
                                    |
                                    +--> release_manifest_matches_the_data
                                    +--> day3_manifests_hold_no_wall_time


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

## Two hashes per artifact, because they answer different questions

A rebuild of this release was meant to be checked by hashing the Parquet files and
comparing. That turned out to check two things at once.

Polars writes its own version and build id into the Parquet footer. Every artifact
here carries `Polars (python) version 2.0.0` in its last few kilobytes. So the byte
hash of an artifact is a hash of the data and of the library that wrote it, and a
rebuild under a different polars build is bound to produce a different hash from
identical rows.

So each artifact in `measurements/release.json` carries two hashes.

`sha256` is over the file's bytes. It says whether this is the file v0.1 published,
and it only reproduces under the writer named beside it.

`content_sha256` is computed in `stormfactory/digest.py` from the rows themselves. It
ignores compression, row group layout and file metadata. Floats go in as their
IEEE-754 bit patterns rather than as decimal text, because a decimal rendering is one
more thing a library version can change underneath you. Strings go in with length
prefixes, so no separator can be smuggled inside a narrative.

The digest is deliberately sensitive to column names, dtypes, null positions and row
order. That last one is why every artifact declares a sort key in
`stormfactory/release.py` and why a key that does not identify a row is refused. A
digest over rows whose order is not pinned is a number that moves for no reason.

`scripts/verify_rebuild.py` compares two release manifests and judges the two hashes
differently. A content digest that disagrees is a failed rebuild. Byte hashes that
disagree under different writers are expected. Byte hashes that disagree under the
SAME writer are a finding, and that is what the first rebuild found.

## What the first rebuild found

Seven of nine artifacts rebuilt bit for bit in a clean clone. Two did not, and the
polars build was identical on both sides, so the writer was not the reason.

`spc_reports` and `fema_declarations` held exactly the same rows in a different order.
The content digests agreed and the bytes did not. Two separate causes.

The first is `unique` in `stormfactory/spc.py`. Polars is free to return the rows of a
`unique` in any order unless it is told otherwise, and it does. Two calls on one file
inside one process returned frames holding the same rows in different orders.

The second is OpenFEMA. The API pages its records in no stated order, so a second
download arrives shuffled and the Parquet built from it is a record of the download
rather than of the data.

The fix is one rule rather than two patches. Every release artifact is now written
through `release.write_artifact`, which sorts by the key the registry declares for it.
The row order of a published artifact is a property of the data and of nothing else.
`unique` also got `maintain_order=True`, so the function is deterministic on its own
rather than only through the writer.

After that, all nine artifacts rebuild bit for bit.

```
python3 scripts/verify_rebuild.py ../clean-clone
release v0.1 against the rebuild at ../clean-clone
digest digest-v1 against digest-v1
  event_counties       same data, same bytes, rows 341869 against 341869
  event_splits         same data, same bytes, rows 336014 against 336014
  fema_declarations    same data, same bytes, rows 14039 against 14039
  spc_reports          same data, same bytes, rows 141518 against 141518
  storm_events_2019    same data, same bytes, rows 67864 against 67864
  storm_events_2020    same data, same bytes, rows 61281 against 61281
  storm_events_2021    same data, same bytes, rows 61389 against 61389
  storm_events_2022    same data, same bytes, rows 69887 against 69887
  storm_events_2023    same data, same bytes, rows 75593 against 75593
all 9 artifacts agree bit for bit and by content
```

## The splits, and the leak the random one has

The next thing this repo gets is the same baseline trained on a random split and on
a blocked split. The gap between them is the leakage. So the splits come first and all
three live in one artifact, `data/parquet/event_splits/part.parquet`.

A storm here is the NOAA episode and not the event. An episode is one weather system
and its events are that system observed in different places and of different types,
so two events of one episode are the same storm filed twice. The county resolution
fans an event out further still.

| Scheme | Grouped on | Episodes cut in two | States cut in two | Test events in a storm train has seen |
|---|---|---|---|---|
| random_split | nothing, one event at a time | 27,907 | 67 | 93.75 percent |
| storm_split | the episode | 0 | 67 | 0 percent |
| blocked_split | the state | 0 | 0 | 0 percent |

The first row is the number worth reading. Under a random cut, 47,553 of the 50,724
test events sit in a storm that the training set has already seen. A model does not
have to generalise to score on that. It has to remember.

The spatial unit is the state because it is the coarsest unit every event in this
release has. 136,316 rows carry no latitude, so any block built from a point drops
them. The state code is on every row.

Whole states cannot hit a 70, 15 and 15 target exactly. `assign_grouped` fills largest
group first into whichever split is furthest behind, which is bounded by the largest
group's share of the whole and lands at 70.00, 15.00 and 15.00 here. Hashing each
state on its own was the first attempt and it misses by up to ten points, because
Texas alone carries 26,212 events.

The two constraints do not fight each other and that was measured rather than
assumed. No episode in these five years crosses a state line, and neither does any
event's set of counties. So the blocked split keeps storms whole for free. A check
asserts it, and the day a sixth year carries a derecho across a state border that
check fails and the blocking unit has to be built from both.

This is a block and not a fence. Two counties either side of a state line are still
neighbours, so spatial adjacency survives at the borders. The 26,320 events with no
county are assigned and flagged rather than dropped, because they carry a state and an
episode and nothing county keyed can use them.

## Checking a download against something other than itself

Both lock files were written by hashing whatever was already on disk. That is worth
something on the second run and nothing on the run that writes the lock. A download
truncated by a dropped connection gets hashed as it stands and recorded as the truth.

Re-downloading everything to compare costs 57 MB a run. Three cheaper routes now run
before a hash reaches a lock, and each source gets the one its format and its
publisher allow. The route is recorded beside the hash.

| Source | Route | What it costs |
|---|---|---|
| NOAA details files | the gzip stream's own CRC32 and length, plus the remote byte count | five HEAD requests |
| Census county codes | the remote byte count and a ranged GET of the last 64 KB | one HEAD and 64 KB |
| NWS zone crosswalk | a second download of the whole file, compared by hash | 350 KB |

The crosswalk gets the expensive route because weather.gov leaves no choice. It serves
that file chunked with no `Content-Length`, advertises no `Accept-Ranges`, and answers
a `Range` header with 200 and the whole body rather than 206. The lock records why.

The gzip route is the strongest of the three and it is the cheapest. Decompressing a
file checks its own CRC32 and its stored length with no network at all, and it catches
a flipped byte in the middle rather than only a short file. The ranged route cannot do
that, and a check in `tests/test_release.py` asserts the limit rather than leaving a
reader to assume more.

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
| Release artifacts | 9 |
| Release artifact bytes | 31,140,831 |
| States and territories | 69 |
| SPC distinct reports | 140,621 |
| SPC om numbers reused between report types | 6,526 |
| Episodes a random split cuts in two | 27,907 |
| Episodes a storm split cuts in two | 0 |
| States a blocked split cuts in two | 0 |
| Checks in the suite | 145 |

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
time and a sequence. It is text now.

The pair of counts that used to sit in this paragraph was 29,097 and 108,730 and both
are gone. Neither re-derives from the committed sources, and the two of them add up to
fewer rows than the files hold. Measured over the 140,621 distinct reports, 29,122
carry the integer form and 111,499 carry the composite one. Those two are in
`measurements/spc-om.json` and a check re-derives them.

`tests/fixtures/spc_om_forms.csv` carries three of each form, so the refusal and the
shape of the composite identifier both grade in a clean clone with no raw data.

A second thing about `om` turned up while fixing it. It is not unique. SPC reuses an
om between report types, 6,526 times across these five years, so the join manifest's
`distinct_reports` was counting om values and undercounting reports by 6,614. The key
is om plus report type. Both figures are published now and the old name says what it
actually counts.

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
- Nothing is in Cloud Storage yet. The DVC remote in `.dvc/config` is a directory beside
  the repo, and `scripts/dvc_roundtrip.sh` pushes to it and pulls back out of an empty
  checkout. TODO: swap that url for a GCS bucket once the OIDC trust for CI exists.
  Terraform for the bucket and the service account is not written yet.
- The bit-for-bit claim is a claim about one polars build. All nine artifacts rebuild
  byte for byte under `polars 2.0.0`, and the footer carries a build hash rather than
  only a version, so a wheel rebuilt from the same tag would move every byte hash
  while every content digest held. The content digest is the one that survives.
- Nothing here has run on Python 3.10, which `pyproject.toml` declares as the floor.
  Every figure in this file was measured on 3.13. A CI matrix is not written yet.
- The content digest holds a frame in memory to sort it. The largest artifact is
  336,014 rows and that is fine. It is not a streaming implementation and it would
  need to become one before this release grows by an order of magnitude.
- A split is only as blocked as its unit. State blocks leave adjacency at every state
  border, and a model that learns a regional signal can still carry it across one.
- The content digest is defined by `stormfactory/digest.py` and by nothing else. It is
  independent of the Parquet writer and completely dependent on this repo. No other
  tool can verify a `content_sha256` without reimplementing those rules, so it proves
  a rebuild here agrees with a release here and it is not a portable identifier.
- `digest.py` has a rule for five kinds of column and refuses everything else. The
  five are integers and floats and booleans and dates and strings. That refusal is
  deliberate and it is also a trap waiting for the first artifact carrying a timestamp
  or a nested column. It stops the release rather than hashing something arbitrary,
  and the rule has to be written before imagery metadata lands.
- The failing branch of `no_storm_spans_two_splits` has never fired on real data. It
  is driven from both sides in `tests/test_splits.py` on a small frame, and on the real
  release it reports the random column's 27,907 beside the two zeros so the measurement
  is visibly working rather than merely quiet.
- The DVC remote has only ever been a directory on the same filesystem. Nothing here
  has exercised a pull over a network.

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
stormfactory/digest.py     the writer independent content digest
stormfactory/splits.py     the three split schemes and the span checks
stormfactory/integrity.py  checking a download against something other than itself
stormfactory/release.py    the artifact registry and the release manifest
stormfactory/assets.py     twelve Dagster assets and eleven asset checks
tests/fixtures/            the labelled narrative sample and the refusal rows
stormfactory/paths.py      where data and release files live
scripts/build_release.py   build every partition headlessly
scripts/verify_rebuild.py  compare a rebuild against the release manifest
scripts/dvc_roundtrip.sh   push to the dvc remote and pull back into an empty checkout
tests/run_all.py           the whole suite, no test framework needed
```

Run the suite with `python tests/run_all.py`. Browse the asset graph with
`dagster dev -m stormfactory.definitions`.

DVC is a command line tool and nothing in `stormfactory` imports it, so it is pinned
in `release-tools.txt` rather than in `requirements.txt`. It also does not have to
share an interpreter with the project, and on the machine this was built on it could
not. See the note at the top of `scripts/dvc_roundtrip.sh`.
