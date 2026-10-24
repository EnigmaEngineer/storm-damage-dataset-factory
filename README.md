# Storm-Damage Research Dataset Factory

Builds traceable research datasets from public storm and damage records, so that an
experiment can say exactly which rows it trained on. One command materialises every
partition, and a partition that fails its contract is never written.

```
python scripts/build_release.py
```

Release v0.2 is ten Parquet artifacts. None of them are in git. What git carries is
`measurements/release.json` and one DVC pointer, and between them they are enough to
say whether a rebuild produced the same dataset. v0.1 was nine artifacts and the
imagery layer is the tenth, so the version moved rather than the name covering two
different datasets.

## What is real and what is synthetic

Every row of data in this repo is real and public. It comes from NOAA's Storm Events
Database, served as gzipped CSV by the National Centers for Environmental Information.
Nothing is generated or simulated.

The only invented rows live in `tests/test_schema.py`. They exist to drive the
contract's refusals and they never reach a Parquet file.

## What is here so far

The Storm Events layer for five storm years, 2019 through 2023. Storm Prediction
Center reports and OpenFEMA declarations joined to it by county and date. A scrub of
the personal detail in the narratives. Release v0.2 under DVC, with three train and
test splits beside it. Aerial imagery tiles over one event's damage track, before and
after, and a labelling pass over them.

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

`sha256` is over the file's bytes. It says whether this is the file the release published,
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

Seven of the nine artifacts v0.1 published rebuilt bit for bit in a clean clone. Two did not, and the
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

After that, every artifact rebuilds bit for bit.

```
python3 scripts/verify_rebuild.py ../clean-clone
release v0.2 against the rebuild at ../clean-clone
digest digest-v1 against digest-v1
  event_counties       same data, same bytes, rows 341869 against 341869
  event_splits         same data, same bytes, rows 336014 against 336014
  fema_declarations    same data, same bytes, rows 14039 against 14039
  imagery_tiles        same data, same bytes, rows 37 against 37
  spc_reports          same data, same bytes, rows 141518 against 141518
  storm_events_2019    same data, same bytes, rows 67864 against 67864
  storm_events_2020    same data, same bytes, rows 61281 against 61281
  storm_events_2021    same data, same bytes, rows 61389 against 61389
  storm_events_2022    same data, same bytes, rows 69887 against 69887
  storm_events_2023    same data, same bytes, rows 75593 against 75593
writers here:    Polars (python) version 2.0.0 (build 22a147de3d2bb2e44b97338a2510816c7105c9f2)
writers there:   Polars (python) version 2.0.0 (build 22a147de3d2bb2e44b97338a2510816c7105c9f2)
all 10 artifacts agree bit for bit and by content
```

## The splits, and the leak the random one has

The plan here was that one baseline trained on a random cut and on a blocked cut would
give a gap, and that the gap would be the leakage. It is not, and the section after this
one has the measurement that says so. The splits still come first and all three live in
one artifact, `data/parquet/event_splits/part.parquet`.

A storm here is the NOAA episode and not the event. An episode is one weather system
and its events are that system observed in different places and of different types,
so two events of one episode are the same storm filed twice. The county resolution
fans an event out further still.

| Scheme | Grouped on | Episodes cut in two | States cut in two | Test events in a storm train has seen |
|---|---|---|---|---|
| random_split | nothing, one event at a time | 27,907 | 67 | 93.75 percent |
| storm_split | the episode | 0 | 67 | 0 percent |
| blocked_split | the state | 0 | 0 | 0 percent |

The first row is an exposure count and not an effect. Under a random cut, 47,553 of the
50,724 test events sit in a storm the training set has already seen. That says a model
is free to remember rather than generalise. It does not say how much remembering is
worth, and the next section measures that separately because the two turned out to be
very different sizes.

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

## What the baseline says the random cut actually costs

One model, `HistGradientBoostingClassifier` at a hundred iterations and a fixed seed,
fitted once per arm. The target is whether an event carries recorded property damage
above zero. It is read off the parsed damage column, and the 72,920 events with no
damage value in the source at all are dropped rather than called zeroes, which leaves
the 263,094 rows the table above already counts. 62,961 of those carry damage.

Fourteen features, all of them properties of the event. Assembled by hand from the
column list rather than captured from a command, so they sit in a table.

| Group | Columns |
|---|---|
| when | month and hour of day and length in minutes |
| what | event type and zone type |
| how big | magnitude and its type and the three tornado columns |
| where | begin latitude and longitude and whether a point exists and whether a county resolved |

The excluded list is published in `measurements/baseline.json` with a reason against
each name, because a feature list is only honest next to what it refuses.

Two of those refusals decide the result. The event and episode identifiers are out
because an identifier is how a model memorises a row. Every categorical place label is
out for a different reason, which covers the state, the forecast office and the county
name. An unseen state is not a degraded feature. It is an absent one, and every blocked
test row would carry one, so keeping it would make the gap a measurement of lost feature
support. A latitude stays, because a tree can still split either side of it.

| Arm | Trained on | Test rows | Test rows sharing a storm with train | ROC AUC | Average precision |
|---|---|---|---|---|---|
| random_split | 184,120 | 39,714 | 36,878 | 0.9545 | 0.8909 |
| storm_split | 183,109 | 40,072 | 0 | 0.9446 | 0.8631 |
| blocked_split | 178,477 | 43,185 | 0 | 0.9244 | 0.7956 |

The figure the plan asked for is 0.9545 against 0.9244, so a gap of 0.0301 AUC. Three
things move between those two arms and only one of them is leakage. The test sets hold
different events. The share of rows whose damage field is absent differs between the
cuts, 21.7 percent of the random test set against 14.3 of the blocked one. And the
blocked training set holds none of the states its test set holds.

Splitting the gap at the storm cut separates two of those. The storm cut keeps the state
mix and removes sibling events, so random to storm is 0.0099 and storm to blocked is
0.0202. Two thirds of the headline sits in the spatial term, which is the cost of
predicting into states the model never saw rather than anything a random cut did wrong.

### The matched experiment

None of the three arms above is a controlled comparison, because each one scores a
different set of rows. So the leak gets its own experiment, and it holds the evaluation
set and the training size fixed and varies one thing.

The storm cut is the base, because under it a test episode's state is still represented
in training. Its test episodes are split by the parity of each event's rank inside its
own episode. The odd ranks are the evaluation set at 23,809 rows and 5,259 positives.
The even ranks are 16,263 sibling events of those same storms.

Parity is deterministic and it is not random, so the two halves could have differed
systematically and the gap would have carried that. They do not. The evaluation half is
22.09 percent positive against 22.18 for the siblings, and the widest
disagreement between them on any event type is 1.72 percentage points.

| Arm | Training rows | Of which siblings of an evaluation storm | Evaluation rows sharing a storm | ROC AUC | Average precision |
|---|---|---|---|---|---|
| without siblings | 183,109 | 0 | 0 | 0.9416 | 0.8554 |
| with siblings | 183,109 | 16,263 | 19,352 | 0.9449 | 0.8641 |
| base only, no siblings | 166,846 | 0 | 0 | 0.9428 | 0.8581 |

The first two arms train on 183,109 rows and all three are scored on the
same 23,809. The leaky arm spends 16,263 of its budget on siblings, so it
gives up that many rows of the base. A check asserts the first two sizes and all three
evaluation sets are equal, because the whole claim of the experiment is that nothing
else moved.

The third arm is there because the first two leave an obvious objection open. The leaky
arm does not only gain siblings. It also gives up 16,263 ordinary rows, and a reader
is entitled to ask whether the gap is the siblings arriving or those rows leaving. The
third arm is that same reduced base with nothing added. Putting 16,263 sibling rows
on it is worth 0.9428 to 0.9449. Putting the same number of ordinary rows on it is
worth 0.9428 to 0.9416, which is nothing, and slightly less than nothing. At this
training size more ordinary rows do not move the score and sibling rows do.

The equal size gap is 0.0033 AUC and 0.0087 average precision. Holding the
base identical instead gives 0.0021. So the sibling premium sits between those two
framings, and the headline gap of 0.0301 is around 9 times either of them.

So 93.75 percent of test events sharing a storm with training sounds like a ruined
benchmark and costs about a third of a point of AUC. Both of those are true and the
exposure count is the one that reads as alarming.

### What the gap does not settle

The sibling arm gains the same storms and nothing else, so 0.0026 is a measurement of
sibling membership at this model and this feature list. A model with the narrative text
in it, or one keyed on the county, has more to memorise and would not inherit this
number. The figure belongs to the baseline rather than to the dataset.

The ranking metrics are deterministic given the artifacts, the feature list and the
seed. Two fits in one session agree to four decimal places with threads pinned to one.
They are not a property of the data alone, and that is measured rather than hedged. The
same commit on `scikit-learn` 1.9.1 with `numpy` 2.5.3 returns a blocked AUC of 0.9253
and a matched gap of 0.0026, against 0.9244 and 0.0033 on the declared pair. Same code,
same rows, same seed. So every figure here names the stack in `requirements.txt` and
nothing else.

A check cannot re-derive a metric without refitting, which is taking the measurement
again rather than grading it. So the checks re-derive every count in the manifest and
compute every gap from the published metrics, and that boundary is in the manifest.

## The imagery, and why a declared footprint picked the wrong sortie

NOAA's National Geodetic Survey flies aerial imagery after a landfalling storm and
publishes each sortie as a tile pyramid. The catalogue across 28 storms holds
208 collections, and 115 of them declare a bounding box. The other
93 are only visible as tile layer arguments inside the viewer's own script, because
the older storms publish no service document at all. Both shapes are parsed and each
collection records which one it came from.

The event is tornado 979262. An EF0 that crossed Hancock County in
Mississippi during Hurricane Ida. It is the highest recorded property damage among
release events that hold a point and that the catalogue can supply both a before frame
and an after frame for, which is 26 events in all. That count is a lower bound. The
candidate pass prefilters on the declared box, because probing coverage for every event
in the release would be hundreds of thousands of requests, so the 93 collections that
publish no footprint can never put an event into it. NOAA surveyed it remotely from
satellite imagery and put street names in the narrative, so the ground truth for this one
is unusually specific about where to look.

The grid is cut along the surveyed track rather than boxed around it. A tile is kept when
its centre is within half the surveyed path width plus half a tile of the line from the
begin point to the end point, which is 223.4 m here, and that leaves 37 tiles at zoom 17.

**Every one of the 7 candidate sorties declares a footprint containing the whole track.
Measured coverage over those 37 tiles ranges from none of them to all of them.**

| Collection | Frame | Flown | Tiles held of 37 |
|---|---|---|---|
| 20210902a | after | 2021-09-02 | 37 |
| 20210831b | after | 2021-08-31 | 9 |
| 20210831a | after | 2021-08-31 | 0 |
| 20210902b | after | 2021-09-02 | 0 |
| 20201030b | before | 2020-10-30 | 37 |
| 20201029c | before | 2020-10-29 | 27 |
| 20201029a | before | 2020-10-29 | 18 |

A declared box is the hull of a sortie's flight lines, so most of the box can hold no
imagery. The tile server does not help either. It answers a tile it has no imagery for
with a redirect to a transparent placeholder and an HTTP 200, so a run reading status
codes concludes it has imagery everywhere. The placeholder is identified by its hash,
measured once and written into `imagery.lock.json`, and a tile matching it is never
written to disk.

Selection asks with a HEAD, because the redirect survives one and probing every
candidate with a GET would pull several collections of imagery to keep one. The chosen
pair is then fetched and hashed, and the two verdicts are compared over every tile. They
agree on all of them, which is what makes the cheap question safe to select on.

The sortie this release uses is `20210902a`, flown 3 days after the event. The before
frame is `20201030b`, flown 304 days before it. **Choosing by date rather than by measured
coverage would have taken `20210831b`, two days after the event, and produced a project
with 9 answerable tiles instead of 37.** Coverage is ranked first and the date only breaks
a tie.

The before frame is itself a storm sortie. NOAA does not fly a county for no reason, so
the only pre-event imagery at this resolution is the response to an earlier storm, and
here that is Hurricane Zeta. Before means before Ida. It does not mean undamaged.

## The labelling project, and the agreement that is not reported

The labelling unit is a tile pair and not a tile. An annotator looking at the after frame
alone cannot tell a roof that lost its shingles from a roof that was always that colour.
`labelstudio/label_config.xml` puts the two frames side by side and asks one question with
four answers, and `labels/tile_labels.json` is the pass over all 37 answerable tiles.

| Label | Tiles |
|---|---|
| none | 15 |
| possible | 20 |
| clear | 1 |
| not_assessable | 1 |

**Agreement is unmeasured and it will stay that way until a second annotator exists.**
There is one, so there is no second set of labels to agree with, and a single annotator
scored against themselves is not a reliability figure. `labelling.agreement` returns the
refusal rather than a number, an asset check asserts that it does, and `cohens_kappa` is
written and tested against fixtures so the path is real code rather than a promise.

The one annotator is an automated run, which is recorded in the export beside the labels.
These are a worked example of the path from sortie to project to graded sample. They are
not an expert ground truth and nothing here treats them as one.

The distribution is the honest result. One tile carries tarps across an apartment roof and
a row of houses, which is the blue roof signature and is not ambiguous. One is open water.
The other 35 split between nothing a storm explains and a difference that could be
damage or could be the ten months and the different sun angle between the two frames. At
1.0313 m per pixel a house is about fifteen pixels across, and thinning canopy beside an
intact roof is exactly the case a single pass cannot settle.

## The timestamp, and what the zone field is worth

`event_begin_utc` is the first time zone aware column any artifact here carries, and it is
the reason `digest.py` grew a rule for timestamps before the artifact was written.

NOAA writes the local clock time in `BEGIN_DATE_TIME` and names the zone in `CZ_TIMEZONE`
as an abbreviation with an offset, `CST-6`. The offset is applied literally and it is not
corrected, because the field is wrong in both directions and no correction derived from it
would be right. 130,942 rows name a standard zone on a June to August date, which is
daylight time across most of the country. 282 rows name a daylight zone, and 103 of
those sit in December to February, where no daylight offset exists at all.

So the column is the instant under the offset the row states. For this event that reads
2021-08-30T03:11:00+00:00 from a local 29-AUG-21 21:11:00 and a stated CST-6, and late August in
Mississippi is daylight time, so the true instant is likely an hour earlier. That is
within the gap between the event and the sortie by three orders of magnitude, so it
changes nothing here and it would matter to anything joining on an hour.

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
| Release artifacts | 10 |
| Release artifact bytes | 31,153,566 |
| States and territories | 69 |
| SPC distinct reports | 140,621 |
| SPC om numbers reused between report types | 6,526 |
| Episodes a random split cuts in two | 27,907 |
| Episodes a storm split cuts in two | 0 |
| Events carrying damage above zero | 62,961 |
| States a blocked split cuts in two | 0 |
| Imagery collections in the catalogue | 208 |
| Imagery collections declaring a footprint | 115 |
| Storms whose imagery catalogue was read | 28 |
| Events the catalogue frames before and after | 26 |
| Tiles over the damage track | 37 |
| Tiles over the track at one zoom deeper | 102 |
| Tiles holding both frames | 37 |
| Tile pairs labelled | 37 |
| Annotators | 1 |
| Rows naming a standard zone in summer | 130,942 |
| Rows naming a daylight zone | 282 |
| Rows naming a daylight zone in winter | 103 |
| Checks in the suite | 216 |

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
  checkout. That directory is created by whichever machine ran the roundtrip and it is
  not in git, so a fresh clone has no remote to pull from and has to rebuild the release
  from the raw sources. The rebuild is one command and takes about a minute. It is still
  not what a release that claims to be retrievable should need.
  TODO: swap that url for a GCS bucket once the OIDC trust for CI exists.
  Terraform for the bucket and the service account is not written yet.
- The leakage gap belongs to this baseline and not to the dataset. 0.0033 AUC is
  what sibling events of the test storms buy a gradient boosted tree reading fourteen
  event properties. A model given the narratives, or keyed on the county, has more to memorise
  and would not inherit that figure.
- The ranking metrics are not verified across machines. They are deterministic given the
  artifacts, the feature list and the seed, and two fits in one session agreed to four
  places with threads pinned to one. Nothing has run them on a second machine.
- A numeric feature with fewer than two distinct values in a training arm is refused by
  name rather than binned. All eight satisfy that over the full release. A narrower slice
  of one year or one state need not. The refusal is what makes that legible instead of a
  stride error from inside the estimator.
- The bit-for-bit claim is a claim about one polars build. Every artifact rebuilds
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
- `digest.py` has a rule for six kinds of column and refuses everything else. The six
  are integers and floats and booleans and dates and strings and timestamps. The
  timestamp rule was written the day imagery metadata arrived and it hashes the
  integer the column physically holds, so the time unit and the zone live in the dtype
  string rather than in a conversion. A Decimal, a Categorical or a nested column still
  stops the release rather than being hashed arbitrarily.
- No published standard was found that canonicalises typed tabular data for hashing.
  The candidates are canonicalisation schemes for other shapes, JSON and RDF, and the
  table formats that do carry hashes hash files rather than content. So `digest-v1` is
  this repo's rule and a second implementation has to follow the module. That search
  is a search and not a proof, and a standard that exists and was missed would be
  worth adopting over this.
- The failing branch of `no_storm_spans_two_splits` has never fired on real data. It
  is driven from both sides in `tests/test_splits.py` on a small frame, and on the real
  release it reports the random column's 27,907 beside the two zeros so the measurement
  is visibly working rather than merely quiet.
- The DVC remote has only ever been a directory on the same filesystem. Nothing here
  has exercised a pull over a network.
- The imagery tiles are not in the release. What is published is the metadata, the two
  content hashes per tile and the URL each one came from. The pixels stay on NOAA's
  tile server, so a rebuild depends on that server continuing to answer and on the
  collection continuing to hold the same bytes. Nothing here mirrors them.
- The Label Studio project has never been loaded into Label Studio. A scheduled run has
  no browser, so the committed labels were made from contact sheets instead and the
  export is written in Label Studio's task shape rather than confirmed by a server. The
  checks assert the config parses, that it offers exactly the label set, and that every
  field it renders is present in every task. None of that is the same as the project
  opening.
- One annotator, so agreement is unmeasured and the label distribution is one person's
  reading. Twenty of the thirty-seven tiles are `possible`, which is a statement about
  what a single pass at one metre per pixel over a ten month gap can settle, and a
  second annotator is what would turn it into a number.
- The before frame is the response to an earlier storm, because that is the only
  imagery at this resolution that predates the event. It is pre-Ida and it is not
  pre-damage.
- The tile grid is cut at zoom 17 because 37 pairs is what one labelling pass covered.
  Both sorties publish to zoom 19, and the same corridor holds 102 pairs one zoom
  deeper at half the ground sample distance. The shallower grid is a labelling budget
  and not a resolution judgement.
- The imagery layer covers one event out of 336,014. It proves the path from a sortie
  to a graded sample and it is not a dataset.

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
stormfactory/baseline.py   the damage baseline and the matched leakage experiment
stormfactory/imagery.py    the sortie catalogue, the tile grid and the coverage probe
stormfactory/labelling.py  the Label Studio project and the agreement guard
stormfactory/assets.py     the Dagster assets and their checks
tests/fixtures/            the labelled narrative sample and the refusal rows
labelstudio/               the labelling config the project is built from
labels/                    the labelling pass over the tile pairs
stormfactory/paths.py      where data and release files live
scripts/build_release.py   build every partition headlessly
scripts/verify_rebuild.py  compare a rebuild against the release manifest
scripts/contact_sheets.py  lay the tile pairs out for a pass without a browser
scripts/dvc_roundtrip.sh   push to the dvc remote and pull back into an empty checkout
tests/run_all.py           the whole suite, no test framework needed
```

Run the suite with `python tests/run_all.py`. Browse the asset graph with
`dagster dev -m stormfactory.definitions`.

DVC is a command line tool and nothing in `stormfactory` imports it, so it is pinned
in `release-tools.txt` rather than in `requirements.txt`. It also does not have to
share an interpreter with the project, and on the machine this was built on it could
not. See the note at the top of `scripts/dvc_roundtrip.sh`.
