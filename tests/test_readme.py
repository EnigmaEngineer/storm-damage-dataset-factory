"""Every figure the README states about this repo, re-derived from the repo.

A number a document publishes about itself is the cheapest kind of stale number to
prevent, so none of these are typed in two places.
"""

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _readme_table() -> dict[str, int]:
    rows = {}
    for line in (REPO_ROOT / "README.md").read_text().splitlines():
        m = re.match(r"\|\s*([A-Za-z][^|]+?)\s*\|\s*([0-9,]+)\s*\|$", line)
        if m:
            rows[m.group(1)] = int(m.group(2).replace(",", ""))
    return rows


def _manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "day1-ingest.json").read_text())


def _lock() -> dict:
    return json.loads((REPO_ROOT / "source.lock.json").read_text())


def _join_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "day2-join.json").read_text())


def _scrub_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "day2-scrub.json").read_text())


def _release_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "release.json").read_text())


def _splits_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "splits.json").read_text())


def _om_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "spc-om.json").read_text())


NUMBER_WORDS = {
    9: "nine", 10: "ten", 11: "eleven", 12: "twelve", 13: "thirteen", 14: "fourteen",
}


def _readme_prose() -> str:
    """The README with its line wrapping collapsed.

    Every paragraph here is hard wrapped at about 90 characters, so a sentence a check
    looks for is usually split across two lines. A substring test against the raw text
    fails on the wrap rather than on the content, which reads as a finding and is not.
    """
    return re.sub(r"\s+", " ", (REPO_ROOT / "README.md").read_text())


def _embeddings_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "embeddings.json").read_text())


def _graph_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "graph.json").read_text())


def _baseline_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "baseline.json").read_text())


def _readme_arm_rows() -> dict[str, list[str]]:
    """The six column arm tables, keyed on the first cell.

    The two column regex above cannot see these, which is why they get their own reader
    rather than being left ungraded because the first one did not match them.
    """
    rows = {}
    for line in (REPO_ROOT / "README.md").read_text().splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")] if line.startswith("|") else []
        if len(cells) == 6 and cells[0] not in ("Arm", "---"):
            rows[cells[0]] = cells[1:]
    return rows


def check_the_table_was_parsed_at_all():
    # Without this, a README whose table stopped matching the regex would make every
    # check below pass against an empty mapping.
    assert len(_readme_table()) == 59, _readme_table()


def check_the_county_join_counts():
    t = _readme_table()
    j = _join_manifest()
    assert t["Event county pairs"] == j["event_county_pairs"]
    assert t["Events resolved to a county"] == j["distinct_events_with_a_county"]
    assert t["Distinct counties reached"] == j["distinct_counties"]


def check_the_unresolved_count_is_the_difference_and_not_a_second_tally():
    t = _readme_table()
    assert t["Events resolved to no county"] == t["Event rows"] - t["Events resolved to a county"]


def check_the_spc_and_fema_counts():
    t = _readme_table()
    j = _join_manifest()
    assert t["SPC report county rows"] == j["spc"]["report_county_rows"]
    assert t["FEMA county designations"] == j["fema"]["county_designations"]
    assert t["Events inside a weather declaration window"] == j["fema"]["distinct_events_weather_incidents"]


def check_the_scrub_counts():
    t = _readme_table()
    s = _scrub_manifest()["applied_to_the_release"]
    assert t["Narratives the scrub changes"] == s["narratives_changed"]
    text = (REPO_ROOT / "README.md").read_text()
    assert f"{s['name_spans']} name spans and {s['casualty_spans']} casualty spans" in text


def check_the_pandemic_share_in_the_prose_matches_the_manifest():
    share = _join_manifest()["fema"]["pandemic_share_of_all_matches_pct"]
    text = (REPO_ROOT / "README.md").read_text()
    assert f"{share} percent" in text, share


def check_the_naive_baseline_precision_in_the_prose_matches_the_manifest():
    base = _scrub_manifest()["graded_on_the_labelled_sample"]["naive_capitalised_bigram_baseline"]
    text = (REPO_ROOT / "README.md").read_text()
    assert f"fires {base['narratives_it_fires_on']} times" in text, base
    assert f"Precision {base['precision_pct']:.0f} percent" in text, base


def check_row_and_episode_counts():
    t = _readme_table()
    m = _manifest()["total"]
    assert t["Event rows"] == m["rows"]
    assert t["Distinct storm episodes"] == m["episodes"]


def check_point_location_counts():
    t = _readme_table()
    m = _manifest()["total"]
    assert t["Rows carrying a point location"] == m["rows_with_a_point"]
    assert t["Rows with no point location"] == m["rows"] - m["rows_with_a_point"]


def check_damage_count():
    assert _readme_table()["Rows with a property damage figure"] == _manifest()["total"]["rows_with_property_damage"]


def check_year_and_byte_counts():
    t = _readme_table()
    lock = _lock()
    assert t["Storm years"] == len(lock["years"])
    assert t["Source bytes downloaded"] == sum(f["bytes"] for f in lock["files"])


def check_suite_size():
    from tests.runner import collect

    assert _readme_table()["Checks in the suite"] == len(collect())


def check_the_prose_counts_match_the_table():
    # Three figures also appear in sentences. They are the ones most likely to rot.
    text = (REPO_ROOT / "README.md").read_text()
    t = _readme_table()
    assert text.count(f"{t['Event rows']:,}") >= 2
    assert text.count(f"{t['Rows with no point location']:,}") >= 2


def check_the_cast_refusal_count_is_stated():
    assert "168,041" in (REPO_ROOT / "README.md").read_text()


def check_the_cast_refusal_count_is_real():
    # The raw files are not committed, so the derivation only runs where they are
    # present. The figure itself is graded unconditionally by the check above.
    import glob

    import polars as pl

    files = sorted(glob.glob(str(REPO_ROOT / "data" / "raw" / "*.csv.gz")))
    if not files:
        print("skipped, no raw files in data/raw to re-derive 168,041 from")
        return
    total = 0
    for p in files:
        s = pl.read_csv(p, infer_schema_length=0)["BEGIN_RANGE"].drop_nulls()
        total += len(s.filter(~s.str.contains(r"^-?[0-9]+$")))
    assert f"{total:,}" in (REPO_ROOT / "README.md").read_text(), total


def check_the_release_counts():
    t = _readme_table()
    r = _release_manifest()
    assert t["Release artifacts"] == len(r["artifacts"])
    assert t["Release artifact bytes"] == sum(a["bytes"] for a in r["artifacts"].values())


def check_the_split_counts():
    t = _readme_table()
    s = _splits_manifest()
    assert t["States and territories"] == s["states"]
    assert t["Episodes a random split cuts in two"] == s["schemes"]["random_split"]["episodes_in_more_than_one_split"]
    assert t["Episodes a storm split cuts in two"] == s["schemes"]["storm_split"]["episodes_in_more_than_one_split"]
    assert t["States a blocked split cuts in two"] == s["schemes"]["blocked_split"]["states_in_more_than_one_split"]


def check_the_spc_report_counts():
    t = _readme_table()
    j = _join_manifest()["spc"]
    assert t["SPC distinct reports"] == j["distinct_reports"]
    assert t["SPC om numbers reused between report types"] == j["om_numbers_reused_between_report_types"]


def check_the_undercount_in_the_prose_is_the_difference_and_not_a_second_tally():
    j = _join_manifest()["spc"]
    assert f"{j['distinct_reports'] - j['distinct_om_numbers']:,}" in _flowed()


def check_the_leakage_rate_in_the_table_matches_the_manifest():
    leak = _splits_manifest()["schemes"]["random_split"]["leakage"]
    text = _flowed()
    assert f"{leak['share_pct']} percent" in text, leak
    assert f"{leak['test_events_in_a_storm_train_has_seen']:,} of the {leak['test_events']:,}" in text, leak


def _flowed() -> str:
    """The README with its line wrapping removed.

    A figure that lands next to a line break is still in the sentence, and a check
    that reads the file line by line cannot see it. Rewrapping a paragraph is not a
    change to what it says.
    """
    return " ".join((REPO_ROOT / "README.md").read_text().split())


def check_the_om_form_counts_in_the_prose_match_the_manifest():
    counts = _om_manifest()["counts"]
    text = _flowed()
    assert f"{counts['distinct_reports']:,} distinct reports" in text, counts
    assert f"{counts['reports_whose_om_is_an_integer']:,} carry the integer form" in text, counts
    assert f"{counts['reports_whose_om_is_composite']:,} carry the composite" in text, counts


def check_the_superseded_om_pair_is_still_named_in_the_readme():
    """A figure that moved says what it was. Deleting it quietly is the failure."""
    text = (REPO_ROOT / "README.md").read_text()
    old = _om_manifest()["superseded"]["stated_before"]
    assert f"{old['integer']:,}" in text and f"{old['composite']:,}" in text, old


def check_the_split_proportions_in_the_prose_match_the_manifest():
    s = _splits_manifest()
    text = _flowed()
    counts = s["schemes"]["blocked_split"]["events"]
    total = s["events"]
    shares = [f"{100 * counts[name] / total:.2f}" for name in ("train", "val", "test")]
    assert ", ".join(shares[:2]) + " and " + shares[2] in text, shares


def check_the_largest_state_count_in_the_prose_is_real():
    """The Texas figure justifies the greedy fill, so it gets a derivation."""
    import polars as pl

    text = _flowed()
    path = REPO_ROOT / "data" / "parquet" / "event_splits" / "part.parquet"
    if not path.exists():
        print("skipped, no event_splits artifact to re-derive the largest state from")
        return
    biggest = (
        pl.read_parquet(path, columns=["STATE"])
        .group_by("STATE").agg(pl.len().alias("n")).sort("n", descending=True)
    )
    assert f"Texas alone carries {biggest['n'][0]:,} events" in text, biggest.head(1)


def check_the_writer_string_in_the_prose_matches_the_release():
    """The whole two hash argument rests on this string being in the files."""
    writers = _release_manifest()["writers"]
    text = _flowed()
    assert len(writers) == 1, writers
    version = writers[0].split("version ")[1].split(" ")[0]
    assert f"`polars {version}`" in text, writers
    assert f"Polars (python) version {version}" in text, writers


def check_the_baseline_arm_tables_were_parsed_at_all():
    # Six arms across two tables. Without this, a table that stopped matching would
    # make every check below pass against an empty mapping.
    rows = _readme_arm_rows()
    assert sorted(rows) == [
        "base only, no siblings", "blocked_split", "random_split",
        "storm_split", "with siblings", "without siblings",
    ], sorted(rows)


def check_the_baseline_population_is_the_damage_row_count_already_published():
    """The baseline models the rows the README already counts, rather than a second tally."""
    population = _baseline_manifest()["population"]
    assert population["events_carrying_a_damage_value"] == _readme_table()["Rows with a property damage figure"]
    assert population["events_in_the_release"] == _readme_table()["Event rows"]
    assert (
        population["events_dropped_for_an_absent_damage_value"]
        == population["events_in_the_release"] - population["events_carrying_a_damage_value"]
    )


def check_the_positive_count_in_the_table_matches_the_baseline_manifest():
    assert _readme_table()["Events carrying damage above zero"] == _baseline_manifest()["population"]["positives"]


def check_the_scheme_arm_figures_in_the_readme_match_the_manifest():
    rows = _readme_arm_rows()
    schemes = _baseline_manifest()["schemes"]
    for name, stated in schemes.items():
        cells = rows[name]
        assert int(cells[0].replace(",", "")) == stated["train_rows"], (name, cells)
        assert int(cells[1].replace(",", "")) == stated["score_rows"], (name, cells)
        assert int(cells[2].replace(",", "")) == stated["test_rows_in_a_storm_train_has_seen"], (name, cells)
        assert float(cells[3]) == stated["roc_auc"], (name, cells)
        assert float(cells[4]) == stated["average_precision"], (name, cells)


def check_the_matched_arm_figures_in_the_readme_match_the_manifest():
    rows = _readme_arm_rows()
    matched = _baseline_manifest()["matched"]
    for label, arm in (
        ("without siblings", "without_siblings"),
        ("with siblings", "with_siblings"),
        ("base only, no siblings", "shared_base_only"),
    ):
        cells = rows[label]
        stated = matched[arm]
        assert int(cells[0].replace(",", "")) == stated["train_rows"], (label, cells)
        assert int(cells[2].replace(",", "")) == stated["episodes_shared_with_evaluation"], (label, cells)
        assert float(cells[3]) == stated["roc_auc"], (label, cells)
        assert float(cells[4]) == stated["average_precision"], (label, cells)
    assert int(rows["with siblings"][1].replace(",", "")) == matched["sibling_rows"]
    assert int(rows["without siblings"][1].replace(",", "")) == 0
    assert int(rows["base only, no siblings"][1].replace(",", "")) == 0
    # The third arm is smaller on purpose. If it ever matched the other two it would be
    # answering the same question they do.
    assert matched["shared_base_only"]["train_rows"] < matched["training_rows"]


def check_every_baseline_gap_in_the_prose_matches_the_manifest():
    """Each gap is stated once in the prose and each one is read back out of it."""
    text = (REPO_ROOT / "README.md").read_text()
    m = _baseline_manifest()
    for value in (
        m["random_against_blocked"]["roc_auc_gap"],
        m["decomposition"]["episode_term_random_to_storm"],
        m["decomposition"]["spatial_term_storm_to_blocked"],
        m["matched"]["roc_auc_gap"],
        m["matched"]["average_precision_gap"],
    ):
        assert f"{value:.4f}" in text, value


def check_the_absent_damage_shares_in_the_prose_match_the_manifest():
    text = (REPO_ROOT / "README.md").read_text()
    by_scheme = _baseline_manifest()["random_against_blocked"]["test_rows_with_no_damage_value_by_scheme"]
    assert f"{by_scheme['random_split']['pct']} percent" in text, by_scheme
    assert f"{by_scheme['blocked_split']['pct']}" in text, by_scheme


def check_the_matched_evaluation_size_in_the_prose_matches_the_manifest():
    text = (REPO_ROOT / "README.md").read_text()
    matched = _baseline_manifest()["matched"]
    for value in (matched["evaluation_rows"], matched["evaluation_positives"], matched["sibling_rows"]):
        assert f"{value:,}" in text, value


def check_the_readme_does_not_call_a_grouped_cut_leakage_free():
    """The blocked cut keeps storms whole and that is not the same claim.

    It holds because no episode in these five years crosses a state line, and a state
    block leaves spatial adjacency at every border. A README saying leakage free would
    be overclaiming both.
    """
    text = (REPO_ROOT / "README.md").read_text().lower()
    for phrase in ("leakage free", "leakage-free", "no leakage", "zero leakage"):
        assert phrase not in text, phrase


def check_the_feature_count_in_the_prose_is_the_allowlist_length():
    from stormfactory import baseline as b

    text = (REPO_ROOT / "README.md").read_text()
    words = {14: "Fourteen", 13: "Thirteen", 15: "Fifteen", 12: "Twelve"}
    assert f"{words[len(b.FEATURES)]} features" in text, len(b.FEATURES)
    assert sorted(_baseline_manifest()["features"]["used"]) == sorted(b.FEATURES)


def check_the_two_halves_of_each_test_storm_are_described_from_the_manifest():
    """The parity split is not random, so the README states how alike the halves are."""
    text = (REPO_ROOT / "README.md").read_text()
    comp = _baseline_manifest()["matched"]["the_two_halves_of_each_test_storm"]
    for value in ("evaluation_positive_rate_pct", "sibling_positive_rate_pct",
                  "largest_event_type_share_gap_pct"):
        assert str(comp[value]) in text, (value, comp[value])


def check_the_fixed_base_gap_in_the_prose_is_the_difference_and_not_a_second_tally():
    text = (REPO_ROOT / "README.md").read_text()
    matched = _baseline_manifest()["matched"]
    fixed = round(matched["with_siblings"]["roc_auc"] - matched["shared_base_only"]["roc_auc"], 4)
    assert f"{fixed:.4f}" in text, fixed


def _imagery_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "imagery.json").read_text())


def _labels_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "labels.json").read_text())


def _imagery_lock() -> dict:
    return json.loads((REPO_ROOT / "imagery.lock.json").read_text())


def check_the_catalogue_counts_in_the_table_match_the_lock():
    table, lock = _readme_table(), _imagery_lock()
    assert table["Imagery collections in the catalogue"] == lock["collections"]
    assert table["Imagery collections declaring a footprint"] == lock["collections_with_a_declared_footprint"]
    assert table["Storms whose imagery catalogue was read"] == len(lock["storms"])


def check_the_catalogue_shape_split_in_the_prose_is_the_difference_and_not_a_second_tally():
    """The README gives the two shapes as a total and a remainder. A second tally typed
    beside the first is the thing this whole file exists to stop."""
    lock = _imagery_lock()
    bare = lock["collections"] - lock["collections_with_a_declared_footprint"]
    text = (REPO_ROOT / "README.md").read_text()
    assert f"The other\n{bare} are only visible as tile layer arguments" in text
    assert lock["collections_by_catalogue_shape"]["js/app.js"] == bare


def check_the_tile_counts_in_the_table_match_the_imagery_manifest():
    table, img = _readme_table(), _imagery_manifest()
    assert table["Tiles over the damage track"] == img["grid"]["tiles_in_corridor"]
    assert table["Tiles over the track at one zoom deeper"] == img["grid"]["tiles_in_corridor_at_the_deeper_zoom"]
    assert table["Tiles holding both frames"] == img["chosen"]["tiles_with_both_panels"]
    assert table["Events the catalogue frames before and after"] == img["selection"]["events_with_a_before_and_an_after_candidate"]


def check_the_coverage_table_in_the_readme_matches_the_manifest():
    """Every candidate sortie, its flight date and how much of the track it holds. This
    is the table the day turned on, so it is re-derived row by row."""
    img = _imagery_manifest()
    stated = {}
    for line in (REPO_ROOT / "README.md").read_text().splitlines():
        m = re.match(r"\|\s*(\d{8}[a-z])\s*\|\s*(after|before)\s*\|\s*([\d-]+)\s*\|\s*(\d+)\s*\|$", line)
        if m:
            stated[m.group(1)] = (m.group(2), m.group(3), int(m.group(4)))
    assert stated, "the coverage table was not parsed at all"
    for frame, key in (("after", "post_candidates"), ("before", "pre_candidates")):
        for c in img["coverage_against_the_declared_footprint"][key]:
            got = stated.get(c["collection"])
            assert got is not None, f"{c['collection']} is in the manifest and not in the table"
            assert got == (frame, c["acquired"], c["tiles_with_imagery"]), f"{c['collection']} states {got}"
    assert len(stated) == img["coverage_against_the_declared_footprint"]["candidates"]


def check_the_chosen_sorties_and_their_gaps_match_the_manifest():
    img, text = _imagery_manifest(), (REPO_ROOT / "README.md").read_text()
    for side, word in (("post", "after"), ("pre", "before")):
        c = img["chosen"][side]
        assert f"`{c['collection']}`" in text, f"{c['collection']} is not named"
        days = c["days_after_begin"] if side == "post" else c["days_before_begin"]
        assert f"flown {days} days {word}" in text, f"the {side} gap of {days} days is not stated"


def check_the_label_counts_in_the_table_match_the_label_manifest():
    table, lab = _readme_table(), _labels_manifest()
    for label, count in lab["label_counts"].items():
        assert table[label] == count, f"{label} states {table[label]} and the manifest holds {count}"
    assert table["Tile pairs labelled"] == lab["annotations"]
    assert table["Annotators"] == lab["agreement"]["annotators"]


def check_the_ambiguous_share_in_the_prose_is_the_sum_and_not_a_second_tally():
    lab = _labels_manifest()
    total = lab["label_counts"]["none"] + lab["label_counts"]["possible"]
    text = (REPO_ROOT / "README.md").read_text()
    assert f"The other {total} split between" in text


def check_the_readme_never_calls_the_agreement_measured():
    """The one claim on this page that a reader would most want to be a number."""
    lab = _labels_manifest()
    assert lab["agreement"]["status"] == "unmeasured"
    text = (REPO_ROOT / "README.md").read_text().lower()
    for phrase in ("inter-annotator agreement of", "kappa of", "agreement was measured",
                   "agreement is 0", "agreement of 0"):
        assert phrase not in text, f"the readme states {phrase!r} with one annotator"
    assert "agreement is unmeasured" in text


def check_the_zone_field_counts_in_the_table_match_the_manifest():
    table, tz = _readme_table(), _imagery_manifest()["timezone_field"]
    assert table["Rows naming a standard zone in summer"] == tz["rows_naming_a_standard_zone_in_june_to_august"]
    assert table["Rows naming a daylight zone"] == tz["rows_naming_a_daylight_zone"]
    assert table["Rows naming a daylight zone in winter"] == tz["daylight_named_in_december_to_february"]


def check_the_release_version_and_artifact_count_agree_with_the_manifest():
    rel, table = _release_manifest(), _readme_table()
    prose = _readme_prose()
    count = len(rel["artifacts"])
    assert table["Release artifacts"] == count
    assert f"Release {rel['release']} is {NUMBER_WORDS[count]} Parquet artifacts" in prose
    assert rel["supersedes"] in prose, "the superseded version is not named"


def check_the_rebuild_transcript_says_how_much_of_the_release_it_covers():
    """The clean clone rebuild is a transcript of one run over the artifacts of its day.

    The old form of this check asserted the transcript named the current artifact
    count, which quietly required editing a record of a past run every time the
    release grew. The honest version reads the count out of the transcript and makes
    the README say so when it is short of the release.
    """
    rel, prose = _release_manifest(), _readme_prose()
    count = len(rel["artifacts"])
    m = re.search(r"all (\d+) artifacts agree bit for bit", prose)
    assert m, "the rebuild transcript is gone from the readme"
    rebuilt = int(m.group(1))
    assert rebuilt <= count, f"the transcript rebuilt {rebuilt} of {count} artifacts"
    if rebuilt < count:
        assert f"covers {NUMBER_WORDS[rebuilt]} of the {NUMBER_WORDS[count]} artifacts" in prose, (
            f"the transcript covers {rebuilt} of {count} artifacts and the readme does "
            "not say which ones the bit for bit claim leaves out"
        )


def check_the_metres_per_pixel_in_the_prose_matches_the_manifest():
    img, text = _imagery_manifest(), (REPO_ROOT / "README.md").read_text()
    assert f"At\n{img['grid']['metres_per_pixel']} m per pixel" in text


def check_the_vector_counts_in_the_table_match_the_embeddings_manifest():
    t = _readme_table()
    e = _embeddings_manifest()
    assert t["Narratives carrying a vector"] == e["documents"]
    assert t["Events with no narrative"] == e["events_without_a_narrative"]
    assert t["Vector width"] == e["dimension"]
    assert t["Term vocabulary"] == e["vocabulary"]
    assert t["Distinct narrative vectors"] == e["distinct_vectors"]
    assert t["Rows sharing a vector with another row"] == e["rows_sharing_a_vector_with_another_row"]


def check_the_narrative_counts_are_the_difference_and_not_a_second_tally():
    """With and without a narrative have to add up to the events in the release."""
    e = _embeddings_manifest()
    assert e["documents"] + e["events_without_a_narrative"] == e["events_in_partitions"]
    assert _readme_table()["Event rows"] == e["events_in_partitions"]


def check_the_distinct_and_duplicate_vectors_are_the_difference_and_not_a_second_tally():
    e = _embeddings_manifest()
    assert e["distinct_vectors"] + e["rows_sharing_a_vector_with_another_row"] == e["documents"]


def check_every_recall_row_in_the_readme_matches_the_manifest():
    """The recall table, read back cell by cell.

    Three numbers per row and all three come from the manifest. The plan count is the
    one that decides what the recall figure means, so a row carrying the recall and
    not the plan count would be the defect this re-derivation exists to stop.
    """
    e = _embeddings_manifest()
    rows = {
        "no filter": "unfiltered",
        "event type": "event_type",
        "event type and state": "event_type_and_state",
    }
    text = (REPO_ROOT / "README.md").read_text()
    seen = 0
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 5 or cells[0] not in rows:
            continue
        seen += 1
        shape = rows[cells[0]]
        assert float(cells[1]) == e[f"recall_{shape}"], (cells, shape)
        used = f'{e[f"queries_planned_through_the_vector_index_{shape}"]} of {e["recall_queries"]}'
        assert cells[2] == used, (cells[2], used)
        assert int(cells[3]) == e[f"queries_differing_only_by_a_distance_tie_{shape}"], cells
        assert int(cells[4].replace(",", "")) == e[f"median_rows_matching_the_filter_{shape}"], cells
    assert seen == len(rows), f"parsed {seen} recall rows and expected {len(rows)}"


def check_the_readme_says_the_selective_filter_never_reached_the_vector_index():
    """The finding has to survive an edit that only refreshes the numbers.

    The third row's recall is not an index recall. A README that dropped the
    sentence saying so would publish a number that reads as the opposite of what it is.
    """
    e = _embeddings_manifest()
    assert e["queries_planned_through_the_vector_index_event_type_and_state"] == 0, (
        "the selective shape reached the vector index this time, so the README prose "
        "about it is now wrong and needs rewriting rather than renumbering"
    )
    assert "never used the vector index" in _readme_prose()


def check_the_readme_publishes_no_retrieval_quality_figure():
    """There is no labelled question set, so there is no retrieval quality to report."""
    prose = _readme_prose().lower()
    for phrase in ("retrieval accuracy", "retrieval quality of", "ndcg", "mrr of", "semantic accuracy"):
        assert phrase not in prose, f"the readme publishes {phrase}"
    assert "no retrieval quality figure is published" in prose


def check_the_graph_counts_in_the_table_match_the_graph_manifest():
    t = _readme_table()
    g = _graph_manifest()
    assert t["Graph events"] == g["nodes_in_graph"]["Event"]
    assert t["Graph counties"] == g["nodes_in_graph"]["County"]
    assert t["Declarations in the graph"] == g["nodes_in_graph"]["Declaration"]
    assert t["Declaration county edges"] == g["edges_in_graph"]["COVERS"]
    assert t["Declarations with no incident end date"] == g["open_ended_declarations"]


def check_the_cross_check_counts_in_the_table_match_the_manifest():
    t = _readme_table()
    cross = _graph_manifest()["cross_check"]
    assert t["Declarations the labelled tiles reach"] == cross["graph_distinct_declarations"]
    assert t["Paths to those declarations"] == cross["graph_paths"]
    assert cross["graph_paths"] == cross["parquet_paths"]


def check_the_readme_says_the_open_ended_branch_went_unexercised():
    """An agreement on a question that never asks is a weaker claim and says so."""
    g = _graph_manifest()
    prose = _readme_prose()
    if g["cross_check"]["open_ended_paths"] == 0:
        assert "does not show the two engines treat the null the same way" in prose
    else:
        assert "does not show the two engines treat the null the same way" not in prose, (
            "the branch was exercised this time and the README still says it was not"
        )


def check_the_release_artifact_bytes_match_the_manifest():
    r = _release_manifest()
    assert _readme_table()["Release artifact bytes"] == sum(
        a["bytes"] for a in r["artifacts"].values()
    )
