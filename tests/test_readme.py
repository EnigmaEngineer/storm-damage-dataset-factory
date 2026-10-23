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
    assert len(_readme_table()) == 25, _readme_table()


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
