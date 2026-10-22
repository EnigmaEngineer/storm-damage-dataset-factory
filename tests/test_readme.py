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


def check_the_table_was_parsed_at_all():
    # Without this, a README whose table stopped matching the regex would make every
    # check below pass against an empty mapping.
    assert len(_readme_table()) == 24, _readme_table()


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
