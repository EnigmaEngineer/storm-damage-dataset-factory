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


def check_the_table_was_parsed_at_all():
    # Without this, a README whose table stopped matching the regex would make every
    # check below pass against an empty mapping.
    assert len(_readme_table()) == 8, _readme_table()


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
