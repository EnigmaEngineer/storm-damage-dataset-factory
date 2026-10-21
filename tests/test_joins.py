"""The join conditions, on frames small enough to check by hand."""

from datetime import date

import polars as pl

from stormfactory import joins

PAIRS = pl.DataFrame({
    "EVENT_ID": [1, 2, 3],
    "county_fips": [39041, 39041, 5003],
    "event_date": [date(2019, 5, 28), date(2019, 7, 1), date(2019, 5, 28)],
})

REPORTS = pl.DataFrame({
    "county_fips": [39041, 39041, 39041],
    "report_date": [date(2019, 5, 28), date(2019, 5, 28), date(2019, 9, 9)],
    "report_type": ["hail", "wind", "torn"],
})

DECLARATIONS = pl.DataFrame({
    "county_fips": [39041, 39041],
    "declaration": ["DR-4000-OH", "EM-3999-OH"],
    "incident_type": ["Severe Storm", "Biological"],
    "incident_begin": [date(2019, 5, 27), date(2019, 1, 1)],
    "incident_end": [date(2019, 5, 29), None],
})


def check_a_report_on_the_same_day_matches_and_a_different_day_does_not():
    got = joins.same_day_reports(PAIRS, REPORTS)
    assert sorted(got["EVENT_ID"].to_list()) == [1, 1], got["EVENT_ID"].to_list()


def check_a_report_in_another_county_on_the_same_day_does_not_match():
    got = joins.same_day_reports(PAIRS, REPORTS)
    assert 3 not in got["EVENT_ID"].to_list()


def check_two_report_types_on_one_day_produce_two_matched_pairs():
    """The fan-out is the thing to know before reading a matched pair count."""
    got = joins.same_day_reports(PAIRS, REPORTS)
    assert len(got) == 2, got
    assert sorted(got["report_type"].to_list()) == ["hail", "wind"]


def check_an_event_inside_a_closed_window_matches():
    got = joins.declarations_covering(PAIRS, DECLARATIONS)
    storm = got.filter(pl.col("declaration") == "DR-4000-OH")
    assert storm["EVENT_ID"].to_list() == [1], storm


def check_an_event_outside_every_closed_window_matches_only_the_open_one():
    got = joins.declarations_covering(PAIRS, DECLARATIONS)
    two = got.filter(pl.col("EVENT_ID") == 2)
    assert two["declaration"].to_list() == ["EM-3999-OH"], two


def check_a_null_end_date_is_treated_as_a_window_that_is_still_running():
    got = joins.declarations_covering(PAIRS, DECLARATIONS)
    open_hits = got.filter(pl.col("declaration") == "EM-3999-OH")
    assert sorted(open_hits["EVENT_ID"].to_list()) == [1, 2], open_hits


def check_the_day_before_a_window_opens_does_not_match():
    early = pl.DataFrame({"EVENT_ID": [9], "county_fips": [39041], "event_date": [date(2019, 5, 26)]})
    got = joins.declarations_covering(early, DECLARATIONS.head(1))
    assert len(got) == 0, got


def check_the_last_day_of_a_window_still_matches():
    last = pl.DataFrame({"EVENT_ID": [9], "county_fips": [39041], "event_date": [date(2019, 5, 29)]})
    got = joins.declarations_covering(last, DECLARATIONS.head(1))
    assert len(got) == 1, got
