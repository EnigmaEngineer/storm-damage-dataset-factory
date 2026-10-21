"""The two joins, as functions, because the interesting part is the join condition.

An SPC report and a NOAA storm event are two offices filing the same observation, so
the test is an equality on county and calendar date. A FEMA declaration is a decision
made days or weeks later covering a window, so the test is containment. Writing them
as one join with a date tolerance would hide that difference.
"""

from __future__ import annotations

import polars as pl


def same_day_reports(pairs: pl.DataFrame, reports: pl.DataFrame) -> pl.DataFrame:
    """Event-county pairs that an SPC report also names on the same date."""
    return pairs.join(
        reports.select("county_fips", "report_date", "report_type").unique(),
        left_on=["county_fips", "event_date"],
        right_on=["county_fips", "report_date"],
        how="inner",
    )


def declarations_covering(pairs: pl.DataFrame, declarations: pl.DataFrame) -> pl.DataFrame:
    """Event-county pairs falling inside a declaration window for the same county.

    A null `incident_end` means FEMA has not closed the incident, so the window is
    still running and the event is inside it. Dropping those rows would quietly lose
    every open declaration, which is 55 of the 14,039 county designations here.
    """
    joined = pairs.join(
        declarations.select("county_fips", "declaration", "incident_type",
                            "incident_begin", "incident_end"),
        on="county_fips",
        how="inner",
    )
    return joined.filter(
        (pl.col("event_date") >= pl.col("incident_begin"))
        & (pl.col("incident_end").is_null() | (pl.col("event_date") <= pl.col("incident_end")))
    )
