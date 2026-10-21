"""Resolving a storm event to the counties it happened in.

The plan said join on county and date. That assumed every event has a county,
and it does not. NOAA codes an event against either a county or a National Weather
Service forecast zone, and `CZ_TYPE` says which. On a zone row `CZ_FIPS` is a zone
number, which is a different namespace from a county FIPS code and often collides
with a real one in the same state. Arkansas zone 3 is BOONE and Arkansas county 3 is
Ashley. So the naive join does not merely miss the zone rows, it mis-joins them.

Two reference files make the honest version possible. The NWS zone-county correlation
file maps a zone to every county it covers, which is one to many. The Census national
county file is the authority on what a county code is.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

# The correlation file's name carries the date it was issued, the same way a NOAA
# details file carries its revision. So the page has to be read rather than a name
# guessed, which is why this is a pattern and not a constant.
ZONE_INDEX_URL = "https://www.weather.gov/gis/ZoneCounty"
# The stamp is a day then a three letter month then a two digit year. So bp16ap26.dbx.
# The length is a range because the first version of this pattern fixed it at seven and
# matched nothing. Nothing is the one answer that reads like the page changed.
ZONE_FILE_NAME = re.compile(r"/source/gis/Shapefiles/County/(?P<name>bp(?P<stamp>\w{5,8})\.dbx)")
ZONE_BASE_URL = "https://www.weather.gov"

COUNTY_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"

# NOAA's STATE_FIPS column is not a FIPS code for five jurisdictions. It carries 99
# for Puerto Rico where FIPS is 72, and three more of the same shape. Measured on the
# 2019 to 2023 details files, where 2,806 rows key on one of these.
NOAA_STATE_CODE_FIXUP = {
    99: 72,  # Puerto Rico
    98: 66,  # Guam
    97: 60,  # American Samoa
    96: 78,  # Virgin Islands
}


class GeoError(Exception):
    pass


@dataclass(frozen=True)
class ZoneFile:
    name: str
    stamp: str

    @property
    def url(self) -> str:
        return f"{ZONE_BASE_URL}/source/gis/Shapefiles/County/{self.name}"


def newest_zone_file(html: str) -> ZoneFile:
    """The correlation files the page offers, newest last.

    The stamp is a day-month-year string like `16ap26` rather than a sortable date, so
    ordering is positional. The page lists them oldest first and that is the only
    ordering available without parsing a three letter month.
    """
    found = [ZoneFile(m.group("name"), m.group("stamp")) for m in ZONE_FILE_NAME.finditer(html)]
    if not found:
        raise GeoError("the zone-county page offers no .dbx correlation file")
    return found[-1]


def state_fips(noaa_code: int) -> int:
    return NOAA_STATE_CODE_FIXUP.get(noaa_code, noaa_code)


def read_counties(path: Path) -> dict[int, str]:
    """Every county code the Census publishes, keyed as state * 1000 + county."""
    out = {}
    with Path(path).open(newline="") as fh:
        for row in csv.DictReader(fh, delimiter="|"):
            out[int(row["STATEFP"]) * 1000 + int(row["COUNTYFP"])] = row["COUNTYNAME"]
    if not out:
        raise GeoError(f"{path} yielded no county codes")
    return out


def read_zone_crosswalk(path: Path) -> dict[tuple[int, int], tuple[int, ...]]:
    """Zone to counties, from the pipe separated correlation file.

    No header and eleven fields. The ones that matter are state postal code, zone
    number and the county FIPS at index 6. A zone covers up to ten counties.
    """
    acc: dict[tuple[int, int], set[int]] = {}
    with Path(path).open() as fh:
        for line in fh:
            parts = line.rstrip("\n").split("|")
            if len(parts) < 7:
                continue
            fips = parts[6].strip()
            if not fips.isdigit() or len(fips) != 5 or not parts[1].strip().isdigit():
                continue
            key = (int(fips[:2]), int(parts[1]))
            acc.setdefault(key, set()).add(int(fips))
    if not acc:
        raise GeoError(f"{path} yielded no zone to county mapping")
    return {k: tuple(sorted(v)) for k, v in acc.items()}


def resolve_counties(
    noaa_state: int,
    cz_type: str,
    cz_fips: int,
    counties: dict[int, str],
    crosswalk: dict[tuple[int, int], tuple[int, ...]],
) -> tuple[int, ...]:
    """The county codes one event row belongs to, possibly none and possibly several.

    A county row resolves to itself and is checked against the Census list rather than
    trusted, because 1,191 rows over the five years carry a code that is not a county.
    A zone row goes through the crosswalk and fans out. Returning an empty tuple is a
    real answer: a marine zone has no county and never will.
    """
    st = state_fips(noaa_state)
    if cz_type == "C":
        code = st * 1000 + cz_fips
        return (code,) if code in counties else ()
    if cz_type == "Z":
        return crosswalk.get((st, cz_fips), ())
    raise GeoError(f"CZ_TYPE {cz_type!r} is neither C nor Z and the contract allows only those")


def crosswalk_frame(crosswalk: dict[tuple[int, int], tuple[int, ...]]) -> "object":
    """The zone crosswalk as a frame, one row per zone and county.

    The row by row version of the resolution below was a Python loop over 336,014 rows
    inside a vectorised engine, which is the first thing a reviewer flags. As two
    joins it is the same answer and the engine does the work.
    """
    import polars as pl

    rows = [{"state_fips": st, "cz_fips": z, "county_fips": c}
            for (st, z), codes in crosswalk.items() for c in codes]
    return pl.DataFrame(rows, schema={"state_fips": pl.Int64, "cz_fips": pl.Int64,
                                      "county_fips": pl.Int64})


def county_frame(counties: dict[int, str]) -> "object":
    import polars as pl

    return pl.DataFrame(
        {"county_fips": list(counties), "county_name": [counties[c] for c in counties]},
        schema={"county_fips": pl.Int64, "county_name": pl.String},
    )


def state_fips_expr(column: str = "STATE_FIPS") -> "object":
    """The territory remap as an expression rather than a dict lookup per row."""
    import polars as pl

    expr = pl.col(column)
    for noaa, real in NOAA_STATE_CODE_FIXUP.items():
        expr = pl.when(pl.col(column) == noaa).then(pl.lit(real, dtype=pl.Int64)).otherwise(expr)
    return expr.alias("state_fips")


def resolve_frame(events: "object", counties: dict[int, str],
                  crosswalk: dict[tuple[int, int], tuple[int, ...]]) -> "object":
    """Every event-county pair, as two joins rather than a loop.

    A county row joins against the Census list, which both resolves it and checks it.
    A zone row joins against the crosswalk and fans out. An event matching neither
    produces no row, which is the right answer for a marine zone.
    """
    import polars as pl

    ev = events.with_columns(state_fips_expr())
    county_rows = ev.filter(pl.col("CZ_TYPE") == "C").with_columns(
        (pl.col("state_fips") * 1000 + pl.col("CZ_FIPS")).alias("county_fips")
    ).join(county_frame(counties), on="county_fips", how="inner")

    zone_rows = ev.filter(pl.col("CZ_TYPE") == "Z").join(
        crosswalk_frame(crosswalk), left_on=["state_fips", "CZ_FIPS"],
        right_on=["state_fips", "cz_fips"], how="inner",
    ).join(county_frame(counties), on="county_fips", how="inner")

    keep = [c for c in county_rows.columns if c in zone_rows.columns]
    return pl.concat([county_rows.select(keep), zone_rows.select(keep)])
