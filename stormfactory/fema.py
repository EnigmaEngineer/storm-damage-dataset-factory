"""OpenFEMA disaster declarations, read as county level date ranges.

A declaration is not an event. It is a decision made days or weeks later that covers
a county for an incident window, so it joins to a storm event by county plus a date
falling inside that window rather than by an equality on date. FEMA uses real FIPS
codes in both parts, which NOAA does not, and `fipsCountyCode` of 000 means the
designation is statewide rather than county level.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

API_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

FIELDS = (
    "femaDeclarationString", "disasterNumber", "state", "declarationType",
    "declarationDate", "incidentType", "declarationTitle", "incidentBeginDate",
    "incidentEndDate", "fipsStateCode", "fipsCountyCode", "designatedArea",
    "ihProgramDeclared", "paProgramDeclared",
)

STATEWIDE = "000"

# A declaration is not a storm. The COVID-19 declaration is filed as Biological, it
# covers 3,238 counties and its window is 1,207 days, so joining storm events to
# declarations on county and date without separating it reports a pandemic as a
# weather result. It is 91.5 percent of the matches over 2019 to 2023.
PANDEMIC_INCIDENT = "Biological"

# All 27 values the declarations table carries, read off the API rather than off the
# five years this release covers. The first version of this list held the 12 that
# happened to appear in the slice, which is the same mistake as declaring a column
# non-null because it was never null in the years that were looked at.
#
# Weather means the incident is something NOAA Storm Events also records. Fire is in
# because Storm Events carries Wildfire as an event type. Dam/Levee Break is out
# because the declaration is about the structure and not the weather that loaded it.
WEATHER_INCIDENTS = (
    "Coastal Storm", "Drought", "Fire", "Flood", "Freezing", "Hurricane",
    "Mud/Landslide", "Severe Ice Storm", "Severe Storm", "Snowstorm",
    "Straight-Line Winds", "Tornado", "Tropical Depression", "Tropical Storm",
    "Typhoon", "Winter Storm",
)

NOT_WEATHER_INCIDENTS = (
    "Biological", "Chemical", "Dam/Levee Break", "Earthquake", "Fishing Losses",
    "Human Cause", "Other", "Terrorist", "Toxic Substances", "Tsunami",
    "Volcanic Eruption",
)

# The two lists together are the whole published vocabulary, so a type FEMA adds shows
# up as a gap rather than being silently sorted into the wrong half.
KNOWN_INCIDENTS = tuple(sorted(WEATHER_INCIDENTS + NOT_WEATHER_INCIDENTS))


def unknown_incident_types(frame: pl.DataFrame) -> list[str]:
    """Incident types in the data that neither list claims."""
    return sorted(set(frame["incident_type"].unique().to_list()) - set(KNOWN_INCIDENTS))


class FemaError(Exception):
    pass


def page_query(years: list[int], top: int, skip: int) -> str:
    lo, hi = min(years), max(years) + 1
    from urllib.parse import urlencode

    return urlencode({
        "$filter": f"incidentBeginDate ge '{lo}-01-01' and incidentBeginDate lt '{hi}-01-01'",
        "$top": top,
        "$skip": skip,
        "$orderby": "disasterNumber,fipsStateCode,fipsCountyCode",
        "$select": ",".join(FIELDS),
    })


def _date(column: str, name: str) -> pl.Expr:
    return (
        pl.col(column).cast(pl.String).str.slice(0, 10).str.to_date("%Y-%m-%d").alias(name)
    )


def read_declarations(path: Path) -> pl.DataFrame:
    """County level declaration designations, statewide rows dropped.

    An incident with no end date is still open. Those are kept with a null end and the
    join treats a null end as still running, because dropping them would quietly lose
    every declaration FEMA has not closed.
    """
    records = json.loads(Path(path).read_text())
    if not records:
        raise FemaError(f"{path} holds no declaration records")
    df = pl.DataFrame(records, infer_schema_length=None)

    missing = sorted(set(FIELDS) - set(df.columns))
    if missing:
        raise FemaError(f"{path} is missing {missing}, so the API shape changed")

    county = df.filter(pl.col("fipsCountyCode") != STATEWIDE)
    return county.select(
        pl.col("femaDeclarationString").alias("declaration"),
        pl.col("disasterNumber").cast(pl.Int64).alias("disaster_number"),
        pl.col("state"),
        pl.col("declarationType").alias("declaration_type"),
        pl.col("incidentType").alias("incident_type"),
        pl.col("declarationTitle").alias("title"),
        pl.col("designatedArea").alias("designated_area"),
        (pl.col("fipsStateCode").cast(pl.Int64) * 1000
         + pl.col("fipsCountyCode").cast(pl.Int64)).alias("county_fips"),
        # Cast to text before slicing. The real file has 55 open incidents among
        # 15,570 records, so the column infers as String and the slice works. A page
        # where every record is open infers as Null and it does not, which a test
        # found and a run against the live API would not have.
        _date("incidentBeginDate", "incident_begin"),
        _date("incidentEndDate", "incident_end"),
        _date("declarationDate", "declared_on"),
        pl.col("paProgramDeclared").alias("public_assistance"),
        pl.col("ihProgramDeclared").alias("individual_housing"),
    ).unique(subset=["declaration", "county_fips"])


def statewide_count(path: Path) -> int:
    """How many designations are statewide, which the county join cannot use."""
    records = json.loads(Path(path).read_text())
    return sum(1 for r in records if r.get("fipsCountyCode") == STATEWIDE)
