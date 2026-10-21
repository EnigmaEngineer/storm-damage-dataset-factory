"""Storm Prediction Center severe weather reports, normalised across three types.

SPC publishes one file per year per report type and the three types do not share a
header. Tornado files carry a trailing `fc` column, wind files carry `mt`, hail files
carry neither. So a reader that assumes one header silently shifts a column, which is
the failure this module is shaped to avoid.

The county key is already in the file and it is already one to many. Columns `f1`
through `f4` hold up to four county FIPS codes a single report touched, with 0 used as
a filler rather than a null. A report spanning three counties is one row here and
three rows after normalisation.

The report identifier is text. The comment on `om` in `read_reports` says why, and
names the year it changed shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

BASE_URL = "https://www.spc.noaa.gov/wcm/data/"
REPORT_TYPES = ("torn", "hail", "wind")

# Shared by all three, in SPC's order. The trailing column differs by type and is
# dropped rather than renamed, because nothing downstream reads it yet.
SHARED_COLUMNS = (
    "om", "yr", "mo", "dy", "date", "time", "tz", "st", "stf", "stn", "mag", "inj",
    "fat", "loss", "closs", "slat", "slon", "elat", "elon", "len", "wid", "ns", "sn",
    "sg", "f1", "f2", "f3", "f4",
)

COUNTY_COLUMNS = ("f1", "f2", "f3", "f4")


class SpcError(Exception):
    pass


@dataclass(frozen=True)
class SpcFile:
    year: int
    report_type: str

    @property
    def filename(self) -> str:
        return f"{self.year}_{self.report_type}.csv"

    @property
    def url(self) -> str:
        return BASE_URL + self.filename


def files_for(years: list[int]) -> list[SpcFile]:
    return [SpcFile(y, t) for y in sorted(years) for t in REPORT_TYPES]


def read_reports(path: Path, report_type: str) -> pl.DataFrame:
    """One SPC file as reports, one row per county the report touched.

    Every column is read as text first for the same reason the Storm Events reader
    does it. The shared header is asserted rather than assumed, so a column SPC adds
    in the middle stops the build instead of arriving under the wrong name.
    """
    df = pl.read_csv(path, infer_schema_length=0)
    head = tuple(df.columns[: len(SHARED_COLUMNS)])
    if head != SHARED_COLUMNS:
        raise SpcError(
            f"{Path(path).name} header starts {head} and the contract expects {SHARED_COLUMNS}"
        )

    df = df.select(SHARED_COLUMNS).with_columns(
        pl.col("yr").cast(pl.Int64, strict=True).alias("year"),
        pl.col("stf").cast(pl.Int64, strict=True).alias("state_fips"),
        pl.col("date").str.to_date("%Y-%m-%d", strict=True).alias("report_date"),
        pl.col("mag").cast(pl.Float64, strict=True).alias("magnitude"),
        pl.col("inj").cast(pl.Int64, strict=True).alias("injuries"),
        pl.col("fat").cast(pl.Int64, strict=True).alias("fatalities"),
        # `om` is an identifier and not a number, which is the same mistake the storm
        # events contract made with BEGIN_RANGE one day earlier. SPC changed its format
        # between 2019 and 2020. 2019 carries a plain integer like 617025 and every year
        # after carries a composite like 2001040929-01, which is a date, a time and a
        # sequence. 29,097 rows parse as an integer and 108,730 do not, so the strict
        # cast refused and it was right to. Text, and unique within a year only.
        pl.col("om").alias("spc_id"),
        pl.lit(report_type).alias("report_type"),
    )

    # 0 is SPC's filler for an unused county slot, so it unpivots to a null and drops.
    long = df.unpivot(
        on=COUNTY_COLUMNS,
        index=["spc_id", "report_type", "year", "report_date", "state_fips",
               "magnitude", "injuries", "fatalities"],
        variable_name="county_slot",
        value_name="county_raw",
    )
    return (
        long.with_columns(pl.col("county_raw").cast(pl.Int64, strict=True).alias("county_code"))
        .filter(pl.col("county_code") > 0)
        .with_columns((pl.col("state_fips") * 1000 + pl.col("county_code")).alias("county_fips"))
        .drop("county_raw", "county_slot")
        .unique(subset=["spc_id", "report_type", "year", "county_fips"])
    )
