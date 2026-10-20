"""The contract every Storm Events partition is held to before it is written.

Validation runs at the asset boundary rather than inside a notebook, so a bad
partition never reaches Parquet. Each rule returns a sentence naming the column and
the count, because a refusal that says "schema validation failed" costs the same
investigation every time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import polars as pl

# NOAA's own order, read off the 2023 header. Order is part of the contract because a
# silently reordered CSV is a format change and the run should stop rather than guess.
RAW_COLUMNS: tuple[str, ...] = (
    "BEGIN_YEARMONTH", "BEGIN_DAY", "BEGIN_TIME", "END_YEARMONTH", "END_DAY", "END_TIME",
    "EPISODE_ID", "EVENT_ID", "STATE", "STATE_FIPS", "YEAR", "MONTH_NAME", "EVENT_TYPE",
    "CZ_TYPE", "CZ_FIPS", "CZ_NAME", "WFO", "BEGIN_DATE_TIME", "CZ_TIMEZONE",
    "END_DATE_TIME", "INJURIES_DIRECT", "INJURIES_INDIRECT", "DEATHS_DIRECT",
    "DEATHS_INDIRECT", "DAMAGE_PROPERTY", "DAMAGE_CROPS", "SOURCE", "MAGNITUDE",
    "MAGNITUDE_TYPE", "FLOOD_CAUSE", "CATEGORY", "TOR_F_SCALE", "TOR_LENGTH", "TOR_WIDTH",
    "TOR_OTHER_WFO", "TOR_OTHER_CZ_STATE", "TOR_OTHER_CZ_FIPS", "TOR_OTHER_CZ_NAME",
    "BEGIN_RANGE", "BEGIN_AZIMUTH", "BEGIN_LOCATION", "END_RANGE", "END_AZIMUTH",
    "END_LOCATION", "BEGIN_LAT", "BEGIN_LON", "END_LAT", "END_LON", "EPISODE_NARRATIVE",
    "EVENT_NARRATIVE", "DATA_SOURCE",
)

INT_COLUMNS = (
    "BEGIN_YEARMONTH", "BEGIN_DAY", "BEGIN_TIME", "END_YEARMONTH", "END_DAY", "END_TIME",
    "EPISODE_ID", "EVENT_ID", "STATE_FIPS", "YEAR", "CZ_FIPS", "INJURIES_DIRECT",
    "INJURIES_INDIRECT", "DEATHS_DIRECT", "DEATHS_INDIRECT", "TOR_OTHER_CZ_FIPS",
)

# BEGIN_RANGE and END_RANGE are here because the strict cast refused them. They read
# like counts and NOAA documents them as a distance in miles, so most of the values
# carry decimals. The first version of this contract had them as Int64.
FLOAT_COLUMNS = (
    "MAGNITUDE", "TOR_LENGTH", "TOR_WIDTH", "BEGIN_LAT", "BEGIN_LON", "END_LAT", "END_LON",
    "BEGIN_RANGE", "END_RANGE",
)

# Measured over 2019 to 2023 before this list was written. Everything outside it is
# null somewhere in those five years, including both latitude columns.
REQUIRED = (
    "EPISODE_ID", "EVENT_ID", "STATE", "STATE_FIPS", "YEAR", "MONTH_NAME", "EVENT_TYPE",
    "CZ_TYPE", "CZ_FIPS", "CZ_NAME", "WFO", "BEGIN_DATE_TIME", "END_DATE_TIME",
    "INJURIES_DIRECT", "INJURIES_INDIRECT", "DEATHS_DIRECT", "DEATHS_INDIRECT",
    "DATA_SOURCE",
)

DERIVED = ("damage_property_usd", "damage_crops_usd", "has_point")

CZ_TYPES = frozenset({"C", "Z"})

MULTIPLIER = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}

DAMAGE_TEXT = re.compile(r"^(?P<number>[0-9]*\.?[0-9]+)(?P<suffix>[KMBkmb]?)$")


class SchemaError(Exception):
    pass


def parse_damage(text: str | None) -> int | None:
    """NOAA writes damage as a number with a magnitude letter. `25.00K` is 25000 dollars.

    Returns None for a blank, which means not reported and never zero. Those are
    different claims and the distinction survives into the Parquet as a null.
    """
    if text is None:
        return None
    s = text.strip()
    if not s:
        return None
    m = DAMAGE_TEXT.match(s)
    if m is None:
        raise SchemaError(f"damage value {text!r} is not a number with an optional K, M or B")
    number = float(m.group("number"))
    suffix = m.group("suffix").upper()
    return round(number * MULTIPLIER.get(suffix, 1))


def read_raw(path) -> pl.DataFrame:
    """Read a details CSV with every column as text.

    Letting polars infer types here would make the contract below untestable, because
    a column it got wrong would arrive already wrong and the cast would be the only
    place the error could surface.
    """
    return pl.read_csv(path, infer_schema_length=0, quote_char='"')


def cast_and_derive(df: pl.DataFrame) -> pl.DataFrame:
    casts = [pl.col(c).cast(pl.Int64, strict=True) for c in INT_COLUMNS]
    casts += [pl.col(c).cast(pl.Float64, strict=True) for c in FLOAT_COLUMNS]
    out = df.with_columns(casts)
    return out.with_columns(
        pl.col("DAMAGE_PROPERTY")
        .map_elements(parse_damage, return_dtype=pl.Int64)
        .alias("damage_property_usd"),
        pl.col("DAMAGE_CROPS")
        .map_elements(parse_damage, return_dtype=pl.Int64)
        .alias("damage_crops_usd"),
        pl.col("BEGIN_LAT").is_not_null().alias("has_point"),
    )


@dataclass
class Violation:
    rule: str
    detail: str

    def __str__(self) -> str:
        return f"{self.rule}: {self.detail}"


def validate(df: pl.DataFrame, year: int) -> list[Violation]:
    """Every rule runs, so one call reports all of them rather than the first.

    A partition that fails three rules and is fixed one call at a time costs three
    runs over a 60,000 row file.
    """
    bad: list[Violation] = []
    expected = list(RAW_COLUMNS) + list(DERIVED)
    if list(df.columns) != expected:
        extra = sorted(set(df.columns) - set(expected))
        missing = sorted(set(expected) - set(df.columns))
        bad.append(Violation("columns", f"missing {missing}, unexpected {extra}, order differs"))
        return bad

    for col in INT_COLUMNS:
        if df.schema[col] != pl.Int64:
            bad.append(Violation("dtype", f"{col} is {df.schema[col]} and should be Int64"))
    for col in FLOAT_COLUMNS:
        if df.schema[col] != pl.Float64:
            bad.append(Violation("dtype", f"{col} is {df.schema[col]} and should be Float64"))

    for col in REQUIRED:
        n = df[col].null_count()
        if n:
            bad.append(Violation("required", f"{col} is null on {n} rows"))

    dupes = len(df) - df["EVENT_ID"].n_unique()
    if dupes:
        bad.append(Violation("unique", f"EVENT_ID repeats on {dupes} rows"))

    off_year = df.filter(pl.col("YEAR") != year).height
    if off_year:
        bad.append(Violation("partition", f"{off_year} rows carry a YEAR other than {year}"))

    odd = sorted(set(df["CZ_TYPE"].unique().to_list()) - CZ_TYPES)
    if odd:
        bad.append(Violation("domain", f"CZ_TYPE holds {odd} and the contract allows C and Z"))

    for col, limit in (("BEGIN_LAT", 90.0), ("END_LAT", 90.0), ("BEGIN_LON", 180.0), ("END_LON", 180.0)):
        n = df.filter(pl.col(col).abs() > limit).height
        if n:
            bad.append(Violation("range", f"{col} exceeds {limit} on {n} rows"))

    # A null damage figure where NOAA wrote something is a parse that gave up quietly.
    for raw, parsed in (("DAMAGE_PROPERTY", "damage_property_usd"), ("DAMAGE_CROPS", "damage_crops_usd")):
        n = df.filter(pl.col(raw).is_not_null() & pl.col(raw).str.strip_chars().ne("") & pl.col(parsed).is_null()).height
        if n:
            bad.append(Violation("derived", f"{parsed} is null on {n} rows where {raw} is not blank"))

    return bad


def validate_or_raise(df: pl.DataFrame, year: int) -> None:
    bad = validate(df, year)
    if bad:
        joined = "\n  ".join(str(v) for v in bad)
        raise SchemaError(f"{year} partition fails {len(bad)} rules:\n  {joined}")
