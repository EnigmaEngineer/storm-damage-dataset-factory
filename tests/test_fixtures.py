"""Checks whose whole job is to give a published figure a derivation that travels.

`data/` is gitignored, so a figure re-derived from the raw release does not re-derive
in a clean clone. The BEGIN_RANGE refusal count was published with only a grep for the
literal behind it in that case. These rows give it a derivation that travels.
"""

from pathlib import Path

import polars as pl

from stormfactory import schema

FIXTURES = Path(__file__).resolve().parent / "fixtures"
BEGIN_RANGE_ROWS = FIXTURES / "begin_range_rows.csv"


def check_a_strict_integer_cast_refuses_the_begin_range_fixture():
    """The original mistake, reproducible without the 57 MB of source files.

    The values here are lifted from the real refusal message, so the fixture carries
    the shapes that stopped the build rather than invented ones.
    """
    df = schema.read_raw(BEGIN_RANGE_ROWS)
    try:
        df.with_columns(pl.col("BEGIN_RANGE").cast(pl.Int64, strict=True))
    except pl.exceptions.InvalidOperationError as exc:
        assert "BEGIN_RANGE" in str(exc), str(exc)
        return
    raise AssertionError("a strict integer cast should refuse a distance in miles")


def check_the_fixture_carries_the_decimal_shapes_the_real_files_carry():
    df = schema.read_raw(BEGIN_RANGE_ROWS)
    values = df["BEGIN_RANGE"].to_list()
    non_integer = [v for v in values if "." in v]
    assert len(non_integer) == 7, non_integer
    assert ".53" in values and "117.53" in values, values


def check_the_float_cast_the_contract_declares_accepts_them_all():
    df = schema.read_raw(BEGIN_RANGE_ROWS)
    out = df.with_columns(pl.col("BEGIN_RANGE").cast(pl.Float64, strict=True))
    assert out["BEGIN_RANGE"].null_count() == 0
    assert "BEGIN_RANGE" in schema.FLOAT_COLUMNS


def check_the_fixture_passes_the_whole_contract():
    """Otherwise the two checks above would be grading a row the release would reject."""
    df = schema.cast_and_derive(schema.read_raw(BEGIN_RANGE_ROWS))
    assert schema.validate(df, 2019) == []
