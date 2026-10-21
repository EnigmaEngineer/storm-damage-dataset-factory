"""The SPC report reader, which normalises three files that do not share a header."""

import tempfile
from pathlib import Path

import polars as pl

from stormfactory import spc

SHARED = ",".join(spc.SHARED_COLUMNS)

# One tornado report touching three counties, and one touching one. The trailing `fc`
# column is the tornado file's own and the reader is expected to drop it.
TORN = SHARED + ",fc\n" + "\n".join([
    "617025,2019,01,04,2019-01-04,04:38:00,3,AL,01,0,0,0,0,0,0,31.61,-85.27,31.61,-85.27,0.15,35,1,1,1,67,69,71,0,0",
    "617026,2019,01,05,2019-01-05,05:10:00,3,GA,13,0,1,0,0,10000,0,31.59,-84.96,31.60,-84.96,0.23,90,1,1,1,61,0,0,0,0",
]) + "\n"

# 2020 onward. The identifier stops being an integer here.
TORN_2020 = SHARED + ",fc\n" + (
    "2001040929-01,2020,04,09,2020-04-09,12:00:00,3,AL,01,0,1,0,0,0,0,31.0,-85.0,31.0,-85.0,1.0,50,1,1,1,67,0,0,0,0\n"
)

WIND = SHARED + ",mt\n" + (
    "640864,2019,01,04,2019-01-04,16:29:00,3,KY,21,0,52,0,0,0,0,37.35,-84.35,37.35,-84.35,0,0,0,0,0,203,0,0,0,EG\n"
)


def _write(text: str) -> Path:
    p = Path(tempfile.mkdtemp(prefix="stormfactory-spc-")) / "reports.csv"
    p.write_text(text)
    return p


def check_a_report_touching_three_counties_becomes_three_rows():
    df = spc.read_reports(_write(TORN), "torn")
    first = df.filter(pl.col("spc_id") == "617025")
    assert len(first) == 3, first
    assert sorted(first["county_fips"].to_list()) == [1067, 1069, 1071], first["county_fips"].to_list()


def check_the_zero_filler_in_an_unused_county_slot_is_dropped():
    df = spc.read_reports(_write(TORN), "torn")
    assert len(df.filter(pl.col("spc_id") == "617026")) == 1
    assert df["county_fips"].min() > 0


def check_the_identifier_is_text_so_the_2020_format_change_does_not_refuse():
    """SPC's `om` is an integer in 2019 and a composite from 2020 onward.

    29,097 rows over the five release years parse as an integer and 108,730 do not.
    The first version of this reader cast it and the build stopped on the 2020 file.
    """
    df = spc.read_reports(_write(TORN_2020), "torn")
    assert df["spc_id"].dtype == pl.String, df["spc_id"].dtype
    assert df["spc_id"].to_list() == ["2001040929-01"]


def check_the_trailing_column_differs_by_type_and_both_read():
    assert len(spc.read_reports(_write(WIND), "wind")) == 1
    assert len(spc.read_reports(_write(TORN), "torn")) == 4


def check_a_reordered_header_raises_rather_than_shifting_a_column():
    cols = list(spc.SHARED_COLUMNS)
    cols[8], cols[9] = cols[9], cols[8]
    bad = ",".join(cols) + "\n" + ",".join(["1"] * len(cols)) + "\n"
    try:
        spc.read_reports(_write(bad), "torn")
    except spc.SpcError as exc:
        assert "contract expects" in str(exc)
        return
    raise AssertionError("a reordered header should raise")


def check_the_report_date_is_parsed_and_not_derived_from_the_year_column():
    df = spc.read_reports(_write(TORN), "torn")
    dates = sorted({str(d) for d in df["report_date"].to_list()})
    assert dates == ["2019-01-04", "2019-01-05"], dates


def check_the_file_names_cover_every_year_and_type():
    files = spc.files_for([2019, 2020])
    assert [f.filename for f in files] == [
        "2019_torn.csv", "2019_hail.csv", "2019_wind.csv",
        "2020_torn.csv", "2020_hail.csv", "2020_wind.csv",
    ]
    assert files[0].url.endswith("/wcm/data/2019_torn.csv")
