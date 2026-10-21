"""The county key, which had to be settled before anything could be joined to it.

The cases here are the ones that cost a measurement to find. A zone number that is
also a county code in the same state, and a STATE_FIPS column that is not FIPS.
"""

from pathlib import Path

from stormfactory import geo

FIXTURES = Path(__file__).resolve().parent / "fixtures"

# Four real rows of the correlation file plus one zone that covers two counties.
CROSSWALK_LINES = """NM|201|ABQ|Northwest Plateau|NM201|McKinley|35031|M|nw|36.4270|-108.4064
NM|201|ABQ|Northwest Plateau|NM201|San Juan|35045|M|nw|36.4270|-108.4064
AR|074|JAN|Ashley|AR074|Ashley|05003|C|se|33.1912|-91.7679
OH|041|ILN|Delaware|OH041|Delaware|39041|C|cc|40.2800|-82.9900
"""

COUNTY_LINES = """STATE|STATEFP|COUNTYFP|COUNTYNS|COUNTYNAME|CLASSFP|FUNCSTAT
AR|05|003|00066841|Ashley County|H1|A
NM|35|031|00933048|McKinley County|H1|A
NM|35|045|00929116|San Juan County|H1|A
OH|39|041|01074054|Delaware County|H1|A
PR|72|127|01804550|San Juan Municipio|H1|A
"""


def _tables(tmp):
    (tmp / "zc.dbx").write_text(CROSSWALK_LINES)
    (tmp / "counties.txt").write_text(COUNTY_LINES)
    return geo.read_counties(tmp / "counties.txt"), geo.read_zone_crosswalk(tmp / "zc.dbx")


def _tmpdir():
    import tempfile
    return Path(tempfile.mkdtemp(prefix="stormfactory-geo-"))


def check_a_zone_fans_out_to_every_county_it_covers():
    counties, crosswalk = _tables(_tmpdir())
    got = geo.resolve_counties(35, "Z", 201, counties, crosswalk)
    assert got == (35031, 35045), got


def check_a_county_row_resolves_to_itself():
    counties, crosswalk = _tables(_tmpdir())
    assert geo.resolve_counties(39, "C", 41, counties, crosswalk) == (39041,)


def check_a_zone_number_that_is_also_a_county_code_does_not_resolve_as_that_county():
    """Arkansas zone 3 is BOONE and Arkansas county 3 is Ashley.

    This is the whole reason the crosswalk exists. Over the five release years 55,933
    zone rows carry a number that is a real county code in the same state, so the
    naive join mis-joins them rather than missing them.
    """
    counties, crosswalk = _tables(_tmpdir())
    assert 5003 in counties
    assert geo.resolve_counties(5, "Z", 3, counties, crosswalk) == ()


def check_noaa_state_fips_is_remapped_for_puerto_rico():
    """NOAA writes 99 for Puerto Rico in a column it calls STATE_FIPS. FIPS is 72."""
    counties, crosswalk = _tables(_tmpdir())
    assert geo.state_fips(99) == 72
    assert geo.resolve_counties(99, "C", 127, counties, crosswalk) == (72127,)


def check_a_county_code_that_is_not_a_county_resolves_to_nothing():
    counties, crosswalk = _tables(_tmpdir())
    assert geo.resolve_counties(39, "C", 999, counties, crosswalk) == ()


def check_an_unknown_cz_type_raises_rather_than_returning_empty():
    counties, crosswalk = _tables(_tmpdir())
    try:
        geo.resolve_counties(39, "X", 41, counties, crosswalk)
    except geo.GeoError as exc:
        assert "X" in str(exc), str(exc)
        return
    raise AssertionError("an unknown CZ_TYPE should raise")


def check_the_newest_correlation_file_is_taken_from_the_page():
    html = ('<a href="/source/gis/Shapefiles/County/bp18mr25.dbx">older</a>'
            '<a href="/source/gis/Shapefiles/County/bp16ap26.dbx">newer</a>')
    f = geo.newest_zone_file(html)
    assert f.name == "bp16ap26.dbx", f
    assert f.url.endswith("/County/bp16ap26.dbx")


def check_a_page_offering_no_correlation_file_raises():
    try:
        geo.newest_zone_file("<html>nothing here</html>")
    except geo.GeoError:
        return
    raise AssertionError("an empty page should raise rather than return a guess")


def check_an_empty_county_file_raises():
    tmp = _tmpdir()
    (tmp / "empty.txt").write_text("STATE|STATEFP|COUNTYFP|COUNTYNS|COUNTYNAME|CLASSFP|FUNCSTAT\n")
    try:
        geo.read_counties(tmp / "empty.txt")
    except geo.GeoError:
        return
    raise AssertionError("a header with no rows should raise")


def check_the_frame_resolution_agrees_with_the_row_by_row_one():
    """The joins replaced a Python loop over 336,014 rows and have to agree with it.

    Both versions run here on a handful of rows covering every case: a county row, a
    zone row that fans out to two counties, a zone number that collides with a county
    code, and a territory whose STATE_FIPS is not FIPS.
    """
    import polars as pl

    counties, crosswalk = _tables(_tmpdir())
    events = pl.DataFrame({
        "EVENT_ID": [1, 2, 3, 4, 5],
        "STATE_FIPS": [39, 35, 5, 99, 39],
        "CZ_TYPE": ["C", "Z", "Z", "C", "C"],
        "CZ_FIPS": [41, 201, 3, 127, 999],
        "EVENT_TYPE": ["Hail"] * 5,
        "BEGIN_YEARMONTH": [201905] * 5,
        "BEGIN_DAY": [1] * 5,
        "has_point": [True] * 5,
        "damage_property_usd": [None] * 5,
        "DEATHS_DIRECT": [0] * 5,
        "INJURIES_DIRECT": [0] * 5,
    }, schema_overrides={"STATE_FIPS": pl.Int64, "CZ_FIPS": pl.Int64,
                         "BEGIN_YEARMONTH": pl.Int64, "BEGIN_DAY": pl.Int64,
                         "damage_property_usd": pl.Int64})

    by_frame = geo.resolve_frame(events, counties, crosswalk)
    by_row = []
    for r in events.iter_rows(named=True):
        for code in geo.resolve_counties(r["STATE_FIPS"], r["CZ_TYPE"], r["CZ_FIPS"],
                                         counties, crosswalk):
            by_row.append((r["EVENT_ID"], code))

    got = sorted(zip(by_frame["EVENT_ID"].to_list(), by_frame["county_fips"].to_list()))
    assert got == sorted(by_row), (got, sorted(by_row))
    assert got == [(1, 39041), (2, 35031), (2, 35045), (4, 72127)], got
