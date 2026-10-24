"""Checks on the tile grid, the catalogue parsing and the time zone field.

Nothing here reaches the network and nothing here reads `data/`. The grid is arithmetic
and the rest is parsing, so both travel to a machine that has never fetched a tile.
"""

from datetime import date, datetime, timezone

import polars as pl

from stormfactory import imagery

# Waveland, Mississippi. The tornado this release tiles, with its surveyed endpoints.
START = (30.2664, -89.3864)
END = (30.3108, -89.4086)


def check_a_collection_name_gives_its_acquisition_date():
    assert imagery.collection_acquired("20210902a") == date(2021, 9, 2)
    assert imagery.collection_acquired("20201030b") == date(2020, 10, 30)


def check_a_collection_name_without_a_date_is_refused():
    for bad in ("rgb", "2021a", "ida-2021"):
        try:
            imagery.collection_acquired(bad)
        except imagery.ImageryError:
            continue
        raise AssertionError(f"{bad!r} was dated and it carries no stamp")


def check_a_tile_contains_the_point_it_was_derived_from():
    """The round trip the whole grid rests on.

    An off by one here produces a tile set that is plausible and wrong, and the imagery
    would still look like imagery.
    """
    for z in (14, 16, 17, 18):
        x, y = imagery.tile_of(*START, z)
        west, south, east, north = imagery.tile_bounds(z, x, y)
        assert west <= START[1] <= east, f"zoom {z} put the longitude outside its tile"
        assert south <= START[0] <= north, f"zoom {z} put the latitude outside its tile"


def check_a_tile_centre_lands_back_in_its_own_tile():
    z = 17
    x, y = imagery.tile_of(*START, z)
    assert imagery.tile_of(*imagery.tile_centre(z, x, y), z) == (x, y)


def check_one_zoom_deeper_halves_the_ground_each_pixel_covers():
    a = imagery.metres_per_pixel(START[0], 17)
    b = imagery.metres_per_pixel(START[0], 18)
    assert abs(a / b - 2.0) < 1e-9, f"{a} and {b} are not a factor of two apart"


def check_the_corridor_holds_both_endpoints_and_nothing_beyond_the_buffer():
    buffer_m = 223.4
    tiles = imagery.corridor_tiles(START, END, 17, buffer_m)
    assert imagery.tile_of(*START, 17) in tiles
    assert imagery.tile_of(*END, 17) in tiles
    for x, y in tiles:
        d = imagery.distance_to_segment_m(imagery.tile_centre(17, x, y), START, END)
        assert d <= buffer_m, f"tile {x},{y} is {d:.1f} m out and the buffer is {buffer_m}"


def check_a_wider_buffer_never_drops_a_tile():
    narrow = set(imagery.corridor_tiles(START, END, 17, 150))
    wide = set(imagery.corridor_tiles(START, END, 17, 300))
    assert narrow <= wide, "widening the buffer lost tiles, so the candidate box is too tight"


def check_the_corridor_follows_the_track_rather_than_boxing_it():
    """A line buffer has to be cheaper than the rectangle it sits in, or it is not one."""
    tiles = imagery.corridor_tiles(START, END, 17, 223.4)
    xs = [x for x, _ in tiles]
    ys = [y for _, y in tiles]
    box = (max(xs) - min(xs) + 1) * (max(ys) - min(ys) + 1)
    assert len(tiles) < box, f"{len(tiles)} tiles fills the whole {box} tile box"


def check_a_point_off_the_end_of_the_track_measures_to_the_end_and_not_to_the_line():
    """The segment distance is clamped, so a point past the end is not on an extension."""
    beyond = (END[0] + 0.05, END[1] - 0.05)
    d = imagery.distance_to_segment_m(beyond, START, END)
    assert abs(d - imagery.haversine_m(beyond, END)) < 50, f"{d} is not the distance to the endpoint"


def check_the_stated_offset_is_read_from_the_digits():
    assert imagery.stated_offset_hours("CST-6") == -6
    assert imagery.stated_offset_hours("EST-5") == -5
    assert imagery.stated_offset_hours("GST10") == 10


def check_a_zone_field_that_is_not_an_abbreviation_and_an_offset_is_refused():
    for bad in ("CST", "-6", "Central-6:00"):
        try:
            imagery.stated_offset_hours(bad)
        except imagery.ImageryError:
            continue
        raise AssertionError(f"{bad!r} was parsed as a zone field")


def check_the_begin_instant_applies_the_stated_offset_and_nothing_else():
    got = imagery.begin_utc("29-AUG-21 21:11:00", "CST-6")
    assert got == datetime(2021, 8, 30, 3, 11, tzinfo=timezone.utc)
    # Late August is daylight time in Mississippi and the field still says CST. The
    # conversion does not correct for that, which is the whole point of the column's name.
    assert got.hour == 3, "the offset was corrected for daylight saving somewhere"


def check_the_zone_audit_counts_both_directions_of_the_field_being_wrong():
    frame = pl.DataFrame({
        "CZ_TIMEZONE": ["CST-6", "CDT-5", "PDT-7", "EST-5"],
        "BEGIN_DATE_TIME": ["01-JUL-21 10:00:00"] * 4,
        "MONTH_NAME": ["July", "July", "January", "July"],
    })
    audit = imagery.timezone_field_audit(frame)
    assert audit["rows"] == 4
    assert audit["rows_naming_a_daylight_zone"] == 2
    assert audit["daylight_named_in_december_to_february"] == 1
    assert audit["rows_naming_a_standard_zone_in_june_to_august"] == 2


def check_a_box_that_holds_a_point_says_so_and_one_without_a_box_never_does():
    boxed = imagery.Collection("20210902a", "ida", "services/index.json", date(2021, 9, 2),
                               (-90.0, 30.0, -89.0, 30.5))
    bare = imagery.Collection("20190904a", "dorian", "js/app.js", date(2019, 9, 4), None)
    assert boxed.box_holds(*START)
    assert not boxed.box_holds(40.0, -74.0)
    # An older catalogue publishes no footprint, so it can never be prefiltered in. A
    # `True` here would quietly put every undated footprint into every candidate list.
    assert not bare.box_holds(*START)


def check_the_tile_url_puts_x_before_y():
    """NOAA serves z/x/y and the ArcGIS convention is z/y/x. Getting this round the wrong
    way returns imagery, somewhere else entirely."""
    c = imagery.Collection("20210902a", "ida", "services/index.json", date(2021, 9, 2), None)
    assert c.tile_url(17, 32986, 53951).endswith("/17/32986/53951")


def check_coverage_counts_only_the_tiles_that_hold_imagery():
    held = [imagery.TileFetch(17, 1, 1, 200, 9000, "a" * 64, True, "u"),
            imagery.TileFetch(17, 1, 2, 200, 9000, "a" * 64, True, "u"),
            imagery.TileFetch(17, 1, 3, 200, 95, "b" * 64, False, "clear.png")]
    got = imagery.coverage(held)
    assert got == {"tiles_probed": 3, "tiles_with_imagery": 2,
                   "tiles_placeholder": 1, "distinct_image_hashes": 1}


def check_the_probe_request_asks_for_a_head():
    """Selection probes every candidate over the whole tile set, so this being a GET
    would pull several collections of imagery to keep one. urllib sends GET unless the
    request says otherwise, and the override is one method nothing else would catch."""
    req = imagery._Head("https://example.invalid/17/1/1")
    assert req.get_method() == "HEAD"
