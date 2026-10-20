import polars as pl

from stormfactory import schema


def check_magnitude_letters_become_dollars():
    assert schema.parse_damage("25.00K") == 25_000
    assert schema.parse_damage("12.34M") == 12_340_000
    assert schema.parse_damage("1.50B") == 1_500_000_000


def check_a_bare_number_is_dollars():
    # No bare values appear in 2019 to 2023. The format allows them and silently
    # reading 7 as 7000 would be worse than refusing it.
    assert schema.parse_damage("7") == 7


def check_blank_is_not_reported_rather_than_zero():
    assert schema.parse_damage("") is None
    assert schema.parse_damage("   ") is None
    assert schema.parse_damage(None) is None
    assert schema.parse_damage("0.00K") == 0


def check_an_unknown_magnitude_letter_is_refused():
    for bad in ("25.00T", "abc", "1,000K", "-5K"):
        try:
            schema.parse_damage(bad)
        except schema.SchemaError:
            continue
        raise AssertionError(f"{bad!r} was accepted")


def _one_good_row(year=2021):
    row = {c: None for c in schema.RAW_COLUMNS}
    row.update(
        BEGIN_YEARMONTH=202106, BEGIN_DAY=1, BEGIN_TIME=1200, END_YEARMONTH=202106,
        END_DAY=1, END_TIME=1300, EPISODE_ID=1, EVENT_ID=10, STATE="OHIO", STATE_FIPS=39,
        YEAR=year, MONTH_NAME="June", EVENT_TYPE="Hail", CZ_TYPE="C", CZ_FIPS=35,
        CZ_NAME="CUYAHOGA", WFO="CLE", BEGIN_DATE_TIME="01-JUN-21 12:00:00",
        CZ_TIMEZONE="EST-5", END_DATE_TIME="01-JUN-21 13:00:00", INJURIES_DIRECT=0,
        INJURIES_INDIRECT=0, DEATHS_DIRECT=0, DEATHS_INDIRECT=0, DAMAGE_PROPERTY="5.00K",
        DAMAGE_CROPS="", SOURCE="Public", BEGIN_LAT=41.5, BEGIN_LON=-81.7,
        TOR_OTHER_CZ_FIPS=None, BEGIN_RANGE=1, END_RANGE=1, DATA_SOURCE="CSV",
    )
    return row


def _frame(rows):
    df = pl.DataFrame(rows, schema={c: pl.Utf8 for c in schema.RAW_COLUMNS}, orient="row")
    return schema.cast_and_derive(df)


def _as_text(row):
    return [None if v is None else str(v) for v in (row[c] for c in schema.RAW_COLUMNS)]


def check_a_clean_partition_passes():
    df = _frame([_as_text(_one_good_row())])
    assert schema.validate(df, 2021) == []


def check_a_repeated_event_id_is_caught():
    a, b = _one_good_row(), _one_good_row()
    df = _frame([_as_text(a), _as_text(b)])
    rules = [v.rule for v in schema.validate(df, 2021)]
    assert "unique" in rules, rules


def check_a_row_from_another_year_is_caught():
    df = _frame([_as_text(_one_good_row(year=1999))])
    bad = schema.validate(df, 2021)
    partition = [v for v in bad if v.rule == "partition"]
    assert partition, bad
    assert "2021" in partition[0].detail, partition[0].detail


def check_a_missing_required_field_is_caught():
    row = _one_good_row()
    row["WFO"] = None
    df = _frame([_as_text(row)])
    assert any(v.rule == "required" and "WFO" in v.detail for v in schema.validate(df, 2021))


def check_an_out_of_range_longitude_is_caught():
    row = _one_good_row()
    row["BEGIN_LON"] = -999.0
    df = _frame([_as_text(row)])
    assert any(v.rule == "range" for v in schema.validate(df, 2021))


def check_an_unknown_cz_type_is_caught():
    row = _one_good_row()
    row["CZ_TYPE"] = "X"
    df = _frame([_as_text(row)])
    assert any(v.rule == "domain" and "X" in v.detail for v in schema.validate(df, 2021))


def check_every_rule_reports_rather_than_the_first_one():
    row = _one_good_row()
    row["WFO"] = None
    row["CZ_TYPE"] = "X"
    row["BEGIN_LAT"] = 999.0
    df = _frame([_as_text(row)])
    assert len(schema.validate(df, 2021)) >= 3


def check_validate_or_raise_names_the_count():
    row = _one_good_row()
    row["WFO"] = None
    df = _frame([_as_text(row)])
    try:
        schema.validate_or_raise(df, 2021)
    except schema.SchemaError as e:
        assert "fails 1 rules" in str(e), str(e)
    else:
        raise AssertionError("a bad partition was accepted")


def check_has_point_follows_begin_lat():
    row = _one_good_row()
    row["BEGIN_LAT"] = None
    row["BEGIN_LON"] = None
    df = _frame([_as_text(row)])
    assert df["has_point"].to_list() == [False]
