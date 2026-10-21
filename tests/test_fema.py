"""The declarations reader, and the pandemic that sits inside it."""

import json
import tempfile
from pathlib import Path

from stormfactory import fema


def _record(**over):
    base = {
        "femaDeclarationString": "DR-4000-OH", "disasterNumber": 4000, "state": "OH",
        "declarationType": "DR", "declarationDate": "2019-06-01T00:00:00.000Z",
        "incidentType": "Severe Storm", "declarationTitle": "SEVERE STORMS",
        "incidentBeginDate": "2019-05-27T00:00:00.000Z",
        "incidentEndDate": "2019-05-29T00:00:00.000Z",
        "fipsStateCode": "39", "fipsCountyCode": "041",
        "designatedArea": "Delaware (County)",
        "ihProgramDeclared": True, "paProgramDeclared": True,
    }
    base.update(over)
    return base


def _write(records):
    p = Path(tempfile.mkdtemp(prefix="stormfactory-fema-")) / "decl.json"
    p.write_text(json.dumps(records))
    return p


def check_a_county_designation_becomes_one_row_with_a_full_fips_code():
    df = fema.read_declarations(_write([_record()]))
    assert len(df) == 1
    assert df["county_fips"].to_list() == [39041]
    assert str(df["incident_begin"][0]) == "2019-05-27"


def check_a_statewide_designation_is_dropped_and_counted():
    path = _write([_record(), _record(fipsCountyCode="000", femaDeclarationString="DR-4001-OH")])
    assert len(fema.read_declarations(path)) == 1
    assert fema.statewide_count(path) == 1


def check_an_open_incident_keeps_a_null_end_rather_than_being_dropped():
    df = fema.read_declarations(_write([_record(incidentEndDate=None)]))
    assert len(df) == 1
    assert df["incident_end"][0] is None


def check_a_missing_field_raises_because_the_api_shape_changed():
    bad = _record()
    del bad["incidentType"]
    try:
        fema.read_declarations(_write([bad]))
    except fema.FemaError as exc:
        assert "incidentType" in str(exc)
        return
    raise AssertionError("a missing field should raise")


def check_an_empty_file_raises():
    try:
        fema.read_declarations(_write([]))
    except fema.FemaError:
        return
    raise AssertionError("no records should raise")


def check_the_pandemic_is_named_and_is_not_in_the_weather_list():
    """The COVID-19 declaration is 91.5 percent of every county-and-date match.

    It is filed as Biological, covers 3,238 counties and runs 1,207 days, so a join
    that does not separate it reports a pandemic as a weather result.
    """
    assert fema.PANDEMIC_INCIDENT == "Biological"
    assert fema.PANDEMIC_INCIDENT not in fema.WEATHER_INCIDENTS
    assert "Severe Storm" in fema.WEATHER_INCIDENTS


def check_the_same_declaration_and_county_does_not_repeat():
    df = fema.read_declarations(_write([_record(), _record()]))
    assert len(df) == 1


def check_the_incident_type_lists_cover_the_whole_published_vocabulary():
    """27 values, read off the API rather than off one release's five years.

    The first version of the weather list held only the 12 types that appeared in the
    slice, which is the same mistake as calling a column non-null because it happened
    never to be null in the years that were looked at.
    """
    assert len(fema.KNOWN_INCIDENTS) == 27, len(fema.KNOWN_INCIDENTS)
    assert len(set(fema.WEATHER_INCIDENTS) & set(fema.NOT_WEATHER_INCIDENTS)) == 0
    for name in ("Straight-Line Winds", "Tropical Depression", "Typhoon"):
        assert name in fema.WEATHER_INCIDENTS, name
    for name in ("Earthquake", "Tsunami", "Terrorist", "Volcanic Eruption"):
        assert name in fema.NOT_WEATHER_INCIDENTS, name


def check_an_incident_type_outside_both_lists_is_reported_rather_than_sorted():
    import polars as pl

    frame = pl.DataFrame({"incident_type": ["Flood", "Space Weather"]})
    assert fema.unknown_incident_types(frame) == ["Space Weather"]


def check_no_incident_type_in_the_release_is_unclaimed():
    import polars as pl

    frame = pl.DataFrame({"incident_type": list(fema.KNOWN_INCIDENTS)})
    assert fema.unknown_incident_types(frame) == []
