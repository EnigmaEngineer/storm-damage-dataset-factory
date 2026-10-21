"""The narrative scrub, graded against labels written by hand.

The negative cases matter more than the positive ones here. There are two names in
the whole 97 row labelled sample and roughly a hundred things that look like names and
are roads, agencies and shop signs. A detector that takes those redacts the geography
this dataset is for.
"""

import json
from pathlib import Path

from stormfactory import grading, scrub

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _cases():
    return json.loads((FIXTURES / "scrub_cases.json").read_text())


def _sample():
    return json.loads((FIXTURES / "labelled_narratives.json").read_text())


def check_every_positive_case_has_all_its_names_found():
    for case in _cases()["positive"]:
        got = [s.text for s in scrub.person_names(case["text"])]
        for want in case["expect_names"]:
            assert any(want in g or g in want for g in got), (case["why"], want, got)


def check_no_negative_case_yields_a_name():
    for case in _cases()["negative"]:
        got = scrub.person_names(case["text"])
        assert not got, (case["why"], [s.text for s in got])


def check_an_age_on_something_that_is_not_a_person_is_left_alone():
    text = "A 60 year old oak tree was uprooted 3 miles NNW of Decatur."
    assert scrub.casualty_descriptors(text) == []


def check_an_age_with_a_sex_word_is_covered_including_the_sex_word():
    spans = scrub.casualty_descriptors("A 19 year old male drowned at Fort Morgan.")
    assert len(spans) == 1, spans
    assert spans[0].text == "19 year old male", spans[0].text


def check_an_age_with_no_sex_word_stops_at_the_age_phrase():
    spans = scrub.casualty_descriptors("A 53 year old drowned while swimming at the beach.")
    assert len(spans) == 1, spans
    assert spans[0].text == "53 year old", spans[0].text


def check_the_scrub_replaces_what_it_finds_and_nothing_else():
    text = "A 19 year old male drowned near Rock River, where the Highway Patrol reported debris."
    out, spans = scrub.scrub(text)
    assert out == "A [AGE] drowned near Rock River, where the Highway Patrol reported debris.", out
    assert len(spans) == 1


def check_a_surname_confirmed_by_an_honorific_is_found_again_without_one():
    text = "Mr. Ardwell was reported missing. Theodore R. Ardwell was recovered later."
    got = [s.text for s in scrub.person_names(text)]
    assert "Mr. Ardwell" in got, got
    assert any("Ardwell" in g and g != "Mr. Ardwell" for g in got), got


def check_dr_is_not_treated_as_an_honorific():
    """Over the five release years Dr. matches 272 narratives and 269 are Drive."""
    assert scrub.person_names("A tree fell across Dr. Brock Road south of Meridian.") == []


def check_the_labelled_sample_withholds_the_narratives_that_carry_real_names():
    sample = _sample()
    withheld = [r for r in sample["rows"] if r.get("withheld")]
    assert withheld, "the sample should record which rows are withheld"
    for row in withheld:
        assert row["narrative"] is None
        assert row["narrative_sha256"]
        assert any(l["kind"] == "name" for l in row["labels"])


def check_grading_reports_how_many_rows_it_could_not_grade():
    """A recall figure over a sample missing its positives has to say so."""
    sample = _sample()
    graded = grading.grade(sample["rows"], Path("/nonexistent"))
    assert graded["rows_skipped_because_withheld"] == 2, graded
    assert graded["rows_graded"] == len(sample["rows"]) - 2


def check_the_naive_baseline_is_measured_and_not_asserted():
    sample = _sample()
    graded = grading.grade(sample["rows"], Path("/nonexistent"))
    base = graded["naive_capitalised_bigram_baseline"]
    assert base["narratives_it_fires_on"] > 20, base
    assert base["precision_pct"] == 0.0, base
