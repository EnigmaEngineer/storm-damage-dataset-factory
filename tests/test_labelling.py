"""Checks on the Label Studio project and on the agreement that is not reported.

The guard is the one worth having. Everything else here is parsing, and a parser that
loses a label fails loudly. A guard that quietly turns one annotator into a reliability
figure fails silently and publishes a number.
"""

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import polars as pl

from stormfactory import labelling

REPO_ROOT = Path(__file__).resolve().parent.parent


def _labels(pairs) -> pl.DataFrame:
    return pl.DataFrame(
        [{"tile_id": t, "annotator": a, "label": l} for t, a, l in pairs]
    )


def check_the_committed_config_is_well_formed_xml():
    ET.fromstring((REPO_ROOT / "labelstudio" / "label_config.xml").read_text())


def check_the_committed_config_is_the_one_the_module_defines():
    """A config edited in the file and not in the module is a project nobody can rebuild."""
    on_disk = (REPO_ROOT / "labelstudio" / "label_config.xml").read_text()
    assert on_disk == labelling.LABEL_CONFIG


def check_the_config_offers_exactly_the_label_set():
    root = ET.fromstring(labelling.LABEL_CONFIG)
    choices = [c.attrib["value"] for c in root.iter("Choice")]
    assert choices == list(labelling.LABELS), f"{choices} is not {list(labelling.LABELS)}"
    assert all(c.attrib.get("hint") for c in root.iter("Choice")), "a choice carries no hint"


def check_every_field_the_config_names_is_in_every_task():
    """Label Studio renders `$field` from the task data and shows an empty panel when the
    field is missing, so a project can look complete and be unanswerable."""
    wanted = set(re.findall(r"\$([a-z_]+)", labelling.LABEL_CONFIG))
    export = json.loads((REPO_ROOT / "labels" / "tile_labels.json").read_text())
    for task in export["tasks"]:
        missing = wanted - set(task["data"])
        assert not missing, f"{task['data'].get('tile_id')} is missing {sorted(missing)}"


def check_the_committed_pass_parses_and_carries_one_label_per_tile():
    labels = labelling.read_labels(REPO_ROOT / "labels" / "tile_labels.json")
    assert labels.height == labels["tile_id"].n_unique(), "a tile carries two annotations"
    assert set(labels["label"].unique()) <= set(labelling.LABELS)


def check_a_label_outside_the_set_is_refused(tmp=None):
    bad = {"tasks": [{"data": {"tile_id": "17/1/1"}, "annotations": [
        {"completed_by": "x", "result": [
            {"from_name": "damage", "to_name": "post", "type": "choices",
             "value": {"choices": ["catastrophic"]}}]}]}]}
    path = REPO_ROOT / "tests" / "fixtures" / "_tmp_labels.json"
    path.write_text(json.dumps(bad))
    try:
        labelling.read_labels(path)
    except labelling.LabellingError:
        return
    finally:
        path.unlink()
    raise AssertionError("a label outside the set was accepted")


def check_one_annotator_gives_unmeasured_and_no_number():
    got = labelling.agreement(_labels([("a", "run", "none"), ("b", "run", "clear")]))
    assert got["status"] == "unmeasured"
    assert got["annotators"] == 1
    assert not any(isinstance(v, float) for v in got.values()), "an unmeasured result carries a float"
    assert "kappa" not in json.dumps(got).lower()


def check_two_annotators_give_a_kappa_over_the_tiles_they_share():
    got = labelling.agreement(_labels([
        ("a", "one", "none"), ("a", "two", "none"),
        ("b", "one", "clear"), ("b", "two", "clear"),
        ("c", "one", "none"), ("c", "two", "clear"),
        ("d", "one", "clear"),
    ]))
    assert got["status"] == "measured"
    assert got["tiles_both_labelled"] == 3, "the tile only one annotator saw was counted"
    assert -1.0 <= got["cohens_kappa"] <= 1.0


def check_kappa_is_one_on_total_agreement_and_zero_on_chance():
    assert abs(labelling.cohens_kappa(["a", "b", "a", "b"], ["a", "b", "a", "b"]) - 1.0) < 1e-12
    # Two annotators splitting the same way with no relation between them. Observed
    # agreement is a half and chance agreement is a half, so kappa is zero rather than the
    # fifty percent a raw agreement figure would advertise.
    assert abs(labelling.cohens_kappa(["a", "a", "b", "b"], ["a", "b", "a", "b"])) < 1e-12


def check_kappa_refuses_a_case_it_cannot_answer():
    for a, b in ((["a"], ["a", "b"]), ([], []), (["a", "a"], ["a", "a"])):
        try:
            labelling.cohens_kappa(a, b)
        except labelling.LabellingError:
            continue
        raise AssertionError(f"kappa answered for {a} against {b}")


def check_tasks_leave_out_a_tile_that_is_missing_a_panel():
    frame = pl.DataFrame({
        "tile_id": ["17/1/1", "17/1/2"],
        "zoom": [17, 17], "tile_x": [1, 1], "tile_y": [1, 2],
        "post_has_imagery": [True, True],
        "pre_has_imagery": [True, False],
        "post_collection": ["p", "p"], "pre_collection": ["q", "q"],
        "post_acquired": ["2021-09-02"] * 2, "pre_acquired": ["2020-10-30"] * 2,
        "metres_per_pixel": [1.03, 1.03], "metres_from_track": [10.0, 20.0],
    })
    tasks = labelling.tasks_for_tiles(frame, "post/{z}/{x}/{y}", "pre/{z}/{x}/{y}")
    assert [t["data"]["tile_id"] for t in tasks] == ["17/1/1"]
    assert tasks[0]["data"]["post_url"] == "post/17/1/1"
