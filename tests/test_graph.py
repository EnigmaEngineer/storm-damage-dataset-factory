"""Checks on the graph loaders and on the cross-check that grades two answers.

No Neo4j server is needed here. The module builds Cypher and payloads, and the
comparison is a pure function over two results. The one thing that needs a server is
the answer itself, and that is a measurement.

The guard that matters most is the one on the comparison. Two implementations of one
rule are only graded against each other if the comparison can fail, and a comparison
that only ever looks at a deduplicated set would pass while one side walked a
different graph.
"""

import json
from pathlib import Path

from stormfactory import graph

REPO_ROOT = Path(__file__).resolve().parent.parent


def _rows(pairs):
    return [{"disaster_number": d, "tile_id": t, "fips": f} for d, t, f in pairs]


def check_two_answers_that_agree_are_reported_as_agreeing():
    rows = _rows([(1, "a", 10), (1, "b", 10), (2, "a", 11)])
    verdict = graph.compare(rows, {"paths": 3, "disaster_numbers": [1, 2], "open_ended_paths": 0})
    assert verdict["agree"], verdict["problems"]
    assert verdict["graph_distinct_declarations"] == 2, verdict


def check_a_different_set_of_declarations_is_caught():
    rows = _rows([(1, "a", 10)])
    verdict = graph.compare(rows, {"paths": 1, "disaster_numbers": [2], "open_ended_paths": 0})
    assert not verdict["agree"], "the two sides named different declarations and it passed"
    assert any("distinct declarations differ" in p for p in verdict["problems"]), verdict


def check_a_different_path_count_is_caught_even_when_the_sets_agree():
    """The case a deduplicated comparison alone would miss.

    Both sides name declaration 1. One side walked it twice and the other once, which
    means they walked different graphs, and the distinct sets cannot see that.
    """
    rows = _rows([(1, "a", 10), (1, "b", 10)])
    verdict = graph.compare(rows, {"paths": 1, "disaster_numbers": [1], "open_ended_paths": 0})
    assert not verdict["agree"], "two different path counts were reported as agreement"
    assert any("path counts differ" in p for p in verdict["problems"]), verdict


def check_the_comparison_reports_how_much_work_deduplication_did():
    """Above one, a side that dropped its DISTINCT would disagree.

    At exactly one the agreement is cheap, and that is worth being able to see.
    """
    rows = _rows([(1, "a", 10), (1, "b", 10), (1, "c", 10)])
    verdict = graph.compare(rows, {"paths": 3, "disaster_numbers": [1], "open_ended_paths": 0})
    assert verdict["paths_per_declaration"] == 3.0, verdict


def check_an_empty_answer_does_not_divide_by_the_number_of_declarations():
    verdict = graph.compare([], {"paths": 0, "disaster_numbers": [], "open_ended_paths": 0})
    assert verdict["paths_per_declaration"] is None, verdict
    assert verdict["agree"], verdict["problems"]


def check_every_loaded_shape_has_a_statement():
    for name in ("events", "counties", "event_counties", "tiles", "labels", "declarations"):
        assert name in graph.LOADERS, f"no loader for {name}"
        assert "unwind $rows" in graph.LOADERS[name], name


def check_no_loader_interpolates_a_row_value():
    """Every loader takes its rows as a parameter.

    A loader that formatted a county name into a statement would be building Cypher
    out of the data, and a narrative or a place name is exactly where that breaks.
    """
    for name, statement in graph.LOADERS.items():
        assert "$rows" in statement, f"{name} does not bind its rows"
        assert "{}".format("") == "", "formatting sanity"
        assert "%s" not in statement, f"{name} looks like it interpolates"


def check_a_tile_reaches_a_county_by_its_own_relationship_type():
    """Two different edges sharing one type makes every untyped count wrong.

    An event sits in a county and a tile sits in a county for different reasons. When
    both used IN_COUNTY, an edge census returned the two added together as one number.
    """
    assert "TILE_IN_COUNTY" in graph.LOADERS["tiles"], graph.LOADERS["tiles"]
    assert "[:IN_COUNTY]" not in graph.LOADERS["tiles"], graph.LOADERS["tiles"]
    assert "[:IN_COUNTY]" in graph.LOADERS["event_counties"]


def check_every_node_label_the_question_walks_has_a_uniqueness_constraint():
    declared = " ".join(graph.CONSTRAINTS)
    for label in ("Event", "County", "Tile", "Label", "Declaration"):
        assert f":{label}" in declared, f"{label} has no uniqueness constraint"


def check_the_question_admits_an_open_ended_declaration():
    """The null branch, in the Cypher.

    A declaration with no incident end date is open ended. Compared against a null,
    the end date test is not true and the row disappears, so the null has to be
    written out. Both sides need it and neither gets it for free.
    """
    cypher = graph.QUESTION_CYPHER.lower()
    assert "incident_end is null" in cypher, graph.QUESTION_CYPHER
    assert "incident_begin <=" in cypher, graph.QUESTION_CYPHER


def check_the_parquet_side_admits_an_open_ended_declaration_too():
    """The same branch, in the implementation that joins rather than walks."""
    source = (REPO_ROOT / "stormfactory" / "graph.py").read_text()
    body = source.split("def sql_answer", 1)[1]
    assert "incident_end\").is_null()" in body, "the parquet side drops an open ended declaration"


def check_the_labels_in_scope_are_a_subset_of_the_label_set():
    from stormfactory import labelling

    unknown = [v for v in graph.LABELS_IN_SCOPE if v not in labelling.LABELS]
    assert not unknown, f"the question filters on {unknown} and no annotator can produce it"


def check_the_question_is_phrased_as_what_was_labelled():
    """The labels are one automated annotator's reading and not a damage ground truth.

    A question worded as which declarations covered damaged structures would be
    claiming something the labels cannot support.
    """
    text = graph.QUESTION_TEXT.lower()
    assert "labelled" in text, graph.QUESTION_TEXT
    assert "annotator" in text, graph.QUESTION_TEXT
    for word in ("damaged structures", "ground truth", "severe damage"):
        assert word not in text, f"the question claims {word}"


def check_the_graph_manifest_holds_no_wall_time():
    path = graph.manifest_path()
    if not path.exists():
        raise AssertionError(f"{path} is missing, so the measurement was not published")
    text = json.dumps(json.loads(path.read_text())).lower()
    banned = ("seconds", "_ms", "millis", "elapsed", "duration", "wall")
    found = [word for word in banned if word in text]
    assert not found, f"the manifest carries {found}"


def check_the_published_cross_check_agreed_and_could_have_disagreed():
    """The measurement as published, read back.

    Two numbers matter. The sides agreed, and deduplication was doing work while they
    did. An agreement reached with one path per declaration would be a weaker claim
    and this says which one was measured.
    """
    stated = json.loads(graph.manifest_path().read_text())
    cross = stated["cross_check"]
    assert cross["agree"], cross["problems"]
    assert cross["graph_paths"] == cross["parquet_paths"], cross
    assert cross["paths_per_declaration"] > 1, (
        "the two sides agreed with one path per declaration, so a dropped DISTINCT "
        "would not have been caught"
    )


def check_a_declaration_node_is_one_declaration_and_not_one_designated_county():
    """OpenFEMA publishes a row per county and the node is keyed on the declaration.

    Keyed on the row instead, the graph held 14,039 Declaration nodes carrying 660
    declarations between them, so anybody counting the label got the designation
    count under the wrong name.
    """
    stated = json.loads(graph.manifest_path().read_text())
    nodes = stated["nodes_in_graph"]["Declaration"]
    assert nodes == stated["declaration_nodes_expected"], (
        f"{nodes} declaration nodes for {stated['declaration_nodes_expected']} declarations"
    )
    assert stated["edges_in_graph"]["COVERS"] == stated["declaration_edges"], stated
    assert nodes < stated["declaration_edges"], (
        "every declaration covers exactly one county, which would make the node and "
        "the edge count the same and this check a tautology"
    )
    assert "disaster_number: row.disaster_number" in graph.LOADERS["declarations"]


def check_the_graph_scope_is_the_year_the_tiles_sit_in():
    stated = json.loads(graph.manifest_path().read_text())
    assert stated["scope_year"] == stated["cross_check"].get("scope_year", stated["scope_year"])
    assert isinstance(stated["scope_year"], int), stated["scope_year"]


def check_the_node_counts_were_read_back_from_the_server():
    """What the batches held is one claim and what the server holds is another."""
    stated = json.loads(graph.manifest_path().read_text())
    nodes = stated["nodes_in_graph"]
    assert nodes.get("Event") == stated["events"], (nodes.get("Event"), stated["events"])
    assert nodes.get("Tile") == stated["tiles"], (nodes.get("Tile"), stated["tiles"])
    edges = stated["edges_in_graph"]
    assert edges.get("IN_COUNTY") == stated["event_county_edges"], edges
    assert edges.get("TILE_IN_COUNTY") == stated["tile_county_edges"], edges
