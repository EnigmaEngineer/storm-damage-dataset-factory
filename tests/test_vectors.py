"""Checks on the vector column and on the statements that load and query it.

None of these need a Postgres server. The module under test builds statements and
payloads, and a check that needed a running database would be a gate only on a
machine that happened to have one. The one thing that genuinely needs a server is
the recall figure, and that is a measurement rather than a gate.

The guards worth having here are the ones that fail quietly otherwise. A vectoriser
that is not a function of its input, a COPY payload that loses a row to a tab inside
a value, and a filter composed from a caller's string are all things that look fine
in a passing run.
"""

import datetime
import json
from pathlib import Path

import numpy as np
import polars as pl

from stormfactory import digest, release, vectors, vectorstore

REPO_ROOT = Path(__file__).resolve().parent.parent

# Enough distinct documents to support a small reduction, built rather than typed so
# the corpus is wide enough for a vocabulary to exist at all.
CORPUS = [
    f"tornado touched down near the river and removed the roof of barn {n} "
    f"with hail reported {n} miles south of town"
    for n in range(40)
] + [
    f"flash flooding closed highway {n} after heavy rain and a water rescue "
    f"was reported in the county {n}"
    for n in range(40)
]


def _small_frame(n: int = 4, width: int = 3) -> pl.DataFrame:
    rng = np.random.default_rng(0)
    return pl.DataFrame(
        {"EVENT_ID": list(range(n)), "vector": rng.random((n, width)).astype("float32")},
        schema={"EVENT_ID": pl.Int64, "vector": pl.Array(pl.Float32, width)},
    )


def check_the_reduction_is_a_function_of_its_input():
    """Two runs over the same documents give the same vectors.

    This is the claim the release rests on. A content digest over a column that
    moves between rebuilds is a number that fails for no reason a reader can act on.
    """
    first, first_facts = vectors.vectorise(CORPUS, dimension=8)
    second, second_facts = vectors.vectorise(CORPUS, dimension=8)
    assert first == second, "the same corpus produced two different reductions"
    assert first_facts == second_facts, f"{first_facts} against {second_facts}"


def check_two_reductions_in_one_process_do_not_see_each_other():
    """A second call on a different corpus does not inherit the first fit."""
    _, wide = vectors.vectorise(CORPUS, dimension=8)
    _, narrow = vectors.vectorise(CORPUS[:60], dimension=8)
    assert wide["documents"] == len(CORPUS), wide["documents"]
    assert narrow["documents"] == 60, narrow["documents"]


def check_a_corpus_too_small_to_reduce_is_refused():
    """Fewer documents than the width cannot support the reduction and must raise."""
    try:
        vectors.vectorise(CORPUS[:5], dimension=8)
    except vectors.VectorError:
        return
    raise AssertionError("a corpus smaller than the width was reduced anyway")


def check_an_empty_corpus_is_refused():
    try:
        vectors.vectorise([], dimension=8)
    except vectors.VectorError:
        return
    raise AssertionError("an empty corpus did not raise")


def check_the_vectorised_text_went_through_the_scrubber():
    """The column is built from scrubbed narratives and not from raw ones.

    The expectation comes from the scrubber rather than from a hard coded string, so
    this check is about the wiring and the scrubber's own rules stay its own business.
    """
    from stormfactory import scrub

    raw = "Mr. Johnson was injured when the roof failed, a 54 year old man was hurt"
    frame = pl.DataFrame({vectors.TEXT_COLUMN: [raw]})
    out = vectors.scrubbed_texts(frame)
    expected, spans = scrub.scrub(raw)
    assert out == [expected], f"{out} against {[expected]}"
    if spans:
        assert out[0] != raw, "the scrubber found something and the text came back unchanged"


def check_the_begin_date_comes_out_of_the_two_integer_columns():
    frame = pl.DataFrame(
        {"BEGIN_YEARMONTH": [202103, 201912], "BEGIN_DAY": [25, 1]}
    )
    got = vectors._event_date(frame).to_list()
    assert [str(d) for d in got] == ["2021-03-25", "2019-12-01"], got


def check_every_filter_column_has_a_type_in_the_store():
    """The artifact declares what is filterable and the store has to be able to hold it."""
    missing = [c for c in vectors.FILTER_COLUMNS if c not in vectorstore.FILTER_TYPES]
    assert not missing, f"no column type for {missing}"


def check_the_create_statement_names_every_filter_column():
    sql = vectorstore.create_sql()
    for name in vectors.FILTER_COLUMNS:
        assert f'"{name}"' in sql, f"{name} is filterable and the table has no column for it"
    assert f"vector({vectors.DIMENSION})" in sql, sql


def check_the_fingerprint_moves_when_a_value_moves():
    frame = _small_frame()
    before = vectors.vector_fingerprint(frame)
    nudged = frame.with_columns(
        pl.Series(
            "vector",
            [[v + (0.5 if i == 0 else 0.0) for i, v in enumerate(row)] for row in frame["vector"].to_list()],
            dtype=pl.Array(pl.Float32, 3),
        )
    )
    assert vectors.vector_fingerprint(nudged) != before, "the fingerprint ignored a changed value"


def check_the_fingerprint_ignores_the_row_order_it_is_given():
    frame = _small_frame()
    assert vectors.vector_fingerprint(frame) == vectors.vector_fingerprint(
        frame.reverse()
    ), "the fingerprint depends on the order the rows arrived in"


def check_the_digest_has_a_rule_for_a_vector_column():
    """A float array is a dtype the digest had no rule for until the column existed."""
    frame = _small_frame()
    assert digest.frame_digest(frame, ["EVENT_ID"])


def check_the_digest_separates_two_widths():
    narrow = digest.frame_digest(_small_frame(4, 3), ["EVENT_ID"])
    wide = digest.frame_digest(_small_frame(4, 4), ["EVENT_ID"])
    assert narrow != wide, "a three wide and a four wide column digested the same"


def check_the_digest_refuses_a_nan_inside_a_vector():
    frame = pl.DataFrame(
        {"EVENT_ID": [1], "vector": [[float("nan"), 0.0]]},
        schema={"EVENT_ID": pl.Int64, "vector": pl.Array(pl.Float32, 2)},
    )
    try:
        digest.frame_digest(frame, ["EVENT_ID"])
    except digest.DigestError:
        return
    raise AssertionError("a NaN inside a vector was hashed rather than refused")


def check_an_identifier_with_a_quote_is_doubled():
    assert vectorstore.quote_identifier('odd"name') == '"odd""name"'


def check_an_identifier_with_a_null_byte_is_refused():
    try:
        vectorstore.quote_identifier("bad\x00name")
    except vectorstore.StoreError:
        return
    raise AssertionError("a null byte in an identifier was quoted rather than refused")


def check_a_filter_name_the_module_does_not_define_is_refused():
    try:
        vectorstore.compose_where(["event_type", "something_invented"])
    except vectorstore.StoreError:
        return
    raise AssertionError("an unknown filter name was composed into a statement")


def check_no_filter_shape_interpolates_a_value():
    """Every fragment binds its value rather than carrying one.

    A fragment that formatted a value into the string would put a caller's text into
    SQL, and the fragments are the only place a predicate comes from.
    """
    for name, fragment in vectorstore.FILTERS.items():
        assert "%(" in fragment, f"{name} carries no bound parameter"


def check_the_search_statement_binds_the_vector_and_the_row_count():
    sql = vectorstore.search_sql(vectorstore.compose_where(["event_type"]))
    assert "%(q)s" in sql and "%(k)s" in sql, sql
    assert "where" in sql, sql
    assert "<=>" in sql, "the search does not order by a distance"


def check_the_search_statement_without_a_filter_has_no_where_clause():
    assert "where" not in vectorstore.search_sql(None)


def check_the_copy_payload_carries_one_line_per_row():
    frame = pl.DataFrame(
        {
            "EVENT_ID": [2, 1],
            "narrative_chars": [10, 20],
            "event_type": ["Hail", "Tornado"],
            "state": ["TEXAS", "IOWA"],
            "state_fips": [48, 19],
            "event_date": [datetime.date(2021, 3, 1), datetime.date(2021, 4, 2)],
            "vector": [[0.5, 0.25], [1.0, 2.0]],
        },
        schema={
            "EVENT_ID": pl.Int64,
            "narrative_chars": pl.Int64,
            "event_type": pl.String,
            "state": pl.String,
            "state_fips": pl.Int64,
            "event_date": pl.Date,
            "vector": pl.Array(pl.Float32, 2),
        },
    )
    lines = vectorstore.copy_buffer(frame).read().strip().split("\n")
    assert len(lines) == 2, lines
    # Sorted on the way out, like every other artifact in this release.
    assert lines[0].split("\t")[0] == "1", lines[0]
    assert lines[0].split("\t")[2].startswith("["), lines[0]
    assert len(lines[0].split("\t")) == 3 + len(vectors.FILTER_COLUMNS), lines[0]


def check_the_copy_payload_refuses_a_value_holding_a_tab():
    """A tab inside a value would silently shift every later column on that row."""
    frame = pl.DataFrame(
        {
            "EVENT_ID": [1],
            "narrative_chars": [1],
            "event_type": ["Ha\til"],
            "state": ["TEXAS"],
            "state_fips": [48],
            "event_date": [datetime.date(2021, 1, 1)],
            "vector": [[0.1, 0.2]],
        },
        schema={
            "EVENT_ID": pl.Int64,
            "narrative_chars": pl.Int64,
            "event_type": pl.String,
            "state": pl.String,
            "state_fips": pl.Int64,
            "event_date": pl.Date,
            "vector": pl.Array(pl.Float32, 2),
        },
    )
    try:
        vectorstore.copy_buffer(frame)
    except vectorstore.StoreError:
        return
    raise AssertionError("a tab inside a value was written into a tab separated payload")


def check_the_copy_column_order_matches_the_payload():
    assert vectorstore.copy_columns()[:3] == ["event_id", "narrative_chars", "vector"]
    assert vectorstore.copy_columns()[3:] == list(vectors.FILTER_COLUMNS)


def check_recall_counts_a_set_and_not_an_order():
    assert vectorstore.recall([1, 2, 3], [3, 2, 1]) == 1.0
    assert vectorstore.recall([1, 2, 3, 4], [1, 2, 9, 9]) == 0.5


def check_recall_on_an_empty_exact_result_is_refused():
    try:
        vectorstore.recall([], [1])
    except vectorstore.StoreError:
        return
    raise AssertionError("recall was reported against an empty exact result")


def check_a_tie_is_told_apart_from_an_approximation():
    """Equal distances mean the two results differ by a tie and not by index error."""
    assert vectorstore.tie_equivalent([0.1, 0.2], [0.2, 0.1])
    assert not vectorstore.tie_equivalent([0.1, 0.2], [0.1, 0.3])
    assert not vectorstore.tie_equivalent([0.1], [0.1, 0.2])


def check_the_vector_artifact_is_registered_with_a_sort_key_that_identifies_a_row():
    entry = release.ARTIFACTS["narrative_vectors"]
    assert entry["sort_by"] == ["EVENT_ID"], entry


def check_the_embeddings_manifest_holds_no_wall_time():
    """A measurement manifest carries counts. A duration does not reproduce elsewhere."""
    path = REPO_ROOT / "measurements" / "embeddings.json"
    if not path.exists():
        raise AssertionError(f"{path} is missing, so the measurement was not published")
    text = json.dumps(json.loads(path.read_text()))
    banned = ("seconds", "_ms", "millis", "elapsed", "duration", "wall")
    found = [word for word in banned if word in text.lower()]
    assert not found, f"the manifest carries {found}"


def check_the_recall_figures_say_whether_the_vector_index_was_used():
    """A recall figure whose plan never touched the vector index is not about the index.

    This is the guard. The first version of the measurement reported recall for three
    query shapes without recording the plan, and the selective shape turned out never
    to reach the vector index at all. A reader given only the number would have read
    it as index error.
    """
    path = REPO_ROOT / "measurements" / "embeddings.json"
    stated = json.loads(path.read_text())
    shapes = [k[len("recall_"):] for k in stated if k.startswith("recall_") and k != "recall_k"]
    assert shapes, "the manifest publishes no recall figure"
    for shape in shapes:
        if shape == "queries":
            continue
        key = f"queries_planned_through_the_vector_index_{shape}"
        assert key in stated, f"{shape} has a recall figure and no plan count"
        tie = f"queries_differing_only_by_a_distance_tie_{shape}"
        assert tie in stated, f"{shape} has a recall figure and no tie count"


def _fidelity_manifest() -> dict:
    return json.loads((REPO_ROOT / "measurements" / "vector-fidelity.json").read_text())


def check_the_fidelity_table_in_the_readme_matches_the_manifest():
    """Every cell of the width table, read back.

    Four numbers per row. The recovery figure is the one a reader would quote, and it
    is meaningless without the width and the byte count beside it.
    """
    stated = _fidelity_manifest()["widths"]
    text = (REPO_ROOT / "README.md").read_text()
    seen = 0
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 4 or cells[0] not in stated:
            continue
        seen += 1
        entry = stated[cells[0]]
        assert float(cells[1]) == entry["explained_variance_cumulative"], cells
        assert float(cells[2]) == entry["tfidf_neighbourhood_recovery"], cells
        assert int(cells[3].replace(",", "")) == entry["column_bytes_uncompressed"], cells
    assert seen == len(stated), f"parsed {seen} width rows and expected {len(stated)}"


def check_the_published_width_is_the_one_the_table_recommends_keeping():
    """The README argues the width from the curve, so the curve has to still be rising.

    If a later measurement flattens it, the paragraph claiming a narrower column would
    cost something real becomes wrong and has to be rewritten rather than renumbered.
    """
    stated = _fidelity_manifest()
    assert stated["published_width"] == vectors.DIMENSION
    assert stated["recovery_still_rising_at_the_published_width"], (
        "recovery has levelled off at the published width, so the readme paragraph "
        "arguing the width from a rising curve is now wrong"
    )
    prose = " ".join((REPO_ROOT / "README.md").read_text().split())
    assert "has not levelled off" in prose


def check_the_fidelity_manifest_names_the_stack_that_built_the_column():
    stack = _fidelity_manifest()["build_stack"]
    for name in ("scikit_learn", "numpy", "python"):
        assert stack.get(name), f"the manifest does not name {name}"


def check_the_fidelity_manifest_agrees_with_the_embeddings_manifest():
    """Two manifests describing one column have to describe the same column."""
    fid, emb = _fidelity_manifest(), json.loads(
        (REPO_ROOT / "measurements" / "embeddings.json").read_text()
    )
    assert fid["published_width"] == emb["dimension"], (fid["published_width"], emb["dimension"])
    assert fid["documents"] == emb["documents"], (fid["documents"], emb["documents"])
    assert fid["vocabulary"] == emb["vocabulary"]
    assert fid["seed"] == emb["seed"]


def check_the_readme_does_not_read_the_recovery_figure_as_a_quality_score():
    """A latent reduction is meant to depart from the lexical neighbourhood.

    Reporting recovery as if one were the target would turn a design property into a
    failure, and reporting it without the caveat would let a reader do that.
    """
    prose = " ".join((REPO_ROOT / "README.md").read_text().split())
    assert "is not a score of" in prose
    assert "part of it is the technique working" in prose


def check_the_fidelity_manifest_holds_no_wall_time():
    text = json.dumps(_fidelity_manifest()).lower()
    found = [w for w in ("seconds", "_ms", "millis", "elapsed", "duration", "wall") if w in text]
    assert not found, f"the manifest carries {found}"
