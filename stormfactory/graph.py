"""The release as a graph, and one answer computed twice.

The Parquet artifacts are five tables that join on county and date. A researcher's
real question runs along a path rather than across a join, and the path is the thing
a graph says plainly: a storm event sits in counties, a county holds imagery tiles
over that event, a tile carries a label, and a county appears in a disaster
declaration. Asking which declarations covered counties where a tile was labelled as
possibly damaged is one pattern here.

The reason this module exists in a dataset repository, rather than in whatever a
reader builds downstream, is the cross-check. A graph query and a SQL query over the
same rows should give the same answer, and when they do not, one of them is wrong
about the data rather than slow. So the question below is written twice, once in
Cypher and once over the Parquet, and a check compares them.

Two implementations of one rule have to be graded against each other rather than
each against a truth nobody has. That only means something if they could have
disagreed, so the comparison reports the undeduplicated path count beside the
distinct count. The pattern below reaches a declaration by more than one path for
most declarations it finds. A version of either side that forgot to deduplicate
would return a larger number, and the two sides would part company. Reporting only
the distinct counts would hide the fact that the agreement was ever at risk.

The place the two engines genuinely differ is the null. A declaration with no
incident end date is open ended, and the date window has to admit it. SQL drops a
row when a comparison against null yields unknown, and Cypher drops it too, so both
sides need the null spelled out and neither inherits it for free. That is written
explicitly on both sides and the count of open ended declarations is published, so a
reader can see the branch was exercised rather than trusting that it was.

The graph is bounded to the year the imagery layer covers. The vector column spans
every year in the release and this does not, because the tiles and their labels all
sit in one year and a graph carrying four more years of events that no tile reaches
would answer the same question with more nodes.

A label here is one automated annotator's reading of a tile pair. The graph stores
the annotator on the label node for that reason. Nothing in this module treats a
label as a damage ground truth, and the question is phrased as what was labelled
rather than what was damaged.
"""

from __future__ import annotations

import json

import polars as pl

from . import labelling, paths, release

# The year the tiles sit in. Derived below from the tile artifact rather than typed,
# so a later sortie in another year moves this without an edit here.
LABELS_IN_SCOPE = ("possible", "clear")

CONSTRAINTS = (
    "create constraint event_id if not exists for (e:Event) require e.event_id is unique",
    "create constraint county_fips if not exists for (c:County) require c.fips is unique",
    "create constraint tile_id if not exists for (t:Tile) require t.tile_id is unique",
    "create constraint label_key if not exists for (l:Label) require l.key is unique",
    "create constraint declaration_number if not exists for (d:Declaration) "
    "require d.disaster_number is unique",
)

# One statement per shape, each fed a batch through UNWIND. A statement per row would
# be a round trip per row, and the edge count here is in the tens of thousands.
#
# A tile reaches a county by its own relationship type rather than by IN_COUNTY. Two
# different edges sharing one type reads as tidy and makes every untyped count wrong.
# An edge census over IN_COUNTY would have returned the event edges plus the tile
# edges as one number, and the first query anybody writes against a graph is a count.
LOADERS = {
    "events": (
        "unwind $rows as row "
        "merge (e:Event {event_id: row.event_id}) "
        "set e.event_type = row.event_type, e.event_date = date(row.event_date), "
        "    e.state = row.state, e.state_fips = row.state_fips"
    ),
    "counties": (
        "unwind $rows as row "
        "merge (c:County {fips: row.fips}) set c.name = row.name"
    ),
    "event_counties": (
        "unwind $rows as row "
        "match (e:Event {event_id: row.event_id}) match (c:County {fips: row.fips}) "
        "merge (e)-[:IN_COUNTY]->(c)"
    ),
    "tiles": (
        "unwind $rows as row "
        "merge (t:Tile {tile_id: row.tile_id}) "
        "set t.zoom = row.zoom, t.metres_from_track = row.metres_from_track, "
        "    t.post_collection = row.post_collection, t.pre_collection = row.pre_collection "
        "with t, row match (e:Event {event_id: row.event_id}) "
        "merge (t)-[:OVER_EVENT]->(e) "
        "with t, row match (c:County {fips: row.county_fips}) "
        "merge (t)-[:TILE_IN_COUNTY]->(c)"
    ),
    "labels": (
        "unwind $rows as row "
        "merge (l:Label {key: row.key}) "
        "set l.value = row.value, l.annotator = row.annotator "
        "with l, row match (t:Tile {tile_id: row.tile_id}) "
        "merge (t)-[:HAS_LABEL]->(l)"
    ),
    # A declaration node is one declaration and not one designated area. OpenFEMA
    # publishes a row per county, so keying the node on the row would have given
    # 14,039 nodes carrying 660 declarations between them, and a reader counting
    # Declaration nodes would get the designation count under the wrong name. Every
    # attribute below is constant within a disaster number, which a check asserts,
    # so collapsing the rows onto one node loses nothing. The county is the edge.
    "declarations": (
        "unwind $rows as row "
        "merge (d:Declaration {disaster_number: row.disaster_number}) "
        "set d.incident_type = row.incident_type, "
        "    d.declaration_type = row.declaration_type, "
        "    d.incident_begin = date(row.incident_begin), "
        "    d.incident_end = CASE WHEN row.incident_end IS NULL THEN null ELSE date(row.incident_end) END "
        "with d, row match (c:County {fips: row.county_fips}) "
        "merge (d)-[:COVERS]->(c)"
    ),
}

# The question, in Cypher. The date window admits an open ended declaration through
# the explicit null test, which is the branch SQL would also drop silently.
QUESTION_CYPHER = """
match (l:Label)<-[:HAS_LABEL]-(t:Tile)-[:OVER_EVENT]->(e:Event)-[:IN_COUNTY]->(c:County)
match (d:Declaration)-[:COVERS]->(c)
where l.value in $labels
  and d.incident_begin <= e.event_date
  and (d.incident_end is null or e.event_date <= d.incident_end)
return d.disaster_number as disaster_number, t.tile_id as tile_id, c.fips as fips
"""

QUESTION_TEXT = (
    "Which disaster declarations covered a county holding a tile that one annotator "
    "labelled possible or clear, where the declaration's incident window contains the "
    "event's begin date"
)


def tile_frame() -> pl.DataFrame:
    return pl.read_parquet(release.artifact_path("imagery_tiles"))


def scope_year(tiles: pl.DataFrame) -> int:
    """The year the graph covers, read off the tiles rather than written down."""
    years = sorted({d.year for d in tiles["event_begin_utc"].dt.date().to_list()})
    if len(years) != 1:
        raise GraphError(f"the tiles span {years} and the graph scope assumes one year")
    return years[0]


class GraphError(Exception):
    pass


def _county_of_tile(tiles: pl.DataFrame, event_counties: pl.DataFrame) -> pl.DataFrame:
    """Which county each tile sits in.

    A tile has a centre point and the event has counties. The tile is attached to the
    event's counties rather than to a county resolved from its own coordinates,
    because this release has no county boundary geometry and inventing one here would
    be a spatial join nothing checks. So a tile over a two county event attaches to
    both, and the question above counts a path through each. That is why the path
    count is larger than the distinct count, and it is a property of the data rather
    than of either query.
    """
    event_ids = tiles["EVENT_ID"].unique().to_list()
    pairs = event_counties.filter(pl.col("EVENT_ID").is_in(event_ids)).select(
        ["EVENT_ID", "county_fips"]
    )
    return tiles.select(["tile_id", "EVENT_ID"]).join(pairs, on="EVENT_ID", how="inner")


def payload() -> dict:
    """Every batch the loaders need, plus the counts a manifest publishes."""
    tiles = tile_frame()
    year = scope_year(tiles)
    events = pl.read_parquet(release.artifact_path(f"storm_events_{year}"))
    event_counties = pl.read_parquet(release.artifact_path("event_counties")).filter(
        pl.col("year") == year
    )
    declarations = pl.read_parquet(release.artifact_path("fema_declarations"))
    labels = labelling.read_labels()

    dates = pl.DataFrame(
        {
            "EVENT_ID": events["EVENT_ID"],
            "y": events["BEGIN_YEARMONTH"] // 100,
            "m": events["BEGIN_YEARMONTH"] % 100,
            "d": events["BEGIN_DAY"],
        }
    ).select(
        ["EVENT_ID", pl.date(pl.col("y"), pl.col("m"), pl.col("d")).alias("event_date")]
    )

    event_rows = (
        events.select(["EVENT_ID", "EVENT_TYPE", "STATE", "STATE_FIPS"])
        .join(dates, on="EVENT_ID")
        .rename(
            {
                "EVENT_ID": "event_id",
                "EVENT_TYPE": "event_type",
                "STATE": "state",
                "STATE_FIPS": "state_fips",
            }
        )
    )
    # Only the events the graph can reach a county from. An event with no county row
    # is in the release and contributes nothing to this pattern.
    reachable = set(event_counties["EVENT_ID"].to_list())
    event_rows = event_rows.filter(pl.col("event_id").is_in(list(reachable)))

    counties = (
        event_counties.select(["county_fips", "county_name"])
        .unique(subset=["county_fips"], keep="first", maintain_order=True)
        .rename({"county_fips": "fips", "county_name": "name"})
    )
    declaration_counties = declarations.select(["county_fips"]).unique()
    extra = declaration_counties.filter(
        ~pl.col("county_fips").is_in(counties["fips"].to_list())
    ).rename({"county_fips": "fips"}).with_columns(pl.lit(None, dtype=pl.String).alias("name"))
    counties = pl.concat([counties, extra]).sort("fips")

    tile_counties = _county_of_tile(tiles, event_counties)
    tile_rows = (
        tiles.select(
            ["tile_id", "EVENT_ID", "zoom", "metres_from_track", "post_collection", "pre_collection"]
        )
        .join(tile_counties, on=["tile_id", "EVENT_ID"], how="inner")
        .rename({"EVENT_ID": "event_id"})
    )

    label_rows = labels.with_columns(
        (pl.col("annotator") + pl.lit("|") + pl.col("label")).alias("key")
    ).rename({"label": "value"}).select(["key", "value", "annotator", "tile_id"])

    # One node per declaration means every attribute set on it has to be a property
    # of the declaration rather than of the designated county. Checked here rather
    # than assumed, because the collapse would otherwise keep whichever row landed
    # last and nothing would say so.
    varying = (
        declarations.group_by("disaster_number")
        .agg(
            [
                pl.col(column).n_unique().alias(column)
                for column in ("incident_type", "declaration_type", "incident_begin", "incident_end")
            ]
        )
        .filter(
            (pl.col("incident_type") > 1)
            | (pl.col("declaration_type") > 1)
            | (pl.col("incident_begin") > 1)
            | (pl.col("incident_end") > 1)
        )
    )
    if varying.height:
        raise GraphError(
            f"{varying.height} declarations carry more than one value for an attribute "
            "the graph stores on the declaration node"
        )

    declaration_rows = declarations.select(
        [
            "disaster_number",
            "incident_type",
            "declaration_type",
            "county_fips",
            pl.col("incident_begin").cast(pl.String),
            pl.col("incident_end").cast(pl.String),
        ]
    )

    return {
        "scope_year": year,
        "batches": {
            "events": event_rows.with_columns(pl.col("event_date").cast(pl.String)).to_dicts(),
            "counties": counties.to_dicts(),
            "event_counties": event_counties.select(
                [pl.col("EVENT_ID").alias("event_id"), pl.col("county_fips").alias("fips")]
            )
            .unique()
            .to_dicts(),
            "tiles": tile_rows.rename({"county_fips": "county_fips"}).to_dicts(),
            "labels": label_rows.to_dicts(),
            "declarations": declaration_rows.to_dicts(),
        },
        "counts": {
            "scope_year": year,
            "events": event_rows.height,
            "counties": counties.height,
            "event_county_edges": event_counties.select(["EVENT_ID", "county_fips"]).unique().height,
            "tiles": tiles.height,
            "tile_county_edges": tile_rows.height,
            "labels": label_rows.height,
            "distinct_label_nodes": label_rows["key"].n_unique(),
            "declaration_edges": declaration_rows.height,
            "declaration_nodes_expected": declarations["disaster_number"].n_unique(),
            "open_ended_declarations": int(declarations["incident_end"].is_null().sum()),
        },
    }


def sql_answer(labels_in_scope=LABELS_IN_SCOPE) -> dict:
    """The same question over the Parquet, with the same null branch written out.

    This is the second implementation. It joins rather than walks, and it has to
    deduplicate for the same reason the Cypher does.
    """
    tiles = tile_frame()
    year = scope_year(tiles)
    event_counties = pl.read_parquet(release.artifact_path("event_counties")).filter(
        pl.col("year") == year
    )
    declarations = pl.read_parquet(release.artifact_path("fema_declarations"))
    events = pl.read_parquet(release.artifact_path(f"storm_events_{year}"))
    labels = labelling.read_labels().filter(pl.col("label").is_in(list(labels_in_scope)))

    dates = pl.DataFrame(
        {
            "EVENT_ID": events["EVENT_ID"],
            "y": events["BEGIN_YEARMONTH"] // 100,
            "m": events["BEGIN_YEARMONTH"] % 100,
            "d": events["BEGIN_DAY"],
        }
    ).select(
        ["EVENT_ID", pl.date(pl.col("y"), pl.col("m"), pl.col("d")).alias("event_date")]
    )

    walked = (
        labels.select(["tile_id"])
        .join(tiles.select(["tile_id", "EVENT_ID"]), on="tile_id", how="inner")
        .join(event_counties.select(["EVENT_ID", "county_fips"]).unique(), on="EVENT_ID", how="inner")
        .join(dates, on="EVENT_ID", how="inner")
        .join(declarations.select(["disaster_number", "county_fips", "incident_begin", "incident_end"]),
              on="county_fips", how="inner")
    )
    # The null branch, spelled out. Without the first term an open ended declaration
    # is dropped, because a comparison against a null end date is not true.
    within = walked.filter(
        (pl.col("incident_begin") <= pl.col("event_date"))
        & (pl.col("incident_end").is_null() | (pl.col("event_date") <= pl.col("incident_end")))
    )
    return {
        "paths": within.height,
        "disaster_numbers": sorted(set(within["disaster_number"].to_list())),
        "open_ended_paths": int(within["incident_end"].is_null().sum()),
    }


def compare(graph_rows: list[dict], sql: dict) -> dict:
    """Grade the two answers against each other and say where they could have parted.

    `graph_rows` is one row per path, which is what the Cypher returns without a
    DISTINCT. Both the path count and the distinct set are compared, because the
    distinct sets agreeing while the path counts differ would mean one side walked a
    different graph and the deduplication hid it.
    """
    graph_paths = len(graph_rows)
    graph_numbers = sorted({int(r["disaster_number"]) for r in graph_rows})
    sql_numbers = [int(n) for n in sql["disaster_numbers"]]
    problems = []
    if graph_numbers != sql_numbers:
        problems.append(
            f"distinct declarations differ, graph {graph_numbers} and parquet {sql_numbers}"
        )
    if graph_paths != sql["paths"]:
        problems.append(f"path counts differ, graph {graph_paths} and parquet {sql['paths']}")
    return {
        "question": QUESTION_TEXT,
        "labels_in_scope": list(LABELS_IN_SCOPE),
        "graph_paths": graph_paths,
        "parquet_paths": sql["paths"],
        "graph_distinct_declarations": len(graph_numbers),
        "parquet_distinct_declarations": len(sql_numbers),
        "disaster_numbers": sql_numbers,
        "open_ended_paths": sql["open_ended_paths"],
        # The agreement means something only if deduplication was doing work. When
        # this is greater than one, a side that dropped its DISTINCT would disagree.
        "paths_per_declaration": (
            round(graph_paths / len(graph_numbers), 3) if graph_numbers else None
        ),
        "agree": not problems,
        "problems": problems,
    }


def manifest_path():
    return paths.release_root() / "measurements" / "graph.json"


def write_manifest(payload: dict) -> None:
    manifest_path().write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
