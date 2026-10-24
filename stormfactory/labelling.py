"""A Label Studio project over the tile pairs, and the agreement it cannot report yet.

The labelling unit is a tile pair rather than a tile. An annotator looking at post-event
imagery alone cannot tell a roof that lost its shingles from a roof that was always that
colour, and the pre-event sortie is what makes the judgement possible at all. So the task
carries both panels and the label is about the change between them.

The label set is four values and no more. A damage scale with six grades invites an
annotator to split hairs they cannot see at this ground sample distance, and the first
thing a second annotator would disagree about is the boundary between two grades nobody
defined. `not_assessable` is a real answer here and not a refusal to work: a tile can be
open water, or cloud, or the pre-event sortie can be missing.

Agreement is the part worth being careful about. There is one annotator, so there is no
agreement to report, and the only number this module will produce from one annotator is
the refusal below. Scoring a single annotator against themselves would be a figure that
looks like reliability and measures nothing.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import polars as pl

from . import paths

LABEL_NAME = "damage"
LABELS = ("none", "possible", "clear", "not_assessable")

# Written as a file rather than built with an XML library. Label Studio's config is a
# small fixed document, a reader of this repo has to be able to see exactly what the
# annotator saw, and a generated tree makes that a round trip through two formats.
LABEL_CONFIG = """<View>
  <Header value="Did the storm change this ground?"/>
  <Text name="tile" value="Tile $tile_id, $metres_per_pixel m per pixel, $metres_from_track m from the surveyed track"/>
  <View style="display: flex">
    <View>
      <Header value="Before, $pre_collection, flown $pre_acquired"/>
      <Image name="pre" value="$pre_url" zoom="true" zoomControl="true"/>
    </View>
    <View>
      <Header value="After, $post_collection, flown $post_acquired"/>
      <Image name="post" value="$post_url" zoom="true" zoomControl="true"/>
    </View>
  </View>
  <Choices name="damage" toName="post" choice="single" showInLine="true">
    <Choice value="none" hint="Nothing in the after frame differs in a way a storm explains"/>
    <Choice value="possible" hint="Something differs and it could be seasonal or could be damage"/>
    <Choice value="clear" hint="Structural or vegetation damage visible in the after frame"/>
    <Choice value="not_assessable" hint="Water or cloud or a missing panel, or too coarse to judge"/>
  </Choices>
  <TextArea name="note" toName="post" placeholder="What decided it" rows="2" maxSubmissions="1"/>
</View>
"""


class LabellingError(Exception):
    pass


def config_path() -> Path:
    return paths.release_root() / "labelstudio" / "label_config.xml"


def labels_path() -> Path:
    return paths.release_root() / "labels" / "tile_labels.json"


def tasks_path() -> Path:
    return paths.data_root() / "labelstudio" / "tasks.json"


def tasks_for_tiles(frame: pl.DataFrame, post_template: str, pre_template: str) -> list[dict]:
    """Label Studio tasks, one per tile pair that has both panels.

    The image URLs point at NOAA's tile server rather than at a local copy. Label Studio
    loads them in the browser, so the project needs no storage configured and the
    annotator sees the same bytes the manifest hashed. A tile missing either panel is left
    out of the project entirely, because a task that cannot be answered still has to be
    answered before Label Studio will call the project done.
    """
    both = frame.filter(pl.col("post_has_imagery") & pl.col("pre_has_imagery"))
    tasks = []
    for row in both.sort("tile_id").iter_rows(named=True):
        z, x, y = row["zoom"], row["tile_x"], row["tile_y"]
        tasks.append({
            "data": {
                "tile_id": row["tile_id"],
                "post_url": post_template.format(z=z, x=x, y=y),
                "pre_url": pre_template.format(z=z, x=x, y=y),
                "post_collection": row["post_collection"],
                "pre_collection": row["pre_collection"],
                "post_acquired": str(row["post_acquired"]),
                "pre_acquired": str(row["pre_acquired"]),
                "metres_per_pixel": round(row["metres_per_pixel"], 3),
                "metres_from_track": round(row["metres_from_track"], 1),
            }
        })
    return tasks


def read_labels(path: Path | None = None) -> pl.DataFrame:
    """The labelling pass, read from a Label Studio export.

    Only the choice is read. The free text note is kept in the committed export for a
    reader and is deliberately not a column, because it is not a label and a frame that
    carries it invites somebody to group by it.
    """
    path = path or labels_path()
    export = json.loads(path.read_text())
    rows = []
    for task in export["tasks"]:
        tile = task["data"]["tile_id"]
        for ann in task["annotations"]:
            choices = [
                r["value"]["choices"][0]
                for r in ann["result"]
                if r.get("from_name") == LABEL_NAME
            ]
            if len(choices) != 1:
                raise LabellingError(f"{tile} carries {len(choices)} choices for one annotation")
            if choices[0] not in LABELS:
                raise LabellingError(f"{tile} carries label {choices[0]!r}, which is not in the set")
            rows.append({"tile_id": tile, "annotator": ann["completed_by"], "label": choices[0]})
    if not rows:
        raise LabellingError(f"{path} holds no annotations")
    return pl.DataFrame(rows).sort(["tile_id", "annotator"])


def cohens_kappa(a: list[str], b: list[str]) -> float:
    """Cohen's kappa over two annotators' labels for the same units, in the same order.

    Here so that the agreement path is real code with a test behind it rather than a
    promise. Nothing in this release calls it with real data, because there is one
    annotator.
    """
    if len(a) != len(b):
        raise LabellingError(f"kappa needs equal length label lists, got {len(a)} and {len(b)}")
    if not a:
        raise LabellingError("kappa over no units is undefined")
    n = len(a)
    observed = sum(1 for x, y in zip(a, b) if x == y) / n
    ca, cb = Counter(a), Counter(b)
    expected = sum(ca[k] / n * cb[k] / n for k in set(ca) | set(cb))
    if expected == 1.0:
        raise LabellingError("kappa is undefined when chance agreement is total")
    return (observed - expected) / (1 - expected)


def agreement(labels: pl.DataFrame) -> dict:
    """Agreement across annotators, or the reason there is none.

    The guard is the point of this function. One annotator gives `unmeasured` and a
    sentence, never a number. Two or more give kappa over the tiles they both labelled.
    """
    annotators = sorted(labels["annotator"].unique().to_list())
    if len(annotators) < 2:
        return {
            "status": "unmeasured",
            "annotators": len(annotators),
            "annotator_ids": annotators,
            "reason": (
                "one annotator labelled this sample, so there is no second set of labels "
                "to agree with. A single annotator scored against themselves is not a "
                "reliability figure"
            ),
        }
    first, second = annotators[0], annotators[1]
    wide = (
        labels.filter(pl.col("annotator").is_in([first, second]))
        .pivot(on="annotator", index="tile_id", values="label")
        .drop_nulls()
        .sort("tile_id")
    )
    return {
        "status": "measured",
        "annotators": len(annotators),
        "annotator_ids": annotators,
        "pair": [first, second],
        "tiles_both_labelled": wide.height,
        "cohens_kappa": cohens_kappa(wide[first].to_list(), wide[second].to_list()),
    }


def label_counts(labels: pl.DataFrame) -> dict[str, int]:
    counts = dict(Counter(labels["label"].to_list()))
    return {k: int(counts.get(k, 0)) for k in LABELS}
