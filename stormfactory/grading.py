"""Grading the scrub against labels written by hand.

A prediction counts when it overlaps a label of the same kind. Exact offsets would be
the stricter test and they would also grade the tokeniser rather than the detector,
because `56-year-old man` and `56-year-old` are the same finding about the same person.

Two narratives in the labelled sample are withheld from the repo because they carry
real names. Their labels are offsets into the NOAA text, so they grade only where the
raw release is present. The count of rows graded and the count skipped are both
reported, because a recall figure over a sample missing its positives is a lie by
omission and this is the one sample where that could happen silently.
"""

from __future__ import annotations

import glob
import json
from pathlib import Path

import polars as pl

from . import scrub

KINDS = ("name", "casualty")

NAIVE_BIGRAM = scrub.re.compile(
    r"\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\s+(?:reported|measured|called|stated|said|observed)"
)


def _withheld_text(event_ids: set[int], parquet_dir: Path) -> dict[int, str]:
    files = sorted(glob.glob(str(Path(parquet_dir) / "year=*" / "part.parquet")))
    if not files:
        return {}
    df = pl.read_parquet(files, columns=["EVENT_ID", "EVENT_NARRATIVE"]).filter(
        pl.col("EVENT_ID").is_in(list(event_ids))
    )
    return dict(zip(df["EVENT_ID"].to_list(), df["EVENT_NARRATIVE"].to_list()))


def _overlaps(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start < b_end and b_start < a_end


def grade(rows: list[dict], parquet_dir: Path) -> dict:
    withheld = {r["event_id"] for r in rows if r.get("withheld")}
    recovered = _withheld_text(withheld, parquet_dir) if withheld else {}

    tally = {k: {"true_positive": 0, "false_positive": 0, "false_negative": 0} for k in KINDS}
    graded = skipped = 0
    naive_fires = naive_wrong = 0

    for row in rows:
        text = row.get("narrative")
        if text is None:
            text = recovered.get(row["event_id"])
            if text is None:
                skipped += 1
                continue
        graded += 1

        labels = row["labels"]
        predicted = scrub.person_names(text) + scrub.casualty_descriptors(text)

        for kind in KINDS:
            want = [l for l in labels if l["kind"] == kind]
            got = [p for p in predicted if p.kind == kind]
            matched_labels = set()
            for p in got:
                hit = next((i for i, l in enumerate(want)
                            if i not in matched_labels and _overlaps(p.start, p.end, l["start"], l["end"])), None)
                if hit is None:
                    tally[kind]["false_positive"] += 1
                else:
                    matched_labels.add(hit)
                    tally[kind]["true_positive"] += 1
            tally[kind]["false_negative"] += len(want) - len(matched_labels)

        # The baseline the plan implied: treat a capitalised pair before a reporting
        # verb as a person. Counted on every graded narrative, not only its stratum.
        if NAIVE_BIGRAM.search(text):
            naive_fires += 1
            if not any(l["kind"] == "name" for l in labels):
                naive_wrong += 1

    out = {"rows_graded": graded, "rows_skipped_because_withheld": skipped, "per_detector": {}}
    for kind, t in tally.items():
        tp, fp, fn = t["true_positive"], t["false_positive"], t["false_negative"]
        out["per_detector"][kind] = {
            **t,
            "labelled_spans": tp + fn,
            "precision_pct": round(100 * tp / (tp + fp), 1) if tp + fp else None,
            "recall_pct": round(100 * tp / (tp + fn), 1) if tp + fn else None,
        }
    out["naive_capitalised_bigram_baseline"] = {
        "narratives_it_fires_on": naive_fires,
        "of_those_carrying_no_name": naive_wrong,
        "precision_pct": round(100 * (naive_fires - naive_wrong) / naive_fires, 1) if naive_fires else None,
    }
    return out


def load_sample(path: Path) -> dict:
    return json.loads(Path(path).read_text())
