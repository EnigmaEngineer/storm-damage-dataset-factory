"""Vectors over the event narratives, and what they are not.

The narratives are the only free text in this release. A researcher looking for
"roof torn off a barn" wants the events that read like that, not the events whose
`EVENT_TYPE` happens to match a keyword. That is what a vector column is for.

These are lexical vectors and not a pretrained sentence encoder. The pipeline is a
TF-IDF term matrix reduced by a truncated SVD, which is latent semantic analysis.
Two narratives land near each other when they share weighted vocabulary after the
reduction. They do not land near each other because a language model understood
them.

That choice is deliberate and the reasons are worth stating, because the obvious
alternative is a sentence transformer.

A released dataset has to rebuild. The whole release is pinned by content digests
and judged by whether a second machine reproduces them. An SVD with a fixed seed
over a fixed vocabulary is a function of the input rows. A neural encoder pins the
numbers to a model checkpoint, a tokeniser and a framework build, and a reader who
cannot download that exact checkpoint cannot reproduce the column at all.

The declared floor is Python 3.10 and the pinned stack is five packages. The encoder
route adds a deep learning framework to a repository whose job is publishing Parquet.

The cost is real and it belongs in the open. Lexical vectors miss a paraphrase that
shares no vocabulary. Nothing here measures how often that happens, because
measuring it needs a question set with relevance labels and this release has none.
So no retrieval quality figure is published anywhere in this repository. What does
get published is the determinism of the column and the behaviour of the index built
over it, and both of those are properties a reader can check.

The text that goes in is the scrubbed narrative. Embedding the raw narrative would
put the person names this release exists to remove into a column that travels with
the data, and a vector is harder to inspect than a sentence. So the scrubber runs
first and the vectors are built from its output.
"""

from __future__ import annotations

import hashlib

import numpy as np
import polars as pl
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer

from . import release, scrub

# The width of the vector column. 256 is a working latent semantic analysis
# dimensionality rather than a tuned choice, and nothing here claims it is optimal.
# The cumulative explained variance at this width is measured and published, so a
# reader can see what the reduction kept.
DIMENSION = 256

# Fixed so the column is a function of the rows. The randomised SVD solver draws a
# projection, and an unseeded draw would give a different column every rebuild while
# every input byte stayed the same.
SEED = 0

# A term has to appear in three narratives to earn a column, and a term in more than
# half of them carries no signal worth keeping. Both are ordinary TF-IDF hygiene and
# both change the vocabulary, so both are published in the manifest.
MIN_DOCUMENT_FREQUENCY = 3
MAX_DOCUMENT_FRACTION = 0.5

TEXT_COLUMN = "EVENT_NARRATIVE"

# The columns a filtered search is allowed to filter on. This list is the contract
# between the artifact and the store, and the store builds its table from it rather
# than from a second copy written out by hand.
FILTER_COLUMNS = ("event_type", "event_date", "state_fips", "state")


class VectorError(Exception):
    pass


def _event_frame(years: list[int]) -> pl.DataFrame:
    """Every event in the given years that carries a narrative, with its filters."""
    frames = []
    for year in years:
        path = release.artifact_path(f"storm_events_{year}")
        frames.append(
            pl.read_parquet(
                path,
                columns=[
                    "EVENT_ID",
                    "EVENT_TYPE",
                    "STATE",
                    "STATE_FIPS",
                    "BEGIN_YEARMONTH",
                    "BEGIN_DAY",
                    TEXT_COLUMN,
                ],
            )
        )
    frame = pl.concat(frames)
    return frame.filter(
        pl.col(TEXT_COLUMN).is_not_null()
        & (pl.col(TEXT_COLUMN).str.strip_chars().str.len_chars() > 0)
    )


def _event_date(frame: pl.DataFrame) -> pl.Series:
    """The begin date as a real date, out of the two integer columns NOAA publishes.

    `BEGIN_YEARMONTH` is `yyyymm` and `BEGIN_DAY` is the day of month. This is the
    event's local begin date under the row's own time zone column, and that column is
    wrong in both directions across this release, so nothing here converts it to UTC.
    A date filter on this column is a filter on the date NOAA stated.
    """
    year = frame["BEGIN_YEARMONTH"] // 100
    month = frame["BEGIN_YEARMONTH"] % 100
    return pl.DataFrame({"y": year, "m": month, "d": frame["BEGIN_DAY"]}).select(
        pl.date(pl.col("y"), pl.col("m"), pl.col("d")).alias("event_date")
    )["event_date"]


def scrubbed_texts(frame: pl.DataFrame) -> list[str]:
    return [scrub.scrub(t)[0] for t in frame[TEXT_COLUMN].to_list()]


def vectorise(texts: list[str], dimension: int = DIMENSION) -> tuple[list[list[float]], dict]:
    """The vectors, and the facts about the reduction that produced them.

    Returned rather than stored on a module global, because two calls in one process
    must not be able to see each other's fitted state.

    The width is an argument so a check can exercise this on a corpus of a few dozen
    documents. The release always builds at the module's declared width.
    """
    if not texts:
        raise VectorError("no narratives to vectorise, so there is nothing to reduce")
    if len(texts) <= dimension:
        raise VectorError(
            f"{len(texts)} narratives cannot support a {dimension} wide reduction"
        )

    tfidf = TfidfVectorizer(
        min_df=MIN_DOCUMENT_FREQUENCY,
        max_df=MAX_DOCUMENT_FRACTION,
        strip_accents="unicode",
        lowercase=True,
        stop_words="english",
    )
    term_matrix = tfidf.fit_transform(texts)
    svd = TruncatedSVD(n_components=dimension, random_state=SEED, algorithm="randomized")
    reduced = svd.fit_transform(term_matrix)

    facts = {
        "documents": int(term_matrix.shape[0]),
        "vocabulary": int(term_matrix.shape[1]),
        "dimension": dimension,
        "seed": SEED,
        "min_document_frequency": MIN_DOCUMENT_FREQUENCY,
        "max_document_fraction": MAX_DOCUMENT_FRACTION,
        # Rounded on the way out. The full float carries solver noise in its last
        # places and a manifest figure that moves in the sixteenth decimal is a
        # figure that fails a rebuild for no reason anybody can act on.
        "explained_variance_cumulative": round(float(svd.explained_variance_ratio_.sum()), 6),
    }
    return [[float(v) for v in row] for row in reduced], facts


def build(years: list[int] | None = None) -> tuple[pl.DataFrame, dict]:
    """The vector artifact and the manifest facts, from the released event partitions."""
    years = list(release.YEARS if years is None else years)
    events = _event_frame(years)
    texts = scrubbed_texts(events)
    vectors, facts = vectorise(texts)

    frame = pl.DataFrame(
        {
            "EVENT_ID": events["EVENT_ID"],
            "event_type": events["EVENT_TYPE"],
            "state": events["STATE"],
            "state_fips": events["STATE_FIPS"],
            "event_date": _event_date(events),
            "narrative_chars": events[TEXT_COLUMN].str.len_chars().cast(pl.Int64),
            "vector": pl.Series(
                "vector", vectors, dtype=pl.Array(pl.Float32, DIMENSION)
            ),
        }
    )

    facts["events_in_partitions"] = sum(
        pl.read_parquet(
            release.artifact_path(f"storm_events_{year}"), columns=["EVENT_ID"]
        ).height
        for year in years
    )
    facts["events_without_a_narrative"] = facts["events_in_partitions"] - frame.height
    facts["event_types"] = frame["event_type"].n_unique()
    facts["states"] = frame["state"].n_unique()
    facts["earliest_event_date"] = str(frame["event_date"].min())
    facts["latest_event_date"] = str(frame["event_date"].max())
    facts["filter_columns"] = list(FILTER_COLUMNS)
    return frame, facts


def vector_fingerprint(frame: pl.DataFrame) -> str:
    """A short hash of the vector column alone, in EVENT_ID order.

    The release already digests the whole artifact. This exists so a determinism
    check can say the vectors themselves repeated, without a difference in some
    other column being able to answer for them.
    """
    ordered = frame.sort("EVENT_ID")
    # The raw little endian float32 buffer rather than a rendered number per value.
    # There are width times height floats here and formatting each one as text costs
    # minutes on a release sized artifact for no gain.
    buf = ordered["vector"].to_numpy().reshape(-1).astype("<f4", copy=False)
    return hashlib.sha256(buf.tobytes()).hexdigest()[:16]
