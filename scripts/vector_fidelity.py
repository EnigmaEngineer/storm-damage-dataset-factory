"""How much of the lexical neighbourhood the reduced column keeps, by width.

The vector column has no relevance labels behind it, so no retrieval quality figure
can be published. This is the closest honest substitute, and it needs no labels.

The reduction approximates one specific thing, which is the cosine neighbourhood of
the TF-IDF matrix it was fitted on. That matrix is available, so the neighbourhood it
gives can be computed exactly and the reduced column can be scored against it. The
figure is recovery at k, meaning the share of a query's top k in the full TF-IDF space
that the reduced space also returns in its top k.

What the figure is not is a quality score. A latent semantic reduction is supposed to
depart from the lexical neighbourhood, because that is how it reaches a document that
shares meaning and not vocabulary. So a recovery below one is expected and some of the
departure is the technique working. Nothing here can say how much, because saying that
needs a question set with relevance labels and this release has none.

What the figure does settle is the width. Recovery against a fixed reference rises
with width, and the shape of that curve says whether a narrower column would cost
anything. It also puts a number on how far the published column sits from the only
reference available, which a reader deserves before building on it.

The build stack goes in the manifest beside the figures. A count reproduces across
library versions and a decomposition does not, so the column is a function of its rows
and of the versions that reduced them, and a manifest that names only the rows is
claiming more reproducibility than it has.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import polars as pl
import sklearn
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize

from stormfactory import paths, release, vectors

# The widths to score. The published width is in here and so are the ones a reader
# would ask about, because a single point says nothing about the shape of the curve.
WIDTHS = (64, 128, 192, 256)
K = 10
QUERIES = 50


def build_stack() -> dict:
    """The versions the column is a function of, beside the rows."""
    return {
        "python": ".".join(str(n) for n in sys.version_info[:3]),
        "scikit_learn": sklearn.__version__,
        "numpy": np.__version__,
        "polars": pl.__version__,
    }


def main() -> int:
    events = vectors._event_frame(list(release.YEARS))
    texts = vectors.scrubbed_texts(events)

    tfidf = TfidfVectorizer(
        min_df=vectors.MIN_DOCUMENT_FREQUENCY,
        max_df=vectors.MAX_DOCUMENT_FRACTION,
        strip_accents="unicode",
        lowercase=True,
        stop_words="english",
    )
    matrix = tfidf.fit_transform(texts)
    print(f"tfidf {matrix.shape[0]} documents, {matrix.shape[1]} terms")

    # Query rows picked by position in EVENT_ID order, so the selection is a function
    # of the data rather than of a random draw.
    order = np.argsort(events["EVENT_ID"].to_numpy())
    stride = max(1, len(order) // QUERIES)
    chosen = order[::stride][:QUERIES]

    unit = normalize(matrix)
    reference = []
    for row in chosen:
        sims = (unit @ unit[row].T).toarray().ravel()
        sims[row] = -1
        reference.append(set(np.argpartition(-sims, K)[:K]))

    facts = {
        "k": K,
        "queries": QUERIES,
        "documents": int(matrix.shape[0]),
        "vocabulary": int(matrix.shape[1]),
        "published_width": vectors.DIMENSION,
        "seed": vectors.SEED,
        "build_stack": build_stack(),
        "reference": (
            "the cosine top k of the full TF-IDF matrix the reduction was fitted on, "
            "which is what the reduction approximates and not a relevance judgement"
        ),
        "widths": {},
    }

    for width in WIDTHS:
        svd = TruncatedSVD(n_components=width, random_state=vectors.SEED, algorithm="randomized")
        reduced = normalize(svd.fit_transform(matrix).astype("float32"))
        recovered = []
        for n, row in enumerate(chosen):
            sims = reduced @ reduced[row]
            sims[row] = -1
            recovered.append(len(set(np.argpartition(-sims, K)[:K]) & reference[n]) / K)
        facts["widths"][str(width)] = {
            "explained_variance_cumulative": round(float(svd.explained_variance_ratio_.sum()), 6),
            "tfidf_neighbourhood_recovery": round(float(np.mean(recovered)), 4),
            "column_bytes_uncompressed": int(matrix.shape[0]) * width * 4,
        }
        print(
            f"  width {width}: explained "
            f"{facts['widths'][str(width)]['explained_variance_cumulative']}, "
            f"recovery {facts['widths'][str(width)]['tfidf_neighbourhood_recovery']}"
        )

    # Whether the curve has levelled off by the published width. If it has not, the
    # width is a cost against fidelity that nobody optimised, and saying so is the
    # point of measuring more than one.
    scores = [facts["widths"][str(w)]["tfidf_neighbourhood_recovery"] for w in WIDTHS]
    facts["recovery_still_rising_at_the_published_width"] = scores[-1] > scores[-2]
    facts["recovery_gained_by_the_last_width_step"] = round(scores[-1] - scores[-2], 4)

    out = paths.release_root() / "measurements" / "vector-fidelity.json"
    out.write_text(json.dumps(facts, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
