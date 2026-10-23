"""One damage baseline, scored under each cut, and a matched experiment that isolates the leak.

The plan here was that one baseline on a random cut and one on a blocked cut would give a
gap, and that the gap would be the leakage. Both numbers are here and the gap is not the
headline, because three things move between those two arms at once.

The test sets hold different events. The share of events whose damage field is populated
at all differs between them, 21.7 percent missing on one and 14.3 on the other. And the
blocked training set loses every state the blocked test set holds, so a model is
extrapolating in space as well as working without sibling events. Only the last of those
is leakage and the other two are confounds.

So the matched experiment holds the evaluation set fixed and the training size fixed, and
varies one thing. Whether the training set contains other events of the same storms. That
is the number worth defending.

What a storm is here is the NOAA episode, for the reason splits.py gives.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

from stormfactory import splits

SEED = 0

# The salt for the matched experiment's base sample. Named here rather than inline so the
# experiment is reproducible from the constant and not from a literal buried in a call.
MATCHED_SALT = "matched-v1"

# The target is whether the event carries recorded property damage. It is read off
# damage_property_usd, which the ingest parses out of the DAMAGE_PROPERTY string.
TARGET_SOURCE = "damage_property_usd"

# An absent damage field is not a zero. 72,920 of the 336,014 events have no
# DAMAGE_PROPERTY value in the source at all, and NOAA does not say that blank means
# none. Treating those as negatives would invent 72,920 labels, so they are dropped from
# the modelling population and the count is published.
#
# The drop is not uniform across the cuts, which is the first reason the random against
# blocked gap is not a clean measurement.

NUMERIC = (
    "month",
    "begin_hour",
    "event_minutes",
    "magnitude",
    "tor_length",
    "tor_width",
    "begin_lat",
    "begin_lon",
)

CATEGORICAL = ("event_type", "cz_type", "magnitude_type", "tor_f_scale")

BOOLEAN = ("has_point", "has_a_county")

FEATURES = NUMERIC + CATEGORICAL + BOOLEAN

# Kept out on purpose, with the reason, because a feature list is only honest next to the
# list of what it refuses. A check asserts none of these reaches the matrix.
EXCLUDED = {
    "EVENT_ID": "an identifier, and an identifier is how a model memorises a row",
    "EPISODE_ID": "the storm identifier, which is the thing the cuts exist to hide",
    "STATE": "a categorical place label, undefined on an unseen state",
    "STATE_FIPS": "the same label as a number",
    "WFO": "the forecast office, which is a place label with another name",
    "CZ_NAME": "the county or zone name, a finer place label",
    "CZ_FIPS": "the same label as a number",
    "BEGIN_LOCATION": "a place name",
    "END_LOCATION": "a place name",
    "YEAR": "no cut here is temporal, so a year effect cannot generalise to a later year",
    "DAMAGE_PROPERTY": "the target before parsing",
    "damage_property_usd": "the target",
    "DAMAGE_CROPS": "recorded in the same pass as the property damage",
    "damage_crops_usd": "the same",
    "INJURIES_DIRECT": "an outcome of the event, not known before it",
    "INJURIES_INDIRECT": "the same",
    "DEATHS_DIRECT": "the same",
    "DEATHS_INDIRECT": "the same",
    "EVENT_NARRATIVE": "it describes the damage in words",
    "EPISODE_NARRATIVE": "it describes the storm in words",
}

# Why a continuous coordinate stays and a categorical place label goes.
#
# An unseen state is not a degraded feature, it is an absent one. Every blocked test row
# would carry a category the encoder has never seen, so the gap would be measuring lost
# feature support rather than leakage. A latitude is a number the tree can still split on
# either side of, so it degrades rather than vanishing.
#
# The coordinate still extrapolates in space under the blocked cut, which is what spatial
# blocking is. That confound is not solved by a feature list. It is solved by the matched
# experiment below, and it is why the storm cut is reported as the middle term.

SCHEMES = ("random_split", "storm_split", "blocked_split")


class BaselineError(Exception):
    pass


def feature_frame(events: pl.DataFrame, split_table: pl.DataFrame) -> pl.DataFrame:
    """Join the cuts onto the events and derive the features, one row per event.

    Rows whose damage field is absent are dropped here rather than in the caller, so
    every arm sees the same modelling population.
    """
    needed = [
        "EVENT_ID",
        "EPISODE_ID",
        "BEGIN_YEARMONTH",
        "BEGIN_DAY",
        "BEGIN_TIME",
        "END_YEARMONTH",
        "END_DAY",
        "END_TIME",
        "EVENT_TYPE",
        "CZ_TYPE",
        "MAGNITUDE",
        "MAGNITUDE_TYPE",
        "TOR_F_SCALE",
        "TOR_LENGTH",
        "TOR_WIDTH",
        "BEGIN_LAT",
        "BEGIN_LON",
        "has_point",
        TARGET_SOURCE,
    ]
    frame = events.select(needed).join(
        split_table.select("EVENT_ID", "has_a_county", *SCHEMES), on="EVENT_ID", how="inner"
    )

    begin = pl.datetime(
        pl.col("BEGIN_YEARMONTH") // 100,
        pl.col("BEGIN_YEARMONTH") % 100,
        pl.col("BEGIN_DAY"),
        pl.col("BEGIN_TIME") // 100,
        pl.col("BEGIN_TIME") % 100,
    )
    end = pl.datetime(
        pl.col("END_YEARMONTH") // 100,
        pl.col("END_YEARMONTH") % 100,
        pl.col("END_DAY"),
        pl.col("END_TIME") // 100,
        pl.col("END_TIME") % 100,
    )

    frame = frame.with_columns(
        (pl.col("BEGIN_YEARMONTH") % 100).alias("month"),
        (pl.col("BEGIN_TIME") // 100).alias("begin_hour"),
        ((end - begin).dt.total_minutes()).alias("event_minutes"),
        pl.col("MAGNITUDE").alias("magnitude"),
        pl.col("TOR_LENGTH").alias("tor_length"),
        pl.col("TOR_WIDTH").alias("tor_width"),
        pl.col("BEGIN_LAT").alias("begin_lat"),
        pl.col("BEGIN_LON").alias("begin_lon"),
        pl.col("EVENT_TYPE").alias("event_type"),
        pl.col("CZ_TYPE").alias("cz_type"),
        pl.col("MAGNITUDE_TYPE").fill_null("absent").alias("magnitude_type"),
        pl.col("TOR_F_SCALE").fill_null("absent").alias("tor_f_scale"),
        (pl.col(TARGET_SOURCE) > 0).alias("label"),
    )

    # A day that does not exist in its month, such as the 31st of February, comes back
    # null from pl.datetime rather than raising. Those rows keep every other feature and
    # lose that length, which is what a null in a numeric column is for.
    return frame.filter(pl.col(TARGET_SOURCE).is_not_null()).drop(TARGET_SOURCE)


def design_matrix(train: pl.DataFrame, score: pl.DataFrame):
    """Encode the allowlist into two float arrays, fitting the encoding on train alone.

    A category the training set never saw becomes 0 rather than an error, so the column
    survives an unseen state without the matrix pretending to know it.
    """
    for name in FEATURES:
        if name in EXCLUDED:
            raise BaselineError(f"{name} is on the allowlist and the excluded list at once")

    columns_train, columns_score = [], []
    for name in NUMERIC:
        # A numeric column the training set has no two distinct values for cannot be
        # binned, and what the estimator raises for it is a numpy stride error naming
        # neither the column nor the arm. Refusing it here costs one pass over the
        # column and turns that into a sentence. The full release satisfies this on
        # every feature. A narrower slice, one year or one state, need not.
        distinct = train[name].drop_nulls().n_unique()
        if distinct < 2:
            raise BaselineError(
                f"{name} carries {distinct} distinct values in this training arm, "
                "so it cannot be binned"
            )
        columns_train.append(train[name].cast(pl.Float64).to_numpy())
        columns_score.append(score[name].cast(pl.Float64).to_numpy())
    for name in BOOLEAN:
        columns_train.append(train[name].cast(pl.Float64).to_numpy())
        columns_score.append(score[name].cast(pl.Float64).to_numpy())
    for name in CATEGORICAL:
        seen = {value: i + 1 for i, value in enumerate(sorted(train[name].unique().to_list()))}
        columns_train.append(
            np.array([seen.get(v, 0) for v in train[name].to_list()], dtype=np.float64)
        )
        columns_score.append(
            np.array([seen.get(v, 0) for v in score[name].to_list()], dtype=np.float64)
        )

    categorical_mask = [False] * (len(NUMERIC) + len(BOOLEAN)) + [True] * len(CATEGORICAL)
    return (
        np.column_stack(columns_train),
        np.column_stack(columns_score),
        categorical_mask,
    )


def fit_and_score(train: pl.DataFrame, score: pl.DataFrame) -> dict:
    """One model, trained on train, scored on score. Counts and metrics, no timings.

    The classifier handles a null in a numeric column itself, which is why the nulls in
    magnitude and the tornado columns are passed through rather than imputed. An
    imputation here would be a modelling decision dressed up as data cleaning.
    """
    x_train, x_score, categorical_mask = design_matrix(train, score)
    y_train = train["label"].to_numpy()
    y_score = score["label"].to_numpy()

    if len(set(y_train.tolist())) < 2 or len(set(y_score.tolist())) < 2:
        raise BaselineError("an arm has only one class, so no ranking metric is defined")

    model = HistGradientBoostingClassifier(
        random_state=SEED,
        categorical_features=categorical_mask,
        max_iter=100,
    )
    model.fit(x_train, y_train)
    predicted = model.predict_proba(x_score)[:, 1]

    return {
        "train_rows": int(train.height),
        "train_positives": int(y_train.sum()),
        "score_rows": int(score.height),
        "score_positives": int(y_score.sum()),
        "roc_auc": round(float(roc_auc_score(y_score, predicted)), 4),
        "average_precision": round(float(average_precision_score(y_score, predicted)), 4),
    }


def episodes_shared(train: pl.DataFrame, score: pl.DataFrame) -> int:
    """How many evaluation rows sit in a storm the training set has already seen."""
    seen = set(train["EPISODE_ID"].to_list())
    return int(score["EPISODE_ID"].is_in(list(seen)).sum()) if seen else 0


def score_one_scheme(frame: pl.DataFrame, scheme: str) -> dict:
    """Train on that scheme's train split, score on its test split."""
    train = frame.filter(pl.col(scheme) == "train")
    test = frame.filter(pl.col(scheme) == "test")
    result = fit_and_score(train, test)
    result["test_rows_in_a_storm_train_has_seen"] = episodes_shared(train, test)
    return result


def _largest_share_gap(left: pl.DataFrame, right: pl.DataFrame, column: str) -> float:
    """The widest percentage point difference between the two halves on one column.

    A single number rather than a table, because what matters is whether the worst
    disagreement is small. If it is, the parity split did not sort the events.
    """
    left_share = left[column].value_counts(normalize=True)
    right_share = right[column].value_counts(normalize=True)
    joined = left_share.join(right_share, on=column, how="full", suffix="_right").fill_null(0.0)
    widest = (joined["proportion"] - joined["proportion_right"]).abs().max()
    return round(100 * float(widest), 2)


def matched_experiment(frame: pl.DataFrame) -> dict:
    """The same evaluation rows and the same training size, with and without siblings.

    The storm cut is the base, because under it a test episode's state is still
    represented in train. So the only thing the two arms differ on is whether the
    training set holds other events of the evaluation rows' own storms, which is what a
    random cut hands a model and what the two grouped cuts take away.

    Half of each test episode's events are the evaluation set and half are the siblings.
    The split is by the parity of the event's rank inside its episode, so it is a
    property of the data rather than of a generator, and an episode carrying one event
    contributes to the evaluation set and no sibling.
    """
    base = frame.filter(pl.col("storm_split") == "train")
    held = frame.filter(pl.col("storm_split") == "test").with_columns(
        pl.col("EVENT_ID").rank("ordinal").over("EPISODE_ID").alias("rank_in_episode")
    )
    evaluation = held.filter(pl.col("rank_in_episode") % 2 == 1).drop("rank_in_episode")
    siblings = held.filter(pl.col("rank_in_episode") % 2 == 0).drop("rank_in_episode")

    # Both arms train on the same number of rows. The leaky arm spends part of its budget
    # on the siblings, so it gives up that many rows of the base.
    #
    # Which rows it gives up matters. Taking them off the front of an EVENT_ID sort was
    # the first version and it is wrong, because EVENT_ID rises with time, so the leaky
    # arm would be missing the most recent rows and the gap would carry a year of drift
    # in it. The rows dropped are a hashed sample instead, spread across the base and
    # stable under any input order, using the same hash the cuts themselves use.
    budget = base.height
    leaky_from_base = budget - siblings.height
    if leaky_from_base <= 0:
        raise BaselineError("the sibling set alone fills the training budget")

    ordered = base.with_columns(
        pl.col("EVENT_ID")
        .map_elements(lambda e: splits.unit_fraction(MATCHED_SALT, e), return_dtype=pl.Float64)
        .alias("matched_draw")
    ).sort("matched_draw", "EVENT_ID")

    honest_train = ordered.drop("matched_draw")
    leaky_train = pl.concat(
        [ordered.head(leaky_from_base).drop("matched_draw"), siblings]
    )

    # The third arm answers the obvious objection to the first two. The leaky arm does
    # not only gain siblings, it also gives up 16,263 ordinary rows of the base, so a
    # reader is entitled to ask whether the gap is the siblings arriving or those rows
    # leaving. This arm is the leaky arm's base with nothing added, so the three together
    # separate the two. It is deliberately smaller than the other two and the manifest
    # says so, because the alternative is three arms that answer one question each.
    shared_base = ordered.head(leaky_from_base).drop("matched_draw")

    honest = fit_and_score(honest_train, evaluation)
    leaky = fit_and_score(leaky_train, evaluation)
    base_only = fit_and_score(shared_base, evaluation)

    honest["episodes_shared_with_evaluation"] = episodes_shared(honest_train, evaluation)
    leaky["episodes_shared_with_evaluation"] = episodes_shared(leaky_train, evaluation)
    base_only["episodes_shared_with_evaluation"] = episodes_shared(shared_base, evaluation)

    # Whether the two halves of each test episode are alike. The parity split is
    # deterministic and it is not random, so a systematic difference between the half
    # that is scored and the half that goes into training would sit inside the gap.
    composition = {
        "evaluation_positive_rate_pct": round(100 * float(evaluation["label"].mean()), 2),
        "sibling_positive_rate_pct": round(100 * float(siblings["label"].mean()), 2),
        "event_types_in_the_evaluation_half": int(evaluation["event_type"].n_unique()),
        "event_types_in_the_sibling_half": int(siblings["event_type"].n_unique()),
        "largest_event_type_share_gap_pct": _largest_share_gap(evaluation, siblings, "event_type"),
    }

    return {
        "design": "one evaluation set, one training size, sibling membership varied",
        "evaluation_rows": int(evaluation.height),
        "evaluation_positives": int(evaluation["label"].sum()),
        "sibling_rows": int(siblings.height),
        "training_rows": int(budget),
        "the_two_halves_of_each_test_storm": composition,
        "without_siblings": honest,
        "with_siblings": leaky,
        "shared_base_only": {
            **base_only,
            "note": (
                "the leaky arm's base with no siblings added, so it is smaller than the "
                "other two on purpose and separates rows arriving from rows leaving"
            ),
        },
        "roc_auc_gap": round(leaky["roc_auc"] - honest["roc_auc"], 4),
        "average_precision_gap": round(
            leaky["average_precision"] - honest["average_precision"], 4
        ),
    }
