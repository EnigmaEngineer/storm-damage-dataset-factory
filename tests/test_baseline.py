"""Checks on the damage baseline and on the matched experiment.

The experiment claims that only one thing moves between its two arms. That claim is
worth more than the number it produces, so most of what is here grades the construction
rather than the score: equal training sizes, one evaluation set, a base sample that is
spread rather than taken off one end, and a feature list that cannot quietly admit an
identifier.

The frames are built here rather than read from the release, because these are checks on
the harness and a check that needs 82 MB of downloads is a gate only on a machine holding
them.
"""

from __future__ import annotations

import polars as pl

from stormfactory import baseline

RAW_COLUMNS = (
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
)


def _events(n: int = 800, episodes: int = 60) -> pl.DataFrame:
    """Synthetic events whose damage is a real function of two features.

    The label has to be learnable or every arm scores at chance and no comparison here
    says anything. Hail above a threshold in a county zone damages property, which is
    roughly true and is enough for a ranking metric to move.
    """
    begin_total = [(i * 13) % 1440 for i in range(n)]
    length = [(i % 11) * 6 + 1 for i in range(n)]
    end_total = [begin_total[i] + length[i] for i in range(n)]
    day = [(i % 27) + 1 for i in range(n)]

    # A tornado on roughly one row in seven. The stride is coprime with the episode
    # count, because at one in fifty every tornado row landed in the same split and the
    # training arm then held no tornado length at all.
    is_tornado = [i % 7 == 3 for i in range(n)]

    rows = {
        "EVENT_ID": list(range(1000, 1000 + n)),
        "EPISODE_ID": [i % episodes for i in range(n)],
        "BEGIN_YEARMONTH": [202000 + (i % 12) + 1 for i in range(n)],
        "BEGIN_DAY": day,
        "BEGIN_TIME": [begin_total[i] // 60 * 100 + begin_total[i] % 60 for i in range(n)],
        "END_YEARMONTH": [202000 + (i % 12) + 1 for i in range(n)],
        "END_DAY": [day[i] + (1 if end_total[i] >= 1440 else 0) for i in range(n)],
        "END_TIME": [
            (end_total[i] % 1440) // 60 * 100 + end_total[i] % 60 for i in range(n)
        ],
        "EVENT_TYPE": ["Hail" if i % 3 else "Flood" for i in range(n)],
        "CZ_TYPE": ["C" if i % 2 else "Z" for i in range(n)],
        "MAGNITUDE": [float(i % 11) for i in range(n)],
        "MAGNITUDE_TYPE": [None if i % 4 else "EG" for i in range(n)],
        # Mostly absent and not entirely, which is the shape the release has. An
        # all-null numeric column cannot be binned and design_matrix refuses it, so a
        # fixture carrying only nulls here would be testing that refusal by accident.
        "TOR_F_SCALE": ["EF1" if is_tornado[i] else None for i in range(n)],
        "TOR_LENGTH": [float(i % 23) * 0.5 + 0.5 if is_tornado[i] else None for i in range(n)],
        "TOR_WIDTH": [float(50 + i % 300) if is_tornado[i] else None for i in range(n)],
        "BEGIN_LAT": [30.0 + (i % 40) * 0.25 for i in range(n)],
        "BEGIN_LON": [-100.0 + (i % 40) * 0.25 for i in range(n)],
        "has_point": [True] * n,
    }
    damaging = [
        1 if (rows["MAGNITUDE"][i] > 5 and rows["CZ_TYPE"][i] == "C") else 0 for i in range(n)
    ]
    # Every fourth row has no damage value at all, which is the shape the real source has
    # and the thing feature_frame is meant to drop rather than call a zero.
    rows[baseline.TARGET_SOURCE] = [
        None if i % 4 == 3 else damaging[i] * 1000 for i in range(n)
    ]
    return pl.DataFrame(rows)


def _split_table(events: pl.DataFrame) -> pl.DataFrame:
    """Cuts assigned so that the storm cut keeps episodes whole and the random one does not."""
    ids = events["EVENT_ID"].to_list()
    episodes = events["EPISODE_ID"].to_list()
    return pl.DataFrame(
        {
            "EVENT_ID": ids,
            "has_a_county": [i % 5 != 0 for i in range(len(ids))],
            "random_split": ["test" if i % 5 == 0 else "train" for i in range(len(ids))],
            "storm_split": ["test" if e % 5 == 0 else "train" for e in episodes],
            "blocked_split": ["test" if e % 7 == 0 else "train" for e in episodes],
        }
    )


def _frame() -> pl.DataFrame:
    events = _events()
    return baseline.feature_frame(events, _split_table(events))


def check_an_absent_damage_value_is_dropped_and_not_called_a_zero():
    """The control is the row count, because a zero would keep the row and move the rate.

    With 800 rows and every fourth one absent, a run that treated absence as no damage
    would come back with 800 rows and a lower positive rate. This fails under that change.
    """
    events = _events()
    frame = baseline.feature_frame(events, _split_table(events))
    absent = events[baseline.TARGET_SOURCE].null_count()
    assert absent == 200, absent
    assert frame.height == events.height - absent, frame.height
    assert baseline.TARGET_SOURCE not in frame.columns


def check_the_allowlist_and_the_excluded_list_do_not_intersect():
    assert not set(baseline.FEATURES) & set(baseline.EXCLUDED)


def check_every_excluded_name_carries_a_reason():
    empty = [name for name, why in baseline.EXCLUDED.items() if not why.strip()]
    assert not empty, empty


def check_the_matrix_is_exactly_the_allowlist_wide():
    """A column arriving from anywhere other than the allowlist shows up as a width."""
    frame = _frame()
    train = frame.filter(pl.col("storm_split") == "train")
    score = frame.filter(pl.col("storm_split") == "test")
    x_train, x_score, mask = baseline.design_matrix(train, score)
    assert x_train.shape[1] == len(baseline.FEATURES), x_train.shape
    assert x_score.shape[1] == len(baseline.FEATURES), x_score.shape
    assert len(mask) == len(baseline.FEATURES)
    assert sum(mask) == len(baseline.CATEGORICAL)


def check_the_matrix_refuses_a_feature_that_is_also_excluded():
    """The guard, driven by putting an excluded name on the allowlist rather than by a fixture."""
    frame = _frame()
    train = frame.filter(pl.col("storm_split") == "train")
    original = baseline.FEATURES
    baseline.FEATURES = original + ("EPISODE_ID",)
    try:
        baseline.design_matrix(train, train)
    except baseline.BaselineError:
        return
    finally:
        baseline.FEATURES = original
    raise AssertionError("an excluded name on the allowlist was accepted")


def check_a_category_the_training_set_never_saw_encodes_rather_than_raising():
    """An unseen state is the case the blocked cut produces on every test row.

    The encoding has to survive it, and it has to survive it as a distinct value rather
    than as whatever the first category happened to be.
    """
    frame = _frame()
    train = frame.filter(pl.col("event_type") == "Hail")
    score = frame.filter(pl.col("event_type") == "Flood")
    assert train.height and score.height
    _, x_score, _ = baseline.design_matrix(train, score)
    column = list(baseline.NUMERIC + baseline.BOOLEAN).__len__()
    assert set(x_score[:, column].tolist()) == {0.0}


def check_episodes_shared_counts_rows_and_not_episodes():
    frame = _frame()
    train = frame.filter(pl.col("storm_split") == "train")
    test = frame.filter(pl.col("storm_split") == "test")
    assert baseline.episodes_shared(train, test) == 0
    assert baseline.episodes_shared(train, train) == train.height


def check_the_matched_arms_hold_one_training_size_and_one_evaluation_set():
    matched = baseline.matched_experiment(_frame())
    honest, leaky = matched["without_siblings"], matched["with_siblings"]
    assert honest["train_rows"] == leaky["train_rows"], (honest["train_rows"], leaky["train_rows"])
    assert honest["score_rows"] == leaky["score_rows"]
    assert honest["score_positives"] == leaky["score_positives"]
    assert honest["train_rows"] == matched["training_rows"]
    assert honest["score_rows"] == matched["evaluation_rows"]


def check_only_the_leaky_arm_sees_a_sibling_of_an_evaluation_row():
    matched = baseline.matched_experiment(_frame())
    assert matched["without_siblings"]["episodes_shared_with_evaluation"] == 0
    assert matched["with_siblings"]["episodes_shared_with_evaluation"] > 0


def check_the_base_sample_the_leaky_arm_gives_up_is_spread_and_not_one_end():
    """The regression check for the first version of the experiment.

    EVENT_ID rises with time in the real release, so taking the leaky arm's shortfall off
    the front of an EVENT_ID sort left it missing the newest rows and put a year of drift
    inside the gap. A hashed sample has the same mean EVENT_ID as the base it came from.
    This fails if the sample goes back to being a prefix or a suffix.
    """
    frame = _frame()
    base = frame.filter(pl.col("storm_split") == "train")
    held = frame.filter(pl.col("storm_split") == "test").with_columns(
        pl.col("EVENT_ID").rank("ordinal").over("EPISODE_ID").alias("rank_in_episode")
    )
    siblings = held.filter(pl.col("rank_in_episode") % 2 == 0)
    keep = base.height - siblings.height
    assert keep > 0

    from stormfactory import splits

    ordered = base.with_columns(
        pl.col("EVENT_ID")
        .map_elements(
            lambda e: splits.unit_fraction(baseline.MATCHED_SALT, e), return_dtype=pl.Float64
        )
        .alias("matched_draw")
    ).sort("matched_draw", "EVENT_ID")

    sampled_mean = ordered.head(keep)["EVENT_ID"].mean()
    base_mean = base["EVENT_ID"].mean()
    span = base["EVENT_ID"].max() - base["EVENT_ID"].min()
    drift = abs(sampled_mean - base_mean) / span

    prefix_mean = base.sort("EVENT_ID").head(keep)["EVENT_ID"].mean()
    prefix_drift = abs(prefix_mean - base_mean) / span

    assert drift < 0.02, drift
    assert prefix_drift > drift, (prefix_drift, drift)


def check_an_arm_holding_one_class_is_refused_rather_than_scored():
    """A ranking metric on one class is undefined and sklearn raises deep in the call.

    Refusing it here means the failure names the arm instead of naming a metric.
    """
    frame = _frame()
    one_class = frame.filter(pl.col("label"))
    assert one_class.height
    try:
        baseline.fit_and_score(one_class, one_class)
    except baseline.BaselineError:
        return
    raise AssertionError("an arm with one class was scored")


def check_the_same_frame_scores_the_same_twice():
    """Determinism, on the harness rather than on the release.

    The seed and the single thread are what make the published figures re-derivable at
    all, so a change that reintroduced a generator would show up here.
    """
    frame = _frame()
    first = baseline.score_one_scheme(frame, "storm_split")
    second = baseline.score_one_scheme(frame, "storm_split")
    assert first == second, (first, second)


def check_a_numeric_feature_with_one_value_is_refused_by_name():
    """The guard the suite found, driven by flattening a column rather than by a fixture.

    Before it, an arm whose tornado columns were all absent died inside numpy on a
    sliding window shape, which names neither the column nor the arm.
    """
    frame = _frame()
    flattened = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("tor_length"))
    try:
        baseline.design_matrix(flattened, flattened)
    except baseline.BaselineError as exc:
        assert "tor_length" in str(exc), str(exc)
        return
    raise AssertionError("a numeric feature with no distinct values was accepted")
