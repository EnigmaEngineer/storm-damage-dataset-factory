"""Three ways to cut this release into train, validation and test.

The point of having three is that two of them are honest and the first one is the
control. A random cut over events puts most of a storm in train and the rest of the
same storm in test, so a model gets to see the answer under another row id. The
storm cut and the blocked cut each remove one route for that.

What a storm is here is the NOAA episode and not the event. An episode is one weather
system and its events are that system observed in different places and of different
types, so two events of one episode are the same storm twice. The county resolution
is one to many, so an event can carry several county rows as well. Both
grains sit under the episode, which is why every assignment here is made for an
episode or a state and then joined down.
"""

from __future__ import annotations

import hashlib

import polars as pl

SPLITS = ("train", "val", "test")
TARGETS = {"train": 0.70, "val": 0.15, "test": 0.15}

RANDOM_SALT = "random-v1"
STORM_SALT = "storm-v1"
BLOCKED_SALT = "blocked-v1"


class SplitError(Exception):
    pass


def unit_fraction(salt: str, key) -> float:
    """A stable number in [0, 1) for one key.

    Hashed rather than drawn from a generator, so an assignment does not depend on
    the order the keys arrive in or on how many there are.
    """
    digest = hashlib.sha256(f"{salt}:{key}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def assign_by_fraction(keys, salt: str) -> dict:
    """Each key independently, by where its hash falls in the target intervals."""
    edges = []
    running = 0.0
    for name in SPLITS:
        running += TARGETS[name]
        edges.append((running, name))
    out = {}
    for key in keys:
        u = unit_fraction(salt, key)
        out[key] = next(name for edge, name in edges if u < edge)
    return out


def assign_grouped(sizes: dict, salt: str) -> dict:
    """Whole groups into splits, largest group first into whatever is furthest behind.

    Hashing each group independently is wrong once the groups differ in size by
    orders of magnitude. Texas carries 26,212 events and the smallest territory
    carries single figures, so one unlucky draw moves several points of the whole
    release. Largest first into the biggest deficit is the standard greedy fill and
    it is deterministic, with the salted hash breaking ties between groups of equal
    size rather than the dictionary order doing it.
    """
    if not sizes:
        raise SplitError("nothing to assign")
    total = sum(sizes.values())
    want = {name: TARGETS[name] * total for name in SPLITS}
    have = {name: 0 for name in SPLITS}
    out = {}
    ordered = sorted(sizes.items(), key=lambda kv: (-kv[1], unit_fraction(salt, kv[0])))
    for key, size in ordered:
        name = min(SPLITS, key=lambda s: (have[s] - want[s], s))
        out[key] = name
        have[name] += size
    return out


def random_split(events: pl.DataFrame) -> pl.DataFrame:
    """The control. One event at a time, which is what leaks."""
    mapping = assign_by_fraction(events["EVENT_ID"].to_list(), RANDOM_SALT)
    return events.with_columns(
        pl.col("EVENT_ID").replace_strict(mapping, return_dtype=pl.String).alias("random_split")
    )


def storm_split(events: pl.DataFrame) -> pl.DataFrame:
    """Whole episodes. An event never leaves its storm."""
    sizes = dict(
        events.group_by("EPISODE_ID").agg(pl.len().alias("n")).iter_rows()
    )
    mapping = assign_grouped(sizes, STORM_SALT)
    return events.with_columns(
        pl.col("EPISODE_ID").replace_strict(mapping, return_dtype=pl.String).alias("storm_split")
    )


def blocked_split(events: pl.DataFrame) -> pl.DataFrame:
    """Whole states, which is the coarsest spatial unit every event in this release has.

    A grid or a radius would be finer and neither is available for all of it. 136,316
    rows carry no latitude because NOAA coded them against a forecast zone, so any
    block built from a point drops them. The state code is on every row.

    This is a block and not a fence. Two counties either side of a state line are
    still neighbours, so adjacency survives at the borders and the README says so.
    """
    sizes = dict(
        events.group_by("STATE_FIPS").agg(pl.len().alias("n")).iter_rows()
    )
    mapping = assign_grouped(sizes, BLOCKED_SALT)
    return events.with_columns(
        pl.col("STATE_FIPS").replace_strict(mapping, return_dtype=pl.String).alias("blocked_split")
    )


def units_spanning_two_splits(frame: pl.DataFrame, unit: str, split_col: str) -> pl.DataFrame:
    """Units that landed in more than one split, which is the leak as a count."""
    return (
        frame.group_by(unit)
        .agg(
            pl.col(split_col).n_unique().alias("splits"),
            pl.col(split_col).unique().sort().alias("which"),
        )
        .filter(pl.col("splits") > 1)
        .sort(unit)
    )


def shared_storm_rate(frame: pl.DataFrame, split_col: str) -> dict:
    """How many test events sit in a storm that train has already seen.

    This is the quantity the three schemes exist to move, so it is measured the same
    way for all three rather than argued about.
    """
    train_episodes = frame.filter(pl.col(split_col) == "train")["EPISODE_ID"].unique()
    test = frame.filter(pl.col(split_col) == "test")
    shared = test.filter(pl.col("EPISODE_ID").is_in(train_episodes))
    return {
        "test_events": test.height,
        "test_events_in_a_storm_train_has_seen": shared.height,
        "share_pct": round(100 * shared.height / test.height, 2) if test.height else None,
    }
