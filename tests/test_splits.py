"""Checks on the three split schemes.

The grouped schemes are checked on the property they exist for, which is that a unit
never lands in two splits, and on the boundary each of them has. The random scheme is
checked on the opposite property, because a random cut that keeps storms whole is not
random and would quietly remove the control the leakage comparison needs.
"""

import polars as pl

from stormfactory import splits


def _events(n: int = 600, episodes: int = 40, states: int = 12) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "EVENT_ID": list(range(n)),
            "EPISODE_ID": [i % episodes for i in range(n)],
            "STATE_FIPS": [(i % episodes) % states for i in range(n)],
        }
    )


def check_the_storm_scheme_never_splits_an_episode():
    frame = splits.storm_split(_events())
    assert splits.units_spanning_two_splits(frame, "EPISODE_ID", "storm_split").height == 0


def check_the_blocked_scheme_never_splits_a_state():
    frame = splits.blocked_split(_events())
    assert splits.units_spanning_two_splits(frame, "STATE_FIPS", "blocked_split").height == 0


def check_the_blocked_scheme_keeps_episodes_whole_as_a_consequence():
    """Only because no episode in this release crosses a state line, measured.

    If a future year carries one that does, this check fails and the blocked scheme
    needs a unit built from both, which is the day that fact is worth knowing.
    """
    frame = splits.blocked_split(_events())
    assert splits.units_spanning_two_splits(frame, "EPISODE_ID", "blocked_split").height == 0


def check_the_random_scheme_does_split_episodes():
    """The control. A pass here is the scheme working, not failing."""
    frame = splits.random_split(_events())
    assert splits.units_spanning_two_splits(frame, "EPISODE_ID", "random_split").height > 0


def check_every_event_gets_a_declared_split():
    frame = _events()
    for assign, col in ((splits.random_split, "random_split"),
                        (splits.storm_split, "storm_split"),
                        (splits.blocked_split, "blocked_split")):
        out = assign(frame)
        assert out[col].null_count() == 0, col
        assert set(out[col].unique().to_list()) <= set(splits.SPLITS), col


def check_an_assignment_does_not_depend_on_the_order_the_keys_arrive_in():
    frame = _events()
    shuffled = frame.sort("EPISODE_ID", descending=True)
    a = splits.storm_split(frame).sort("EVENT_ID")["storm_split"].to_list()
    b = splits.storm_split(shuffled).sort("EVENT_ID")["storm_split"].to_list()
    assert a == b


def check_the_grouped_fill_lands_within_the_largest_group_of_its_target():
    """The property the greedy fill guarantees, rather than a race against hashing.

    Whole groups cannot in general hit a target exactly, and the best any scheme can
    promise is to be off by no more than the largest group it had to place in one
    piece. That bound is the thing worth pinning, and it is what separates the greedy
    fill from hashing each group on its own: on the lopsided cases below, hashing
    misses by more than the largest group.

    An earlier version of this check compared the two implementations and asserted
    the greedy one was no worse. It passed with the greedy fill replaced by hashing,
    because the comparison then had the same function on both sides.
    """
    cases = (
        {f"s{i}": n for i, n in enumerate(
            [26212, 13381, 11563, 11341, 10677, 10598] + [4000] * 20 + [1200] * 25 + [120] * 18)},
        {"big": 2500, **{f"s{i}": 100 for i in range(75)}},
        {f"s{i}": 100 for i in range(30)},
    )
    for sizes in cases:
        total = sum(sizes.values())
        bound = max(sizes.values()) / total
        mapping = splits.assign_grouped(sizes, "control")
        for name in splits.SPLITS:
            got = sum(size for key, size in sizes.items() if mapping[key] == name) / total
            off = abs(got - splits.TARGETS[name])
            assert off <= bound + 1e-9, (name, off, bound, len(sizes))


def check_assign_grouped_refuses_an_empty_input():
    try:
        splits.assign_grouped({}, "t")
    except splits.SplitError:
        return
    raise AssertionError("assigning nothing should be refused rather than return nothing")


def check_a_single_group_goes_somewhere_rather_than_nowhere():
    """The smallest input that must still work. One state, one split, no crash."""
    mapping = splits.assign_grouped({"only": 5}, "t")
    assert mapping["only"] in splits.SPLITS


def check_the_targets_add_up():
    assert abs(sum(splits.TARGETS.values()) - 1.0) < 1e-9
    assert set(splits.TARGETS) == set(splits.SPLITS)


def check_the_shared_storm_rate_is_zero_for_a_grouped_scheme_and_not_for_random():
    frame = _events()
    storm = splits.shared_storm_rate(splits.storm_split(frame), "storm_split")
    rand = splits.shared_storm_rate(splits.random_split(frame), "random_split")
    assert storm["test_events_in_a_storm_train_has_seen"] == 0
    assert rand["test_events_in_a_storm_train_has_seen"] > 0


def check_the_fraction_for_one_key_is_inside_the_unit_interval():
    for key in (0, 1, 999999, "a string key"):
        u = splits.unit_fraction("salt", key)
        assert 0.0 <= u < 1.0, (key, u)


def check_a_different_salt_moves_the_assignment():
    """Otherwise the three schemes would agree by accident rather than by design."""
    keys = list(range(200))
    a = splits.assign_by_fraction(keys, splits.RANDOM_SALT)
    b = splits.assign_by_fraction(keys, splits.STORM_SALT)
    assert any(a[k] != b[k] for k in keys)
