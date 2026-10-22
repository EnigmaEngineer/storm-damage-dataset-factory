"""Checks on the content digest, driven from both sides.

Every one of these asserts something the digest must NOT be sensitive to, or
something it must be. A hash function nobody has tried to fool is not evidence.
"""

import polars as pl

from stormfactory import digest


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [3, 1, 2],
            "name": ["gamma", "alpha", None],
            "value": [1.5, None, -0.0],
            "flag": [True, False, None],
        }
    )


def check_the_digest_ignores_the_order_rows_arrive_in():
    a = _frame()
    b = a.sort("name", nulls_last=True)
    assert digest.frame_digest(a, ["id"]) == digest.frame_digest(b, ["id"])


def check_the_digest_ignores_the_order_columns_are_declared_in():
    a = _frame()
    b = a.select(["flag", "value", "name", "id"])
    assert digest.frame_digest(a, ["id"]) == digest.frame_digest(b, ["id"])


def check_the_digest_moves_when_one_value_moves():
    a = _frame()
    b = a.with_columns(pl.when(pl.col("id") == 1).then(99.0).otherwise(pl.col("value")).alias("value"))
    assert digest.frame_digest(a, ["id"]) != digest.frame_digest(b, ["id"])


def check_a_moved_null_changes_the_digest():
    """The null mask goes in separately for exactly this case.

    The two frames hold the SAME non-null values in the same order and differ only in
    which row the null sits on. An earlier version of this check compared a null
    against a zero, which also changes how many values there are, so it passed with
    the mask ripped out and was testing nothing.
    """
    a = pl.DataFrame({"id": [1, 2, 3], "value": [None, 1.0, 2.0]})
    b = pl.DataFrame({"id": [1, 2, 3], "value": [1.0, 2.0, None]})
    assert a["value"].drop_nulls().to_list() == b["value"].drop_nulls().to_list()
    assert digest.frame_digest(a, ["id"]) != digest.frame_digest(b, ["id"])


def check_a_renamed_column_changes_the_digest():
    a = _frame()
    b = a.rename({"value": "amount"})
    assert digest.frame_digest(a, ["id"]) != digest.frame_digest(b, ["id"])


def check_a_retyped_column_changes_the_digest():
    """Two integer widths holding the same values must not hash alike.

    The dtype string is the only thing separating them, because every integer column
    is encoded through the same 64 bit path. Comparing an integer against a float
    instead proved nothing, since those already differ in their value bytes.
    """
    a = pl.DataFrame({"id": [1, 2], "value": pl.Series([1, 2], dtype=pl.Int64)})
    b = pl.DataFrame({"id": [1, 2], "value": pl.Series([1, 2], dtype=pl.Int32)})
    assert digest.frame_digest(a, ["id"]) != digest.frame_digest(b, ["id"])


def check_a_large_unsigned_value_does_not_overflow_the_encoder():
    """Every integer went in through a signed 64 bit array, which cannot hold this."""
    big = 2**63 + 7
    a = pl.DataFrame({"id": [1, 2], "value": pl.Series([big, 1], dtype=pl.UInt64)})
    first = digest.frame_digest(a, ["id"])
    b = a.with_columns(pl.when(pl.col("id") == 1).then(pl.lit(big + 1, dtype=pl.UInt64))
                       .otherwise(pl.col("value")).alias("value"))
    assert first != digest.frame_digest(b, ["id"])


def check_strings_cannot_be_shifted_between_rows():
    """Length prefixes rather than a separator, so this pair must not collide.

    Without the prefixes both columns concatenate to the same bytes and the digest
    would agree on two different frames.
    """
    a = pl.DataFrame({"id": [1, 2], "text": ["ab", "c"]})
    b = pl.DataFrame({"id": [1, 2], "text": ["a", "bc"]})
    assert digest.frame_digest(a, ["id"]) != digest.frame_digest(b, ["id"])


def check_a_repeating_sort_key_is_refused():
    a = pl.DataFrame({"id": [1, 1], "value": [1.0, 2.0]})
    try:
        digest.frame_digest(a, ["id"])
    except digest.DigestError as exc:
        assert "repeats" in str(exc), str(exc)
        return
    raise AssertionError("a sort key that does not identify a row should be refused")


def check_a_sort_key_naming_a_missing_column_is_refused():
    try:
        digest.frame_digest(_frame(), ["nope"])
    except digest.DigestError as exc:
        assert "nope" in str(exc), str(exc)
        return
    raise AssertionError("a sort key naming a column that is not there should be refused")


def check_a_nan_is_refused_rather_than_hashed():
    """NaN has more than one bit pattern, so hashing one would be a silent coin flip."""
    a = pl.DataFrame({"id": [1, 2], "value": [float("nan"), 1.0]})
    try:
        digest.frame_digest(a, ["id"])
    except digest.DigestError as exc:
        assert "NaN" in str(exc), str(exc)
        return
    raise AssertionError("a NaN should stop the digest rather than go into it")


def check_an_unsupported_dtype_is_refused_rather_than_skipped():
    """A column the digest has no rule for must stop it, not be left out quietly."""
    a = pl.DataFrame({"id": [1, 2], "value": [[1, 2], [3]]})
    try:
        digest.frame_digest(a, ["id"])
    except digest.DigestError as exc:
        assert "no rule" in str(exc), str(exc)
        return
    raise AssertionError("an unsupported dtype should stop the digest")


def check_the_digest_version_is_part_of_the_hash():
    """Changing the rules has to change every digest, or an old one reads as current."""
    a = _frame()
    first = digest.frame_digest(a, ["id"])
    original = digest.DIGEST_VERSION
    try:
        digest.DIGEST_VERSION = "digest-vX"
        assert digest.frame_digest(a, ["id"]) != first
    finally:
        digest.DIGEST_VERSION = original
