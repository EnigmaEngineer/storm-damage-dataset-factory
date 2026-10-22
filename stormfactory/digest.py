"""A content digest for a Parquet artifact that does not depend on who wrote it.

The byte hash of a Parquet file is not a hash of its data. Polars writes its own
version and build id into the file footer, so the same rows written by two polars
builds hash differently while every value agrees. A release that pins its data by
byte hash alone is pinning its writer as well, and the two claims are worth keeping
apart.

So each artifact carries two hashes. The byte hash answers "is this the file the
release published". The content digest here answers "is this the same data", and it
is invariant to compression, row group layout, file metadata and writer version.

It is not invariant to column names or dtypes. It is not invariant to row order or
to where the nulls sit. That is why an artifact has to declare the key its rows are
sorted by.

Floats go in as their IEEE-754 bit patterns rather than as decimal text, because a
decimal rendering is another thing a library version can change underneath you.
"""

from __future__ import annotations

import hashlib
import sys
from array import array
from pathlib import Path

import polars as pl

DIGEST_VERSION = "digest-v1"

NULL_MASK_PRESENT = 1
NULL_MASK_ABSENT = 0


class DigestError(Exception):
    pass


def _little_endian(code: str, values: list) -> bytes:
    buf = array(code, values)
    if sys.byteorder != "little":
        buf.byteswap()
    return buf.tobytes()


def _column_bytes(name: str, series: pl.Series) -> list[bytes]:
    """One column as a list of byte blobs, fed to the hasher in order.

    The null mask goes in separately from the values, so a null and a zero in the
    same position can never produce the same bytes.
    """
    dtype = series.dtype
    mask = _little_endian(
        "B",
        [NULL_MASK_PRESENT if v else NULL_MASK_ABSENT for v in series.is_null().to_list()],
    )
    present = series.drop_nulls()
    parts = [name.encode("utf-8"), str(dtype).encode("utf-8"), mask]

    if dtype == pl.Boolean:
        parts.append(_little_endian("B", [1 if v else 0 for v in present.to_list()]))
    elif dtype.is_integer():
        # Unsigned goes through 'Q'. A UInt64 above 2**63 does not fit a signed array
        # and raised OverflowError rather than hashing, which a check now pins. The
        # dtype string is already in the hash, so a signed and an unsigned column of
        # the same values stay apart whichever array code carried them.
        code = "Q" if dtype in (pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64) else "q"
        parts.append(_little_endian(code, present.to_list()))
    elif dtype == pl.Float64:
        for v in present.to_list():
            if v != v:
                raise DigestError(f"{name} holds a NaN, which has no single bit pattern")
        parts.append(_little_endian("d", present.to_list()))
    elif dtype == pl.Date:
        parts.append(_little_endian("q", [v.toordinal() for v in present.to_list()]))
    elif dtype == pl.String:
        texts = [v.encode("utf-8") for v in present.to_list()]
        # Length prefixes rather than a separator. A narrative can contain any
        # character a separator might use, and two different columns of strings
        # must not be able to concatenate into the same bytes.
        parts.append(_little_endian("q", [len(t) for t in texts]))
        parts.append(b"".join(texts))
    else:
        raise DigestError(f"{name} is {dtype} and the digest has no rule for it")
    return parts


def frame_digest(df: pl.DataFrame, sort_by: list[str]) -> str:
    """The digest of a frame, sorted by the declared key.

    Raises if the key does not identify a row. A digest over rows whose order is
    not pinned is a number that changes for no reason, which is worse than not
    having one.
    """
    missing = [c for c in sort_by if c not in df.columns]
    if missing:
        raise DigestError(f"sort key names {missing} and the frame has no such column")
    if df.select(sort_by).unique().height != df.height:
        raise DigestError(f"sort key {sort_by} repeats, so the row order is not pinned")

    ordered = df.sort(sort_by, nulls_last=True)
    h = hashlib.sha256()
    h.update(DIGEST_VERSION.encode("utf-8"))
    h.update(_little_endian("q", [ordered.height]))
    h.update(("|".join(sort_by)).encode("utf-8"))
    for name in sorted(ordered.columns):
        for blob in _column_bytes(name, ordered[name]):
            h.update(blob)
    return h.hexdigest()


def parquet_digest(path: Path, sort_by: list[str]) -> str:
    return frame_digest(pl.read_parquet(path), sort_by)


def byte_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def writer_of(path: Path) -> str:
    """The writer string out of the Parquet footer, read without a Parquet library.

    This is the thing that makes the byte hash writer dependent, so the release
    records it. The footer holds it as plain text near the end of the file.
    """
    with path.open("rb") as fh:
        size = path.stat().st_size
        fh.seek(max(0, size - 4096))
        tail = fh.read()
    marker = b"Polars"
    at = tail.find(marker)
    if at < 0:
        return "unknown"
    end = at
    while end < len(tail) and 32 <= tail[end] < 127:
        end += 1
    return tail[at:end].decode("ascii", "replace")
