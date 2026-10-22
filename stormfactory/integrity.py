"""Checking a downloaded source file against something other than itself.

The lock files are written by hashing whatever is already on disk. That is fine on
the second run, when the hash is compared against a lock somebody else wrote, and it
is worth nothing on the run that writes the lock. A download truncated by a dropped
connection gets hashed as it stands and recorded as the truth, and every later check
agrees with it forever.

Re-downloading to compare costs 57 MB a run. These two routes cost almost nothing
and each one suits a different file format.

For the gzip sources, the file carries its own CRC32 and uncompressed length in its
last eight bytes, so decompressing it locally proves the compressed bytes are intact
with no network at all. That is the strong check and it is free.

For the plain text reference files there is no internal checksum, so the remote is
asked instead: a HEAD for the byte count, and a ranged GET of the tail. Those two
catch a truncation and a changed tail for a few kilobytes rather than for the whole
file. Neither catches a corruption in the middle that preserves the length, and the
release manifest says so rather than implying a stronger guarantee.

Not every publisher offers either. weather.gov serves the zone crosswalk chunked with
no Content-Length, no Accept-Ranges, and it answers a Range header with 200 and the
whole body rather than 206. So that one source is checked by downloading it again and
comparing, which is the expensive route and costs 350 KB rather than 57 MB. Each
source records which route checked it, because the routes do not prove the same thing.
"""

from __future__ import annotations

import gzip
import hashlib
import urllib.request
import zlib

TAIL_WINDOW = 1 << 16

USER_AGENT = "storm-damage-dataset-factory (portfolio project, contact via GitHub EnigmaEngineer)"


class IntegrityError(Exception):
    pass


def gzip_is_intact(path) -> dict:
    """Decompress the whole file so gzip's own CRC32 and length are checked.

    Returns what it learned rather than a boolean, because the uncompressed size is
    worth recording and a caller that only gets True throws it away.
    """
    total = 0
    try:
        with gzip.open(path, "rb") as fh:
            while chunk := fh.read(1 << 20):
                total += len(chunk)
    except (OSError, EOFError, zlib.error) as exc:
        # zlib.error rather than only OSError. A truncated file ends early and raises
        # EOFError, and a flipped byte in the middle raises zlib.error, which is not
        # an OSError and went straight past the first version of this guard.
        raise IntegrityError(f"{path.name} failed its own gzip checksum: {exc}") from exc
    return {"uncompressed_bytes": total, "checked_by": "gzip crc32 and isize"}


def remote_bytes(url: str, timeout: int = 60) -> int | None:
    """The byte count the server reports, with no payload transferred.

    None rather than an exception when the server does not say. A chunked response
    is a fact about the publisher and the caller picks another route, so raising
    here would turn a routing decision into a failure.
    """
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        length = resp.headers.get("Content-Length")
    return None if length is None else int(length)


def remote_tail_sha256(url: str, size: int, window: int = TAIL_WINDOW, timeout: int = 60) -> dict:
    """Hash of the last `window` bytes as the server has them.

    A server free to ignore a Range header would answer 200 with the whole body and
    the hash would silently be of something else, so the status is asserted.
    """
    start = max(0, size - window)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Range": f"bytes={start}-{size - 1}"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = resp.status
        body = resp.read()
    if status != 206:
        raise IntegrityError(f"{url} answered {status} to a Range request rather than 206")
    return {"tail_window": len(body), "tail_sha256": hashlib.sha256(body).hexdigest()}


def local_tail_sha256(path, window: int = TAIL_WINDOW) -> str:
    size = path.stat().st_size
    with path.open("rb") as fh:
        fh.seek(max(0, size - window))
        return hashlib.sha256(fh.read()).hexdigest()


def _assert_size_agrees(path, url: str) -> int | None:
    local_size = path.stat().st_size
    reported = remote_bytes(url)
    if reported is not None and reported != local_size:
        raise IntegrityError(
            f"{path.name} is {local_size} bytes on disk and the server reports {reported}"
        )
    return reported


def check_gzip_source(path, url: str) -> dict:
    """What a gzip source records about having been checked against something else."""
    reported = _assert_size_agrees(path, url)
    out = {"route": "gzip-crc", **gzip_is_intact(path)}
    if reported is not None:
        out["remote_bytes"] = reported
        out["checked_by"] = "gzip crc32 and isize, and the remote byte count"
    return out


def check_plain_source(path, url: str) -> dict:
    """A file with no internal checksum, checked against the remote tail."""
    reported = _assert_size_agrees(path, url)
    if reported is None:
        return check_by_full_download(path, url)
    remote = remote_tail_sha256(url, reported)
    mine = local_tail_sha256(path, remote["tail_window"])
    if mine != remote["tail_sha256"]:
        raise IntegrityError(
            f"{path.name} tail hashes {mine} and the server's tail hashes {remote['tail_sha256']}"
        )
    return {
        "route": "remote-tail",
        "remote_bytes": reported,
        **remote,
        "checked_by": "the remote byte count and the remote tail range",
        "cannot_catch": "a change in the middle of the file that keeps its length",
    }


def check_by_full_download(path, url: str, timeout: int = 180) -> dict:
    """The expensive route, for a publisher that offers no length and no range.

    Streamed into a hasher rather than onto disk, so the local copy the rest of the
    build reads is never replaced by this check.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    h = hashlib.sha256()
    served = 0
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        while chunk := resp.read(1 << 20):
            h.update(chunk)
            served += len(chunk)
    remote_hash = h.hexdigest()
    local_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    if remote_hash != local_hash:
        raise IntegrityError(
            f"{path.name} hashes {local_hash} and the file the server serves hashes {remote_hash}"
        )
    return {
        "route": "full-download",
        "remote_bytes": served,
        "checked_by": "a second download of the whole file, compared by hash",
        "why_this_route": "the server sends no Content-Length and ignores a Range header",
    }
