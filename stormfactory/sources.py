"""Resolving which NOAA Storm Events files a release is built from.

NOAA republishes a storm year whenever records are corrected, and the revision date
is in the file name as the `_cYYYYMMDD` stamp. So "Storm Events 2019 to 2023" does not
name a dataset. The five file names do, and that is why this module exists at all.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, asdict
from pathlib import Path

INDEX_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

# d<storm year>_c<revision date>. Both are in the name and only the first is obvious.
DETAILS_NAME = re.compile(
    r"StormEvents_details-ftp_v(?P<format>[0-9.]+)_d(?P<year>\d{4})_c(?P<revision>\d{8})\.csv\.gz"
)


class SourceError(Exception):
    pass


@dataclass(frozen=True)
class SourceFile:
    year: int
    revision: str
    format_version: str
    filename: str

    @property
    def url(self) -> str:
        return INDEX_URL + self.filename


def parse_index(html: str) -> list[SourceFile]:
    """Every details file the index page offers, deduplicated.

    The page lists each name twice, once as the href and once as the link text.
    """
    found = {}
    for m in DETAILS_NAME.finditer(html):
        f = SourceFile(
            year=int(m.group("year")),
            revision=m.group("revision"),
            format_version=m.group("format"),
            filename=m.group(0),
        )
        found[f.filename] = f
    return sorted(found.values(), key=lambda f: (f.year, f.revision))


def newest_per_year(files: list[SourceFile], years: list[int]) -> list[SourceFile]:
    """Pick one file per requested year, the latest revision.

    Raises rather than returning a short list. A release built from four of five
    requested years with no complaint is the failure this guards against.
    """
    by_year: dict[int, SourceFile] = {}
    for f in files:
        if f.year not in years:
            continue
        current = by_year.get(f.year)
        if current is None or f.revision > current.revision:
            by_year[f.year] = f

    missing = sorted(set(years) - set(by_year))
    if missing:
        raise SourceError(f"index offers no details file for {missing}")
    return [by_year[y] for y in sorted(years)]


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def lock_entry(f: SourceFile, path: Path) -> dict:
    """What a release records about one source file.

    Content hash and byte count, because the file name alone has already lied once:
    NOAA reissued 2022 on 2026-06-25 under a name that differs from the 2019 one
    only in the revision stamp.
    """
    return {
        **asdict(f),
        "bytes": path.stat().st_size,
        "sha256": sha256_of(path),
    }
