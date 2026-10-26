"""The DVC pointers against the release manifest, and the two ways they drift.

Nothing checked this for two releases and the cost was a release nobody could pull.
`data/parquet.dvc` recorded nine files and 31,140,831 bytes while
`measurements/release.json` published eleven artifacts and 264,538,652, because
`dvc add` is a separate act from writing an artifact and the two releases that added
one did the second without the first. Three descriptions of one release, and the third
disagreed with the other two.

The second failure is quieter and these checks exist mostly for it. A `.gitignore`
rule of the shape `data/parquet/*/` leaves the pointers visible to git and invisible
to DVC, and a bare `dvc push` then prints `Everything is up to date` and exits 0
having pushed nothing. There is no error and no non-zero status anywhere in it. The
arrangement that works is the idiomatic one, where `dvc add` writes
`data/parquet/.gitignore` naming each output and the top level file says nothing
about that directory at all.

These are static. They read pointer files and a manifest and never call dvc, so they
grade on a clean clone with no data present and no dvc installed.
"""

from __future__ import annotations

import json
from pathlib import Path

from stormfactory import release

REPO_ROOT = Path(__file__).resolve().parent.parent
POINTER_DIR = REPO_ROOT / "data" / "parquet"
MANIFEST = REPO_ROOT / "measurements" / "release.json"


def _pointers() -> dict[str, dict]:
    """Every `*.dvc` beside the artifacts, parsed without a yaml dependency.

    The format DVC writes for a directory output is four or five fixed keys at one
    indent under a single `outs:` list entry, so a line split is enough and the repo
    does not gain a parser for seven files.
    """
    found = {}
    for path in sorted(POINTER_DIR.glob("*.dvc")):
        fields = {}
        for line in path.read_text().splitlines():
            stripped = line.strip().lstrip("- ")
            if ":" not in stripped:
                continue
            key, _, value = stripped.partition(":")
            fields[key.strip()] = value.strip()
        found[path.stem] = fields
    return found


def _artifacts_by_pointer() -> dict[str, list[str]]:
    """Registry artifacts grouped by the output directory that would hold them.

    `storm_events` is five partitions under one directory and every other artifact is
    one file under its own, so this is not a one to one mapping and the grouping has
    to come from the registry rather than from a count.
    """
    grouped: dict[str, list[str]] = {}
    for name, spec in release.ARTIFACTS.items():
        grouped.setdefault(spec["path"][1], []).append(name)
    return grouped


def check_every_artifact_directory_has_a_pointer():
    expected = set(_artifacts_by_pointer())
    found = set(_pointers())
    missing = sorted(expected - found)
    extra = sorted(found - expected)
    if missing or extra:
        raise AssertionError(
            f"the registry names {sorted(expected)} and the pointers are {sorted(found)}, "
            f"missing {missing}, unexpected {extra}"
        )


def check_pointer_sizes_sum_to_the_published_release():
    """The check whose absence let the pointer fall two artifacts behind the manifest.

    Compared as a sum per output rather than per artifact, because a directory
    pointer records one size for everything under it.
    """
    published = json.loads(MANIFEST.read_text())["artifacts"]
    pointers = _pointers()
    wrong = []
    for directory, names in sorted(_artifacts_by_pointer().items()):
        stated = int(pointers[directory]["size"])
        actual = sum(published[n]["bytes"] for n in names)
        if stated != actual:
            wrong.append(
                f"{directory}.dvc says {stated:,} bytes and the manifest publishes "
                f"{actual:,} across {len(names)} artifact(s)"
            )
    if wrong:
        raise AssertionError("; ".join(wrong))


def check_pointer_file_counts_match_the_registry():
    pointers = _pointers()
    wrong = []
    for directory, names in sorted(_artifacts_by_pointer().items()):
        stated = int(pointers[directory]["nfiles"])
        if stated != len(names):
            wrong.append(
                f"{directory}.dvc records {stated} files and the registry names {len(names)}"
            )
    if wrong:
        raise AssertionError("; ".join(wrong))


def check_dvc_owns_the_gitignore_inside_the_pointer_directory():
    """The arrangement that keeps `dvc push` from lying.

    Asserted by presence and content rather than by running dvc, because the failure
    it guards is silent and a test that shells out to dvc cannot run on a clean clone
    that has not installed it.
    """
    inner = POINTER_DIR / ".gitignore"
    if not inner.exists():
        raise AssertionError(
            f"{inner.relative_to(REPO_ROOT)} is absent. dvc writes it on `dvc add` and "
            "it is what keeps the artifact directories out of git without hiding the "
            "pointers from dvc"
        )
    listed = {line.strip().lstrip("/") for line in inner.read_text().splitlines() if line.strip()}
    expected = set(_artifacts_by_pointer())
    if listed != expected:
        raise AssertionError(
            f"{inner.relative_to(REPO_ROOT)} lists {sorted(listed)} and the outputs are "
            f"{sorted(expected)}"
        )


def check_the_top_level_gitignore_says_nothing_about_the_pointer_directory():
    """`data/parquet/*/` is the rule that makes dvc blind, so it is refused by name.

    Any rule naming that directory is refused rather than that one spelling, because
    the trap has three spellings and only one of them announces itself.
    """
    offending = []
    for line in (REPO_ROOT / ".gitignore").read_text().splitlines():
        rule = line.strip()
        if not rule or rule.startswith("#") or rule.startswith("!"):
            continue
        if rule.rstrip("/").rstrip("*").rstrip("/") == "data/parquet":
            offending.append(rule)
    if offending:
        raise AssertionError(
            f"the top level .gitignore carries {offending}, and any rule over "
            "data/parquet either hides the pointers from git or hides them from dvc. "
            "data/parquet/.gitignore, written by dvc add, is what excludes the data"
        )
