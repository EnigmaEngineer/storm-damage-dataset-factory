"""Compare a rebuild against the release manifest this repo publishes.

Run it against a second checkout that has built the release itself:

    python3 scripts/verify_rebuild.py /path/to/clean/clone

The two hashes are judged differently and that is the whole point of the script.

A content digest that disagrees is a failure. The rebuild produced different data and
the release does not reproduce.

A byte hash that disagrees while the content digest agrees means the same rows came
out in a different file. There are two reasons for that and they are not the same
thing, so the script separates them by comparing the writer strings.

Different writers is expected. Polars stamps its version and build id into the
Parquet footer, so another polars build is bound to produce different bytes from
identical rows.

The SAME writer and different bytes is a finding, and the first rebuild of this
release hit it on two artifacts out of nine. Something in the pipeline was ordering
rows by the engine or by the publisher rather than by the data. Both were traced and
fixed, and the script keeps saying so rather than filing it under the writer.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from stormfactory import release


def load(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"no release manifest at {path}, so there is nothing to compare")
    return json.loads(path.read_text())


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("usage: verify_rebuild.py <path to the rebuilt checkout>")
    mine = load(release.manifest_path())
    theirs = load(Path(argv[1]) / "measurements" / "release.json")

    names = sorted(set(mine["artifacts"]) | set(theirs["artifacts"]))
    content_failures = []
    byte_differences = []

    print(f"release {mine['release']} against the rebuild at {argv[1]}")
    print(f"digest {mine['digest_version']} against {theirs['digest_version']}")
    for name in names:
        a = mine["artifacts"].get(name)
        b = theirs["artifacts"].get(name)
        if a is None or b is None:
            content_failures.append(f"{name} is in only one of the two manifests")
            print(f"  {name:20} MISSING on one side")
            continue
        same_content = a["content_sha256"] == b["content_sha256"]
        same_bytes = a["sha256"] == b["sha256"]
        if not same_content:
            content_failures.append(
                f"{name} content {a['content_sha256'][:12]} against {b['content_sha256'][:12]}"
            )
        if not same_bytes:
            byte_differences.append(name)
        verdict = "same data" if same_content else "DIFFERENT DATA"
        bytes_verdict = "same bytes" if same_bytes else "different bytes"
        print(f"  {name:20} {verdict}, {bytes_verdict}, rows {a['rows']} against {b['rows']}")

    print(f"writers here:    {', '.join(mine['writers'])}")
    print(f"writers there:   {', '.join(theirs['writers'])}")

    if content_failures:
        print(f"CONTENT DIGESTS DISAGREE on {len(content_failures)} artifact(s), so the release does not reproduce")
        for line in content_failures:
            print(f"  {line}")
        return 1
    if byte_differences:
        print(f"every content digest agrees and {len(byte_differences)} artifact(s) differ in bytes")
        print(f"  {', '.join(byte_differences)}")
        if set(mine["writers"]) != set(theirs["writers"]):
            print("the two sides used different Parquet writers, so this is expected")
            return 0
        print("THE WRITERS ARE THE SAME, so the row order is coming from somewhere it should not")
        print("this is not a writer difference and it needs explaining before the release is trusted")
        return 1
    print(f"all {len(names)} artifacts agree bit for bit and by content")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
