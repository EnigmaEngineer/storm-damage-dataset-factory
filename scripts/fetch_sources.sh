#!/usr/bin/env bash
# Pull the source files named in source.lock.json, then check them against the hashes.
# Dagster does this itself. This exists for a clean machine that wants the raw data
# without starting a run.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p data/raw

python3 - <<'PY'
import json, pathlib, urllib.request
lock = json.load(open("source.lock.json"))
for f in lock["files"]:
    dest = pathlib.Path("data/raw") / f["filename"]
    if dest.exists():
        print(f"have {f['filename']}")
        continue
    print(f"fetching {f['filename']}")
    urllib.request.urlretrieve(lock["index_url"] + f["filename"], dest)
PY

python3 - <<'PY'
import hashlib, json, pathlib, sys
lock = json.load(open("source.lock.json"))
bad = []
for f in lock["files"]:
    p = pathlib.Path("data/raw") / f["filename"]
    h = hashlib.sha256(p.read_bytes()).hexdigest()
    if h != f["sha256"]:
        bad.append(f"{f['filename']} hashes {h} and the lock says {f['sha256']}")
    if p.stat().st_size != f["bytes"]:
        bad.append(f"{f['filename']} is {p.stat().st_size} bytes and the lock says {f['bytes']}")
print("\n".join(bad) if bad else f"all {len(lock['files'])} source files match the lock")
sys.exit(1 if bad else 0)
PY
