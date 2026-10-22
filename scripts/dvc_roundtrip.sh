#!/usr/bin/env bash
# Push the release to the dvc remote, pull it back into an empty checkout, and
# compare the pulled bytes against measurements/release.json.
#
# This exists because a dvc remote that has only ever been pushed to is a remote
# nobody has read. The pull half is the one that fails when a pointer file and a
# remote have drifted apart, and it cannot be proved by reading the config.
#
# Swapping the remote for a GCS bucket does not change this script.
set -euo pipefail

# dvc and this project do not have to share an interpreter and here they do not. dvc
# needs omegaconf, which pins a version of antlr4 with no wheel for Python 3.13, so it
# installed into its own virtual environment while polars sits in the system one. Set
# PYTHON if the verification step cannot import polars from whatever `python3` means
# when dvc is on the path.
PYTHON="${PYTHON:-python3}"

cd "$(dirname "$0")/.."
repo="$(pwd)"
scratch="${1:-}"
if [ -z "$scratch" ]; then
    echo "usage: dvc_roundtrip.sh <an empty directory to pull into>" >&2
    exit 2
fi

dvc add data/parquet
dvc push

mkdir -p "$scratch"
{ git ls-files; git ls-files --others --exclude-standard; } | sort -u | tar -cf - -T - | tar -xf - -C "$scratch"
cp -r .dvc "$scratch/.dvc"
rm -rf "$scratch/.dvc/cache" "$scratch/.dvc/tmp"
cp data/parquet.dvc "$scratch/data/parquet.dvc"

# The remote url in .dvc/config is relative to the config file, so the pulling
# checkout needs its own route to the same directory.
ln -sfn "$repo/dvc-remote" "$scratch/dvc-remote"

cd "$scratch"
# dvc refuses to operate outside a repository, and the pulling checkout is a fresh
# directory rather than a clone, so it gets its own.
git init -q
git add -A
git -c user.email=roundtrip@local -c user.name=roundtrip commit -qm "checkout for the dvc pull"
dvc pull

"$PYTHON" - <<'PY'
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))
from stormfactory import digest, release

manifest = json.loads(Path("measurements/release.json").read_text())
bad = []
for name, stated in sorted(manifest["artifacts"].items()):
    got = digest.byte_sha256(release.artifact_path(name))
    if got != stated["sha256"]:
        bad.append(f"{name} pulled {got[:12]} and the release published {stated['sha256'][:12]}")
if bad:
    print("\n".join(bad))
    raise SystemExit(1)
print(f"all {len(manifest['artifacts'])} pulled artifacts match the release byte hashes")
PY
