"""A small collector so the suite needs no test framework in requirements.

Every module in this folder exposes functions named `check_*`. They raise on failure
and return nothing on success.
"""

from __future__ import annotations

import importlib
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MODULES = [
    "tests.test_sources",
    "tests.test_schema",
    "tests.test_manifest",
    "tests.test_readme",
]


def collect(module_names=None):
    found = []
    for name in module_names or MODULES:
        mod = importlib.import_module(name)
        for attr in sorted(dir(mod)):
            if attr.startswith("check_"):
                found.append((name, attr, getattr(mod, attr)))
    return found


def run(module_names=None) -> int:
    cases = collect(module_names)
    failures = []
    for module, name, fn in cases:
        try:
            fn()
        except Exception:
            failures.append((module, name, traceback.format_exc()))
    for module, name, tb in failures:
        print(f"FAIL {module}.{name}\n{tb}")
    print(f"{len(cases) - len(failures)} of {len(cases)} checks passed")
    return 1 if failures else 0
