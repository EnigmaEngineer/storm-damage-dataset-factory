"""Build every partition of the Storm Events layer, then the manifest and its checks.

`dagster dev -m stormfactory.definitions` is the interactive route. This is the one a
run uses, because it is a single command and it exits non-zero when anything fails.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dagster import materialize

from stormfactory import assets
from stormfactory.assets import YEARS


def main() -> int:
    ok = materialize([assets.source_lock]).success
    print(f"source_lock: {'ok' if ok else 'FAILED'}")

    for year in YEARS:
        result = materialize([assets.storm_events], partition_key=str(year))
        ok = ok and result.success
        rows = "unknown"
        for ev in result.get_asset_materialization_events():
            md = ev.step_materialization_data.materialization.metadata
            rows = md.get("rows").value
        print(f"storm_events {year}: {'ok' if result.success else 'FAILED'}, rows {rows}")

    result = materialize(
        [
            assets.ingest_manifest,
            assets.manifest_counts_match_parquet,
            assets.manifest_holds_no_wall_time,
        ]
    )
    ok = ok and result.success
    print(f"ingest_manifest: {'ok' if result.success else 'FAILED'}")
    for ev in result.get_asset_check_evaluations():
        print(f"check {ev.check_name}: {'pass' if ev.passed else 'FAIL'} {dict(ev.metadata)}")

    ok = build_joins_and_scrub(ok)
    return 0 if ok else 1


def build_joins_and_scrub(ok: bool) -> bool:
    """The county join layer and the narrative scrub.

    Each asset is materialised on its own rather than as one job, because every
    dependency here is declared with `deps` and loads nothing, so one failure should
    not take the stages after it with it.
    """
    for label, targets in (
        ("reference_lock", [assets.reference_lock]),
        ("spc_reports", [assets.spc_reports]),
        ("fema_declarations", [assets.fema_declarations]),
        ("event_counties", [assets.event_counties, assets.event_counties_are_unique_and_real]),
        ("join_manifest", [assets.join_manifest, assets.join_manifest_matches_parquet]),
        ("narrative_scrub", [assets.narrative_scrub, assets.day2_manifests_hold_no_wall_time]),
    ):
        result = materialize(targets)
        ok = ok and result.success
        print(f"{label}: {'ok' if result.success else 'FAILED'}")
        for ev in result.get_asset_materialization_events():
            md = ev.step_materialization_data.materialization.metadata
            if md:
                print("   " + ", ".join(f"{k} {v.value}" for k, v in sorted(md.items())))
        for ev in result.get_asset_check_evaluations():
            print(f"   check {ev.check_name}: {'pass' if ev.passed else 'FAIL'} {dict(ev.metadata)}")
    return ok


if __name__ == "__main__":
    sys.exit(main())
