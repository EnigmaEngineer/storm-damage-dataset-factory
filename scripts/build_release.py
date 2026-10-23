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
        ("event_splits", [assets.event_splits, assets.no_storm_spans_two_splits,
                          assets.blocked_split_keeps_each_state_whole,
                          assets.every_event_is_assigned_once]),
        ("spc_om_forms", [assets.spc_om_forms, assets.spc_om_form_counts_re_derive]),
        ("release_manifest", [assets.release_manifest, assets.release_manifest_matches_the_data,
                              assets.day3_manifests_hold_no_wall_time]),
        ("baseline_report", [assets.baseline_report, assets.baseline_counts_re_derive,
                             assets.baseline_arms_share_no_storm_except_the_control,
                             assets.matched_arms_hold_one_evaluation_set_and_one_training_size,
                             assets.baseline_features_hold_nothing_excluded,
                             assets.day4_manifests_hold_no_wall_time]),
    ):
        result = materialize(targets)
        ok = ok and result.success
        print(f"{label}: {'ok' if result.success else 'FAILED'}")
        for ev in result.get_asset_materialization_events():
            md = ev.step_materialization_data.materialization.metadata
            # `path` is the io manager's own scratch directory and it changes every
            # run, so printing it makes this output differ between two identical
            # builds for no reason. Dagster adds it, not an asset.
            shown = {k: v for k, v in md.items() if k != "path"}
            if shown:
                print("   " + ", ".join(f"{k} {v.value}" for k, v in sorted(shown.items())))
        for ev in result.get_asset_check_evaluations():
            print(f"   check {ev.check_name}: {'pass' if ev.passed else 'FAIL'} {dict(ev.metadata)}")
    return ok


if __name__ == "__main__":
    sys.exit(main())
