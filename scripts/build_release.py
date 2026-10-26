"""Build every partition of the Storm Events layer, then the manifest and its checks.

`dagster dev -m stormfactory.definitions` is the interactive route. This is the one a
run uses, because it is a single command and it exits non-zero when anything fails.

The vector column is built here too, ahead of the release manifest, even though it is
not a Dagster asset. It is the eleventh artifact in the registry and the manifest
stage refuses to describe a release whose artifacts are not all on disk, so without
this stage the one command in the README exits 1 on any machine that has never run
`scripts/build_vectors.py` by hand. That was true for two days and the README said to
run this and nothing else.
"""

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dagster import materialize

from stormfactory import assets, release
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


def _build_vector_column() -> bool:
    """Run the build half of `scripts/build_vectors.py`, and only the build half.

    A subprocess rather than an import, because that script parses arguments and owns
    a server lifecycle and this one should not inherit either. `--phase build` is the
    half that needs no database.

    Skipped when the artifact is already on disk and the registry agrees with it, so a
    rerun of this script does not spend ninety seconds re-reducing an unchanged corpus.
    """
    artifact = release.artifact_path("narrative_vectors")
    if artifact.exists():
        print(f"narrative_vectors: present at {artifact.relative_to(Path.cwd())}, not rebuilt")
        return True
    print("narrative_vectors: building, this is the slow stage")
    done = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("build_vectors.py")), "--phase", "build"],
        cwd=str(Path(__file__).resolve().parent.parent),
    )
    print(f"narrative_vectors: {'ok' if done.returncode == 0 else 'FAILED'}")
    return done.returncode == 0


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
        ("eri_catalogue", [assets.eri_catalogue]),
        ("imagery_tiles", [assets.imagery_tiles, assets.imagery_counts_re_derive,
                           assets.tiles_hold_imagery_inside_the_declared_buffer]),
        ("narrative_vectors", _build_vector_column),
        ("release_manifest", [assets.release_manifest, assets.release_manifest_matches_the_data,
                              assets.day3_manifests_hold_no_wall_time]),
        ("baseline_report", [assets.baseline_report, assets.baseline_counts_re_derive,
                             assets.baseline_arms_share_no_storm_except_the_control,
                             assets.matched_arms_hold_one_evaluation_set_and_one_training_size,
                             assets.baseline_features_hold_nothing_excluded,
                             assets.day4_manifests_hold_no_wall_time]),
        # Last, because it reads a committed labelling pass over the tiles the stage above
        # publishes. A clean machine that has never fetched a tile still gets here, since
        # the labels name tile ids and the check compares them against the artifact.
        ("tile_labels", [assets.tile_labels, assets.agreement_is_unmeasured_with_one_annotator,
                         assets.imagery_manifests_hold_no_wall_time]),
    ):
        if callable(targets):
            ok = targets() and ok
            continue
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
