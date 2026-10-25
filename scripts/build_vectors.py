"""Build the vector column, load it into Postgres, and measure what its index returns.

Everything happens in one process because the server does not outlive the shell that
started it. So this brings a cluster up in a temporary directory. It loads, measures
and writes the manifest. Then it stops the cluster. Nothing it leaves behind is read by anything else, and
a second run of this script starts from an empty cluster rather than from whatever the
last one left.

Run it from the repository root:

    python3 scripts/build_vectors.py

It needs a Postgres server binary and the pgvector extension present. If either is
missing it says which and exits non-zero, rather than reporting a measurement it did
not take.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from argparse import ArgumentParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import polars as pl

# At module level rather than inside the function that needs it. A deferred import
# reads as the tidier way to keep a missing driver from breaking the build phase, and
# it hides the dependency from the check that grades declared packages against
# imported ones. The refusal below does that job without hiding anything.
try:
    import psycopg2
except ImportError:
    psycopg2 = None

from stormfactory import digest, paths, release, vectors, vectorstore

PORT = os.environ.get("STORMFACTORY_PG_PORT", "55432")
# How many query vectors the recall measurement uses, and how many neighbours each
# one asks for. Both are published in the manifest, because recall is meaningless
# without the k it was measured at.
QUERIES = 50
K = 10


def _server_root() -> Path:
    for candidate in sorted(Path("/usr/lib/postgresql").glob("*/bin"), reverse=True):
        if (candidate / "initdb").exists():
            return candidate
    raise SystemExit("no Postgres server binaries found under /usr/lib/postgresql")


def _run(args, **kw):
    return subprocess.run(args, check=True, capture_output=True, text=True, **kw)


def main(argv=None) -> int:
    parser = ArgumentParser(description=__doc__)
    # The two phases are separable because the build is expensive and the measurement
    # needs a server. A host that caps a single call runs them one at a time. A host
    # that does not runs the default and gets the same result.
    parser.add_argument("--phase", choices=("build", "measure", "all"), default="all")
    args = parser.parse_args(argv)
    bins = _server_root()
    if psycopg2 is None:
        raise SystemExit("psycopg2 is not installed, so the load cannot run")

    manifest = paths.release_root() / "measurements" / "embeddings.json"
    artifact = release.artifact_path("narrative_vectors")

    if args.phase == "measure":
        if not artifact.exists():
            raise SystemExit(f"{artifact} does not exist, so run the build phase first")
        facts = json.loads(manifest.read_text())
        frame = pl.read_parquet(artifact)
        print(f"reusing {artifact}, {frame.height} rows")
        return _measure(bins, frame, facts, manifest)

    print("building the vector column")
    frame, facts = vectors.build()
    print(f"  {frame.height} narratives, vocabulary {facts['vocabulary']}, "
          f"explained variance {facts['explained_variance_cumulative']}")

    # The artifact lands beside the others and is digested the same way.
    artifact.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(artifact)
    facts["content_sha256"] = digest.parquet_digest(
        artifact, release.ARTIFACTS["narrative_vectors"]["sort_by"]
    )
    facts["vector_fingerprint"] = vectors.vector_fingerprint(frame)
    print(f"  wrote {artifact}")

    # Determinism. The same rows through the same code must give the same vectors,
    # and the only way to say so is to do it twice.
    print("rebuilding to check the column is a function of the rows")
    again, again_facts = vectors.build()
    facts["rebuild_fingerprint_matches"] = (
        vectors.vector_fingerprint(again) == facts["vector_fingerprint"]
    )
    facts["rebuild_explained_variance_matches"] = (
        again_facts["explained_variance_cumulative"] == facts["explained_variance_cumulative"]
    )
    print(f"  fingerprint repeats: {facts['rebuild_fingerprint_matches']}")

    manifest.write_text(json.dumps(facts, indent=2, sort_keys=True) + "\n")
    if args.phase == "build":
        print(f"wrote {manifest}, the measurement phase is still to run")
        return 0
    return _measure(bins, frame, facts, manifest)


def _measure(bins, frame, facts, manifest) -> int:
    data_dir = Path(tempfile.mkdtemp(prefix="stormfactory-pg-"))
    sock = Path(tempfile.mkdtemp(prefix="stormfactory-pgsock-"))
    cluster = data_dir / "cluster"
    started = False
    try:
        # initdb refuses to run as root, and a scheduled build may well be root, so
        # the cluster is owned by whichever unprivileged account is available.
        as_user = []
        if os.geteuid() == 0:
            shutil.chown(data_dir, "postgres")
            shutil.chown(sock, "postgres")
            as_user = ["setpriv", "--reuid=postgres", "--regid=postgres", "--clear-groups"]

        print("starting a cluster")
        _run(as_user + [str(bins / "initdb"), "-D", str(cluster), "-U", "postgres",
                        "--auth=trust", "--encoding=UTF8", "--locale=C"])
        with (cluster / "postgresql.conf").open("a") as conf:
            conf.write(
                f"\nport={PORT}\nlisten_addresses=''\n"
                f"unix_socket_directories='{sock}'\n"
                # This cluster is thrown away at the end of the call. Durability
                # settings that protect a crash are paying for nothing here.
                "fsync=off\nsynchronous_commit=off\nmaintenance_work_mem='256MB'\n"
            )
        _run(as_user + [str(bins / "pg_ctl"), "-D", str(cluster), "-l",
                        str(data_dir / "pg.log"), "-w", "start"])
        started = True

        conn = psycopg2.connect(host=str(sock), port=PORT, user="postgres", dbname="postgres")
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(vectorstore.create_sql())
        cur.execute("select extversion from pg_extension where extname = 'vector'")
        facts["pgvector_version"] = cur.fetchone()[0]
        cur.execute("select version()")
        facts["postgres_version"] = cur.fetchone()[0].split(" on ")[0]
        print(f"  {facts['postgres_version']}, pgvector {facts['pgvector_version']}")

        # How many rows share a vector with another row. This is what the tie counts
        # below are made of. Narratives repeat verbatim across the events of one
        # episode, so a top-k boundary often falls inside a group of identical
        # vectors, and a recall figure published without this reads as index error.
        block = frame["vector"].to_numpy()
        facts["distinct_vectors"] = int(len(np.unique(block, axis=0)))
        facts["rows_sharing_a_vector_with_another_row"] = int(
            frame.height - facts["distinct_vectors"]
        )
        print(f"  distinct vectors {facts['distinct_vectors']}, "
              f"duplicate rows {facts['rows_sharing_a_vector_with_another_row']}")

        print("loading")
        buf = vectorstore.copy_buffer(frame)
        cur.copy_from(buf, vectorstore.TABLE, sep="\t", null="\\N",
                      columns=vectorstore.copy_columns())
        cur.execute(f"select count(*) from {vectorstore.quote_identifier(vectorstore.TABLE)}")
        facts["rows_loaded"] = cur.fetchone()[0]
        if facts["rows_loaded"] != frame.height:
            raise SystemExit(f"loaded {facts['rows_loaded']} of {frame.height} rows")
        print(f"  {facts['rows_loaded']} rows")

        print("building the index")
        facts["ivfflat_lists"] = vectorstore.DEFAULT_LISTS
        facts["ivfflat_probes"] = vectorstore.DEFAULT_PROBES
        cur.execute(vectorstore.index_sql(vectorstore.DEFAULT_LISTS))

        # The index's own name, read out of the catalogue. A plan does not print the
        # access method, so looking for the word ivfflat in it finds nothing even when
        # the index is driving the query, and looking for "Index Scan" finds any index
        # including the ordinary one on the filter column. Both of those answer a
        # different question from the one being asked. The name is unambiguous.
        cur.execute(
            "select indexname from pg_indexes where tablename = %s and indexdef ilike %s",
            (vectorstore.TABLE, "%ivfflat%"),
        )
        found = cur.fetchone()
        if not found:
            raise SystemExit("no ivfflat index on the table, so there is nothing to measure")
        facts["vector_index_name"] = found[0]

        def ivfflat_in_plan(sql, args) -> bool:
            """Whether the planner reaches the vector index for this exact query.

            An index that reports as created is a declaration. Asking the planner is
            a different question, and it has to be asked per query shape rather than
            once. A selective metadata predicate gives the planner a cheaper route
            through an ordinary index on that column, and it takes it. The result is
            then the exact answer rather than an approximate one, so a recall figure
            measured on that shape would be a figure about a plan that never used the
            vector index at all.
            """
            cur.execute("explain (format json) " + sql, args)
            return facts["vector_index_name"] in json.dumps(cur.fetchone()[0])

        # The queries are rows of the table, chosen by position in EVENT_ID order so
        # the selection is a function of the data and not of a random draw.
        ordered = frame.sort("EVENT_ID")
        stride = max(1, ordered.height // QUERIES)
        chosen = ordered[:: stride][:QUERIES]

        measurements = {"unfiltered": [], "event_type": [], "event_type_and_state": []}
        shortfalls = {"unfiltered": 0, "event_type": 0, "event_type_and_state": 0}
        # Per shape, how many of its queries actually planned through the vector
        # index. This is what tells a reader whether the recall below is about the
        # index or about a plan that bypassed it.
        used_index = {"unfiltered": 0, "event_type": 0, "event_type_and_state": 0}
        filtered_rows = {"unfiltered": [], "event_type": [], "event_type_and_state": []}
        # Queries whose two results sat at identical distances. Those differ by a tie
        # broken two ways rather than by the index missing anything.
        tie_only = {"unfiltered": 0, "event_type": 0, "event_type_and_state": 0}
        for row in chosen.iter_rows(named=True):
            qv = "[" + ",".join(repr(round(float(v), 6)) for v in row["vector"]) + "]"
            shapes = {
                "unfiltered": ([], {}),
                "event_type": (["event_type"], {"event_type": row["event_type"]}),
                "event_type_and_state": (
                    ["event_type", "state"],
                    {"event_type": row["event_type"], "state": row["state"]},
                ),
            }
            for name, (filters, params) in shapes.items():
                where = vectorstore.compose_where(filters)
                sql = vectorstore.search_sql(where, "%(k)s")
                args = dict(params, q=qv, k=K)

                # Exact first. Sequential scan on, index scan off, so the order by
                # sorts the whole filtered set and the answer is the definition.
                cur.execute("set enable_seqscan = on")
                cur.execute("set enable_indexscan = off")
                cur.execute("set enable_bitmapscan = off")
                cur.execute(sql, args)
                exact_rows = cur.fetchall()
                exact = [r[0] for r in exact_rows]

                # How many rows the predicate leaves. A recall figure over a
                # filtered set smaller than k is a different measurement and this
                # is what identifies those.
                count_sql = (
                    f"select count(*) from {vectorstore.quote_identifier(vectorstore.TABLE)}"
                    + (f" where {where}" if where else "")
                )
                cur.execute(count_sql, args)
                filtered_rows[name].append(cur.fetchone()[0])

                cur.execute("set enable_indexscan = on")
                cur.execute("set enable_bitmapscan = on")
                cur.execute("set enable_seqscan = off")
                cur.execute(f"set ivfflat.probes = {vectorstore.DEFAULT_PROBES}")
                if ivfflat_in_plan(sql, args):
                    used_index[name] += 1
                cur.execute(sql, args)
                approx_rows = cur.fetchall()
                approximate = [r[0] for r in approx_rows]

                if not exact:
                    continue
                if len(approximate) < len(exact):
                    shortfalls[name] += 1
                measurements[name].append(vectorstore.recall(exact, approximate))
                if vectorstore.tie_equivalent([r[1] for r in exact_rows],
                                              [r[1] for r in approx_rows]):
                    tie_only[name] += 1

        probe = chosen.row(0, named=True)
        probe_vector = "[" + ",".join(repr(round(float(v), 6)) for v in probe["vector"]) + "]"
        cur.execute("explain " + vectorstore.search_sql(None, "%(k)s"),
                    {"q": probe_vector, "k": K})
        facts["unfiltered_plan"] = [r[0].strip() for r in cur.fetchall()]

        # An ivfflat index partitions by k-means from a random start, so a second
        # build over the same rows gives a different index and a different recall.
        # The figure below is one build. The column it reads is deterministic and the
        # index over it is not, and those are two separate claims.
        facts["index_build_is_randomised"] = True
        facts["recall_queries"] = len(measurements["unfiltered"])
        facts["recall_k"] = K
        for name, values in measurements.items():
            facts[f"recall_{name}"] = round(sum(values) / len(values), 4) if values else None
            facts[f"queries_returning_fewer_than_k_{name}"] = shortfalls[name]
            facts[f"queries_planned_through_the_vector_index_{name}"] = used_index[name]
            facts[f"queries_differing_only_by_a_distance_tie_{name}"] = tie_only[name]
            rows = filtered_rows[name]
            facts[f"median_rows_matching_the_filter_{name}"] = (
                sorted(rows)[len(rows) // 2] if rows else None
            )
            print(f"  recall {name}: {facts[f'recall_{name}']}, "
                  f"vector index used by {used_index[name]} of {len(values)} queries, "
                  f"tie only {tie_only[name]}, "
                  f"median filtered rows {facts[f'median_rows_matching_the_filter_{name}']}")

        cur.close()
        conn.close()
    finally:
        if started:
            subprocess.run(as_user + [str(bins / "pg_ctl"), "-D", str(cluster), "-w", "stop"],
                           capture_output=True, text=True)
        shutil.rmtree(data_dir, ignore_errors=True)
        shutil.rmtree(sock, ignore_errors=True)

    manifest.write_text(json.dumps(facts, indent=2, sort_keys=True) + "\n")
    print(f"wrote {manifest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
