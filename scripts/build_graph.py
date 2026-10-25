"""Load the release into Neo4j, ask the question, and ask it again over the Parquet.

One process, same reason as the vector build. The server does not outlive the shell
that started it. So this script owns the whole lifecycle. It starts the server and
loads the release. Then it queries and compares and stops the server.

Run it from the repository root with a Neo4j installation on hand:

    STORMFACTORY_NEO4J_HOME=/path/to/neo4j-community python3 scripts/build_graph.py

It needs the server distribution and a Java runtime. If either is missing it says
which and exits non-zero. It does not fall back to answering the question over the
Parquet alone and calling that a cross-check, because a cross-check with one side
missing is one implementation wearing the word.

The database it writes to is its own. A measurement script that loads into a server
somebody else is reading leaves the next reader with a figure about the run count.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# At module level so the dependency check can see it. A deferred import keeps a
# missing driver from breaking an unrelated path and it also hides the dependency,
# and the refusal in main does the first job without the second.
try:
    from neo4j import GraphDatabase
except ImportError:
    GraphDatabase = None

from stormfactory import graph

BOLT_PORT = os.environ.get("STORMFACTORY_BOLT_PORT", "57687")
BATCH = 5000


def _home() -> Path:
    stated = os.environ.get("STORMFACTORY_NEO4J_HOME")
    if stated:
        home = Path(stated)
        if not (home / "bin" / "neo4j").exists():
            raise SystemExit(f"no Neo4j server under {home}")
        return home
    raise SystemExit(
        "set STORMFACTORY_NEO4J_HOME to a Neo4j community installation. This script "
        "does not download one, because a build that reaches the network for a server "
        "binary is a build that fails differently on a machine with no network"
    )


def _configure(home: Path, data_root: Path) -> None:
    """A throwaway configuration for a throwaway instance.

    Written as a whole file rather than appended, because a setting declared twice is
    a validation error and appending to a shipped configuration is how that happens.
    """
    conf = home / "conf" / "neo4j.conf"
    keep = []
    mine = {
        "dbms.security.auth_enabled",
        "server.bolt.listen_address",
        "server.http.enabled",
        "server.https.enabled",
        "server.directories.data",
        "server.directories.logs",
        "server.memory.heap.initial_size",
        "server.memory.heap.max_size",
        "server.memory.pagecache.size",
    }
    for line in conf.read_text().splitlines():
        if line.split("=")[0].strip() in mine:
            continue
        keep.append(line)
    keep += [
        "dbms.security.auth_enabled=false",
        f"server.bolt.listen_address=:{BOLT_PORT}",
        "server.http.enabled=false",
        "server.https.enabled=false",
        f"server.directories.data={data_root / 'data'}",
        f"server.directories.logs={data_root / 'logs'}",
        "server.memory.heap.initial_size=1g",
        "server.memory.heap.max_size=1g",
        "server.memory.pagecache.size=512m",
    ]
    conf.write_text("\n".join(keep) + "\n")


def main() -> int:
    home = _home()
    if GraphDatabase is None:
        raise SystemExit("the neo4j driver is not installed, so the load cannot run")

    print("reading the release")
    built = graph.payload()
    counts = dict(built["counts"])
    print(f"  scope year {counts['scope_year']}, {counts['events']} events, "
          f"{counts['event_county_edges']} event county edges, {counts['tiles']} tiles")

    data_root = Path(tempfile.mkdtemp(prefix="stormfactory-neo4j-"))
    env = dict(os.environ)
    env.setdefault("JAVA_HOME", "/usr/lib/jvm/default-java")
    # The runtime prints its proxy options to stderr on every invocation and they are
    # not interesting here.
    env["JAVA_TOOL_OPTIONS"] = ""
    started = False
    try:
        _configure(home, data_root)
        print("starting the server")
        start = subprocess.run([str(home / "bin" / "neo4j"), "start"],
                               capture_output=True, text=True, env=env)
        if start.returncode != 0:
            raise SystemExit(f"the server did not start: {start.stdout}\n{start.stderr}")
        started = True

        uri = f"bolt://localhost:{BOLT_PORT}"
        driver = None
        for _ in range(60):
            try:
                driver = GraphDatabase.driver(uri)
                driver.verify_connectivity()
                break
            except Exception:
                if driver is not None:
                    driver.close()
                driver = None
                time.sleep(2)
        if driver is None:
            raise SystemExit(f"the server never accepted a connection on {uri}")

        with driver.session() as session:
            version = session.run(
                "call dbms.components() yield name, versions, edition "
                "return name + ' ' + versions[0] + ' ' + edition as v"
            ).single()["v"]
            counts["neo4j_version"] = version
            print(f"  {version}")

            for statement in graph.CONSTRAINTS:
                session.run(statement)

            for name in ("events", "counties", "event_counties", "tiles", "labels", "declarations"):
                rows = built["batches"][name]
                statement = graph.LOADERS[name]
                for at in range(0, len(rows), BATCH):
                    session.run(statement, rows=rows[at : at + BATCH])
                print(f"  loaded {name}: {len(rows)}")

            # What the server holds, read back rather than assumed from the batches.
            loaded = session.run(
                "match (n) unwind labels(n) as label "
                "return label, count(*) as n order by label"
            ).data()
            counts["nodes_in_graph"] = {r["label"]: r["n"] for r in loaded}
            edges = session.run(
                "match ()-[r]->() return type(r) as type, count(*) as n order by type"
            ).data()
            counts["edges_in_graph"] = {r["type"]: r["n"] for r in edges}
            print(f"  nodes {counts['nodes_in_graph']}")
            print(f"  edges {counts['edges_in_graph']}")

            print("asking the question in Cypher")
            graph_rows = session.run(
                graph.QUESTION_CYPHER, labels=list(graph.LABELS_IN_SCOPE)
            ).data()

        driver.close()
    finally:
        if started:
            subprocess.run([str(home / "bin" / "neo4j"), "stop"],
                           capture_output=True, text=True, env=env)
        shutil.rmtree(data_root, ignore_errors=True)

    print("asking the same question over the Parquet")
    sql = graph.sql_answer()
    verdict = graph.compare(graph_rows, sql)
    print(f"  graph paths {verdict['graph_paths']}, parquet paths {verdict['parquet_paths']}")
    print(f"  distinct declarations {verdict['graph_distinct_declarations']}, "
          f"paths per declaration {verdict['paths_per_declaration']}")
    print(f"  agree: {verdict['agree']}")
    if verdict["problems"]:
        for problem in verdict["problems"]:
            print(f"  PROBLEM {problem}")

    payload = dict(counts)
    payload["cross_check"] = verdict
    graph.write_manifest(payload)
    print(f"wrote {graph.manifest_path()}")
    return 0 if verdict["agree"] else 1


if __name__ == "__main__":
    sys.exit(main())
