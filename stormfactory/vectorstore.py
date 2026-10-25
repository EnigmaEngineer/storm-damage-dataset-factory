"""The vector column in Postgres, and what its index actually returns.

Parquet holds the release. Postgres holds it in the shape a filtered search needs,
which is a vector column beside the columns a researcher filters on. The point of
putting it in a database at all is the combination. "Narratives that read like this,
restricted to hail in Texas in 2021" is one query here and two passes over a file
otherwise.

The part worth measuring is the index. An approximate index answers a different
question from the one the query asks, and it answers it without saying so. A query
against an `ivfflat` index returns the nearest neighbours it found in the lists it
probed, which is not the same set as the nearest neighbours in the table. The
difference has a name and a number, and the number is recall against exact search.

Exact search is the definition here rather than a second opinion. Ordering every row
in the filtered set by distance is what the query means. So recall needs no relevance
labels and no human judgement, which is why it is the one retrieval figure this
repository publishes. It is a property of the index and the probe count. It says
nothing about whether the vectors are any good.

The filtered case is the one to look at. `ivfflat` picks its lists by distance to the
query and the metadata predicate is applied to what those lists held. A selective
filter and a low probe count can leave the index returning fewer rows than the query
asked for, with no error and no warning. That is measured here rather than assumed
either way.

The whole lifecycle fits in one process on purpose. The server here does not outlive
the shell that started it. A script brings it up, measures and stops. There is no long
lived instance for a measurement to depend on, and a measurement that writes into a
database somebody else is reading is its own defect.
"""

from __future__ import annotations

import io

import polars as pl

from . import vectors

TABLE = "narrative_vectors"

# Postgres column types for the filter columns, keyed by the artifact's column name.
# The artifact declares which columns are filterable and this says how each one lands.
# A column in one and not the other is a defect a check reports rather than a surprise
# at load time.
FILTER_TYPES = {
    "event_type": "text",
    "event_date": "date",
    "state_fips": "bigint",
    "state": "text",
}

# `lists` is the number of inverted lists the index partitions the vectors into. The
# usual starting point is rows over 1000 for a table this size. `probes` is how many
# of those lists a query looks at, and it is a query time setting rather than a
# property of the index, which is the whole reason recall can be tuned after the fact.
DEFAULT_LISTS = 272
DEFAULT_PROBES = 10


class StoreError(Exception):
    pass


def quote_identifier(name: str) -> str:
    """A Postgres identifier, quoted.

    An identifier cannot be a bound parameter, so every table and column name that
    reaches a statement goes through here. A name carrying a null byte is refused
    rather than escaped, because there is no correct way to spell it.
    """
    if "\x00" in name:
        raise StoreError("an identifier cannot hold a null byte")
    return '"' + name.replace('"', '""') + '"'


def create_sql(dimension: int = vectors.DIMENSION) -> str:
    missing = [c for c in vectors.FILTER_COLUMNS if c not in FILTER_TYPES]
    if missing:
        raise StoreError(f"the artifact filters on {missing} and this module has no type for them")
    columns = [
        f"{quote_identifier('event_id')} bigint primary key",
        f"{quote_identifier('narrative_chars')} bigint not null",
        f"{quote_identifier('vector')} vector({dimension}) not null",
    ]
    columns += [
        f"{quote_identifier(name)} {FILTER_TYPES[name]}" for name in vectors.FILTER_COLUMNS
    ]
    return (
        "create extension if not exists vector;\n"
        f"drop table if exists {quote_identifier(TABLE)};\n"
        f"create table {quote_identifier(TABLE)} (\n  " + ",\n  ".join(columns) + "\n);"
    )


def index_sql(lists: int = DEFAULT_LISTS) -> str:
    """The approximate index, plus a plain index on the most selective filter.

    Cosine distance, because the vectors are not length normalised by the reduction
    and the question asked of a narrative is about direction rather than magnitude.
    """
    return (
        f"create index on {quote_identifier(TABLE)} "
        f"using ivfflat ({quote_identifier('vector')} vector_cosine_ops) "
        f"with (lists = {int(lists)});\n"
        f"create index on {quote_identifier(TABLE)} ({quote_identifier('event_type')});"
    )


def copy_buffer(frame: pl.DataFrame) -> io.StringIO:
    """The artifact as a tab separated COPY payload.

    `executemany` over a quarter of a million rows of 256 floats does not finish in a
    sensible time. COPY does. The vector goes out in the bracketed form pgvector
    parses, and every other column goes out as text with a literal backslash N for a
    null, which is what COPY reads as one.
    """
    ordered = frame.sort("EVENT_ID")
    out = io.StringIO()
    for row in ordered.iter_rows(named=True):
        fields = [
            str(row["EVENT_ID"]),
            str(row["narrative_chars"]),
            "[" + ",".join(repr(round(float(v), 6)) for v in row["vector"]) + "]",
        ]
        for name in vectors.FILTER_COLUMNS:
            value = row[name]
            if value is None:
                fields.append("\\N")
            else:
                text = str(value)
                if "\t" in text or "\n" in text or "\\" in text:
                    raise StoreError(f"{name} holds a tab, a newline or a backslash and COPY cannot carry it unescaped")
                fields.append(text)
        out.write("\t".join(fields) + "\n")
    out.seek(0)
    return out


def copy_columns() -> list[str]:
    return ["event_id", "narrative_chars", "vector"] + list(vectors.FILTER_COLUMNS)


def search_sql(where: str | None, limit_param: str = "%(k)s") -> str:
    """Nearest neighbours by cosine distance, with an optional metadata predicate.

    The predicate is composed by the caller from a fixed set of fragments in this
    module rather than from a string a caller supplies, which is what keeps this off
    the obvious injection path. The query vector and the row count are bound.
    """
    clause = f"where {where}\n" if where else ""
    return (
        f"select {quote_identifier('event_id')}, "
        f"{quote_identifier('vector')} <=> %(q)s::vector as distance\n"
        f"from {quote_identifier(TABLE)}\n"
        f"{clause}"
        f"order by {quote_identifier('vector')} <=> %(q)s::vector\n"
        f"limit {limit_param}"
    )


# The filter fragments a search may use. Named and bound, so a caller picks a shape
# and supplies values, and nothing a caller passes becomes SQL.
FILTERS = {
    "event_type": f"{quote_identifier('event_type')} = %(event_type)s",
    "state": f"{quote_identifier('state')} = %(state)s",
    "date_range": f"{quote_identifier('event_date')} between %(date_from)s and %(date_to)s",
}


def compose_where(names: list[str]) -> str | None:
    unknown = [n for n in names if n not in FILTERS]
    if unknown:
        raise StoreError(f"no filter fragment named {unknown}")
    if not names:
        return None
    return " and ".join(FILTERS[n] for n in sorted(names))


def tie_equivalent(exact: list[float], approximate: list[float], tolerance: float = 1e-9) -> bool:
    """Whether two results differ only in which of several equidistant rows they took.

    Recall below one has two possible causes and they mean opposite things. The index
    may have missed a nearer row, which is approximation. Or both sides may have
    returned rows at exactly the same distances and broken a tie differently, which is
    not approximation at all and would happen between two exact searches.

    Narratives repeat verbatim across the events of one episode, so identical vectors
    are ordinary here and the k-th distance is often shared by more rows than the
    query asked for. A recall figure that does not separate the two is reporting the
    duplicate rate as if it were index error.
    """
    if len(exact) != len(approximate):
        return False
    return all(
        abs(a - b) <= tolerance for a, b in zip(sorted(exact), sorted(approximate))
    )


def recall(exact: list[int], approximate: list[int]) -> float:
    """Recall of an approximate result against the exact one, over the same k.

    Both sides are the identifiers of the rows returned. Order inside the result is
    not graded, because the query asks for a set of neighbours and a reader sorting
    them again by distance gets the same answer either way.
    """
    if not exact:
        raise StoreError("an empty exact result has no recall to report")
    hit = len(set(exact) & set(approximate))
    return hit / len(exact)
