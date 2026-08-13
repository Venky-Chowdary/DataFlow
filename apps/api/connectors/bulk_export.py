"""Bulk source export readers (Phase F3).

Prefer server-side unload / COPY over OFFSET page scans for warehouse-scale
sources. Today:

* **PostgreSQL** — ``COPY (SELECT …) TO STDOUT WITH (FORMAT csv, HEADER)``
  (implemented, bit-exact CSV round-trip into ``ReadBatch`` pages).
* **Snowflake** — stage ``COPY INTO`` + GET (capability declared; requires
  ``DATAFLOW_BULK_EXPORT=force`` + credentials — otherwise keyset/OFFSET).
* **BigQuery** — Storage Read API (capability declared; same gate).

Fail closed: never silently fall through from a forced bulk path to OFFSET
without recording ``bulk_export_fallback`` in the transfer summary.
"""

from __future__ import annotations

import csv
import io
import logging
import tempfile
from typing import Any, Iterator

from connectors.base import ReadBatch

#: COPY output stays in memory up to this size, then spills to disk. Matches the
#: bound the PostgreSQL COPY writer applies to its own buffer.
_COPY_SPILL_BYTES = 1 * 1024 * 1024

logger = logging.getLogger(__name__)

# Engines with a real or planned bulk path.
BULK_EXPORT_ENGINES = frozenset({"postgresql", "redshift", "snowflake", "bigquery"})


def bulk_export_supported(engine: str) -> bool:
    return (engine or "").lower().strip() in BULK_EXPORT_ENGINES


def bulk_export_implemented(engine: str) -> bool:
    """True when a production bulk reader exists (not just a stub)."""
    # Redshift UNLOAD-to-S3 is a separate path — client COPY TO STDOUT is PG-only.
    return (engine or "").lower().strip() == "postgresql"


def bulk_export_enabled() -> bool:
    """Operator gate — default **off** until F6 publishes measured numbers.

    Set ``DATAFLOW_BULK_EXPORT=1`` (or ``force``) to use PostgreSQL COPY.
    ``auto`` enables only when ``bulk_export_implemented`` (same as ``1`` today).
    """
    from services.brand_env import getenv_brand

    raw = (getenv_brand("BULK_EXPORT", "0") or "0").strip().lower()
    if raw in ("0", "false", "no", "off", ""):
        return False
    if raw in ("1", "true", "yes", "on", "force", "auto"):
        return True
    return False


def bulk_export_forced() -> bool:
    from services.brand_env import getenv_brand

    return (getenv_brand("BULK_EXPORT", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
        "force",
    )


def iter_postgresql_copy_batches(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table: str,
    columns: list[str] | None = None,
    batch_rows: int = 10_000,
) -> Iterator[ReadBatch]:
    """Stream a PostgreSQL table via COPY TO STDOUT, yielding ``ReadBatch`` pages.

    Uses identifier-safe SQL composition. CSV NULL is the empty field with
    ``FORCE_NULL`` semantics avoided — we preserve empty string vs SQL NULL via
    PostgreSQL COPY's default ``\\N`` null marker.
    """
    from psycopg2 import sql

    from connectors.postgresql_conn import get_connection

    schema = schema or "public"
    if batch_rows < 1:
        raise ValueError("batch_rows must be >= 1")

    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    try:
        with conn.cursor() as cur:
            if not columns:
                # COPY leaves cur.description empty, and TEXT format carries no
                # header line, so the column names have to be settled before the
                # copy runs or every batch ships headerless and the mapping and
                # checksum downstream line up against nothing. A zero-row SELECT
                # names them in the same order COPY will emit.
                cur.execute(
                    sql.SQL("SELECT * FROM {sch}.{tbl} LIMIT 0").format(
                        sch=sql.Identifier(schema), tbl=sql.Identifier(table)
                    )
                )
                columns = [str(d[0]) for d in (cur.description or [])]

            col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
            select = sql.SQL("SELECT {cols} FROM {sch}.{tbl}").format(
                cols=col_sql,
                sch=sql.Identifier(schema),
                tbl=sql.Identifier(table),
            )
            # TEXT rather than CSV. In CSV, PostgreSQL writes SQL NULL as a bare
            # \N and a literal "\N" string quoted — but csv.reader strips the
            # quoting, so both arrive here as the same characters and a real \N
            # value would be read as NULL. TEXT escapes instead of quoting, so a
            # literal backslash comes through doubled and the null marker is
            # unambiguous. It is also the exact inverse of the writer's
            # _copy_text_value, which keeps one escaping contract for both
            # directions.
            copy_sql = sql.SQL(
                "COPY ({select}) TO STDOUT WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
            ).format(select=select)

            # psycopg2's copy_expert writes the whole result into the file object
            # before returning, so this cannot page the wire. A StringIO made
            # that the worker's heap: the point of bulk export is warehouse-scale
            # tables, and the first page used to arrive only after the last row
            # was already resident. Spool to disk past a threshold instead —
            # the same bound the COPY writer uses on the way in.
            # Binary spool: copy_expert writes the raw wire bytes, so the text
            # decode belongs on the read side.
            with tempfile.SpooledTemporaryFile(max_size=_COPY_SPILL_BYTES) as raw:
                cur.copy_expert(copy_sql.as_string(cur), raw)
                raw.seek(0)
                buf = io.TextIOWrapper(raw, encoding="utf-8", newline="\n")
                # The SELECT above fixed both the names and their order.
                headers = list(columns)

                page: list[list[str]] = []
                offset = 0
                total = 0
                for line in buf:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    page.append([_copy_cell(c) for c in line.split("\t")])
                    total += 1
                    if len(page) >= batch_rows:
                        yield ReadBatch(
                            headers=list(headers),
                            rows=page,
                            offset=offset,
                            total_rows=None,
                        )
                        offset += len(page)
                        page = []
                if page or offset == 0:
                    yield ReadBatch(
                        headers=list(headers),
                        rows=page,
                        offset=offset,
                        total_rows=total,
                    )
    finally:
        try:
            conn.close()
        except Exception:
            pass


#: COPY TEXT backslash escapes, longest first so ``\\`` is consumed before the
#: single-character forms and a literal backslash cannot start a false escape.
_COPY_TEXT_UNESCAPES = (
    ("\\\\", "\\"),
    ("\\t", "\t"),
    ("\\n", "\n"),
    ("\\r", "\r"),
    ("\\b", "\b"),
    ("\\f", "\f"),
    ("\\v", "\v"),
)


def _copy_cell(raw: str) -> str:
    """Map one COPY TEXT field to the transfer wire string.

    The keyset/OFFSET readers emit ``SQL_NULL_SENTINEL`` for SQL NULL via
    ``cell_to_string(preserve_sql_null=True)``, so returning ``""`` here made
    enabling ``DATAFLOW_BULK_EXPORT`` quietly change what a NULL means: the
    destination wrote an empty string into a nullable column and the checksum
    diverged from a keyset read of the same table.

    An unescaped ``\\N`` is that NULL. A column holding the literal characters
    ``\\N`` arrives doubled as ``\\\\N``, which is why this reads TEXT rather
    than CSV — CSV distinguishes the two by quoting, and ``csv.reader`` strips
    quoting before anything here could tell them apart.
    """
    from services.value_serializer import SQL_NULL_SENTINEL

    if raw == "\\N":
        return SQL_NULL_SENTINEL
    if "\\" not in raw:
        return raw
    out: list[str] = []
    i = 0
    while i < len(raw):
        pair = raw[i : i + 2]
        for token, plain in _COPY_TEXT_UNESCAPES:
            if pair == token:
                out.append(plain)
                i += 2
                break
        else:
            out.append(raw[i])
            i += 1
    return "".join(out)


def read_snowflake_unload_batch(**_kwargs: Any) -> ReadBatch:
    """Snowflake stage unload — not yet production-wired (Phase F3 remaining)."""
    raise NotImplementedError(
        "Snowflake bulk unload (COPY INTO stage + GET) is not enabled in this build. "
        "Unset DATAFLOW_BULK_EXPORT or use keyset/OFFSET pagination."
    )


def read_bigquery_storage_batch(**_kwargs: Any) -> ReadBatch:
    """BigQuery Storage Read API — not yet production-wired (Phase F3 remaining)."""
    raise NotImplementedError(
        "BigQuery Storage Read API bulk export is not enabled in this build. "
        "Unset DATAFLOW_BULK_EXPORT or use keyset/OFFSET pagination."
    )
