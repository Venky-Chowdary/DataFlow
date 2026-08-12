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
from typing import Any, Iterator

from connectors.base import ReadBatch

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
            if columns:
                col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
                select = sql.SQL("SELECT {cols} FROM {sch}.{tbl}").format(
                    cols=col_sql,
                    sch=sql.Identifier(schema),
                    tbl=sql.Identifier(table),
                )
            else:
                select = sql.SQL("SELECT * FROM {sch}.{tbl}").format(
                    sch=sql.Identifier(schema),
                    tbl=sql.Identifier(table),
                )
            copy_sql = sql.SQL(
                "COPY ({select}) TO STDOUT WITH (FORMAT csv, HEADER true, NULL '\\N')"
            ).format(select=select)

            buf = io.StringIO()
            cur.copy_expert(copy_sql.as_string(cur), buf)
            buf.seek(0)
            reader = csv.reader(buf)
            try:
                headers = next(reader)
            except StopIteration:
                yield ReadBatch(headers=columns or [], rows=[], offset=0, total_rows=0)
                return

            page: list[list[str]] = []
            offset = 0
            total = 0
            for row in reader:
                page.append([_copy_cell(c) for c in row])
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


def _copy_cell(raw: str) -> str:
    """Map a COPY CSV field to the transfer wire string.

    The keyset/OFFSET readers emit ``SQL_NULL_SENTINEL`` for SQL NULL via
    ``cell_to_string(preserve_sql_null=True)``, so returning ``""`` here made
    enabling ``DATAFLOW_BULK_EXPORT`` quietly change what a NULL means: the
    destination saw an empty string, wrote one into a nullable column, and the
    checksum diverged from a keyset read of the same table. COPY CSV writes the
    unquoted token ``\\N`` for NULL and ``""`` for an empty string, so the two
    stay distinguishable here.
    """
    from services.value_serializer import SQL_NULL_SENTINEL

    if raw == r"\N":
        return SQL_NULL_SENTINEL
    return raw


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
