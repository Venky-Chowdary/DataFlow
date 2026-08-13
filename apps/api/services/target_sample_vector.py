"""Reading a vector destination back as the rows the operator mapped.

Split out of ``services.target_sample`` (a dispatch at its size budget). A
vector table is a fixed schema — id / content / embedding / metadata — so the
mapped source columns are payload inside the ``metadata`` JSONB rather than
columns to select. Flattening them back out is what lets Gate-8 compare per
cell on a vector destination instead of trusting the writer's own count.
"""

from __future__ import annotations

import json
from typing import Any

from services.keyed_read import execute_keyed_read


def read_pgvector_target_sample(
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str] | None,
    keys: list[Any] | None,
    sort_key: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Read a vector table back as the rows the operator mapped.

    The table is a fixed vector schema, so the mapped source columns are payload
    inside the ``metadata`` JSONB rather than columns to select. Flattening it
    back out is what lets Gate-8 compare per cell on a vector destination.
    """
    from connectors.postgresql_conn import get_connection
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        quote_table_ref,
        require_safe_identifier,
    )

    table_ref = quote_table_ref(table_name, schema or "public", dialect="postgresql")
    select = "SELECT id, content, source_id, chunk_index, metadata "
    conn = get_connection(
        host=dest.get("host", ""),
        port=dest.get("port", 5432),
        database=dest.get("database", ""),
        username=dest.get("username", ""),
        password=dest.get("password", ""),
        connection_string=dest.get("connection_string", ""),
        ssl=bool(dest.get("ssl", False)),
    )
    try:
        with conn.cursor() as cur:
            if keys and sort_key:
                key_col = quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                placeholders = ",".join(["%s"] * len(keys))
                # A vector table's id is text while the source key is an integer,
                # and PostgreSQL refuses `text = integer` rather than coercing.
                execute_keyed_read(
                    conn,
                    cur,
                    f"{select}FROM {table_ref} "  # nosec B608
                    f"WHERE {{key}} IN ({placeholders}) LIMIT %s",
                    key_col,
                    keys,
                    (int(limit or 50),),
                )
            else:
                cur.execute(
                    f"{select}FROM {table_ref} LIMIT %s",  # nosec B608
                    (int(limit or 50),),
                )
            names = [d[0] for d in cur.description] if cur.description else []
            out_rows = []
            for raw in cur.fetchall():
                rec = dict(zip(names, raw))
                meta = rec.get("metadata") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}
                if not isinstance(meta, dict):
                    meta = {}
                row = {
                    "id": rec.get("id"),
                    "content": rec.get("content"),
                    "source_id": rec.get("source_id"),
                    "chunk_index": rec.get("chunk_index"),
                    **meta,
                }
                if cols and cols != ["*"]:
                    row = {k: row.get(k) for k in cols}
                out_rows.append(row)
            return out_rows
    finally:
        conn.close()


