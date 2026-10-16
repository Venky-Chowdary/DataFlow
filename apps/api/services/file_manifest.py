"""Processed-file manifest that commits with the load.

The table lives on the destination database. The claim row and the loaded
rows share one transaction: a crash rolls both back, so the next run neither
skips the file nor inserts it a second time. A committed ``loaded`` row with
the same sha256 is the idempotent re-run.

The claim key is connector, path, sha256, destination table, and schedule.
Two schedules that land the same file in different tables each load it.
A legacy table keyed only on (connector, path, sha256) is upgraded in place;
its rows keep an empty destination and schedule so the original claim still
matches, and they do not suppress a different table.
"""

from __future__ import annotations

import re
from typing import Any

_TABLE = "dataflow_file_manifest"
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COLUMNS = (
    "connector",
    "path",
    "size",
    "mtime",
    "sha256",
    "status",
    "job_id",
    "rows",
    "dest_table",
    "schedule_id",
)


def _schema(schema: str) -> str:
    text = (schema or "public").strip() or "public"
    if not _IDENT.fullmatch(text):
        raise ValueError(f"Refusing manifest schema {schema!r}")
    return text


def _qualified(dialect: str, schema: str) -> str:
    if dialect == "sqlite":
        return _TABLE
    return f'"{_schema(schema)}"."{_TABLE}"'


def _scope(dest_table: str = "", schedule_id: str = "") -> tuple[str, str]:
    return str(dest_table or ""), str(schedule_id or "")


def ensure_sql(dialect: str, schema: str = "public") -> str:
    table = _qualified(dialect, schema)
    size_type = "INTEGER" if dialect == "sqlite" else "BIGINT"
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "connector TEXT NOT NULL, "
        "path TEXT NOT NULL, "
        f"size {size_type} NOT NULL, "
        "mtime TEXT NOT NULL, "
        "sha256 TEXT NOT NULL, "
        "status TEXT NOT NULL, "
        "job_id TEXT NOT NULL, "
        f"rows {size_type} NOT NULL DEFAULT 0, "
        "dest_table TEXT NOT NULL DEFAULT '', "
        "schedule_id TEXT NOT NULL DEFAULT '', "
        "PRIMARY KEY (connector, path, sha256, dest_table, schedule_id))"
    )


def _placeholder(dialect: str) -> str:
    return "?" if dialect == "sqlite" else "%s"


def _pk_covers_scope(definition: str) -> bool:
    text = (definition or "").lower()
    return "dest_table" in text and "schedule_id" in text


def _upgrade_sqlite(cur: Any) -> None:
    row = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (_TABLE,),
    ).fetchone()
    sql = str(row[0] if row else "")
    upper = sql.upper()
    pk_at = upper.rfind("PRIMARY KEY")
    pk_clause = upper[pk_at:] if pk_at >= 0 else ""
    if _pk_covers_scope(pk_clause):
        return
    legacy = f"{_TABLE}_legacy"
    cur.execute(f"ALTER TABLE {_TABLE} RENAME TO {legacy}")
    cur.execute(ensure_sql("sqlite"))
    info = cur.execute(f"PRAGMA table_info({legacy})").fetchall()
    present = {str(col[1]) for col in info}
    defaults = {
        "size": "0",
        "mtime": "''",
        "status": "''",
        "job_id": "''",
        "rows": "0",
        "dest_table": "''",
        "schedule_id": "''",
    }
    select_exprs = [
        name if name in present else defaults.get(name, "''") for name in _COLUMNS
    ]
    cur.execute(
        f"INSERT INTO {_TABLE} "
        f"({', '.join(_COLUMNS)}) "
        f"SELECT {', '.join(select_exprs)} FROM {legacy}"
    )
    cur.execute(f"DROP TABLE {legacy}")


def _upgrade_postgres(cur: Any, table: str, schema: str) -> None:
    cur.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS dest_table TEXT NOT NULL DEFAULT ''"
    )
    cur.execute(
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS schedule_id TEXT NOT NULL DEFAULT ''"
    )
    cur.execute(
        "SELECT c.conname, pg_get_constraintdef(c.oid) "
        "FROM pg_constraint c "
        "JOIN pg_class rel ON rel.oid = c.conrelid "
        "JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace "
        "WHERE rel.relname = %s AND nsp.nspname = %s AND c.contype = 'p'",
        (_TABLE, schema),
    )
    found = cur.fetchone()
    name = str(found[0] if found else f"{_TABLE}_pkey")
    definition = str(found[1] if found else "")
    if _pk_covers_scope(definition):
        return
    cur.execute(f'ALTER TABLE {table} DROP CONSTRAINT "{name}"')
    cur.execute(
        f"ALTER TABLE {table} ADD PRIMARY KEY "
        "(connector, path, sha256, dest_table, schedule_id)"
    )


def ensure_manifest(conn: Any, *, dialect: str, schema: str = "public") -> None:
    """Create the manifest, or widen a legacy three-column key. Caller commits."""
    cur = conn.cursor()
    cur.execute(ensure_sql(dialect, schema))
    if dialect == "sqlite":
        _upgrade_sqlite(cur)
        return
    _upgrade_postgres(cur, _qualified(dialect, schema), _schema(schema))


def claim_file(
    conn: Any,
    *,
    dialect: str,
    schema: str = "public",
    connector: str,
    path: str,
    size: int,
    mtime: str,
    sha256: str,
    job_id: str,
    dest_table: str = "",
    schedule_id: str = "",
) -> str:
    """Insert a ``loading`` claim, or return ``skip`` when this sha256 loaded.

    Caller holds an open transaction. Nothing here commits. ``dest_table`` and
    ``schedule_id`` are part of the key: the same file loads again into a
    different table or schedule.
    """
    digest = (sha256 or "").strip().lower()
    if not digest:
        raise ValueError("A processed-file claim needs a sha256")
    table_name, schedule_name = _scope(dest_table, schedule_id)
    ph = _placeholder(dialect)
    table = _qualified(dialect, schema)
    cur = conn.cursor()
    ensure_manifest(conn, dialect=dialect, schema=schema)
    values = (
        connector,
        path,
        int(size or 0),
        str(mtime or ""),
        digest,
        str(job_id or ""),
        table_name,
        schedule_name,
    )
    if dialect == "sqlite":
        cur.execute(
            f"INSERT OR IGNORE INTO {table} "
            "(connector, path, size, mtime, sha256, status, job_id, rows, dest_table, schedule_id) "
            f"VALUES ({ph},{ph},{ph},{ph},{ph},'loading',{ph},0,{ph},{ph})",
            values,
        )
        inserted = cur.rowcount == 1
    else:
        cur.execute(
            f"INSERT INTO {table} "
            "(connector, path, size, mtime, sha256, status, job_id, rows, dest_table, schedule_id) "
            f"VALUES ({ph},{ph},{ph},{ph},{ph},'loading',{ph},0,{ph},{ph}) "
            "ON CONFLICT (connector, path, sha256, dest_table, schedule_id) DO NOTHING",
            values,
        )
        inserted = cur.rowcount == 1
    if inserted:
        return "load"
    where = (
        f"connector={ph} AND path={ph} AND sha256={ph} "
        f"AND dest_table={ph} AND schedule_id={ph}"
    )
    key = (connector, path, digest, table_name, schedule_name)
    cur.execute(f"SELECT status FROM {table} WHERE {where}", key)
    row = cur.fetchone()
    status = str(row[0] if row else "")
    if status == "loaded":
        return "skip"
    cur.execute(
        f"UPDATE {table} SET status='loading', job_id={ph}, size={ph}, mtime={ph} "
        f"WHERE {where}",
        (str(job_id or ""), int(size or 0), str(mtime or ""), *key),
    )
    return "load"


def complete_file(
    conn: Any,
    *,
    dialect: str,
    schema: str = "public",
    connector: str,
    path: str,
    sha256: str,
    rows: int,
    dest_table: str = "",
    schedule_id: str = "",
) -> None:
    """Mark the open claim loaded. Caller commits with the data."""
    table_name, schedule_name = _scope(dest_table, schedule_id)
    ph = _placeholder(dialect)
    table = _qualified(dialect, schema)
    cur = conn.cursor()
    cur.execute(
        f"UPDATE {table} SET status='loaded', rows={ph} "
        f"WHERE connector={ph} AND path={ph} AND sha256={ph} "
        f"AND dest_table={ph} AND schedule_id={ph}",
        (
            int(rows or 0),
            connector,
            path,
            (sha256 or "").strip().lower(),
            table_name,
            schedule_name,
        ),
    )


def forget_job(conn: Any, *, dialect: str, schema: str = "public", job_id: str) -> None:
    """Drop this job's claims. A failed-batch undo calls this in the same transaction
    as the row delete, so a cleared table cannot stay marked loaded.
    """
    job = str(job_id or "").strip()
    if not job:
        return
    ph = _placeholder(dialect)
    table = _qualified(dialect, schema)
    cur = conn.cursor()
    try:
        if dialect != "sqlite":
            cur.execute("SAVEPOINT df_manifest_forget")
        cur.execute(f"DELETE FROM {table} WHERE job_id={ph}", (job,))
        if dialect != "sqlite":
            cur.execute("RELEASE SAVEPOINT df_manifest_forget")
    except Exception:
        if dialect != "sqlite":
            try:
                cur.execute("ROLLBACK TO SAVEPOINT df_manifest_forget")
            except Exception:
                pass


def list_loaded(
    conn: Any,
    *,
    dialect: str,
    schema: str = "public",
    connector: str,
    dest_table: str = "",
    schedule_id: str = "",
) -> list[dict[str, Any]]:
    """Loaded files for one destination table and schedule.

    Missing table is an empty manifest. An empty scope matches only legacy
    rows that were claimed with no table and no schedule — it does not hide
    a file from a named table.
    """
    table_name, schedule_name = _scope(dest_table, schedule_id)
    ph = _placeholder(dialect)
    table = _qualified(dialect, schema)
    cur = conn.cursor()
    try:
        ensure_manifest(conn, dialect=dialect, schema=schema)
        cur.execute(
            f"SELECT path, size, mtime, sha256, status, job_id, rows FROM {table} "
            f"WHERE connector={ph} AND status='loaded' "
            f"AND dest_table={ph} AND schedule_id={ph}",
            (connector, table_name, schedule_name),
        )
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    out: list[dict[str, Any]] = []
    for path, size, mtime, sha256, status, job_id, rows in cur.fetchall():
        name = str(path or "")
        out.append(
            {
                "name": name,
                "path": name,
                "size": int(size or 0),
                "mtime": str(mtime or ""),
                "checksum": str(sha256 or ""),
                "sha256": str(sha256 or ""),
                "status": str(status or ""),
                "job_id": str(job_id or ""),
                "rows": int(rows or 0),
            }
        )
    return out


def read_loaded_manifest(
    dest_cfg: dict[str, Any],
    *,
    connector: str,
    schema: str = "",
    dest_table: str = "",
    schedule_id: str = "",
) -> list[dict[str, Any]]:
    """Read the destination manifest on its own connection. Does not write."""
    from connectors.postgresql_conn import get_connection

    schema_name = schema or str(dest_cfg.get("schema") or "public")
    conn = get_connection(
        host=str(dest_cfg.get("host") or ""),
        port=int(dest_cfg.get("port") or 5432),
        database=str(dest_cfg.get("database") or ""),
        username=str(dest_cfg.get("username") or ""),
        password=str(dest_cfg.get("password") or ""),
        connection_string=str(dest_cfg.get("connection_string") or ""),
        ssl=bool(dest_cfg.get("ssl") or False),
    )
    try:
        conn.autocommit = True
    except Exception:
        pass
    try:
        return list_loaded(
            conn,
            dialect="postgresql",
            schema=schema_name,
            connector=connector,
            dest_table=dest_table,
            schedule_id=schedule_id,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
