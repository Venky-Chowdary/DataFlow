"""Processed-file manifest that commits with the load.

The table lives on the destination database. The claim row and the loaded
rows share one transaction: a crash rolls both back, so the next run neither
skips the file nor inserts it a second time. A committed ``loaded`` row with
the same sha256 is the idempotent re-run.

Columns: connector, path, size, mtime, sha256, status, job_id, rows.
"""

from __future__ import annotations

import re
from typing import Any

_TABLE = "dataflow_file_manifest"
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _schema(schema: str) -> str:
    text = (schema or "public").strip() or "public"
    if not _IDENT.fullmatch(text):
        raise ValueError(f"Refusing manifest schema {schema!r}")
    return text


def _qualified(dialect: str, schema: str) -> str:
    if dialect == "sqlite":
        return _TABLE
    return f'"{_schema(schema)}"."{_TABLE}"'


def ensure_sql(dialect: str, schema: str = "public") -> str:
    table = _qualified(dialect, schema)
    if dialect == "sqlite":
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "connector TEXT NOT NULL, "
            "path TEXT NOT NULL, "
            "size INTEGER NOT NULL, "
            "mtime TEXT NOT NULL, "
            "sha256 TEXT NOT NULL, "
            "status TEXT NOT NULL, "
            "job_id TEXT NOT NULL, "
            "rows INTEGER NOT NULL DEFAULT 0, "
            "PRIMARY KEY (connector, path, sha256))"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        "connector TEXT NOT NULL, "
        "path TEXT NOT NULL, "
        "size BIGINT NOT NULL, "
        "mtime TEXT NOT NULL, "
        "sha256 TEXT NOT NULL, "
        "status TEXT NOT NULL, "
        "job_id TEXT NOT NULL, "
        "rows BIGINT NOT NULL DEFAULT 0, "
        "PRIMARY KEY (connector, path, sha256))"
    )


def _placeholder(dialect: str) -> str:
    return "?" if dialect == "sqlite" else "%s"


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
) -> str:
    """Insert a ``loading`` claim, or return ``skip`` when this sha256 loaded.

    Caller holds an open transaction. Nothing here commits.
    """
    digest = (sha256 or "").strip().lower()
    if not digest:
        raise ValueError("A processed-file claim needs a sha256")
    ph = _placeholder(dialect)
    table = _qualified(dialect, schema)
    cur = conn.cursor()
    cur.execute(ensure_sql(dialect, schema))
    if dialect == "sqlite":
        cur.execute(
            f"INSERT OR IGNORE INTO {table} "
            f"(connector, path, size, mtime, sha256, status, job_id, rows) "
            f"VALUES ({ph},{ph},{ph},{ph},{ph},'loading',{ph},0)",
            (connector, path, int(size or 0), str(mtime or ""), digest, str(job_id or "")),
        )
        inserted = cur.rowcount == 1
    else:
        cur.execute(
            f"INSERT INTO {table} "
            f"(connector, path, size, mtime, sha256, status, job_id, rows) "
            f"VALUES ({ph},{ph},{ph},{ph},{ph},'loading',{ph},0) "
            f"ON CONFLICT (connector, path, sha256) DO NOTHING",
            (connector, path, int(size or 0), str(mtime or ""), digest, str(job_id or "")),
        )
        inserted = cur.rowcount == 1
    if inserted:
        return "load"
    cur.execute(
        f"SELECT status FROM {table} WHERE connector={ph} AND path={ph} AND sha256={ph}",
        (connector, path, digest),
    )
    row = cur.fetchone()
    status = str(row[0] if row else "")
    if status == "loaded":
        return "skip"
    cur.execute(
        f"UPDATE {table} SET status='loading', job_id={ph}, size={ph}, mtime={ph} "
        f"WHERE connector={ph} AND path={ph} AND sha256={ph}",
        (str(job_id or ""), int(size or 0), str(mtime or ""), connector, path, digest),
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
) -> None:
    """Mark the open claim loaded. Caller commits with the data."""
    ph = _placeholder(dialect)
    table = _qualified(dialect, schema)
    cur = conn.cursor()
    cur.execute(
        f"UPDATE {table} SET status='loaded', rows={ph} "
        f"WHERE connector={ph} AND path={ph} AND sha256={ph}",
        (int(rows or 0), connector, path, (sha256 or "").strip().lower()),
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


def list_loaded(conn: Any, *, dialect: str, schema: str = "public", connector: str) -> list[dict[str, Any]]:
    """Loaded files for pickup selection. Missing table is an empty manifest."""
    ph = _placeholder(dialect)
    table = _qualified(dialect, schema)
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT path, size, mtime, sha256, status, job_id, rows FROM {table} "
            f"WHERE connector={ph} AND status='loaded'",
            (connector,),
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


def read_loaded_manifest(dest_cfg: dict[str, Any], *, connector: str, schema: str = "") -> list[dict[str, Any]]:
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
        return list_loaded(conn, dialect="postgresql", schema=schema_name, connector=connector)
    finally:
        try:
            conn.close()
        except Exception:
            pass
