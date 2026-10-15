"""Snowflake processed-file manifest that commits with the rows.

Snowflake DDL cannot sit inside an explicit transaction, and fakesnow's
MERGE writes a second catalog database, so the file is staged first. One
explicit transaction then claims the sha256, applies last-wins upsert
(UPDATE the keys the stage holds, then INSERT the rest) or appends, and
marks the claim loaded. A crash before that commit rolls the rows and the
claim back; the next run loads the file once. A committed ``loaded`` row
with the same sha256 skips.
"""

from __future__ import annotations

from typing import Any, Callable

from connectors.sql_identifiers import (
    quote_sql_identifier,
    sanitize_identifier,
    snowflake_fold_identifier,
)

_TABLE = "DATAFLOW_FILE_MANIFEST"


def _q(name: str) -> str:
    return quote_sql_identifier(sanitize_identifier(name, preserve_case=True))


def _table(name: str) -> str:
    """Quote a table the way the Snowflake writer stores it."""
    return quote_sql_identifier(snowflake_fold_identifier(name))


def _session(cur: Any, *, database: str, schema: str) -> str:
    schema_name = (schema or "PUBLIC").strip() or "PUBLIC"
    if database:
        database_q = _q(database)
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database_q}")
        cur.execute(f"USE DATABASE {database_q}")
    schema_q = _q(schema_name)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_q}")
    cur.execute(f"USE SCHEMA {schema_q}")
    return schema_name


def _ensure_manifest(cur: Any) -> None:
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {_table(_TABLE)} ("
        "connector VARCHAR NOT NULL, "
        "path VARCHAR NOT NULL, "
        "size NUMBER NOT NULL, "
        "mtime VARCHAR NOT NULL, "
        "sha256 VARCHAR NOT NULL, "
        "status VARCHAR NOT NULL, "
        "job_id VARCHAR NOT NULL, "
        "rows NUMBER NOT NULL DEFAULT 0, "
        "PRIMARY KEY (connector, path, sha256))"
    )


def drop_stage(conn: Any, stage_table: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {_table(stage_table)}")
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def snowflake_file_status(
    conn: Any,
    *,
    database: str,
    schema: str,
    connector: str,
    path: str,
    sha256: str,
) -> str:
    """``loaded`` when this sha256 already committed. Missing table is empty."""
    digest = (sha256 or "").strip().lower()
    if not digest:
        return ""
    cur = conn.cursor()
    try:
        _session(cur, database=database, schema=schema)
        _ensure_manifest(cur)
        cur.execute(
            f"SELECT status FROM {_table(_TABLE)} "
            "WHERE connector = %s AND path = %s AND sha256 = %s",
            (connector, path, digest),
        )
        row = cur.fetchone()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return ""
    return str(row[0] if row else "")


def commit_staged_snowflake_file(
    conn: Any,
    *,
    database: str,
    schema: str,
    stage_table: str,
    target_table: str,
    columns: list[str],
    conflict_columns: list[str],
    write_mode: str,
    connector: str,
    path: str,
    size: int,
    mtime: str,
    sha256: str,
    job_id: str,
    rows: int,
    before_commit: Callable[[], None] | None = None,
) -> str:
    """Commit the staged rows and the manifest claim together.

    Returns ``loaded`` or ``skip``. ``before_commit`` runs after the data
    statement and the loaded update, still inside the open transaction.
    """
    digest = (sha256 or "").strip().lower()
    if not digest:
        raise ValueError("A processed-file claim needs a sha256")
    if not columns:
        raise ValueError("A staged Snowflake file needs columns")
    cur = conn.cursor()
    _session(cur, database=database, schema=schema)
    _ensure_manifest(cur)
    target_name = snowflake_fold_identifier(target_table)
    stage_name = snowflake_fold_identifier(stage_table)
    target_q = quote_sql_identifier(target_name)
    stage_q = quote_sql_identifier(stage_name)
    # The stage already carries the writer’s column types. Create the
    # destination outside the transaction when this is the first load.
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {target_q} AS SELECT * FROM {stage_q} WHERE 1 = 0"
    )
    cur.execute("BEGIN")
    try:
        # INSERT … WHERE NOT EXISTS, not MERGE. fakesnow's MERGE writes its
        # catalog database and aborts an open transaction; real Snowflake
        # accepts this DML inside one explicit transaction, which is the
        # atomic claim.
        cur.execute(
            f"INSERT INTO {_table(_TABLE)} "
            "(connector, path, size, mtime, sha256, status, job_id, rows) "
            "SELECT %s, %s, %s, %s, %s, 'loading', %s, 0 "
            f"WHERE NOT EXISTS (SELECT 1 FROM {_table(_TABLE)} "
            "WHERE connector = %s AND path = %s AND sha256 = %s)",
            (
                connector,
                path,
                int(size or 0),
                str(mtime or ""),
                digest,
                str(job_id or ""),
                connector,
                path,
                digest,
            ),
        )
        cur.execute(
            f"UPDATE {_table(_TABLE)} SET status = 'loading', job_id = %s, size = %s, mtime = %s "
            "WHERE connector = %s AND path = %s AND sha256 = %s AND status <> 'loaded'",
            (str(job_id or ""), int(size or 0), str(mtime or ""), connector, path, digest),
        )
        cur.execute(
            f"SELECT status FROM {_table(_TABLE)} "
            "WHERE connector = %s AND path = %s AND sha256 = %s",
            (connector, path, digest),
        )
        found = cur.fetchone()
        status = str(found[0] if found else "")
        if status == "loaded":
            conn.rollback()
            return "skip"
        quoted_cols = ", ".join(quote_sql_identifier(c) for c in columns)
        if write_mode == "upsert" and conflict_columns:
            # Same last-wins result as MERGE, as DML both engines can hold
            # open: update keys the stage already has, then insert the rest.
            keys = list(conflict_columns)
            on_sql = " AND ".join(
                f"t.{quote_sql_identifier(c)} = s.{quote_sql_identifier(c)}" for c in keys
            )
            update_cols = [c for c in columns if c not in set(keys)]
            if update_cols:
                assignments = ", ".join(
                    f"{quote_sql_identifier(c)} = s.{quote_sql_identifier(c)}"
                    for c in update_cols
                )
                cur.execute(
                    f"UPDATE {target_q} t SET {assignments} FROM {stage_q} s WHERE {on_sql}"
                )
            cur.execute(
                f"INSERT INTO {target_q} ({quoted_cols}) "
                f"SELECT {', '.join('s.' + quote_sql_identifier(c) for c in columns)} "
                f"FROM {stage_q} s WHERE NOT EXISTS ("
                f"SELECT 1 FROM {target_q} t WHERE {on_sql})"
            )
        else:
            cur.execute(
                f"INSERT INTO {target_q} ({quoted_cols}) "
                f"SELECT {quoted_cols} FROM {stage_q}"
            )
        cur.execute(
            f"UPDATE {_table(_TABLE)} SET status = 'loaded', rows = %s "
            "WHERE connector = %s AND path = %s AND sha256 = %s",
            (int(rows or 0), connector, path, digest),
        )
        if before_commit is not None:
            before_commit()
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    return "loaded"


def read_loaded_manifest(
    dest_cfg: dict[str, Any], *, connector: str, schema: str = ""
) -> list[dict[str, Any]]:
    """Loaded Snowflake files for pickup selection. Does not write rows."""
    from connectors.snowflake_conn import get_connection, normalize_account
    from services.connector_auth import engine_login_role

    pem = str(dest_cfg.get("private_key") or "")
    conn = get_connection(
        account=normalize_account(dest_cfg.get("host", "")),
        username=str(dest_cfg.get("username") or ""),
        password=str(dest_cfg.get("password") or ""),
        database=str(dest_cfg.get("database") or ""),
        schema=schema or str(dest_cfg.get("schema") or "PUBLIC"),
        warehouse=str(dest_cfg.get("warehouse") or ""),
        connection_string=str(dest_cfg.get("connection_string") or ""),
        role=engine_login_role(dest_cfg.get("auth_role"), dest_cfg.get("role")),
        private_key=pem,
        private_key_passphrase=str(dest_cfg.get("password") or "") if pem else "",
    )
    try:
        cur = conn.cursor()
        _session(
            cur,
            database=str(dest_cfg.get("database") or ""),
            schema=schema or str(dest_cfg.get("schema") or "PUBLIC"),
        )
        cur.execute(
            f"SELECT path, size, mtime, sha256, status, job_id, rows FROM {_table(_TABLE)} "
            "WHERE connector = %s AND status = 'loaded'",
            (connector,),
        )
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
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass
