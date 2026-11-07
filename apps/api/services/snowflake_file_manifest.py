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
    from connectors.snowflake_conn import use_snowflake_namespace

    use_snowflake_namespace(
        cur, database_q=_q(database) if database else "", schema_q=_q(schema_name), create=True
    )
    return schema_name


def _create_manifest_sql() -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {_table(_TABLE)} ("
        "connector VARCHAR NOT NULL, "
        "path VARCHAR NOT NULL, "
        "size NUMBER NOT NULL, "
        "mtime VARCHAR NOT NULL, "
        "sha256 VARCHAR NOT NULL, "
        "status VARCHAR NOT NULL, "
        "job_id VARCHAR NOT NULL, "
        "rows NUMBER NOT NULL DEFAULT 0, "
        "dest_table VARCHAR NOT NULL DEFAULT '', "
        "schedule_id VARCHAR NOT NULL DEFAULT '', "
        "PRIMARY KEY (connector, path, sha256, dest_table, schedule_id))"
    )


def _manifest_columns(cur: Any) -> set[str]:
    cur.execute(f"SELECT * FROM {_table(_TABLE)} WHERE 1 = 0")
    return {str(item[0]).lower() for item in (cur.description or [])}


def _rebuild_unscoped_manifest(cur: Any) -> None:
    """Copy a legacy (connector, path, sha256) manifest onto the scoped key.

    Existing rows keep an empty destination and schedule. They still match an
    empty-scope claim and do not suppress a named table.
    """
    legacy = "DATAFLOW_FILE_MANIFEST_LEGACY"
    cur.execute(f"ALTER TABLE {_table(_TABLE)} RENAME TO {_table(legacy)}")
    cur.execute(_create_manifest_sql())
    cur.execute(
        f"INSERT INTO {_table(_TABLE)} "
        "(connector, path, size, mtime, sha256, status, job_id, rows, dest_table, schedule_id) "
        f"SELECT connector, path, size, mtime, sha256, status, job_id, rows, '', '' "
        f"FROM {_table(legacy)}"
    )
    cur.execute(f"DROP TABLE IF EXISTS {_table(legacy)}")


def _ensure_manifest(cur: Any) -> None:
    """Create the scoped manifest. A legacy table is rebuilt outside the load transaction."""
    cur.execute(_create_manifest_sql())
    try:
        columns = _manifest_columns(cur)
    except Exception:
        cur.execute(_create_manifest_sql())
        return
    if "dest_table" in columns and "schedule_id" in columns:
        return
    _rebuild_unscoped_manifest(cur)


def _scope(dest_table: str = "", schedule_id: str = "") -> tuple[str, str]:
    return str(dest_table or ""), str(schedule_id or "")


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
    dest_table: str = "",
    schedule_id: str = "",
) -> str:
    """``loaded`` when this sha256 already committed for this table and schedule."""
    digest = (sha256 or "").strip().lower()
    if not digest:
        return ""
    table_name, schedule_name = _scope(dest_table, schedule_id)
    cur = conn.cursor()
    try:
        _session(cur, database=database, schema=schema)
        _ensure_manifest(cur)
        cur.execute(
            f"SELECT status FROM {_table(_TABLE)} "
            "WHERE connector = %s AND path = %s AND sha256 = %s "
            "AND dest_table = %s AND schedule_id = %s",
            (connector, path, digest, table_name, schedule_name),
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
    dest_table: str = "",
    schedule_id: str = "",
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
    table_name, schedule_name = _scope(dest_table, schedule_id)
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
            "(connector, path, size, mtime, sha256, status, job_id, rows, dest_table, schedule_id) "
            "SELECT %s, %s, %s, %s, %s, 'loading', %s, 0, %s, %s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {_table(_TABLE)} "
            "WHERE connector = %s AND path = %s AND sha256 = %s "
            "AND dest_table = %s AND schedule_id = %s)",
            (
                connector,
                path,
                int(size or 0),
                str(mtime or ""),
                digest,
                str(job_id or ""),
                table_name,
                schedule_name,
                connector,
                path,
                digest,
                table_name,
                schedule_name,
            ),
        )
        cur.execute(
            f"UPDATE {_table(_TABLE)} SET status = 'loading', job_id = %s, size = %s, mtime = %s "
            "WHERE connector = %s AND path = %s AND sha256 = %s "
            "AND dest_table = %s AND schedule_id = %s AND status <> 'loaded'",
            (
                str(job_id or ""),
                int(size or 0),
                str(mtime or ""),
                connector,
                path,
                digest,
                table_name,
                schedule_name,
            ),
        )
        cur.execute(
            f"SELECT status FROM {_table(_TABLE)} "
            "WHERE connector = %s AND path = %s AND sha256 = %s "
            "AND dest_table = %s AND schedule_id = %s",
            (connector, path, digest, table_name, schedule_name),
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
            "WHERE connector = %s AND path = %s AND sha256 = %s "
            "AND dest_table = %s AND schedule_id = %s",
            (int(rows or 0), connector, path, digest, table_name, schedule_name),
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
    dest_cfg: dict[str, Any],
    *,
    connector: str,
    schema: str = "",
    dest_table: str = "",
    schedule_id: str = "",
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
        table_name, schedule_name = _scope(dest_table, schedule_id)
        _ensure_manifest(cur)
        cur.execute(
            f"SELECT path, size, mtime, sha256, status, job_id, rows FROM {_table(_TABLE)} "
            "WHERE connector = %s AND status = 'loaded' "
            "AND dest_table = %s AND schedule_id = %s",
            (connector, table_name, schedule_name),
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
