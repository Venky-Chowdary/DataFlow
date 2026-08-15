"""SQLAlchemy dest-owned CDC exactly-once apply.

Same algorithm as the SQLite native path: apply + ``_df_cdc_eos_watermarks``
share one dest transaction. Dialects: PostgreSQL, MySQL, SQL Server, DuckDB,
SQLite-via-SQLAlchemy, Oracle, Snowflake, generic_sql (URL dialect).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from connectors.lsn_guards import DF_LSN_COL
from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier
from services.cdc_exactly_once import (
    WATERMARK_TABLE,
    EosApplyResult,
    EosCrash,
    ExactlyOnceRouteError,
    decide_eos_apply,
)
from services.cdc_effectively_once import should_apply_pk_delete, should_apply_pk_row
from services.cdc_engine import ChangeBatch

_CONFLICT_LIKE = frozenset({"postgresql", "postgres", "redshift", "duckdb", "sqlite"})
_FOR_UPDATE_LIKE = frozenset({"postgresql", "postgres", "mysql", "mariadb"})
_MYSQL_LIKE = frozenset({"mysql", "mariadb"})
_MSSQL_LIKE = frozenset({
    "sqlserver",
    "mssql",
    "azure_sql",
    "azure_sql_database",
    "amazon_rds_sql_server",
})
_ORACLE_LIKE = frozenset({"oracle", "oracle_db", "oracle_autonomous_warehouse"})
_SNOW_LIKE = frozenset({"snowflake"})


def normalize_eos_dialect(dest_type: str, dest_cfg: dict[str, Any] | None = None) -> str:
    dest = (dest_type or "").strip().lower().replace("-", "_")
    cfg = dest_cfg or {}
    if dest == "generic_sql":
        hinted = str(cfg.get("type") or "").strip().lower().replace("-", "_")
        if hinted and hinted != "generic_sql":
            dest = hinted
        else:
            url = str(cfg.get("connection_string") or "").lower()
            if "postgres" in url:
                dest = "postgresql"
            elif "mysql" in url or "mariadb" in url:
                dest = "mysql"
            elif "mssql" in url or "sqlserver" in url:
                dest = "sqlserver"
            elif "duckdb" in url:
                dest = "duckdb"
            elif "sqlite" in url:
                dest = "sqlite"
            elif "oracle" in url:
                dest = "oracle"
            elif "snowflake" in url:
                dest = "snowflake"
    if dest == "postgres":
        dest = "postgresql"
    return dest


def _q(name: str) -> str:
    require_safe_identifier(name)
    return quote_sql_identifier(name)


def _wm_ddl(dialect: str) -> str:
    table = WATERMARK_TABLE
    if dialect in _MSSQL_LIKE:
        return (
            f"IF OBJECT_ID('{table}', 'U') IS NULL "
            f"CREATE TABLE {table} ("
            f"stream_key NVARCHAR(512) NOT NULL PRIMARY KEY, "
            f"committed_lsn NVARCHAR(512) NOT NULL, "
            f"batch_id NVARCHAR(64) NOT NULL, "
            f"committed_at NVARCHAR(64) NOT NULL, "
            f"dest_object NVARCHAR(256) NULL, "
            f"epoch INT NOT NULL DEFAULT 1, "
            f"fence_epoch INT NOT NULL DEFAULT 0, "
            f"prev_lsn NVARCHAR(512) NULL, "
            f"phase NVARCHAR(32) NULL)"
        )
    if dialect in _MYSQL_LIKE:
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            f"stream_key VARCHAR(512) NOT NULL PRIMARY KEY, "
            f"committed_lsn VARCHAR(512) NOT NULL, "
            f"batch_id VARCHAR(64) NOT NULL, "
            f"committed_at VARCHAR(64) NOT NULL, "
            f"dest_object VARCHAR(256) NULL, "
            f"epoch INT NOT NULL DEFAULT 1, "
            f"fence_epoch INT NOT NULL DEFAULT 0, "
            f"prev_lsn VARCHAR(512) NULL, "
            f"phase VARCHAR(32) NULL)"
        )
    if dialect in _ORACLE_LIKE:
        return (
            f"BEGIN EXECUTE IMMEDIATE 'CREATE TABLE {table} ("
            f"stream_key VARCHAR2(512) PRIMARY KEY, "
            f"committed_lsn VARCHAR2(512) NOT NULL, "
            f"batch_id VARCHAR2(64) NOT NULL, "
            f"committed_at VARCHAR2(64) NOT NULL, "
            f"dest_object VARCHAR2(256), "
            f"epoch NUMBER DEFAULT 1 NOT NULL, "
            f"fence_epoch NUMBER DEFAULT 0 NOT NULL, "
            f"prev_lsn VARCHAR2(512), "
            f"phase VARCHAR2(32))'; "
            f"EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        f"stream_key TEXT PRIMARY KEY, "
        f"committed_lsn TEXT NOT NULL, "
        f"batch_id TEXT NOT NULL, "
        f"committed_at TEXT NOT NULL, "
        f"dest_object TEXT, "
        f"epoch INTEGER NOT NULL DEFAULT 1, "
        f"fence_epoch INTEGER NOT NULL DEFAULT 0, "
        f"prev_lsn TEXT, "
        f"phase TEXT)"
    )


def _lock_watermark_sql(dialect: str) -> str:
    if dialect in _FOR_UPDATE_LIKE:
        return (
            f"SELECT committed_lsn, epoch, fence_epoch FROM {WATERMARK_TABLE} "
            f"WHERE stream_key = :k FOR UPDATE"
        )
    if dialect in _MSSQL_LIKE:
        return (
            f"SELECT committed_lsn, epoch, fence_epoch FROM {WATERMARK_TABLE} "
            f"WITH (UPDLOCK, ROWLOCK) WHERE stream_key = :k"
        )
    return (
        f"SELECT committed_lsn, epoch, fence_epoch FROM {WATERMARK_TABLE} "
        f"WHERE stream_key = :k"
    )


def _upsert_watermark_sql(dialect: str) -> str:
    cols = (
        "stream_key, committed_lsn, batch_id, committed_at, dest_object, "
        "epoch, fence_epoch, prev_lsn, phase"
    )
    if dialect in _MYSQL_LIKE:
        return (
            f"INSERT INTO {WATERMARK_TABLE} ({cols}) "
            f"VALUES (:k, :lsn, :bid, :at, :obj, :ep, :fe, :prev, :ph) "
            f"ON DUPLICATE KEY UPDATE committed_lsn = VALUES(committed_lsn), "
            f"batch_id = VALUES(batch_id), committed_at = VALUES(committed_at), "
            f"dest_object = VALUES(dest_object), epoch = VALUES(epoch), "
            f"fence_epoch = VALUES(fence_epoch), prev_lsn = VALUES(prev_lsn), "
            f"phase = VALUES(phase)"
        )
    if dialect in _MSSQL_LIKE:
        return (
            f"MERGE {WATERMARK_TABLE} WITH (HOLDLOCK) AS t "
            f"USING (SELECT :k AS stream_key, :lsn AS committed_lsn, :bid AS batch_id, "
            f":at AS committed_at, :obj AS dest_object, :ep AS epoch, :fe AS fence_epoch, "
            f":prev AS prev_lsn, :ph AS phase) AS s "
            f"ON t.stream_key = s.stream_key "
            f"WHEN MATCHED THEN UPDATE SET committed_lsn = s.committed_lsn, "
            f"batch_id = s.batch_id, committed_at = s.committed_at, "
            f"dest_object = s.dest_object, epoch = s.epoch, "
            f"fence_epoch = s.fence_epoch, prev_lsn = s.prev_lsn, phase = s.phase "
            f"WHEN NOT MATCHED THEN INSERT ({cols}) "
            f"VALUES (s.stream_key, s.committed_lsn, s.batch_id, s.committed_at, "
            f"s.dest_object, s.epoch, s.fence_epoch, s.prev_lsn, s.phase);"
        )
    if dialect in _ORACLE_LIKE:
        return (
            f"MERGE INTO {WATERMARK_TABLE} t "
            f"USING (SELECT :k AS stream_key, :lsn AS committed_lsn, :bid AS batch_id, "
            f":at AS committed_at, :obj AS dest_object, :ep AS epoch, :fe AS fence_epoch, "
            f":prev AS prev_lsn, :ph AS phase FROM dual) s "
            f"ON (t.stream_key = s.stream_key) "
            f"WHEN MATCHED THEN UPDATE SET t.committed_lsn = s.committed_lsn, "
            f"t.batch_id = s.batch_id, t.committed_at = s.committed_at, "
            f"t.dest_object = s.dest_object, t.epoch = s.epoch, "
            f"t.fence_epoch = s.fence_epoch, t.prev_lsn = s.prev_lsn, t.phase = s.phase "
            f"WHEN NOT MATCHED THEN INSERT ({cols}) "
            f"VALUES (s.stream_key, s.committed_lsn, s.batch_id, s.committed_at, "
            f"s.dest_object, s.epoch, s.fence_epoch, s.prev_lsn, s.phase)"
        )
    if dialect in _SNOW_LIKE:
        return (
            f"MERGE INTO {WATERMARK_TABLE} t "
            f"USING (SELECT :k AS stream_key, :lsn AS committed_lsn, :bid AS batch_id, "
            f":at AS committed_at, :obj AS dest_object, :ep AS epoch, :fe AS fence_epoch, "
            f":prev AS prev_lsn, :ph AS phase) s "
            f"ON t.stream_key = s.stream_key "
            f"WHEN MATCHED THEN UPDATE SET t.committed_lsn = s.committed_lsn, "
            f"t.batch_id = s.batch_id, t.committed_at = s.committed_at, "
            f"t.dest_object = s.dest_object, t.epoch = s.epoch, "
            f"t.fence_epoch = s.fence_epoch, t.prev_lsn = s.prev_lsn, t.phase = s.phase "
            f"WHEN NOT MATCHED THEN INSERT ({cols}) "
            f"VALUES (s.stream_key, s.committed_lsn, s.batch_id, s.committed_at, "
            f"s.dest_object, s.epoch, s.fence_epoch, s.prev_lsn, s.phase)"
        )
    return (
        f"INSERT INTO {WATERMARK_TABLE} ({cols}) "
        f"VALUES (:k, :lsn, :bid, :at, :obj, :ep, :fe, :prev, :ph) "
        f"ON CONFLICT (stream_key) DO UPDATE SET "
        f"committed_lsn = excluded.committed_lsn, batch_id = excluded.batch_id, "
        f"committed_at = excluded.committed_at, dest_object = excluded.dest_object, "
        f"epoch = excluded.epoch, fence_epoch = excluded.fence_epoch, "
        f"prev_lsn = excluded.prev_lsn, phase = excluded.phase"
    )


def _ensure_wm_columns(conn: Any, dialect: str) -> None:
    """Additive fence/prev/phase on existing watermark tables."""
    adds = [
        ("fence_epoch", "INTEGER" if dialect not in _MSSQL_LIKE | _MYSQL_LIKE | _ORACLE_LIKE else (
            "INT" if dialect in _MSSQL_LIKE | _MYSQL_LIKE else "NUMBER"
        )),
        ("prev_lsn", "TEXT" if dialect not in _MSSQL_LIKE | _MYSQL_LIKE | _ORACLE_LIKE else (
            "NVARCHAR(512)" if dialect in _MSSQL_LIKE else (
                "VARCHAR(512)" if dialect in _MYSQL_LIKE else "VARCHAR2(512)"
            )
        )),
        ("phase", "TEXT" if dialect not in _MSSQL_LIKE | _MYSQL_LIKE | _ORACLE_LIKE else (
            "NVARCHAR(32)" if dialect in _MSSQL_LIKE else (
                "VARCHAR(32)" if dialect in _MYSQL_LIKE else "VARCHAR2(32)"
            )
        )),
    ]
    table_q = WATERMARK_TABLE
    for col, typ in adds:
        try:
            if dialect in _ORACLE_LIKE:
                conn.execute(text(f"ALTER TABLE {table_q} ADD ({col} {typ})"))
            elif dialect in _MSSQL_LIKE:
                conn.execute(text(f"ALTER TABLE {table_q} ADD {col} {typ}"))
            else:
                conn.execute(text(f"ALTER TABLE {table_q} ADD COLUMN {col} {typ}"))
        except Exception:
            pass


def _col_sql_type(dialect: str, col: str, pk_cols: list[str]) -> str:
    """Bounded PK / LSN types — MySQL/Oracle/MSSQL refuse unbounded TEXT keys."""
    keyed = col in pk_cols or col == DF_LSN_COL
    if dialect in _MSSQL_LIKE:
        return "NVARCHAR(512)" if keyed else "NVARCHAR(MAX)"
    if dialect in _MYSQL_LIKE:
        return "VARCHAR(512)" if keyed else "LONGTEXT"
    if dialect in _ORACLE_LIKE:
        return "VARCHAR2(512)" if keyed else "CLOB"
    return "TEXT"


def _add_column_sql(dialect: str, table_q: str, col: str, typ: str) -> str:
    col_q = _q(col)
    if dialect in _ORACLE_LIKE:
        return f"ALTER TABLE {table_q} ADD ({col_q} {typ})"
    if dialect in _MSSQL_LIKE:
        return f"ALTER TABLE {table_q} ADD {col_q} {typ}"
    return f"ALTER TABLE {table_q} ADD COLUMN {col_q} {typ}"


def _ensure_dest_table(conn: Any, dialect: str, table_name: str, columns: list[str], pk_cols: list[str]) -> None:
    table_q = _q(table_name)
    col_sql = []
    for col in columns:
        typ = _col_sql_type(dialect, col, pk_cols)
        suffix = " PRIMARY KEY" if pk_cols == [col] else ""
        col_sql.append(f"{_q(col)} {typ}{suffix}")
    if len(pk_cols) > 1:
        pk_sql = ", ".join(_q(c) for c in pk_cols)
        col_sql.append(f"PRIMARY KEY ({pk_sql})")
    ddl = f"CREATE TABLE IF NOT EXISTS {table_q} ({', '.join(col_sql)})"
    if dialect in _MSSQL_LIKE:
        ddl = (
            f"IF OBJECT_ID('{table_name}', 'U') IS NULL "
            f"CREATE TABLE {table_q} ({', '.join(col_sql)})"
        )
    if dialect in _ORACLE_LIKE:
        conn.execute(text(
            "BEGIN EXECUTE IMMEDIATE :ddl; EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -955 THEN RAISE; END IF; END;"
        ), {"ddl": f"CREATE TABLE {table_q} ({', '.join(col_sql)})"})
    else:
        conn.execute(text(ddl))
    # Additive columns when the dest table already existed (never invent PK).
    for col in columns:
        try:
            conn.execute(text(
                _add_column_sql(
                    dialect, table_q, col, _col_sql_type(dialect, col, pk_cols)
                )
            ))
        except Exception:
            pass


def _row_values(
    rec: dict[str, Any],
    target_cols: list[str],
    tgt_to_src: dict[str, str],
    incoming_lsn: str,
) -> dict[str, Any]:
    stamped = dict(rec)
    stamped[DF_LSN_COL] = incoming_lsn
    out: dict[str, Any] = {}
    for tgt in target_cols:
        src = tgt_to_src.get(tgt, tgt)
        out[tgt] = stamped[tgt] if tgt in stamped else stamped.get(src)
    return out


def _upsert_row(
    conn: Any,
    table_q: str,
    target_cols: list[str],
    pk_cols: list[str],
    values: dict[str, Any],
) -> int:
    where = " AND ".join(f"{_q(c)} = :pk_{i}" for i, c in enumerate(pk_cols))
    pk_binds = {f"pk_{i}": values.get(c) for i, c in enumerate(pk_cols)}
    existing = conn.execute(
        text(f"SELECT {_q(DF_LSN_COL)} FROM {table_q} WHERE {where}"),
        pk_binds,
    ).fetchone()
    prior = existing[0] if existing else None
    if not should_apply_pk_row(
        existing_lsn=prior, incoming_lsn=values.get(DF_LSN_COL)
    ).applied:
        return 0
    col_binds = {f"c_{i}": values.get(c) for i, c in enumerate(target_cols)}
    if existing is None:
        cols = ", ".join(_q(c) for c in target_cols)
        ph = ", ".join(f":c_{i}" for i in range(len(target_cols)))
        conn.execute(text(f"INSERT INTO {table_q} ({cols}) VALUES ({ph})"), col_binds)
        return 1
    set_cols = [c for c in target_cols if c not in pk_cols]
    if not set_cols:
        return 0
    sets = ", ".join(f"{_q(c)} = :c_{target_cols.index(c)}" for c in set_cols)
    conn.execute(text(f"UPDATE {table_q} SET {sets} WHERE {where}"), {**col_binds, **pk_binds})
    return 1


def apply_eos_sqlalchemy(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    change: ChangeBatch,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    pk_target_cols: list[str],
    stream_key: str,
    incoming_lsn: str,
    batch_id: str,
    crash_after: str | None = None,
    writer_fence: int = 0,
) -> EosApplyResult:
    from connectors.generic_sql import _engine
    from connectors.writer_common import resolve_target_columns
    from services.cdc_snapshot_window import _pk_row_dict
    from services.engine_pool import release_engine

    dialect = normalize_eos_dialect(dest_type, dest_cfg)
    mappings = list(mappings)
    column_types = dict(column_types)
    if not any(m.get("source") == DF_LSN_COL for m in mappings):
        mappings.append({"source": DF_LSN_COL, "target": DF_LSN_COL, "confidence": 1.0})
    column_types.setdefault(DF_LSN_COL, "string")
    target_cols, _logical = resolve_target_columns(
        mappings,
        column_types,
        preserve_case=True,
        table_exists=None,
        dest_db=dialect if dialect != "generic_sql" else "generic_sql",
    )
    if DF_LSN_COL not in target_cols:
        target_cols = list(target_cols) + [DF_LSN_COL]
    if not pk_target_cols:
        raise ExactlyOnceRouteError(
            "exactly_once apply requires destination primary-key columns.",
            reason="exactly_once_requires_primary_key",
        )
    src_to_tgt = {
        str(m.get("source") or ""): str(m.get("target") or m.get("source") or "")
        for m in mappings
        if m.get("source")
    }
    tgt_to_src = {t: s for s, t in src_to_tgt.items() if t}

    cfg = dict(dest_cfg)
    cfg.setdefault("type", dialect if dialect != "generic_sql" else (dest_cfg.get("type") or dest_type))
    engine = _engine(cfg)
    try:
        with engine.begin() as conn:
            conn.execute(text(_wm_ddl(dialect)))
            _ensure_wm_columns(conn, dialect)
            _ensure_dest_table(conn, dialect, dest_table, target_cols, pk_target_cols)
            locked = conn.execute(text(_lock_watermark_sql(dialect)), {"k": stream_key}).fetchone()
            dest_lsn = str(locked[0]) if locked and locked[0] else None
            epoch = int(locked[1] or 0) if locked else 0
            dest_fence = int(locked[2] or 0) if locked and len(locked) > 2 else 0
            action, fence = decide_eos_apply(
                incoming_lsn=incoming_lsn,
                dest_lsn=dest_lsn,
                incoming_fence=writer_fence,
                dest_fence=dest_fence,
                dest_epoch=epoch,
            )
            if action == "already_committed":
                return EosApplyResult(
                    status="already_committed",
                    committed_lsn=dest_lsn,
                    batch_id=batch_id,
                    epoch=epoch,
                    already_committed=True,
                    fence_epoch=fence,
                )
            table_q = _q(dest_table)
            rows_written = 0
            for rec in list(change.inserts or []) + list(change.updates or []):
                values = _row_values(rec, target_cols, tgt_to_src, incoming_lsn)
                rows_written += _upsert_row(conn, table_q, target_cols, pk_target_cols, values)
            deleted = 0
            for key in list(change.deletes or []):
                parts = (
                    _pk_row_dict(pk_target_cols, key)
                    if len(pk_target_cols) > 1
                    else {pk_target_cols[0]: key}
                )
                where = " AND ".join(
                    f"{_q(c)} = :pk_{i}" for i, c in enumerate(pk_target_cols)
                )
                binds = {f"pk_{i}": parts[c] for i, c in enumerate(pk_target_cols)}
                existing = conn.execute(
                    text(f"SELECT {_q(DF_LSN_COL)} FROM {table_q} WHERE {where}"),
                    binds,
                ).fetchone()
                prior = existing[0] if existing else None
                if not should_apply_pk_delete(
                    existing_lsn=prior, incoming_lsn=incoming_lsn
                ).applied:
                    continue
                result = conn.execute(text(f"DELETE FROM {table_q} WHERE {where}"), binds)
                deleted += int(getattr(result, "rowcount", 0) or 0)
            if crash_after == "after_apply_before_watermark":
                raise EosCrash(crash_after)
            new_epoch = epoch + 1
            conn.execute(
                text(_upsert_watermark_sql(dialect)),
                {
                    "k": stream_key,
                    "lsn": incoming_lsn,
                    "bid": batch_id,
                    "at": datetime.now(timezone.utc).isoformat(),
                    "obj": dest_table,
                    "ep": new_epoch,
                    "fe": fence,
                    "prev": dest_lsn,
                    "ph": "streaming",
                },
            )
            if crash_after == "after_watermark_before_commit":
                raise EosCrash(crash_after)
            result = EosApplyResult(
                status="applied" if (rows_written or deleted) else "empty",
                rows_written=rows_written,
                deleted=deleted,
                committed_lsn=incoming_lsn,
                batch_id=batch_id,
                epoch=new_epoch,
                fence_epoch=fence,
            )
        if crash_after == "after_commit_before_ack":
            raise EosCrash(crash_after)
        return result
    except EosCrash:
        raise
    finally:
        release_engine(engine)


def sa_dest_watermark_lsn(dest_cfg: dict[str, Any], stream_key: str, dest_type: str) -> str | None:
    from connectors.generic_sql import _engine
    from services.engine_pool import release_engine

    dialect = normalize_eos_dialect(dest_type, dest_cfg)
    cfg = dict(dest_cfg)
    cfg.setdefault("type", dialect)
    engine = _engine(cfg)
    try:
        with engine.connect() as conn:
            try:
                row = conn.execute(
                    text(
                        f"SELECT committed_lsn FROM {WATERMARK_TABLE} WHERE stream_key = :k"
                    ),
                    {"k": stream_key},
                ).fetchone()
            except Exception:
                return None
            return str(row[0]) if row and row[0] else None
    finally:
        release_engine(engine)


def sa_dest_engine_count(dest_cfg: dict[str, Any], table_name: str, dest_type: str) -> int:
    from connectors.generic_sql import _engine
    from services.engine_pool import release_engine

    dialect = normalize_eos_dialect(dest_type, dest_cfg)
    cfg = dict(dest_cfg)
    cfg.setdefault("type", dialect)
    engine = _engine(cfg)
    try:
        with engine.connect() as conn:
            try:
                row = conn.execute(text(f"SELECT COUNT(*) FROM {_q(table_name)}")).fetchone()
            except Exception:
                return 0
            return int(row[0] or 0) if row else 0
    finally:
        release_engine(engine)
