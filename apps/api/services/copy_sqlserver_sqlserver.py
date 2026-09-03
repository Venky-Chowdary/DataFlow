"""SQL Server → SQL Server identity bulk (same-instance INSERT SELECT).

``INSERT INTO dest WITH (TABLOCK) SELECT … FROM src`` on one connection.
Dest CREATE/DROP is committed first so DDL is not inside the read
transaction. This database has ``ALLOW_SNAPSHOT_ISOLATION`` off by
default; the path uses SNAPSHOT only when ``sys.databases`` already
allows it, else ``FROM src WITH (HOLDLOCK, TABLOCK)``. It never
``ALTER DATABASE``.

Python does not format a row. Proof is dest ``COUNT(*)`` vs the source
count taken in that transaction. A mapped single PK still proves dest
``COUNT(*)`` per key range; a non-empty dest skips complete ranges and
DELETE+reloads partial ones.

Declines (row path keeps quarantine): transforms that change values,
public proxy, cross-host (no BCP yet), copy onto the same table,
occupied dest without a mapped single PK.
"""

from __future__ import annotations

import logging
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_pg_mysql import (
    _jsonable_bound,
    integer_pk_cuts,
    key_ranges_from_cuts,
    mapped_single_pk,
    mysql_pk_range_clause,
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
)

logger = logging.getLogger(__name__)

_INTEGER_PK_TYPES = frozenset({
    "tinyint",
    "smallint",
    "int",
    "integer",
    "bigint",
})

_SQLSERVER_FAMILY = frozenset({
    "sqlserver",
    "mssql",
    "sql_server",
    "microsoft_sql_server",
    "azure_sql",
    "azure_sql_database",
    "amazon_rds_sql_server",
    "google_cloud_sql_sql_server",
})


def sqlserver_family_name(engine: str | None) -> str:
    raw = (engine or "").strip().lower()
    if raw in _SQLSERVER_FAMILY:
        return "sqlserver"
    return raw


def sqlserver_sqlserver_insert_select_enabled() -> bool:
    raw = (getenv_brand("SQLSERVER_SQLSERVER_INSERT_SELECT", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _norm_ss_host(host: str) -> str:
    h = (host or "").strip().lower()
    if h in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        return "127.0.0.1"
    return h


def sqlserver_same_instance(src_cfg: dict[str, Any], dest_cfg: dict[str, Any]) -> bool:
    """True only when host+port are present and equal. Fail closed on blanks."""
    src_host = _norm_ss_host(str(src_cfg.get("host") or ""))
    dest_host = _norm_ss_host(str(dest_cfg.get("host") or ""))
    if not src_host or not dest_host:
        return False
    src_port = int(src_cfg.get("port") or 1433)
    dest_port = int(dest_cfg.get("port") or 1433)
    return src_host == dest_host and src_port == dest_port


def _schema_of(cfg: dict[str, Any], explicit: str | None = None) -> str:
    raw = (explicit or cfg.get("schema") or "dbo")
    return str(raw).strip() or "dbo"


def _ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return quote_sql_identifier(
        require_safe_identifier(name, preserve_case=True, max_len=128),
        "[",
    )


def _table_ref(schema: str, table: str) -> str:
    from connectors.sql_identifiers import quote_table_ref

    return quote_table_ref(
        table, schema or "dbo", dialect="sqlserver", preserve_case=True
    )


def _object_id_name(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _is_connect_failure(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(
        tok in msg
        for tok in (
            "08001",
            "ssl",
            "certificate",
            "login timeout",
            "im002",
            "could not open a connection",
            "unable to connect",
            "adaptive server",
            "hy000",
        )
    )


def _pymssql_connect(cfg: dict[str, Any]) -> Any:
    import pymssql

    conn = pymssql.connect(
        server=str(cfg.get("host") or "127.0.0.1"),
        port=int(cfg.get("port") or 1433),
        user=str(cfg.get("username") or cfg.get("user") or ""),
        password=str(cfg.get("password") or ""),
        database=str(cfg.get("database") or ""),
        login_timeout=10,
        autocommit=False,
    )
    return conn


def _ss_connect(cfg: dict[str, Any]) -> Any:
    from connectors.generic_sql import connection_options, get_connection, with_connection_options

    merged = with_connection_options(cfg)
    kwargs: dict[str, Any] = {
        "host": merged.get("host", ""),
        "port": int(merged.get("port") or 1433),
        "database": merged.get("database", ""),
        "username": merged.get("username") or merged.get("user") or "",
        "password": merged.get("password", ""),
        "connection_string": merged.get("connection_string", ""),
        "ssl": bool(merged.get("ssl", False)),
        "db_type": "sqlserver",
        **connection_options(merged),
    }
    try:
        conn = get_connection(**kwargs)
        try:
            conn.autocommit = False
        except Exception:
            logger.debug("SQL Server autocommit=False skipped", exc_info=True)
        return conn
    except Exception as exc:
        if not _is_connect_failure(exc):
            raise FastPathUnavailable(f"SQL Server connect failed: {exc}") from exc
        logger.info(
            "SQL Server generic_sql handshake failed (%s); falling back to pymssql",
            exc,
        )
        try:
            return _pymssql_connect(merged)
        except Exception as pymssql_exc:
            raise FastPathUnavailable(
                f"SQL Server connect failed: {exc}; pymssql: {pymssql_exc}"
            ) from pymssql_exc


def _format_ss_type(
    type_name: str, max_length: int, precision: int, scale: int
) -> str:
    t = (type_name or "").strip().lower()
    if t in {"nvarchar", "nchar"}:
        if int(max_length) < 0:
            return f"{t.upper()}(MAX)"
        chars = max(1, int(max_length) // 2)
        return f"{t.upper()}({chars})"
    if t in {"varchar", "char", "binary", "varbinary"}:
        if int(max_length) < 0:
            return f"{t.upper()}(MAX)"
        return f"{t.upper()}({max(1, int(max_length))})"
    if t in {"decimal", "numeric"}:
        return f"{t.upper()}({int(precision)},{int(scale)})"
    if t in {"datetime2", "datetimeoffset", "time"}:
        return f"{t.upper()}({int(scale)})"
    if t == "float" and precision:
        return f"FLOAT({int(precision)})"
    return t.upper() or "NVARCHAR(MAX)"


def _ss_table_pk_and_types(
    cur: Any, schema: str, table: str, columns: list[str]
) -> tuple[list[str], dict[str, str]]:
    obj = _object_id_name(schema, table)
    cur.execute(
        "SELECT c.name, ty.name, c.max_length, c.precision, c.scale "
        "FROM sys.columns c "
        "JOIN sys.types ty ON c.system_type_id = ty.system_type_id "
        "AND ty.is_user_defined = 0 "
        "WHERE c.object_id = OBJECT_ID(%s) "
        "ORDER BY c.column_id",
        (obj,),
    )
    types: dict[str, str] = {}
    for name, type_name, max_length, precision, scale in cur.fetchall() or []:
        types[str(name)] = _format_ss_type(
            str(type_name or ""),
            int(max_length or 0),
            int(precision or 0),
            int(scale or 0),
        )
    live_l = {k.lower(): v for k, v in types.items()}
    missing = [c for c in columns if c.lower() not in live_l]
    if missing:
        raise FastPathUnavailable(f"source column {missing[0]!r} absent")
    cur.execute(
        "SELECT c.name FROM sys.indexes i "
        "JOIN sys.index_columns ic ON ic.object_id = i.object_id "
        "AND ic.index_id = i.index_id "
        "JOIN sys.columns c ON c.object_id = ic.object_id "
        "AND c.column_id = ic.column_id "
        "WHERE i.object_id = OBJECT_ID(%s) AND i.is_primary_key = 1 "
        "ORDER BY ic.key_ordinal",
        (obj,),
    )
    pk = [str(r[0]) for r in cur.fetchall() or []]
    return pk, types


def _table_exists(cur: Any, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT OBJECT_ID(%s, 'U')",
        (_object_id_name(schema, table),),
    )
    row = cur.fetchone()
    return bool(row and row[0] is not None)


def _has_identity(cur: Any, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM sys.identity_columns WHERE object_id = OBJECT_ID(%s)",
        (_object_id_name(schema, table),),
    )
    row = cur.fetchone()
    return int(row[0] if row else 0) > 0


def _create_sql(
    dest_ref: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    ddls: list[str],
    pk_dest: list[str],
) -> str:
    cols: list[str] = []
    targets = [t for _s, t in pairs]
    for (_source, target), ddl in zip(pairs, ddls):
        cols.append(f"{_ident(target)} {ddl}")
    pk = [c for c in pk_dest if c in targets]
    if pk:
        pk_sql = ", ".join(_ident(c) for c in pk)
        constraint = f"PK_{dest_table}"[:128]
        cols.append(f"CONSTRAINT {_ident(constraint)} PRIMARY KEY ({pk_sql})")
    return f"CREATE TABLE {dest_ref} ({', '.join(cols)})"


def _drop_sql(dest_ref: str) -> str:
    return f"DROP TABLE IF EXISTS {dest_ref}"


def _exec(cur: Any, sql: str, params: list[Any] | None = None) -> None:
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)


def _count(cur: Any, table_ref: str, lock_hint: str = "") -> int:
    hint = f" {lock_hint}" if lock_hint else ""
    cur.execute(f"SELECT COUNT(*) FROM {table_ref}{hint}")  # nosec B608
    return int(cur.fetchone()[0])


def _range_count(
    cur: Any, table_ref: str, dest_ident: str, part: dict[str, Any]
) -> int:
    clause, params = mysql_pk_range_clause(
        dest_ident,
        part.get("lo"),
        part.get("hi"),
        null_shard=bool(part.get("null_shard")),
    )
    _exec(
        cur,
        f"SELECT COUNT(*) FROM {table_ref} WHERE {clause}",  # nosec B608
        params,
    )
    return int(cur.fetchone()[0])


def _delete_range(
    cur: Any, table_ref: str, dest_ident: str, part: dict[str, Any]
) -> None:
    clause, params = mysql_pk_range_clause(
        dest_ident,
        part.get("lo"),
        part.get("hi"),
        null_shard=bool(part.get("null_shard")),
    )
    _exec(
        cur,
        f"DELETE FROM {table_ref} WHERE {clause}",  # nosec B608
        params,
    )


def _end_tran(cur: Any, conn: Any) -> None:
    try:
        cur.execute("IF @@TRANCOUNT > 0 COMMIT TRANSACTION")
    except Exception:
        logger.debug("T-SQL COMMIT skipped", exc_info=True)
        try:
            conn.commit()
        except Exception:
            logger.debug("DBAPI commit skipped", exc_info=True)


def _snapshot_allowed(cur: Any) -> bool:
    """True only when the database already allows SNAPSHOT. Never ALTER DATABASE."""
    cur.execute(
        "SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID()"
    )
    row = cur.fetchone()
    try:
        state = int(row[0]) if row and row[0] is not None else 0
    except (TypeError, ValueError):
        state = 0
    return state == 1


def _prepare_source_read(cur: Any, conn: Any) -> str:
    """SNAPSHOT when the database already allows it; else HOLDLOCK."""
    _end_tran(cur, conn)
    allowed = _snapshot_allowed(cur)
    _end_tran(cur, conn)
    if allowed:
        cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        cur.execute("BEGIN TRANSACTION")
        return "snapshot"
    try:
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    except Exception:
        logger.debug("READ COMMITTED reset skipped", exc_info=True)
    cur.execute("BEGIN TRANSACTION")
    return "holdlock"


def _insert_select_sql(
    dest_ref: str,
    source_ref: str,
    pairs: list[tuple[str, str]],
    clause: str,
    source_hint: str,
) -> str:
    dest_cols = ", ".join(_ident(t) for _s, t in pairs)
    src_cols = ", ".join(_ident(s) for s, _t in pairs)
    where = f" WHERE {clause}" if clause and clause != "1=1" else ""
    hint = f" {source_hint}" if source_hint else ""
    return (
        f"INSERT INTO {dest_ref} WITH (TABLOCK) ({dest_cols}) "  # nosec B608
        f"SELECT {src_cols} FROM {source_ref}{hint}{where}"
    )


def _fetch_ss_pk_interior_cuts(
    cur: Any, table_q: str, pk_ident: str, workers: int
) -> list[Any]:
    n = max(int(workers or 1), 1)
    if n <= 1:
        return []
    cur.execute(
        f"SELECT COUNT(*) FROM {table_q} WHERE {pk_ident} IS NOT NULL"  # nosec B608
    )
    total = int(cur.fetchone()[0])
    if total <= 1:
        return []
    cuts: list[Any] = []
    for i in range(1, n):
        off = max((i * total) // n, 1) - 1
        cur.execute(
            f"SELECT {pk_ident} FROM {table_q} WHERE {pk_ident} IS NOT NULL "  # nosec B608
            f"ORDER BY {pk_ident} OFFSET %s ROWS FETCH NEXT 1 ROWS ONLY",
            (off,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            cuts.append(row[0])
    return cuts


def _plan_pk_partitions(
    src_cur: Any,
    table_q: str,
    src_ident: str,
    pk_declared: str,
    n_parts: int,
    source_count: int,
) -> list[dict[str, Any]]:
    if n_parts <= 1:
        key_ranges: list[tuple[Any | None, Any | None]] = [(None, None)]
    elif (pk_declared or "").strip().lower() in _INTEGER_PK_TYPES:
        src_cur.execute(
            f"SELECT MIN({src_ident}), MAX({src_ident}) FROM {table_q} "  # nosec B608
            f"WHERE {src_ident} IS NOT NULL"
        )
        row = src_cur.fetchone()
        cuts = (
            integer_pk_cuts(int(row[0]), int(row[1]), n_parts)
            if row and row[0] is not None and row[1] is not None
            else []
        )
        key_ranges = key_ranges_from_cuts(cuts)
    else:
        cuts = _fetch_ss_pk_interior_cuts(src_cur, table_q, src_ident, n_parts)
        key_ranges = key_ranges_from_cuts(cuts)
    src_cur.execute(
        f"SELECT COUNT(*) FROM {table_q} WHERE {src_ident} IS NULL"  # nosec B608
    )
    nulls = int(src_cur.fetchone()[0])
    unbounded = len(key_ranges) == 1 and key_ranges[0] == (None, None)
    plan: list[tuple[str, list[Any], Any, Any, bool]] = []
    if nulls and not unbounded:
        plan.append((f"{src_ident} IS NULL", [], None, None, True))
    for lo, hi in key_ranges:
        clause, params = mysql_pk_range_clause(src_ident, lo, hi)
        plan.append((clause, list(params), lo, hi, False))
    partitions: list[dict[str, Any]] = []
    for clause, params, lo, hi, is_null in plan:
        if clause and clause != "1=1":
            _exec(
                src_cur,
                f"SELECT COUNT(*) FROM {table_q} WHERE {clause}",  # nosec B608
                params,
            )
        else:
            src_cur.execute(f"SELECT COUNT(*) FROM {table_q}")  # nosec B608
            clause = ""
            params = []
        expected = int(src_cur.fetchone()[0])
        partitions.append({
            "lo": lo,
            "hi": hi,
            "null_shard": is_null,
            "source_count": expected,
            "predicate": clause,
            "params": params,
            "action": "load",
        })
    accounted = sum(int(p["source_count"]) for p in partitions)
    if accounted != source_count:
        raise ValueError(
            f"PK range source COUNTs {accounted} != snapshot {source_count}"
        )
    return partitions


def copy_sqlserver_to_sqlserver(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlserver_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    dest_schema: str | None = None,
) -> FastPathResult:
    """Identity SQL Server→SQL Server. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(sqlserver_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not sqlserver_sqlserver_insert_select_enabled():
        raise FastPathUnavailable("SQL Server INSERT SELECT disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: SQL Server bulk copy not assumed")

    if not sqlserver_same_instance(source_cfg, dest_cfg):
        raise FastPathUnavailable(
            "cross-host SQL Server stays on the row path (no BCP yet)"
        )

    src_schema = _schema_of(source_cfg, source_schema)
    dst_schema = _schema_of(dest_cfg, dest_schema)
    src_db = str(source_cfg.get("database") or "")
    dest_db = str(dest_cfg.get("database") or "")
    if (
        src_db.lower() == dest_db.lower()
        and src_schema.lower() == dst_schema.lower()
        and source_table.lower() == dest_table.lower()
    ):
        raise FastPathUnavailable("refusing copy onto the same SQL Server table")

    source_cols = [p[0] for p in pairs]
    source_ref = _table_ref(src_schema, source_table)
    dest_ref = _table_ref(dst_schema, dest_table)

    # Same instance: one session owns DDL commit, then COUNT + INSERT SELECT.
    # Two connections deadlock on dest TABLOCK vs an open dest cursor.
    conn = _ss_connect(dest_cfg)
    created_here = False
    existed_before = False
    pk_map: tuple[str, str] | None = None
    cur = conn.cursor()
    try:
        pk_cols, live = _ss_table_pk_and_types(
            cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        create_ddls: list[str] = []
        for i, col in enumerate(source_cols):
            create_ddls.append(live_l.get(col.lower()) or sqlserver_ddls[i])
        pk_map = mapped_single_pk(pk_cols, pairs)
        pk_dest = [
            rename
            for src_pk in pk_cols
            for src_col, rename in pairs
            if src_col.lower() == src_pk.lower()
        ]

        exists = _table_exists(cur, dst_schema, dest_table)
        existed_before = bool(exists)
        dest_occupied = False
        if replace_destination and exists:
            cur.execute(_drop_sql(dest_ref))
            conn.commit()
            exists = False
        if exists:
            dest_occupied = _count(cur, dest_ref) > 0
            if dest_occupied and pk_map is None:
                raise FastPathUnavailable(
                    "append into non-empty SQL Server dest stays on the row path"
                )
        else:
            cur.execute(
                _create_sql(dest_ref, dest_table, pairs, create_ddls, pk_dest)
            )
            conn.commit()
            created_here = True
        conn.commit()

        isolation = _prepare_source_read(cur, conn)
        source_hint = "WITH (HOLDLOCK, TABLOCK)" if isolation == "holdlock" else ""
        source_count = _count(cur, source_ref, source_hint)
        workers = pg_mysql_copy_workers(source_count)
        n_parts = pg_mysql_copy_partitions(source_count, workers)
        partitions: list[dict[str, Any]] = []
        shard_mode = "serial"
        to_copy: list[dict[str, Any]] = [{"predicate": "", "params": []}]

        if pk_map is not None:
            src_pk, dest_pk = pk_map
            src_ident = _ident(src_pk)
            shard_mode = "pk"
            pk_declared = live_l.get(src_pk.lower()) or ""
            partitions = _plan_pk_partitions(
                cur, source_ref, src_ident, pk_declared, n_parts, source_count
            )
            if dest_occupied:
                dest_ident = _ident(dest_pk)
                to_copy = []
                for part in partitions:
                    already = _range_count(cur, dest_ref, dest_ident, part)
                    expected = int(part["source_count"])
                    if already == expected:
                        part["action"] = "skip"
                        part["dest_count"] = already
                    elif already == 0:
                        part["action"] = "load"
                        to_copy.append(part)
                    else:
                        _delete_range(cur, dest_ref, dest_ident, part)
                        part["action"] = "reload"
                        to_copy.append(part)
            else:
                to_copy = [{"predicate": "", "params": []}]

        identity = _has_identity(cur, dst_schema, dest_table)
        if identity:
            cur.execute(f"SET IDENTITY_INSERT {dest_ref} ON")  # nosec B608
        try:
            for item in to_copy:
                clause = str(item.get("predicate") or "")
                params = list(item.get("params") or [])
                sql = _insert_select_sql(
                    dest_ref, source_ref, pairs, clause, source_hint
                )
                _exec(cur, sql, params)
        finally:
            if identity:
                try:
                    cur.execute(f"SET IDENTITY_INSERT {dest_ref} OFF")  # nosec B608
                except Exception:
                    logger.debug("IDENTITY_INSERT OFF skipped", exc_info=True)
        conn.commit()

        dest_count = _count(cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "SQL Server→SQL Server copy refused: dest COUNT(*) "
                f"{dest_count} != source snapshot {source_count}"
            )
        if shard_mode == "pk" and pk_map is not None:
            dest_ident = _ident(pk_map[1])
            for part in partitions:
                dest_part = _range_count(cur, dest_ref, dest_ident, part)
                part["dest_count"] = dest_part
                if dest_part != int(part["source_count"]):
                    raise ValueError(
                        "PK range dest COUNT "
                        f"{dest_part} != source {part['source_count']} "
                        f"(lo={part['lo']!r} hi={part['hi']!r})"
                    )
        conn.commit()
        proof = f"dest_count:{dest_count}"
        partition_proof = [
            {
                "lo": _jsonable_bound(p.get("lo")),
                "hi": _jsonable_bound(p.get("hi")),
                "null_shard": bool(p.get("null_shard")),
                "source_count": int(p["source_count"]),
                "dest_count": int(p.get("dest_count") or 0),
                "action": str(p.get("action") or "load"),
            }
            for p in partitions
        ]
        return FastPathResult(
            rows_copied=dest_count,
            source_rows=source_count,
            source_checksum=proof,
            target_rows=dest_count,
            target_checksum=proof,
            source_snapshot={
                "sqlserver_isolation": isolation,
                "same_instance": True,
                "copy_workers": 1,
                "copy_split": "insert_select",
                "copy_partitions": len(partitions) or 1,
                "partitions_skipped": sum(
                    1 for p in partitions if p.get("action") == "skip"
                ),
                "shard_mode": shard_mode if partitions else "serial",
                "partition_proof": partition_proof,
            },
            proof_scope=(
                "partition_dest_count_equals_source_snapshot"
                if partition_proof
                else "dest_count_equals_source_snapshot_count"
            ),
        )
    except Exception:
        if created_here:
            try:
                cur.execute(_drop_sql(dest_ref))
                conn.commit()
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before and pk_map is None:
            try:
                cur.execute(f"TRUNCATE TABLE {dest_ref}")  # nosec B608
                conn.commit()
            except Exception:
                logger.debug("dest truncate after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("SQL Server cursor close skipped", exc_info=True)
        try:
            conn.close()
        except Exception:
            logger.debug("SQL Server connection close skipped", exc_info=True)
