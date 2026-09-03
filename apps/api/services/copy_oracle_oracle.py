"""Oracle → Oracle identity bulk (same-instance INSERT SELECT).

``INSERT INTO dest SELECT … FROM src`` after ``LOCK TABLE src IN SHARE
MODE``. Oracle's INSERT SELECT is statement-consistent; the share lock
holds the source still for COUNT + INSERT in one transaction. Dest
CREATE/DROP is DDL (implicit commit on 21c), so it runs first.

Python does not format a row. Proof is dest ``COUNT(*)`` vs the source
count taken under that lock. A mapped single PK still proves dest
``COUNT(*)`` per key range; a non-empty dest skips complete ranges and
DELETE+reloads partial ones.

Declines (row path keeps quarantine): transforms that change values,
public proxy, cross-host (no DB link / Data Pump yet), copy onto the
same table, occupied dest without a mapped single PK.
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
    pg_mysql_copy_partitions,
    pg_mysql_copy_workers,
)

logger = logging.getLogger(__name__)

_INTEGER_PK_TYPES = frozenset({
    "number",
    "integer",
    "int",
    "smallint",
    "float",
    "binary_float",
    "binary_double",
})

_ORACLE_FAMILY = frozenset({
    "oracle",
    "oracle_db",
    "oracledb",
    "oracle_autonomous",
    "oracle_autonomous_warehouse",
    "amazon_rds_oracle",
})


def oracle_family_name(engine: str | None) -> str:
    raw = (engine or "").strip().lower()
    if raw in _ORACLE_FAMILY:
        return "oracle"
    return raw


def oracle_oracle_insert_select_enabled() -> bool:
    raw = (getenv_brand("ORACLE_ORACLE_INSERT_SELECT", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _norm_ora_host(host: str) -> str:
    h = (host or "").strip().lower()
    if h in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
        return "127.0.0.1"
    return h


def _ora_service(cfg: dict[str, Any]) -> str:
    return str(
        cfg.get("service_name")
        or cfg.get("service")
        or cfg.get("database")
        or ""
    ).strip()


def oracle_same_instance(src_cfg: dict[str, Any], dest_cfg: dict[str, Any]) -> bool:
    """True only when host+port+service are present and equal. Fail closed on blanks."""
    src_host = _norm_ora_host(str(src_cfg.get("host") or ""))
    dest_host = _norm_ora_host(str(dest_cfg.get("host") or ""))
    if not src_host or not dest_host:
        return False
    src_port = int(src_cfg.get("port") or 1521)
    dest_port = int(dest_cfg.get("port") or 1521)
    src_svc = _ora_service(src_cfg).lower()
    dest_svc = _ora_service(dest_cfg).lower()
    if not src_svc or not dest_svc:
        return False
    return src_host == dest_host and src_port == dest_port and src_svc == dest_svc


def _fold(name: str) -> str:
    from services.dialect_profiles import fold_identifier

    return fold_identifier("oracle", name)


def _schema_of(cfg: dict[str, Any], explicit: str | None = None) -> str:
    raw = explicit or cfg.get("schema") or cfg.get("username") or cfg.get("user") or ""
    folded = _fold(str(raw).strip())
    return folded or "DATAFLOW"


def _ident(name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    return quote_sql_identifier(
        _fold(require_safe_identifier(name, preserve_case=True, max_len=128)),
        '"',
    )


def _table_ref(schema: str, table: str) -> str:
    from connectors.sql_identifiers import quote_table_ref

    return quote_table_ref(
        table, schema or "DATAFLOW", dialect="oracle", preserve_case=False
    )


def _oracle_connect(cfg: dict[str, Any]) -> Any:
    import oracledb

    user = str(cfg.get("username") or cfg.get("user") or "")
    password = str(cfg.get("password") or "")
    host = str(cfg.get("host") or "127.0.0.1")
    port = int(cfg.get("port") or 1521)
    service = _ora_service(cfg) or "XEPDB1"
    dsn = str(cfg.get("dsn") or cfg.get("connection_string") or "").strip()
    if not dsn or "://" in dsn.lower() or dsn.lower().startswith("oracle"):
        dsn = f"{host}:{port}/{service}"
    try:
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as exc:
        raise FastPathUnavailable(f"Oracle connect failed: {exc}") from exc


def oracle_cfg_is_public_proxy(cfg: dict[str, Any]) -> bool:
    """True when host, connection_string, or dsn names a public TCP proxy."""
    from connectors.write_resilience import is_public_proxy_host

    return any(
        is_public_proxy_host(str(cfg.get(key) or ""))
        for key in ("host", "connection_string", "dsn")
    )


def _format_ora_type(
    data_type: str, data_length: Any, data_precision: Any, data_scale: Any
) -> str:
    t = (data_type or "").strip().upper()
    if t in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"}:
        n = int(data_length or 1)
        return f"{t}({max(1, n)})"
    if t == "NUMBER":
        if data_precision is None:
            return "NUMBER"
        if data_scale is None or int(data_scale) == 0:
            return f"NUMBER({int(data_precision)})"
        return f"NUMBER({int(data_precision)},{int(data_scale)})"
    if t.startswith("TIMESTAMP"):
        return t
    if t in {"DATE", "CLOB", "BLOB", "NCLOB", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "JSON"}:
        return t
    return t or "VARCHAR2(4000)"


def _ora_table_pk_and_types(
    cur: Any, schema: str, table: str, columns: list[str]
) -> tuple[list[str], dict[str, str]]:
    owner = _fold(schema)
    tbl = _fold(table)
    cur.execute(
        "SELECT column_name, data_type, data_length, data_precision, data_scale "
        "FROM all_tab_columns WHERE owner = :1 AND table_name = :2 "
        "ORDER BY column_id",
        [owner, tbl],
    )
    types: dict[str, str] = {}
    for name, data_type, data_length, data_precision, data_scale in cur.fetchall() or []:
        types[str(name)] = _format_ora_type(
            str(data_type or ""), data_length, data_precision, data_scale
        )
    live_l = {k.lower(): v for k, v in types.items()}
    missing = [c for c in columns if c.lower() not in live_l]
    if missing:
        raise FastPathUnavailable(f"source column {missing[0]!r} absent")
    cur.execute(
        "SELECT cols.column_name FROM all_constraints cons "
        "JOIN all_cons_columns cols ON cons.owner = cols.owner "
        "AND cons.constraint_name = cols.constraint_name "
        "WHERE cons.owner = :1 AND cons.table_name = :2 "
        "AND cons.constraint_type = 'P' ORDER BY cols.position",
        [owner, tbl],
    )
    pk = [str(r[0]) for r in cur.fetchall() or []]
    return pk, types


def _table_exists(cur: Any, schema: str, table: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
        [_fold(schema), _fold(table)],
    )
    return int(cur.fetchone()[0]) > 0


def _drop_sql(dest_ref: str) -> str:
    return (
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "  # nosec B608
        f"{dest_ref} PURGE'; "
        "EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


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
        constraint = f"PK_{_fold(dest_table)}"[:128]
        cols.append(f"CONSTRAINT {_ident(constraint)} PRIMARY KEY ({pk_sql})")
    return f"CREATE TABLE {dest_ref} ({', '.join(cols)})"


def _count(cur: Any, table_ref: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
    return int(cur.fetchone()[0])


def oracle_pk_range_clause(
    ident: str, lo: Any, hi: Any, *, null_shard: bool = False
) -> tuple[str, list[Any]]:
    if null_shard:
        return f"{ident} IS NULL", []
    parts: list[str] = []
    params: list[Any] = []
    if lo is not None:
        parts.append(f"{ident} >= :{len(params) + 1}")
        params.append(lo)
    if hi is not None:
        parts.append(f"{ident} < :{len(params) + 1}")
        params.append(hi)
    if not parts:
        return "1=1", []
    return " AND ".join(parts), params


def _exec(cur: Any, sql: str, params: list[Any] | None = None) -> None:
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)


def _range_count(
    cur: Any, table_ref: str, dest_ident: str, part: dict[str, Any]
) -> int:
    clause, params = oracle_pk_range_clause(
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
    clause, params = oracle_pk_range_clause(
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


def _insert_select_sql(
    dest_ref: str,
    source_ref: str,
    pairs: list[tuple[str, str]],
    clause: str,
    *,
    append: bool = False,
) -> str:
    dest_cols = ", ".join(_ident(t) for _s, t in pairs)
    src_cols = ", ".join(_ident(s) for s, _t in pairs)
    where = f" WHERE {clause}" if clause and clause != "1=1" else ""
    hint = " /*+ APPEND */" if append else ""
    return (
        f"INSERT{hint} INTO {dest_ref} ({dest_cols}) "  # nosec B608
        f"SELECT {src_cols} FROM {source_ref}{where}"
    )


def _fetch_ora_pk_interior_cuts(
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
            f"ORDER BY {pk_ident} OFFSET :1 ROWS FETCH NEXT 1 ROWS ONLY",
            [off],
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            cuts.append(row[0])
    return cuts


def _pk_is_integer(declared: str) -> bool:
    raw = (declared or "").strip().upper()
    base = raw.split("(")[0].strip().lower()
    if base not in _INTEGER_PK_TYPES:
        return False
    if base == "number" and "(" in raw:
        inside = raw[raw.find("(") + 1 : raw.find(")")]
        parts = [p.strip() for p in inside.split(",")]
        if len(parts) == 2 and parts[1] not in {"0", ""}:
            return False
    return True


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
    elif _pk_is_integer(pk_declared):
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
        cuts = _fetch_ora_pk_interior_cuts(src_cur, table_q, src_ident, n_parts)
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
        clause, params = oracle_pk_range_clause(src_ident, lo, hi)
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


def copy_oracle_to_oracle(
    *,
    source_cfg: dict[str, Any],
    source_table: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    oracle_ddls: list[str],
    replace_destination: bool,
    source_schema: str | None = None,
    dest_schema: str | None = None,
) -> FastPathResult:
    """Identity Oracle→Oracle. Dest COUNT(*) is the proof."""
    if not pairs or len(pairs) != len(oracle_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if not oracle_oracle_insert_select_enabled():
        raise FastPathUnavailable("Oracle INSERT SELECT disabled")

    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or "") or is_public_proxy_host(
        dest_cfg.get("connection_string") or ""
    ) or is_public_proxy_host(source_cfg.get("host") or ""):
        raise FastPathUnavailable("public proxy: Oracle bulk copy not assumed")

    if not oracle_same_instance(source_cfg, dest_cfg):
        raise FastPathUnavailable(
            "cross-host Oracle stays on the row path (no Data Pump / DB link yet)"
        )

    src_schema = _schema_of(source_cfg, source_schema)
    dst_schema = _schema_of(dest_cfg, dest_schema)
    if (
        src_schema.lower() == dst_schema.lower()
        and source_table.lower() == dest_table.lower()
    ):
        raise FastPathUnavailable("refusing copy onto the same Oracle table")

    source_cols = [p[0] for p in pairs]
    source_ref = _table_ref(src_schema, source_table)
    dest_ref = _table_ref(dst_schema, dest_table)

    conn = _oracle_connect(dest_cfg)
    created_here = False
    existed_before = False
    pk_map: tuple[str, str] | None = None
    cur = conn.cursor()
    try:
        pk_cols, live = _ora_table_pk_and_types(
            cur, src_schema, source_table, source_cols
        )
        live_l = {k.lower(): v for k, v in live.items()}
        create_ddls: list[str] = []
        for i, col in enumerate(source_cols):
            create_ddls.append(live_l.get(col.lower()) or oracle_ddls[i])
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
            exists = False
        if exists:
            dest_occupied = _count(cur, dest_ref) > 0
            if dest_occupied and pk_map is None:
                raise FastPathUnavailable(
                    "append into non-empty Oracle dest stays on the row path"
                )
        else:
            cur.execute(
                _create_sql(dest_ref, dest_table, pairs, create_ddls, pk_dest)
            )
            created_here = True

        cur.execute(f"LOCK TABLE {source_ref} IN SHARE MODE")  # nosec B608
        source_count = _count(cur, source_ref)
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

        use_append = (
            not dest_occupied
            and len(to_copy) == 1
            and not str(to_copy[0].get("predicate") or "")
        )
        copy_split = "insert_select_append" if use_append else "insert_select"
        for item in to_copy:
            clause = str(item.get("predicate") or "")
            params = list(item.get("params") or [])
            sql = _insert_select_sql(
                dest_ref, source_ref, pairs, clause, append=use_append
            )
            _exec(cur, sql, params)
        conn.commit()

        dest_count = _count(cur, dest_ref)
        if dest_count != source_count:
            raise ValueError(
                "Oracle→Oracle copy refused: dest COUNT(*) "
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
                "oracle_lock": "share",
                "same_instance": True,
                "copy_workers": 1,
                "copy_split": copy_split,
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
            except Exception:
                logger.debug("dest drop after copy failure skipped", exc_info=True)
        elif existed_before and pk_map is None:
            try:
                cur.execute(f"TRUNCATE TABLE {dest_ref}")  # nosec B608
            except Exception:
                logger.debug("dest truncate after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            cur.close()
        except Exception:
            logger.debug("Oracle cursor close skipped", exc_info=True)
        try:
            conn.close()
        except Exception:
            logger.debug("Oracle connection close skipped", exc_info=True)
