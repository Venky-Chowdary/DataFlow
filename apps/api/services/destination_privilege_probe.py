"""Destination privilege probe — enterprise G2 write/create honesty.

Connectivity alone must not imply write access. This module measures
``can_write`` / ``can_create_table`` via **metadata privilege checks**, never
by creating tables or inserting rows into the operator's production schema.

Implemented engines
-------------------
* PostgreSQL / Redshift — ``has_*_privilege``
* MySQL / MariaDB — ``SHOW GRANTS``
* Snowflake — ``SHOW GRANTS TO ROLE``
* BigQuery — dataset ``access_entries`` (IAM / legacy roles)
* SQL Server — ``HAS_PERMS_BY_NAME``
* Oracle — ``SESSION_PRIVS`` / ``ALL_TAB_PRIVS``
* SQLite — filesystem ``os.access`` + ``sqlite_master``
* MongoDB — ``connectionStatus.showPrivileges`` (never insert)
* Redis — ``ACL WHOAMI`` / ``ACL GETUSER`` (never SET/DEL)
* Kafka — AdminClient ``describe_acls`` (never produce)
* Elasticsearch — ``security.has_privileges`` (never index a probe doc)
* S3 — ``GetBucketAcl`` grant parse (never PutObject)

Contract
--------
* ``status="ok"`` — privileges measured; ``can_write`` / ``can_create_table`` are booleans.
* ``status="denied"`` — explicit lack of required privilege (fail closed).
* ``status="unavailable"`` — engine unsupported or privilege catalog inaccessible.
  Callers must **not** flake as a hard block; fall back to connectivity with a
  warning so generic_sql / restricted information_schema do not false-fail Validate.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import asdict, dataclass
from typing import Any, Literal

logger = logging.getLogger(__name__)

PrivilegeStatus = Literal["ok", "denied", "unavailable"]

# Engines with a real metadata privilege probe implemented below.
_SUPPORTED = frozenset({
    "postgresql",
    "mysql",
    "redshift",
    "snowflake",
    "bigquery",
    "sqlserver",
    "mssql",
    "oracle",
    "sqlite",
    "mongodb",
    "redis",
    "kafka",
    "elasticsearch",
    "opensearch",
    "s3",
    "minio",
})

_MONGO_WRITE_ACTIONS = frozenset({
    "insert", "update", "remove", "delete", "findAndModify", "anyAction",
})
_MONGO_CREATE_ACTIONS = frozenset({
    "createCollection", "createIndex", "anyAction",
})
_REDIS_WRITE_CMDS = frozenset({
    "+@all", "+@write", "+set", "+hset", "+hmset", "+json.set", "+mset", "+incr",
    "+lpush", "+rpush", "+sadd", "+zadd", "+xadd",
})
_KAFKA_WRITE_OPS = frozenset({"WRITE", "ALL", "ANY"})
_KAFKA_CREATE_OPS = frozenset({"CREATE", "ALL", "ANY"})
_ES_WRITE_PRIVS = frozenset({"index", "write", "create", "create_doc"})
_ES_CREATE_PRIVS = frozenset({"create_index", "manage"})
_S3_WRITE_PERMS = frozenset({"FULL_CONTROL", "WRITE", "WRITE_ACP"})

_BQ_WRITE_ROLES = frozenset({
    "OWNER",
    "WRITER",
    "roles/bigquery.admin",
    "roles/bigquery.dataOwner",
    "roles/bigquery.dataEditor",
    "roles/owner",
    "roles/editor",
})

_SF_WRITE_PRIVS = frozenset({
    "INSERT", "UPDATE", "DELETE", "OWNERSHIP", "ALL", "ALL PRIVILEGES",
})
_SF_CREATE_PRIVS = frozenset({
    "CREATE TABLE", "CREATE ANY TABLE", "OWNERSHIP", "ALL", "ALL PRIVILEGES", "CREATE SCHEMA",
})


@dataclass(frozen=True)
class PrivilegeProbeResult:
    can_write: bool | None
    can_create_table: bool | None
    status: PrivilegeStatus
    detail: str
    engine: str = ""
    method: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalize_engine(db_type: str) -> str:
    engine = (db_type or "").strip().lower()
    if engine in {"amazon_redshift", "redshift"}:
        return "redshift"
    if engine in {
        "amazon_aurora_postgresql", "amazon_rds_postgresql", "supabase", "neon", "pgvector",
    }:
        return "postgresql"
    if engine in {
        "mariadb", "amazon_aurora_mysql", "amazon_rds_mysql", "planetscale",
    }:
        return "mysql"
    if engine in {"mssql", "microsoft_sql_server", "azure_sql"}:
        return "sqlserver"
    if engine in {"google_bigquery"}:
        return "bigquery"
    if engine in {"mongo", "documentdb", "amazon_documentdb"}:
        return "mongodb"
    if engine in {"valkey", "keydb", "dragonfly"}:
        return "redis"
    if engine in {"confluent_kafka", "amazon_msk", "redpanda"}:
        return "kafka"
    if engine in {"opensearch", "amazon_elasticsearch", "elastic_cloud"}:
        return "elasticsearch"
    if engine in {"minio", "wasabi", "backblaze_b2", "digitalocean_spaces", "cloudflare_r2", "amazon_s3"}:
        return "s3"
    return engine


def _finalize(
    *,
    engine: str,
    can_write: bool,
    can_create: bool,
    table_exists: bool,
    table: str,
    schema: str,
    need_update: bool,
    method: str,
    write_action: str = "INSERT",
    create_action: str = "CREATE",
) -> PrivilegeProbeResult:
    """Map measured flags into ok/denied with operator-facing detail."""
    target = f"{schema}.{table}" if schema and table else (table or schema or "destination")
    if table_exists and table and not can_write:
        update_suffix = "/UPDATE" if need_update and write_action == "INSERT" else ""
        return PrivilegeProbeResult(
            can_write=False,
            can_create_table=can_create,
            status="denied",
            detail=f"User can connect but lacks {write_action}{update_suffix} on {target}",
            engine=engine,
            method=method,
        )
    if not table_exists and not can_create:
        return PrivilegeProbeResult(
            can_write=False,
            can_create_table=False,
            status="denied",
            detail=f"User can connect but lacks {create_action} on '{schema or target}'",
            engine=engine,
            method=method,
        )
    return PrivilegeProbeResult(
        can_write=can_write,
        can_create_table=can_create,
        status="ok",
        detail=(
            f"{engine} privileges: write={'yes' if can_write else 'no'}, "
            f"create={'yes' if can_create else 'no'}"
        ),
        engine=engine,
        method=method,
    )


def probe_destination_privileges(
    db_type: str,
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    schema: str = "",
    table: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    table_exists: bool = False,
    sync_mode: str = "",
    ssl: bool = False,
    warehouse: str = "",
    role: str = "",
    account: str = "",
    project_id: str = "",
    dataset: str = "",
    service_account: str = "",
    location: str = "",
    auth_source: str = "",
    api_key: str = "",
) -> PrivilegeProbeResult:
    """Probe write/create privileges for a destination without mutating data."""
    engine = _normalize_engine(db_type)

    if engine not in _SUPPORTED:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=f"Privilege metadata probe not implemented for '{engine}'",
            engine=engine,
        )

    need_update = "upsert" in (sync_mode or "").lower() or "merge" in (sync_mode or "").lower()
    sch = (schema or dataset or "").strip()
    tbl = (table or "").strip()

    try:
        if engine in {"postgresql", "redshift"}:
            return _probe_postgres_family(
                engine=engine,
                host=host,
                port=port or (5439 if engine == "redshift" else 5432),
                database=database,
                schema=sch or "public",
                table=tbl,
                username=username,
                password=password,
                connection_string=connection_string,
                table_exists=table_exists,
                need_update=need_update,
            )
        if engine == "mysql":
            return _probe_mysql(
                host=host,
                port=port or 3306,
                database=database,
                schema=sch or database,
                table=tbl,
                username=username,
                password=password,
                connection_string=connection_string,
                table_exists=table_exists,
                need_update=need_update,
            )
        if engine == "snowflake":
            return _probe_snowflake(
                account=account or host,
                warehouse=warehouse,
                database=database,
                schema=sch or "PUBLIC",
                table=tbl,
                username=username,
                password=password,
                role=role,
                connection_string=connection_string,
                table_exists=table_exists,
                need_update=need_update,
            )
        if engine == "bigquery":
            return _probe_bigquery(
                project_id=project_id or database,
                dataset=dataset or sch,
                table=tbl,
                service_account=service_account or password,
                location=location,
                host=host,
                port=port,
                connection_string=connection_string,
                table_exists=table_exists,
            )
        if engine == "sqlserver":
            return _probe_sqlserver(
                host=host,
                port=port or 1433,
                database=database,
                schema=sch or "dbo",
                table=tbl,
                username=username,
                password=password,
                connection_string=connection_string,
                ssl=ssl,
                table_exists=table_exists,
                need_update=need_update,
            )
        if engine == "oracle":
            return _probe_oracle(
                host=host,
                port=port or 1521,
                database=database,
                schema=sch or (username or "").upper(),
                table=tbl,
                username=username,
                password=password,
                connection_string=connection_string,
                ssl=ssl,
                table_exists=table_exists,
                need_update=need_update,
            )
        if engine == "sqlite":
            return _probe_sqlite(
                database=database,
                connection_string=connection_string,
                host=host,
                table=tbl,
                table_exists=table_exists,
            )
        if engine == "mongodb":
            return _probe_mongodb(
                host=host,
                port=port or 27017,
                database=database,
                collection=tbl,
                username=username,
                password=password,
                connection_string=connection_string,
                ssl=ssl,
                auth_source=auth_source,
                table_exists=table_exists,
                need_update=need_update,
            )
        if engine == "redis":
            return _probe_redis(
                host=host,
                port=port or 6379,
                database=database,
                username=username,
                password=password,
                connection_string=connection_string,
                ssl=ssl,
                key_prefix=tbl or sch,
                table_exists=table_exists,
            )
        if engine == "kafka":
            return _probe_kafka(
                host=host,
                port=port or 9092,
                connection_string=connection_string,
                username=username,
                password=password or api_key,
                security_protocol=sch,
                sasl_mechanism=database or "PLAIN",
                topic=tbl,
                table_exists=table_exists,
            )
        if engine == "elasticsearch":
            return _probe_elasticsearch(
                host=host,
                port=port or 9200,
                index=tbl or database,
                username=username,
                password=password,
                connection_string=connection_string,
                ssl=ssl,
                api_key=api_key or service_account,
                table_exists=table_exists,
            )
        if engine == "s3":
            return _probe_s3(
                host=host,
                port=port,
                bucket=database or tbl,
                username=username,
                password=password,
                connection_string=connection_string,
                ssl=ssl,
                key_prefix=tbl or sch,
                table_exists=table_exists,
            )
    except Exception as exc:  # noqa: BLE001 — never flake Validate on probe errors
        logger.info("privilege probe unavailable for %s: %s", engine, exc)
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=f"Privilege probe unavailable: {exc}",
            engine=engine,
        )

    return PrivilegeProbeResult(
        can_write=None,
        can_create_table=None,
        status="unavailable",
        detail=f"Privilege metadata probe not implemented for '{engine}'",
        engine=engine,
    )


# ── PostgreSQL / Redshift ────────────────────────────────────────────────────

def _probe_postgres_family(
    *,
    engine: str,
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    username: str,
    password: str,
    connection_string: str,
    table_exists: bool,
    need_update: bool,
) -> PrivilegeProbeResult:
    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT has_schema_privilege(current_user, %s, 'CREATE')",
                (schema,),
            )
            can_create = bool(cur.fetchone()[0])

            can_insert = False
            can_update = False
            if table_exists and table:
                cur.execute(
                    "SELECT has_table_privilege(current_user, %s, 'INSERT')",
                    (f"{schema}.{table}",),
                )
                can_insert = bool(cur.fetchone()[0])
                if need_update:
                    cur.execute(
                        "SELECT has_table_privilege(current_user, %s, 'UPDATE')",
                        (f"{schema}.{table}",),
                    )
                    can_update = bool(cur.fetchone()[0])
                else:
                    can_update = True
            elif not table_exists:
                can_insert = can_create
                can_update = True
            else:
                can_insert = can_create
                can_update = True

        can_write = bool(can_insert and (can_update if need_update else True))
        return _finalize(
            engine=engine,
            can_write=can_write,
            can_create=can_create,
            table_exists=table_exists,
            table=table,
            schema=schema,
            need_update=need_update,
            method="has_table_privilege/has_schema_privilege",
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ── MySQL ────────────────────────────────────────────────────────────────────

_MYSQL_GRANT_RE = re.compile(
    r"GRANT\s+(.+?)\s+ON\s+(`?[^`\s]+`?(?:\.`?[^`\s]+`?)?|\*\.\*)\s+TO\s+",
    re.IGNORECASE | re.DOTALL,
)


def _mysql_ident(name: str) -> str:
    return (name or "").strip().strip("`").lower()


def _mysql_grant_covers(
    privileges: set[str],
    scope: str,
    *,
    database: str,
    table: str,
    needed: set[str],
) -> bool:
    """Return True when a GRANT scope covers database/table and includes needed privs."""
    if "ALL" in privileges or "ALL PRIVILEGES" in privileges:
        priv_ok = True
    else:
        priv_ok = needed.issubset(privileges)
    if not priv_ok:
        return False

    scope_n = scope.replace("`", "").lower().strip()
    db = _mysql_ident(database)
    tbl = _mysql_ident(table)
    if scope_n == "*.*":
        return True
    if "." not in scope_n:
        return False
    scope_db, _, scope_tbl = scope_n.partition(".")
    if scope_db != db and scope_db != "*":
        return False
    if scope_tbl in {"*", ""}:
        return True
    return bool(tbl) and scope_tbl == tbl


def _probe_mysql(
    *,
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    username: str,
    password: str,
    connection_string: str,
    table_exists: bool,
    need_update: bool,
) -> PrivilegeProbeResult:
    from connectors.mysql_conn import get_connection

    db_name = schema or database
    conn = get_connection(
        host=host,
        port=port,
        database=database or db_name,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW GRANTS FOR CURRENT_USER()")
            rows = cur.fetchall()
        grant_lines = [str(r[0]) for r in rows if r]

        can_create = False
        can_insert = False
        can_update = False
        for line in grant_lines:
            m = _MYSQL_GRANT_RE.search(line)
            if not m:
                upper = line.upper()
                if "ALL PRIVILEGES" in upper and " ON " in upper:
                    can_create = can_insert = can_update = True
                    break
                continue
            priv_raw = m.group(1).upper()
            scope = m.group(2)
            privileges = {p.strip() for p in priv_raw.split(",") if p.strip()}
            if _mysql_grant_covers(
                privileges, scope, database=db_name, table=table or "*", needed={"CREATE"}
            ):
                can_create = True
            if _mysql_grant_covers(
                privileges, scope, database=db_name, table=table or "*", needed={"INSERT"}
            ):
                can_insert = True
            if _mysql_grant_covers(
                privileges, scope, database=db_name, table=table or "*", needed={"UPDATE"}
            ):
                can_update = True

        if not table_exists:
            can_insert = can_create
            can_update = True
        elif not need_update:
            can_update = True

        can_write = bool(can_insert and can_update)
        return _finalize(
            engine="mysql",
            can_write=can_write,
            can_create=can_create,
            table_exists=table_exists,
            table=table,
            schema=db_name,
            need_update=need_update,
            method="SHOW GRANTS",
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ── Snowflake ────────────────────────────────────────────────────────────────

def _probe_snowflake(
    *,
    account: str,
    warehouse: str,
    database: str,
    schema: str,
    table: str,
    username: str,
    password: str,
    role: str,
    connection_string: str,
    table_exists: bool,
    need_update: bool,
) -> PrivilegeProbeResult:
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        username=username,
        password=password,
        role=role,
        connection_string=connection_string,
    )
    try:
        cur = conn.cursor()
        try:
            current_role = (role or "").strip()
            try:
                cur.execute("SELECT CURRENT_ROLE()")
                row = cur.fetchone()
                if row and row[0]:
                    current_role = str(row[0]).strip()
            except Exception:
                pass

            grant_rows: list[tuple[Any, ...]] = []
            if current_role:
                try:
                    # Identifier quote — role names are case-sensitive when quoted.
                    cur.execute(f'SHOW GRANTS TO ROLE "{current_role}"')
                    grant_rows = list(cur.fetchall())
                except Exception:
                    cur.execute("SHOW GRANTS")
                    grant_rows = list(cur.fetchall())
            else:
                cur.execute("SHOW GRANTS")
                grant_rows = list(cur.fetchall())

            grants = _snowflake_privileges_from_rows(grant_rows)
            can_write, can_create = evaluate_snowflake_privileges(
                grants,
                database=database,
                schema=schema,
                table=table,
                table_exists=table_exists,
                need_update=need_update,
            )
            return _finalize(
                engine="snowflake",
                can_write=can_write,
                can_create=can_create,
                table_exists=table_exists,
                table=table,
                schema=schema,
                need_update=need_update,
                method="SHOW GRANTS TO ROLE",
            )
        finally:
            cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _snowflake_privileges_from_rows(rows: list[tuple[Any, ...]]) -> list[dict[str, str]]:
    """Normalize SHOW GRANTS rows into {privilege, granted_on, name}."""
    out: list[dict[str, str]] = []
    for row in rows:
        if not row or len(row) < 4:
            continue
        out.append({
            "privilege": str(row[1] if len(row) > 1 else "").strip().upper(),
            "granted_on": str(row[2] if len(row) > 2 else "").strip().upper(),
            "name": str(row[3] if len(row) > 3 else "").strip(),
        })
    return out


def evaluate_snowflake_privileges(
    grants: list[dict[str, str]],
    *,
    database: str,
    schema: str,
    table: str,
    table_exists: bool,
    need_update: bool = False,
) -> tuple[bool, bool]:
    """Evaluate Snowflake grant dicts → (can_write, can_create). Public for tests."""
    db_u = (database or "").upper()
    sch_u = (schema or "").upper()
    tbl_u = (table or "").upper()
    fq_table = f"{db_u}.{sch_u}.{tbl_u}" if db_u else f"{sch_u}.{tbl_u}"
    fq_schema = f"{db_u}.{sch_u}" if db_u else sch_u

    can_insert = False
    can_update = False
    can_create = False

    for g in grants:
        priv = (g.get("privilege") or "").upper()
        granted_on = (g.get("granted_on") or "").upper()
        name = (g.get("name") or "").upper()

        name_matches_table = name == fq_table or name.endswith(f".{tbl_u}") or name == tbl_u
        name_matches_schema = (
            name == fq_schema
            or name.endswith(f".{sch_u}")
            or name == sch_u
        )
        name_matches_db = name == db_u

        broad = priv in ("OWNERSHIP", "ALL", "ALL PRIVILEGES")
        scope_table = granted_on in ("TABLE", "VIEW") and name_matches_table
        scope_schema = granted_on == "SCHEMA" and name_matches_schema
        scope_db = granted_on == "DATABASE" and name_matches_db

        if broad and (scope_table or scope_schema or scope_db or not granted_on):
            can_insert = can_update = can_create = True
            continue

        if priv == "INSERT" and (scope_table or scope_schema or scope_db):
            can_insert = True
        if priv == "UPDATE" and (scope_table or scope_schema or scope_db):
            can_update = True
        if priv in _SF_CREATE_PRIVS and (scope_schema or scope_db):
            can_create = True
        if priv == "CREATE TABLE" and (scope_schema or scope_db or granted_on in ("SCHEMA", "DATABASE", "")):
            can_create = True

    if not table_exists:
        can_insert = can_create
        can_update = True
    elif not need_update:
        can_update = True

    can_write = bool(can_insert and (can_update if need_update else True))
    return can_write, can_create


# ── BigQuery ─────────────────────────────────────────────────────────────────

def _probe_bigquery(
    *,
    project_id: str,
    dataset: str,
    table: str,
    service_account: str,
    location: str,
    host: str,
    port: int,
    connection_string: str,
    table_exists: bool,
) -> PrivilegeProbeResult:
    from connectors.bigquery_conn import get_client

    if not project_id or not dataset:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail="project_id and dataset required for BigQuery privilege probe",
            engine="bigquery",
        )

    client = get_client(
        project_id=project_id,
        service_account=service_account,
        location=location,
        host=host,
        port=port,
        connection_string=connection_string,
    )

    exists = table_exists
    if table:
        try:
            client.get_table(f"{project_id}.{dataset}.{table}")
            exists = True
        except Exception:
            # Prefer explicit get_table miss over caller heuristic when reachable.
            if not table_exists:
                exists = False

    try:
        ds = client.get_dataset(f"{project_id}.{dataset}")
    except Exception as exc:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=f"Cannot read BigQuery dataset metadata: {exc}",
            engine="bigquery",
        )

    entries = list(getattr(ds, "access_entries", None) or [])
    can_write, can_create, matched = evaluate_bigquery_access_entries(entries)

    # Dataset WRITER/OWNER implies both insert and create; if table missing and
    # no write role, deny create path.
    return _finalize(
        engine="bigquery",
        can_write=can_write if exists else can_create,
        can_create=can_create,
        table_exists=bool(exists),
        table=table,
        schema=dataset,
        need_update=False,
        method=f"dataset.access_entries({matched or 'none'})",
    )


def evaluate_bigquery_access_entries(
    entries: list[Any],
) -> tuple[bool, bool, str]:
    """Map BigQuery AccessEntry list → (can_write, can_create, matched_role)."""
    matched = ""
    for entry in entries:
        if hasattr(entry, "role"):
            role = str(entry.role or "")
        elif isinstance(entry, dict):
            role = str(entry.get("role") or "")
        else:
            role = str(entry)
        role_norm = role.strip()
        if role_norm in _BQ_WRITE_ROLES or role_norm.upper() in {"OWNER", "WRITER"}:
            return True, True, role_norm
        rl = role_norm.lower()
        if "dataeditor" in rl or "dataowner" in rl or rl.endswith(".admin"):
            return True, True, role_norm
        if not matched and role_norm:
            matched = role_norm
    return False, False, matched


# ── SQL Server ───────────────────────────────────────────────────────────────

def _probe_sqlserver(
    *,
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    table_exists: bool,
    need_update: bool,
) -> PrivilegeProbeResult:
    from connectors.generic_sql import get_sqlalchemy_engine
    import sqlalchemy as sa

    engine = get_sqlalchemy_engine({
        "type": "sqlserver",
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "schema": schema,
        "connection_string": connection_string,
        "ssl": ssl,
    })
    with engine.connect() as conn:
        # Prefer live OBJECT_ID when reachable; fall back to caller flag.
        exists = table_exists
        if table:
            try:
                oid = conn.execute(
                    sa.text("SELECT OBJECT_ID(:obj, 'U')"),
                    {"obj": f"{schema}.{table}"},
                ).scalar()
                exists = oid is not None
            except Exception:
                pass

        insert_perm = False
        update_perm = False
        if exists and table:
            insert_perm = bool(
                conn.execute(
                    sa.text("SELECT HAS_PERMS_BY_NAME(:obj, 'OBJECT', 'INSERT')"),
                    {"obj": f"{schema}.{table}"},
                ).scalar()
            )
            if need_update:
                update_perm = bool(
                    conn.execute(
                        sa.text("SELECT HAS_PERMS_BY_NAME(:obj, 'OBJECT', 'UPDATE')"),
                        {"obj": f"{schema}.{table}"},
                    ).scalar()
                )
            else:
                update_perm = True

        create_perm = bool(
            conn.execute(
                sa.text("SELECT HAS_PERMS_BY_NAME(:sch, 'SCHEMA', 'CREATE TABLE')"),
                {"sch": schema},
            ).scalar()
        )

        if not exists:
            can_write = create_perm
            can_create = create_perm
        else:
            can_write = bool(insert_perm and (update_perm if need_update else True))
            can_create = create_perm

        return _finalize(
            engine="sqlserver",
            can_write=can_write,
            can_create=can_create,
            table_exists=bool(exists),
            table=table,
            schema=schema,
            need_update=need_update,
            method="HAS_PERMS_BY_NAME",
        )


# ── Oracle ───────────────────────────────────────────────────────────────────

def _probe_oracle(
    *,
    host: str,
    port: int,
    database: str,
    schema: str,
    table: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    table_exists: bool,
    need_update: bool,
) -> PrivilegeProbeResult:
    from connectors.generic_sql import get_sqlalchemy_engine
    import sqlalchemy as sa

    engine = get_sqlalchemy_engine({
        "type": "oracle",
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "schema": schema,
        "connection_string": connection_string,
        "ssl": ssl,
    })
    owner = (schema or username or "").upper()
    tbl_u = (table or "").upper()

    with engine.connect() as conn:
        exists = table_exists
        if tbl_u:
            try:
                cnt = conn.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM all_tables "
                        "WHERE owner = :owner AND table_name = :tbl"
                    ),
                    {"owner": owner, "tbl": tbl_u},
                ).scalar()
                exists = int(cnt or 0) > 0
            except Exception:
                pass

        session_privs = {
            str(r[0]).upper()
            for r in conn.execute(sa.text("SELECT privilege FROM session_privs")).fetchall()
        }
        tab_privs: set[str] = set()
        if tbl_u:
            try:
                tab_privs = {
                    str(r[0]).upper()
                    for r in conn.execute(
                        sa.text(
                            "SELECT privilege FROM all_tab_privs "
                            "WHERE table_schema = :owner AND table_name = :tbl"
                        ),
                        {"owner": owner, "tbl": tbl_u},
                    ).fetchall()
                }
            except Exception:
                # Some Oracle builds use OWNER instead of TABLE_SCHEMA.
                tab_privs = {
                    str(r[0]).upper()
                    for r in conn.execute(
                        sa.text(
                            "SELECT privilege FROM all_tab_privs "
                            "WHERE owner = :owner AND table_name = :tbl"
                        ),
                        {"owner": owner, "tbl": tbl_u},
                    ).fetchall()
                }

        can_write, can_create = evaluate_oracle_privileges(
            session_privs=session_privs,
            tab_privs=tab_privs,
            table_exists=bool(exists),
            need_update=need_update,
        )
        return _finalize(
            engine="oracle",
            can_write=can_write,
            can_create=can_create,
            table_exists=bool(exists),
            table=table,
            schema=owner,
            need_update=need_update,
            method="session_privs/all_tab_privs",
        )


def evaluate_oracle_privileges(
    *,
    session_privs: set[str],
    tab_privs: set[str],
    table_exists: bool,
    need_update: bool = False,
) -> tuple[bool, bool]:
    """Evaluate Oracle privilege sets → (can_write, can_create). Public for tests."""
    sp = {p.upper() for p in session_privs}
    tp = {p.upper() for p in tab_privs}

    can_create = bool(sp & {"CREATE TABLE", "CREATE ANY TABLE"} or "DBA" in sp)
    can_insert = bool(
        tp & {"INSERT", "ALL"}
        or sp & {"INSERT ANY TABLE"}
        or "DBA" in sp
    )
    can_update = bool(
        tp & {"UPDATE", "ALL"}
        or sp & {"UPDATE ANY TABLE"}
        or "DBA" in sp
    )
    if not table_exists:
        can_insert = can_create
        can_update = True
    elif not need_update:
        can_update = True
    can_write = bool(can_insert and (can_update if need_update else True))
    return can_write, can_create


# ── SQLite ───────────────────────────────────────────────────────────────────

def _probe_sqlite(
    *,
    database: str,
    connection_string: str,
    host: str,
    table: str,
    table_exists: bool,
) -> PrivilegeProbeResult:
    from connectors.sqlite_common import sqlite_file_path

    path = sqlite_file_path(database, connection_string, host)
    if not path:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail="SQLite database path is empty",
            engine="sqlite",
        )

    if path == ":memory:" or path.lower().startswith("sqlite://:memory:"):
        return PrivilegeProbeResult(
            can_write=True,
            can_create_table=True,
            status="ok",
            detail="SQLite in-memory database is always writable in-process",
            engine="sqlite",
            method="filesystem",
        )

    exists = table_exists
    if os.path.isfile(path):
        can_write_fs = os.access(path, os.W_OK)
        parent = os.path.dirname(path) or "."
        parent_ok = os.access(parent, os.W_OK)
        if table:
            try:
                import sqlite3
                con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
                try:
                    row = con.execute(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                        (table,),
                    ).fetchone()
                    exists = bool(row and row[0])
                finally:
                    con.close()
            except Exception:
                pass
        can_create = can_write_fs and parent_ok
        can_write = can_write_fs if exists else can_create
        return _finalize(
            engine="sqlite",
            can_write=can_write,
            can_create=can_create,
            table_exists=bool(exists),
            table=table,
            schema="main",
            need_update=False,
            method="os.access+sqlite_master",
        )

    parent = os.path.dirname(path) or "."
    parent_ok = os.path.isdir(parent) and os.access(parent, os.W_OK)
    return _finalize(
        engine="sqlite",
        can_write=parent_ok,
        can_create=parent_ok,
        table_exists=False,
        table=table,
        schema="main",
        need_update=False,
        method="os.access",
    )


# ── MongoDB ──────────────────────────────────────────────────────────────────

def _probe_mongodb(
    *,
    host: str,
    port: int,
    database: str,
    collection: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    auth_source: str,
    table_exists: bool,
    need_update: bool,
) -> PrivilegeProbeResult:
    from connectors.mongodb_common import _mongo_client, normalize_mongodb_connection_string

    db_name = (database or "").strip()
    if not db_name and connection_string:
        from connectors.mongodb_common import mongodb_database_from_uri
        db_name = mongodb_database_from_uri(connection_string)
    if not db_name:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail="MongoDB database name required for privilege probe",
            engine="mongodb",
        )

    uri = normalize_mongodb_connection_string(
        connection_string,
        database=db_name,
        host=host,
        port=port,
        username=username,
        password=password,
        ssl=ssl,
        auth_source=auth_source,
    )
    client = _mongo_client(uri)
    # connectionStatus with showPrivileges — read-only privilege catalog.
    try:
        status = client.admin.command({"connectionStatus": 1, "showPrivileges": True})
    except Exception as exc:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=f"MongoDB connectionStatus unavailable: {exc}",
            engine="mongodb",
        )

    auth_info = (status or {}).get("authInfo") or {}
    privileges = list(auth_info.get("authenticatedUserPrivileges") or [])
    roles = list(auth_info.get("authenticatedUserRoles") or [])

    # Unauthenticated local / empty privilege list: cannot assert deny.
    if not privileges and not roles:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=(
                "MongoDB returned no privilege catalog (unauthenticated or restricted); "
                "G2 falls back to connectivity"
            ),
            engine="mongodb",
            method="connectionStatus.showPrivileges",
        )

    exists = table_exists
    if collection:
        try:
            names = set(client[db_name].list_collection_names())
            exists = collection in names
        except Exception:
            pass

    can_write, can_create = evaluate_mongodb_privileges(
        privileges,
        roles=roles,
        database=db_name,
        collection=collection,
        table_exists=bool(exists),
        need_update=need_update,
    )
    return _finalize(
        engine="mongodb",
        can_write=can_write,
        can_create=can_create,
        table_exists=bool(exists),
        table=collection,
        schema=db_name,
        need_update=need_update,
        method="connectionStatus.showPrivileges",
        write_action="insert/update",
        create_action="createCollection",
    )


def evaluate_mongodb_privileges(
    privileges: list[Any],
    *,
    roles: list[Any] | None = None,
    database: str,
    collection: str,
    table_exists: bool,
    need_update: bool = False,
) -> tuple[bool, bool]:
    """Parse connectionStatus privilege docs → (can_write, can_create). Public for tests."""
    db = (database or "").strip()
    coll = (collection or "").strip()
    can_insert = False
    can_update = False
    can_create = False

    # Built-in role shortcuts (Atlas / self-hosted).
    for role in roles or []:
        if isinstance(role, dict):
            name = str(role.get("role") or "").lower()
            role_db = str(role.get("db") or "")
        else:
            name = str(role).lower()
            role_db = ""
        if name in {"root", "dbowner", "dbadmin", "readwriteanydatabase"}:
            return True, True
        if name == "readwrite" and (not role_db or role_db == db):
            can_insert = can_update = can_create = True
        if name == "read" and (not role_db or role_db == db):
            pass  # read-only

    for priv in privileges:
        if not isinstance(priv, dict):
            continue
        resource = priv.get("resource") or {}
        actions = {str(a) for a in (priv.get("actions") or [])}
        if not actions:
            continue

        if resource.get("anyResource") is True or "anyAction" in actions:
            return True, True

        res_db = str(resource.get("db") or "")
        res_coll = str(resource.get("collection") or "")
        # Empty collection string means all collections in db.
        db_ok = res_db in {"", db} or res_db == db
        coll_ok = (not coll) or res_coll in {"", coll} or res_coll == coll
        if not db_ok:
            continue
        if coll and res_coll not in {"", coll} and res_coll != coll:
            # Cluster-wide db resource with empty collection still covers.
            if res_coll:
                continue

        if actions & _MONGO_WRITE_ACTIONS or "insert" in actions:
            can_insert = True
        if actions & {"update", "findAndModify", "anyAction"}:
            can_update = True
        if actions & _MONGO_CREATE_ACTIONS:
            can_create = True
        if coll_ok and "insert" in actions:
            can_insert = True

    if not table_exists:
        can_insert = can_insert or can_create
        can_update = True
    elif not need_update:
        can_update = True

    can_write = bool(can_insert and (can_update if need_update else True))
    return can_write, can_create


# ── Redis ────────────────────────────────────────────────────────────────────

def _probe_redis(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    key_prefix: str,
    table_exists: bool,
) -> PrivilegeProbeResult:
    from connectors.redis_reader import _redis_client

    client = _redis_client({
        "host": host,
        "port": port,
        "database": database or "0",
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
    })
    try:
        # Prefer Redis 6+ ACL catalog — never SET/DEL.
        try:
            who = client.execute_command("ACL", "WHOAMI")
            user = who.decode() if isinstance(who, bytes) else str(who or "default")
        except Exception as exc:
            return PrivilegeProbeResult(
                can_write=None,
                can_create_table=None,
                status="unavailable",
                detail=(
                    f"Redis ACL unavailable ({exc}); cannot distinguish read-only "
                    "vs writable without mutating keys — G2 falls back to connectivity"
                ),
                engine="redis",
                method="ACL",
            )

        raw = client.execute_command("ACL", "GETUSER", user)
        commands, key_patterns = parse_redis_acl_getuser(raw)
        can_write, can_create = evaluate_redis_acl(
            commands=commands,
            key_patterns=key_patterns,
            key_prefix=key_prefix or "*",
            table_exists=table_exists,
        )
        return _finalize(
            engine="redis",
            can_write=can_write,
            can_create=can_create,
            table_exists=table_exists and bool(key_prefix),
            table=key_prefix or "*",
            schema="redis",
            need_update=False,
            method=f"ACL GETUSER ({user})",
            write_action="SET/HSET",
            create_action="key-namespace write",
        )
    finally:
        try:
            client.close()
        except Exception:
            pass


def parse_redis_acl_getuser(raw: Any) -> tuple[list[str], list[str]]:
    """Normalize ACL GETUSER reply → (commands, key_patterns). Public for tests."""
    commands: list[str] = []
    key_patterns: list[str] = []
    if raw is None:
        return commands, key_patterns

    # redis-py may return a dict (RESP3) or alternating list (RESP2).
    if isinstance(raw, dict):
        cmds = raw.get("commands") or raw.get(b"commands") or []
        keys = raw.get("keys") or raw.get(b"keys") or []
        flags = raw.get("flags") or raw.get(b"flags") or []
        commands = [_acl_str(c) for c in (cmds if isinstance(cmds, (list, tuple)) else [cmds])]
        key_patterns = [_acl_str(k) for k in (keys if isinstance(keys, (list, tuple)) else [keys])]
        for f in (flags if isinstance(flags, (list, tuple)) else [flags]):
            fs = _acl_str(f).lower()
            if fs in {"allcommands", "allkeys"}:
                commands.append("+@all")
                key_patterns.append("~*")
        return commands, key_patterns

    if isinstance(raw, (list, tuple)):
        i = 0
        while i < len(raw) - 1:
            key = _acl_str(raw[i]).lower()
            val = raw[i + 1]
            if key == "commands":
                if isinstance(val, (list, tuple)):
                    commands.extend(_acl_str(c) for c in val)
                else:
                    commands.append(_acl_str(val))
            elif key == "keys":
                if isinstance(val, (list, tuple)):
                    key_patterns.extend(_acl_str(k) for k in val)
                else:
                    key_patterns.append(_acl_str(val))
            elif key == "flags":
                flags = val if isinstance(val, (list, tuple)) else [val]
                for f in flags:
                    fs = _acl_str(f).lower()
                    if fs == "allcommands":
                        commands.append("+@all")
                    if fs == "allkeys":
                        key_patterns.append("~*")
            i += 2
    return commands, key_patterns


def _acl_str(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value or "")


def evaluate_redis_acl(
    *,
    commands: list[str],
    key_patterns: list[str],
    key_prefix: str,
    table_exists: bool = True,
) -> tuple[bool, bool]:
    """Evaluate Redis ACL command/key rules → (can_write, can_create). Public for tests."""
    cmds = [c.lower() for c in commands]
    # Explicit deny categories win when present without a broader allow.
    denied_write = any(c in {"-@write", "-@all", "-set", "-hset"} for c in cmds)
    allowed_write = any(
        c in {x.lower() for x in _REDIS_WRITE_CMDS} or c.startswith("+@write") or c == "+@all"
        for c in cmds
    )
    # Category allows: +@all implies write unless -@write follows (order-sensitive;
    # Redis applies left-to-right — approximate with: allow if +@all and not -@write).
    if "+@all" in cmds and "-@write" not in cmds and "-set" not in cmds:
        allowed_write = True

    keys_ok = _redis_keys_cover(key_patterns, key_prefix or "*")
    can_write = bool(allowed_write and keys_ok and not (denied_write and not allowed_write))
    if denied_write and "+@all" not in cmds and "+@write" not in cmds:
        can_write = False
    # Redis has no CREATE TABLE — new keys need the same write ACL.
    can_create = can_write
    if not table_exists:
        can_write = can_create
    return can_write, can_create


def _redis_keys_cover(patterns: list[str], key_prefix: str) -> bool:
    if not patterns:
        # No key rule listed — some Redis builds omit keys when allkeys flag set.
        return True
    prefix = (key_prefix or "*").lstrip("~")
    for pat in patterns:
        p = pat.lstrip("~")
        if p in {"*", "~*"} or p == "*":
            return True
        if prefix == "*" or prefix.startswith(p.rstrip("*")) or p.rstrip("*") in {"", prefix}:
            return True
        # prefix:* style
        if p.endswith("*") and prefix.startswith(p[:-1]):
            return True
        if prefix.endswith("*") and p.startswith(prefix[:-1]):
            return True
    return False


# ── Kafka ────────────────────────────────────────────────────────────────────

def _probe_kafka(
    *,
    host: str,
    port: int,
    connection_string: str,
    username: str,
    password: str,
    security_protocol: str,
    sasl_mechanism: str,
    topic: str,
    table_exists: bool,
) -> PrivilegeProbeResult:
    try:
        from kafka.admin import KafkaAdminClient  # type: ignore
    except ImportError as exc:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=f"kafka-python not installed: {exc}",
            engine="kafka",
        )

    from connectors.kafka_writer import _bootstrap

    bootstrap = _bootstrap(host, port, connection_string)
    kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap.split(","),
        "client_id": "dataflow-privilege-probe",
        "request_timeout_ms": 15000,
    }
    if username and password:
        import ssl as ssl_mod
        sec = (security_protocol or "").upper()
        kwargs["security_protocol"] = sec if sec in {"SASL_SSL", "SASL_PLAINTEXT"} else "SASL_SSL"
        kwargs["sasl_mechanism"] = sasl_mechanism or "PLAIN"
        kwargs["sasl_plain_username"] = username
        kwargs["sasl_plain_password"] = password
        if kwargs["security_protocol"] == "SASL_SSL":
            kwargs["ssl_context"] = ssl_mod.create_default_context()

    admin = KafkaAdminClient(**kwargs)
    try:
        exists = table_exists
        if topic:
            try:
                topics = admin.list_topics()
                exists = topic in set(topics or [])
            except Exception:
                pass

        acls: list[dict[str, str]] = []
        describe = getattr(admin, "describe_acls", None)
        if describe is None:
            return PrivilegeProbeResult(
                can_write=None,
                can_create_table=None,
                status="unavailable",
                detail=(
                    "Kafka AdminClient has no describe_acls; cannot prove produce rights "
                    "without writing — G2 falls back to connectivity"
                ),
                engine="kafka",
                method="AdminClient",
            )

        try:
            # Broad ACL visibility check — never produce.
            raw_acls = None
            try:
                from kafka.admin import (  # type: ignore
                    ACLFilter,
                    ACLOperation,
                    ACLPermissionType,
                    ResourcePattern,
                    ResourceType,
                    ACLResourcePatternType,
                )
                pattern = ResourcePattern(
                    ResourceType.TOPIC,
                    topic or "*",
                    getattr(ACLResourcePatternType, "ANY", 1),
                )
                filt = ACLFilter(
                    resource_pattern=pattern,
                    principal=None,
                    host=None,
                    operation=ACLOperation.ANY,
                    permission_type=ACLPermissionType.ANY,
                )
                raw_acls = describe(filt)
            except Exception:
                # Older kafka-python: describe_acls may accept None / empty filter.
                try:
                    raw_acls = describe(None)
                except TypeError:
                    raw_acls = describe()
            acls = normalize_kafka_acls(raw_acls, topic=topic)
        except Exception as exc:
            # Managed Kafka often denies ACL describe to producers.
            return PrivilegeProbeResult(
                can_write=None,
                can_create_table=None,
                status="unavailable",
                detail=(
                    f"Kafka ACL describe unavailable ({exc}); "
                    "G2 falls back to connectivity (no produce probe)"
                ),
                engine="kafka",
                method="describe_acls",
            )

        if not acls:
            return PrivilegeProbeResult(
                can_write=None,
                can_create_table=None,
                status="unavailable",
                detail=(
                    "Kafka returned no visible ACLs for this principal; "
                    "cannot assert WRITE without producing — G2 falls back to connectivity"
                ),
                engine="kafka",
                method="describe_acls",
            )

        can_write, can_create = evaluate_kafka_acls(
            acls,
            topic=topic,
            table_exists=bool(exists),
        )
        return _finalize(
            engine="kafka",
            can_write=can_write,
            can_create=can_create,
            table_exists=bool(exists),
            table=topic,
            schema="kafka",
            need_update=False,
            method="describe_acls",
            write_action="WRITE",
            create_action="CREATE topic",
        )
    finally:
        try:
            admin.close()
        except Exception:
            pass


def normalize_kafka_acls(raw: Any, *, topic: str) -> list[dict[str, str]]:
    """Normalize AdminClient ACL objects/dicts → [{operation, permission, resource}]."""
    out: list[dict[str, str]] = []
    items = raw
    if isinstance(raw, tuple) and len(raw) == 2:
        items = raw[0]  # (acls, error) form
    if not items:
        return out
    for item in items:
        if isinstance(item, dict):
            out.append({
                "operation": str(item.get("operation") or item.get("operation_type") or "").upper(),
                "permission": str(item.get("permission_type") or item.get("permission") or "ALLOW").upper(),
                "resource": str(item.get("resource_name") or item.get("name") or topic or ""),
                "resource_type": str(item.get("resource_type") or "TOPIC").upper(),
            })
            continue
        op = getattr(item, "operation", None)
        perm = getattr(item, "permission_type", None)
        pattern = getattr(item, "resource_pattern", None) or getattr(item, "resource", None)
        name = ""
        rtype = "TOPIC"
        if pattern is not None:
            name = str(getattr(pattern, "resource_name", None) or getattr(pattern, "name", "") or "")
            rtype = str(getattr(pattern, "resource_type", None) or "TOPIC")
            if hasattr(rtype, "name"):
                rtype = str(rtype.name)
        if hasattr(op, "name"):
            op = op.name
        if hasattr(perm, "name"):
            perm = perm.name
        out.append({
            "operation": str(op or "").upper(),
            "permission": str(perm or "ALLOW").upper(),
            "resource": name,
            "resource_type": str(rtype).upper(),
        })
    return out


def evaluate_kafka_acls(
    acls: list[dict[str, str]],
    *,
    topic: str,
    table_exists: bool,
) -> tuple[bool, bool]:
    """Evaluate normalized Kafka ACLs → (can_write, can_create). Public for tests."""
    topic_u = (topic or "").strip()
    can_write = False
    can_create = False
    denied_write = False

    for acl in acls:
        op = (acl.get("operation") or "").upper()
        perm = (acl.get("permission") or "ALLOW").upper()
        resource = (acl.get("resource") or "").strip()
        rtype = (acl.get("resource_type") or "TOPIC").upper()

        resource_ok = (
            not topic_u
            or resource in {"", "*", topic_u}
            or resource == topic_u
            or (rtype == "CLUSTER")
        )
        if not resource_ok and rtype == "TOPIC":
            continue

        if perm == "DENY" and op in _KAFKA_WRITE_OPS:
            denied_write = True
        if perm != "ALLOW":
            continue
        if op in _KAFKA_WRITE_OPS or op in {"WRITE", "ALL"}:
            can_write = True
        if op in _KAFKA_CREATE_OPS or (rtype == "CLUSTER" and op in {"CREATE", "ALL"}):
            can_create = True

    if denied_write:
        can_write = False
    if not table_exists:
        can_write = can_write or can_create
    return can_write, can_create


# ── Elasticsearch / OpenSearch ───────────────────────────────────────────────

def _probe_elasticsearch(
    *,
    host: str,
    port: int,
    index: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    api_key: str,
    table_exists: bool,
) -> PrivilegeProbeResult:
    from connectors.elasticsearch_reader import _client

    if not index:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail="Elasticsearch index name required for privilege probe",
            engine="elasticsearch",
        )

    client = _client({
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
        "api_key": api_key,
    })

    exists = table_exists
    try:
        exists = bool(client.indices.exists(index=index))
    except Exception:
        pass

    try:
        resp = client.security.has_privileges(
            body={
                "index": [{
                    "names": [index],
                    "privileges": sorted(_ES_WRITE_PRIVS | _ES_CREATE_PRIVS),
                }],
            }
        )
    except Exception as exc:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=(
                f"Elasticsearch security.has_privileges unavailable ({exc}); "
                "security may be disabled — G2 falls back to connectivity"
            ),
            engine="elasticsearch",
            method="security.has_privileges",
        )

    can_write, can_create = evaluate_elasticsearch_privileges(
        resp if isinstance(resp, dict) else {},
        index=index,
        table_exists=bool(exists),
    )
    return _finalize(
        engine="elasticsearch",
        can_write=can_write,
        can_create=can_create,
        table_exists=bool(exists),
        table=index,
        schema="elasticsearch",
        need_update=False,
        method="security.has_privileges",
        write_action="index/write",
        create_action="create_index",
    )


def evaluate_elasticsearch_privileges(
    response: dict[str, Any],
    *,
    index: str,
    table_exists: bool,
) -> tuple[bool, bool]:
    """Parse has_privileges response → (can_write, can_create). Public for tests."""
    if response.get("has_all_requested") is True:
        return True, True

    can_write = False
    can_create = False
    for entry in response.get("index") or []:
        if not isinstance(entry, dict):
            continue
        privs = entry.get("privileges") or {}
        granted: set[str] = set()
        if isinstance(privs, dict):
            for k, v in privs.items():
                if v is True or v == "true":
                    granted.add(str(k).lower())
        elif isinstance(privs, (list, tuple)):
            granted = {str(p).lower() for p in privs}

        if granted & {p.lower() for p in _ES_WRITE_PRIVS}:
            can_write = True
        if granted & {p.lower() for p in _ES_CREATE_PRIVS}:
            can_create = True

    if not table_exists:
        can_write = can_write or can_create
    return can_write, can_create


# ── S3 / MinIO ───────────────────────────────────────────────────────────────

def _probe_s3(
    *,
    host: str,
    port: int,
    bucket: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    key_prefix: str,
    table_exists: bool,
) -> PrivilegeProbeResult:
    from connectors.aws_common import boto3_client

    bucket_name = (bucket or "").strip()
    if not bucket_name:
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail="S3 bucket name required for privilege probe",
            engine="s3",
        )

    cfg = {
        "host": host,
        "port": port,
        "database": bucket_name,
        "username": username,
        "password": password,
        "connection_string": connection_string,
        "ssl": ssl,
    }
    client = boto3_client("s3", cfg)

    exists = table_exists
    try:
        client.head_bucket(Bucket=bucket_name)
        exists = True
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", "") or "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            exists = False
        elif code in {"403", "AccessDenied"}:
            return PrivilegeProbeResult(
                can_write=False,
                can_create_table=False,
                status="denied",
                detail=f"S3 AccessDenied on head_bucket for `{bucket_name}` — check IAM",
                engine="s3",
                method="head_bucket",
            )
        else:
            return PrivilegeProbeResult(
                can_write=None,
                can_create_table=None,
                status="unavailable",
                detail=f"S3 head_bucket failed: {exc}",
                engine="s3",
                method="head_bucket",
            )

    try:
        acl = client.get_bucket_acl(Bucket=bucket_name)
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", "") or "")
        return PrivilegeProbeResult(
            can_write=None,
            can_create_table=None,
            status="unavailable",
            detail=(
                f"S3 GetBucketAcl unavailable ({code or exc}); "
                "Object Ownership may disable ACLs — G2 falls back to connectivity "
                "(never PutObject probe)"
            ),
            engine="s3",
            method="GetBucketAcl",
        )

    grants = list((acl or {}).get("Grants") or [])
    can_write, can_create = evaluate_s3_acl_grants(
        grants,
        table_exists=bool(exists),
        key_prefix=key_prefix,
    )
    return _finalize(
        engine="s3",
        can_write=can_write,
        can_create=can_create,
        table_exists=bool(exists),
        table=key_prefix or bucket_name,
        schema=bucket_name,
        need_update=False,
        method="GetBucketAcl",
        write_action="s3:PutObject",
        create_action="s3:CreateBucket/PutObject",
    )


def evaluate_s3_acl_grants(
    grants: list[Any],
    *,
    table_exists: bool = True,
    key_prefix: str = "",
) -> tuple[bool, bool]:
    """Parse S3 ACL Grants → (can_write, can_create). Public for tests."""
    del key_prefix
    can_write = False
    for g in grants:
        if not isinstance(g, dict):
            continue
        perm = str(g.get("Permission") or "").upper()
        if perm in _S3_WRITE_PERMS:
            can_write = True
            break
    can_create = can_write
    if not table_exists:
        can_write = can_create
    return can_write, can_create


def resolve_write_flags(
    connected: bool,
    probe: PrivilegeProbeResult | None,
) -> tuple[bool, bool, dict[str, Any]]:
    """Merge connectivity with privilege probe into (can_write, can_create, meta).

    Unavailable probes keep connectivity-based defaults (no false block).
    Explicit deny fails closed.
    """
    meta: dict[str, Any] = {}
    if probe is not None:
        meta = probe.to_dict()

    if not connected:
        return False, False, meta

    if probe is None or probe.status == "unavailable":
        return True, True, meta

    can_write = bool(probe.can_write)
    can_create = bool(probe.can_create_table)
    return can_write, can_create, meta
