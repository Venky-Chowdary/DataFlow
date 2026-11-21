"""A destination server that refuses tables without a PRIMARY KEY (QA MySQL D2).

MySQL 8.0.13+ ``sql_require_primary_key=ON`` (and MariaDB's
``innodb_force_primary_key=ON``) reject ``CREATE TABLE`` without a primary key
with error 3750 / 1173. DataFlow deliberately creates an append destination
without a key: a second copy of the same row is legal on append, and only
upsert/merge enforces the source key (services/schema_fidelity
``withhold_append_key_constraints``). So on such a server an append
create-new passed Validate and then failed at the first write.

The verdict belongs to Validate. This module reads the server variable over
the destination's own connection settings and, for every write mode that
would CREATE the table, asks the writer's own planner whether that CREATE
declares a PRIMARY KEY: an append (keys withheld on purpose), an overwrite or
an upsert-without-key of a source that has none, a query result, SCD2
history. When it would not, the variable and the fixes are named as a
target-DDL issue (G6) before anything is written. It never invents a key the
operator did not ask this sync to enforce.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: The MySQL-family variables that force a PRIMARY KEY on CREATE TABLE.
#: MySQL 8.0.13+ has the first, MariaDB the second; each server knows only its own.
_REQUIRE_PK_VARIABLES = ("sql_require_primary_key", "innodb_force_primary_key")
#: ER_UNKNOWN_SYSTEM_VARIABLE: this server does not have that variable.
_ER_UNKNOWN_SYSTEM_VARIABLE = 1193
# Client-side "the server did not answer" codes (CR_CONNECTION_ERROR,
# CR_CONN_HOST_ERROR, CR_SERVER_GONE_ERROR, CR_SERVER_LOST, CR_SERVER_LOST_EXTENDED):
# a blip worth one more try. Access denied, unknown database or a TLS refusal are
# answers, and will not change on a retry.
_TRANSIENT_CONNECT_ERRNOS = frozenset({2002, 2003, 2006, 2013, 2055})
_CONNECT_ATTEMPTS = 2


@dataclass(frozen=True)
class PrimaryKeyRequirement:
    """What the destination server said about forcing a PRIMARY KEY.

    ``variable`` names the setting that is ON (``""`` when none is). ``unreadable``
    says why the server could not be asked; it is never read as "not required".
    """

    variable: str = ""
    unreadable: str = ""
    #: ``SELECT VERSION()`` of the server (``8.0.36``, ``10.11.14-MariaDB-...``).
    server_version: str = ""


def _mysql_errno(exc: BaseException) -> int | None:
    args = getattr(exc, "args", None) or ()
    return args[0] if args and isinstance(args[0], int) else None


#: Seconds a read answer is reused: Validate asks from G6 and from the
#: surrogate-key gate of the same run; a variable change is seen next Validate.
_PROBE_TTL_SECONDS = 60.0
_probe_cache: dict[tuple, tuple[float, PrimaryKeyRequirement]] = {}


def _probe_key(cfg: Mapping[str, Any]) -> tuple:
    return tuple(
        str(cfg.get(k) or "")
        for k in ("host", "port", "database", "username", "connection_string")
    )


def server_requires_primary_key(cfg: Mapping[str, Any] | None) -> PrimaryKeyRequirement:
    """Ask the destination whether its CREATE TABLE demands a PRIMARY KEY.

    Reads ``@@<variable>`` (the session value where the variable has one, which is
    what this account's CREATE obeys). A variable this server does not have
    (1193) is not a requirement; any other failure is reported as unreadable.
    A read answer is reused for ``_PROBE_TTL_SECONDS``; an unreadable one never.
    """
    if not cfg:
        return PrimaryKeyRequirement(unreadable="no destination connection settings")
    key = _probe_key(cfg)
    hit = _probe_cache.get(key)
    if hit and time.monotonic() - hit[0] < _PROBE_TTL_SECONDS:
        return hit[1]
    answer = _read_requirement(cfg)
    if not answer.unreadable:
        _probe_cache[key] = (time.monotonic(), answer)
    return answer


def _read_requirement(cfg: Mapping[str, Any]) -> PrimaryKeyRequirement:
    import pymysql

    from connectors.mysql_conn import get_connection
    from services.egress_guard import EgressRefused

    conn = None
    for attempt in range(1, _CONNECT_ATTEMPTS + 1):
        try:
            conn = get_connection(
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or 3306),
                database=str(cfg.get("database") or ""),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                ssl=bool(cfg.get("ssl")),
                purpose="probe",
            )
            break
        except (pymysql.err.MySQLError, EgressRefused, OSError, RuntimeError, ValueError) as exc:
            transient = isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InterfaceError,
                                         ConnectionError, TimeoutError)) and (
                not isinstance(exc, pymysql.err.MySQLError)
                or _mysql_errno(exc) in _TRANSIENT_CONNECT_ERRNOS
            )
            if transient and attempt < _CONNECT_ATTEMPTS:
                logger.info("require-primary-key probe: connect attempt %d failed (%s); retrying",
                            attempt, type(exc).__name__)
                time.sleep(0.5)
                continue
            logger.warning("require-primary-key probe could not connect: %s", exc)
            return PrimaryKeyRequirement(unreadable=f"could not connect ({type(exc).__name__})")
    if conn is None:  # unreachable: every attempt either connects or returns above
        return PrimaryKeyRequirement(unreadable="could not connect")
    try:
        with conn.cursor() as cur:
            version = ""
            try:
                cur.execute("SELECT VERSION()")
                row = cur.fetchone()
                version = str(row[0]) if row else ""
            except pymysql.err.MySQLError as exc:
                logger.debug("require-primary-key probe: VERSION() unreadable: %s", exc)
            for variable in _REQUIRE_PK_VARIABLES:
                value = None
                try:
                    cur.execute(f"SELECT @@{variable}")
                    row = cur.fetchone()
                    value = row[0] if row else None
                except pymysql.err.MySQLError as exc:
                    if _mysql_errno(exc) != _ER_UNKNOWN_SYSTEM_VARIABLE:
                        logger.warning("require-primary-key probe: %s unreadable: %s",
                                       variable, exc)
                        return PrimaryKeyRequirement(
                            unreadable=f"{variable} unreadable ({_mysql_errno(exc) or type(exc).__name__})"
                        )
                if value is not None and str(value).strip().upper() in {"1", "ON", "TRUE"}:
                    return PrimaryKeyRequirement(variable=variable, server_version=version)
        return PrimaryKeyRequirement(server_version=version)
    finally:
        conn.close()


def planned_create_primary_key(
    *,
    sync_mode: str | None,
    mappings: list[dict[str, Any]] | None,
    operator_key_columns: list[str] | None,
    source_schema_catalog: Any = None,
    destination_config: Mapping[str, Any] | None = None,
    carry_source_keys: bool = False,
) -> list[str]:
    """The PRIMARY KEY the writer's CREATE TABLE would declare on MySQL, or ``[]``.

    ``carry_source_keys`` asks what the writer declares on a server that forces
    a PRIMARY KEY: there it carries the source's own key on every write mode
    (:func:`writer_forces_source_key`), so a keyed source is never refused.

    Asked of the writer's own planner (``schema_fidelity.resolve_create_fidelity_plan``)
    with the inputs the write path gives it (``stream.py`` / ``stream_write.py``):
    The shared duplicate-policy resolver applies the explicit destination
    options to the sync's initial write mode. Keys are carried for effective
    upsert and overwrite (``carry_source_keys``), withheld for plain append;
    the operator key is the destination key on any write mode except an
    explicit append policy, which allows repeated keys
    (``schema_fidelity.create_key_inputs``). SCD2's
    history table never carries the key (``scd2_engine``) and the CDC apply
    table declares its key columns itself (``cdc_eos_sa``). No second rulebook.
    """
    from services.duplicate_policy import (
        DuplicatePolicyError,
        duplicates_allowed,
        resolve_duplicate_policy,
    )
    from services.schema_fidelity import resolve_create_fidelity_plan
    from services.sync_cursor import is_overwrite_sync, normalize_sync_mode, requires_upsert

    mode = normalize_sync_mode(str(sync_mode or ""), default="")
    maps = [m for m in (mappings or []) if isinstance(m, dict) and m.get("target")]
    targets = [str(m.get("target")) for m in maps]
    by_source = {str(m.get("source") or "").casefold(): str(m.get("target")) for m in maps}
    keys = [
        by_source.get(str(k).strip().casefold(), str(k).strip())
        for k in (operator_key_columns or []) if str(k or "").strip()
    ]
    keys = [k for k in keys if k in targets]
    if mode == "cdc":
        return keys
    write_mode = "upsert" if requires_upsert(mode) and keys and mode != "scd2" else "insert"
    rows_allowed = False
    if mode != "scd2":
        cfg = destination_config or {}
        try:
            decision = resolve_duplicate_policy(
                duplicate_policy=str(cfg.get("duplicate_policy") or ""),
                allow_duplicates=bool(cfg.get("allow_duplicates")),
                sync_mode=mode,
                business_key=bool(keys),
                write_mode=write_mode,
            )
        except DuplicatePolicyError:
            return []
        write_mode = decision.write_mode
        rows_allowed = duplicates_allowed(decision)
    from services.schema_fidelity import create_key_inputs

    key_inputs = create_key_inputs(
        write_mode=write_mode,
        conflict_columns=keys if mode != "scd2" else None,
        carry_source_keys=is_overwrite_sync(mode) or (carry_source_keys and not rows_allowed),
        duplicates_allowed=rows_allowed,
    )
    carry = mode != "scd2" and key_inputs["carry_keys"]
    plan = resolve_create_fidelity_plan(
        source_schema_catalog=source_schema_catalog,
        mappings=maps,
        target_columns=targets,
        # Whether a key is declared does not depend on the carriers, and the
        # writer sizes a LOB key to VARCHAR(n) before CREATE (CR-D126), so every
        # column is planned key-capable.
        target_types=["VARCHAR(255)"] * len(targets),
        dest_dialect="mysql",
        carry_keys=carry,
        operator_key_columns=key_inputs["operator_key_columns"],
    )
    if plan.primary_key:
        return list(plan.primary_key)
    pk_clause = any(
        str(c).strip().upper().startswith("PRIMARY KEY") for c in plan.table_constraints
    )
    return list(keys) if pk_clause else []


def source_catalog_for_create(source: Mapping[str, Any] | None, mappings: list[dict] | None) -> Any:
    """The source catalog the writer hands its CREATE, or None (query, file, unreadable).

    ``source`` carries ``config`` / ``connector_id`` / ``table`` / ``workspace_id``.
    """
    if not source:
        return None
    # The writer introspects the source's table name whatever the read mode
    # (stream.py ``_src_rich_catalog``), so this does too: same input, same plan.
    cfg_in = dict(source.get("config") or {})
    table = str(source.get("table") or "").strip()
    if not table:
        return None
    try:
        from services.preflight_source_catalog import _resolve_source
        from src.transfer.adapters_introspect import source_schema_catalog_for_table
        from src.transfer.connector_capabilities import resolve_driver_type, source_read_driver

        cfg, db_type = _resolve_source(
            source_connector_id=str(source.get("connector_id") or ""),
            source_config=cfg_in or None,
            workspace_id=source.get("workspace_id") or None,
        )
        if not cfg:
            return None
        read_type = source_read_driver(resolve_driver_type(db_type or str(cfg.get("type") or "")))
        columns = [str(m.get("source")) for m in (mappings or []) if isinstance(m, dict) and m.get("source")]
        return source_schema_catalog_for_table(read_type, cfg, table, columns)
    except Exception as exc:  # noqa: BLE001 — no catalog means a types-only CREATE
        logger.debug("source catalog for the create-key check failed: %s", exc)
        return None


#: The column Datawrap adds, with approval, to a keyless table on a server that
#: forces a PRIMARY KEY. It is not a source column: parity never compares it.
SURROGATE_KEY_COLUMN = "_dw_row_id"
#: MySQL 8.0.23 added INVISIBLE columns (``SELECT *`` does not return them).
_MYSQL_INVISIBLE_SINCE = (8, 0, 23)


def _is_mariadb(version: str) -> bool:
    return "mariadb" in (version or "").lower()


def _mysql_version(version: str) -> tuple[int, ...]:
    import re

    m = re.match(r"^(\d+)\.(\d+)\.(\d+)", (version or "").strip())
    return tuple(int(x) for x in m.groups()) if m else ()


def surrogate_key_column_sql(server_version: str = "") -> str:
    """The column definition of Datawrap's surrogate key on this server.

    ``INVISIBLE`` on MySQL 8.0.23+ so ``SELECT *`` and the operator's own
    queries do not see it; a plain column on MariaDB and older MySQL.
    """
    invisible = (
        not _is_mariadb(server_version)
        and bool(_mysql_version(server_version))
        and _mysql_version(server_version) >= _MYSQL_INVISIBLE_SINCE
    )
    return (
        f"`{SURROGATE_KEY_COLUMN}` BIGINT NOT NULL"
        + (" INVISIBLE" if invisible else "")
        + " AUTO_INCREMENT PRIMARY KEY"
    )


def server_forces_primary_key_on(cursor: Any) -> tuple[bool, str]:
    """``(forced, server_version)`` read on the writer's own connection.

    The writer asks at CREATE time with the cursor it creates on, so it obeys
    exactly the session the CREATE runs in. A variable this server does not
    have is not a requirement; any other failure reads as not forced (the
    CREATE then reports the server's own answer).
    """
    version = ""
    try:
        cursor.execute("SELECT VERSION()")
        row = cursor.fetchone()
        version = str((row[0] if not isinstance(row, dict) else next(iter(row.values()))) or "")
    except Exception as exc:  # noqa: BLE001
        logger.debug("VERSION() unreadable: %s", exc)
    for variable in _REQUIRE_PK_VARIABLES:
        try:
            cursor.execute(f"SELECT @@{variable}")
            row = cursor.fetchone()
        except Exception:  # noqa: BLE001 — 1193: this server has no such variable
            continue
        value = (row[0] if not isinstance(row, dict) else next(iter(row.values()))) if row else None
        if value is not None and str(value).strip().upper() in {"1", "ON", "TRUE"}:
            return True, version
    return False, version


@dataclass(frozen=True)
class KeylessCreateDecision:
    """What Validate says about a CREATE on a server that forces a PRIMARY KEY.

    ``kind``: ``""`` nothing to say; ``refuse`` a G6 refusal (``message``);
    ``surrogate`` a keyless source that Datawrap can key only by adding
    ``SURROGATE_KEY_COLUMN`` — an approvable DDL change (``column_sql``).
    """

    kind: str = ""
    message: str = ""
    variable: str = ""
    column_sql: str = ""


def keyless_create_decision(
    *,
    dest_db_type: str,
    destination_config: Mapping[str, Any] | None,
    table_exists: bool | None,
    sync_mode: str | None,
    destination_table: str | None = None,
    key_hint: str | None = None,
    mappings: list[dict[str, Any]] | None = None,
    source: Mapping[str, Any] | None = None,
) -> KeylessCreateDecision:
    """Judge a CREATE this run would issue on a server that may force a key.

    Owner decision (QA D396 / D377): a source key is carried on every write
    mode there (the writer does the same, :func:`server_forces_primary_key_on`);
    a keyless source gets Datawrap's surrogate ``_dw_row_id`` only after the
    operator approves the disclosed DDL — never silently. SCD2 history and an
    unreadable variable stay refusals.
    """
    from services.sync_cursor import normalize_sync_mode
    from src.transfer.engine_family import family_or_kind

    if table_exists is not False or not destination_config:
        return KeylessCreateDecision()
    if family_or_kind(dest_db_type or "") != "mysql":
        return KeylessCreateDecision()
    mode = normalize_sync_mode(str(sync_mode or ""), default="")
    keys = [k.strip() for k in str(key_hint or "").split(",") if k.strip()]
    requirement = server_requires_primary_key(destination_config)
    if not requirement.variable and not requirement.unreadable:
        return KeylessCreateDecision()
    catalog = source_catalog_for_create(source, mappings) if mode != "scd2" else None
    plan_args = dict(
        sync_mode=mode, mappings=mappings, operator_key_columns=keys,
        source_schema_catalog=catalog, destination_config=destination_config,
    )
    if planned_create_primary_key(**plan_args):
        return KeylessCreateDecision()
    table = str(destination_table or "the destination table")
    if requirement.unreadable:
        # Not a pass: this CREATE declares no key, and whether the server
        # refuses that (3750) could not be read.
        return KeylessCreateDecision(kind="refuse", message=(
            f"Could not read whether the destination forces a PRIMARY KEY "
            f"(sql_require_primary_key / innodb_force_primary_key: "
            f"{requirement.unreadable}), and this {mode} CREATE of {table} declares "
            "none — if the server forces one, the first write fails with MySQL 3750. "
            "Fix: re-run Validate once the destination can be read, or run upsert "
            "with a key so Datawrap creates PRIMARY KEY on it."
        ))
    variable = requirement.variable
    if mode != "scd2" and planned_create_primary_key(**plan_args, carry_source_keys=True):
        # The source's own key: the writer carries it here (D396 decision b).
        return KeylessCreateDecision()
    if mode == "scd2":
        return KeylessCreateDecision(kind="refuse", variable=variable, message=(
            f"The destination server has {variable}=ON: it refuses CREATE TABLE without "
            f"a PRIMARY KEY (MySQL error 3750). Fix: create {table} yourself with a "
            "surrogate PRIMARY KEY (an AUTO_INCREMENT column) and run scd2 into it; or "
            f"have the DBA set {variable}=OFF for this account. An scd2 history table "
            "holds several versions of each key, so its CREATE declares no PRIMARY KEY. "
            "It would fail at the first write."
        ))
    column_sql = surrogate_key_column_sql(requirement.server_version)
    return KeylessCreateDecision(
        kind="surrogate", variable=variable, column_sql=column_sql, message=(
            f"The destination server has {variable}=ON: it refuses CREATE TABLE without "
            f"a PRIMARY KEY (MySQL error 3750), and this {mode or 'create'} CREATE of "
            f"{table} declares no PRIMARY KEY: there is no key to carry (the source has "
            "none, a query result has no catalog key, or the append policy lets keys "
            f"repeat). Datawrap can add its own row key: {column_sql}. It is not "
            "a source column — the Trust Report lists it as Datawrap-added and parity "
            "never compares it. Approve this destination DDL to add it; or declare a "
            f"source key (run upsert with it); or create {table} yourself with a "
            f"primary key; or have the DBA set {variable}=OFF for this account."
        ),
    )


def keyless_create_refusal(**kwargs: Any) -> str | None:
    """G6 issue text when this run would CREATE a keyless table the server refuses.

    Only the refusals that no approval can clear (an unreadable variable, SCD2
    history). A keyless source Datawrap can key with its surrogate column is
    the approvable ``g_dest_requires_primary_key`` gate instead
    (:func:`require_primary_key_gate`).
    """
    decision = keyless_create_decision(**kwargs)
    return decision.message if decision.kind == "refuse" else None


GATE_ID = "g_dest_requires_primary_key"


def require_primary_key_gate(
    *, acknowledged: bool, destination_table: str, **kwargs: Any,
) -> dict[str, Any] | None:
    """Validate's approvable surrogate-key gate, or None when it does not apply."""
    decision = keyless_create_decision(destination_table=destination_table, **kwargs)
    if decision.kind != "surrogate":
        return None
    planned = (
        f"CREATE TABLE {destination_table} (... mapped columns ..., {decision.column_sql})"
    )
    details = {
        "rule_id": GATE_ID,
        "remediation_kind": "approve_ddl_or_dedupe",
        "planned_ddl": planned,
        "server_variable": decision.variable,
        "datawrap_added_columns": [{
            "name": SURROGATE_KEY_COLUMN,
            "ddl": decision.column_sql,
            "reason": f"{decision.variable}=ON requires a PRIMARY KEY; the source has none",
            "excluded_from_parity": True,
        }],
        "ddl_acknowledged": bool(acknowledged),
        "requires_approval": not acknowledged,
    }
    if acknowledged:
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"Approved destination DDL: Datawrap adds {decision.column_sql} to "
                f"{destination_table} ({decision.variable}=ON; the source has no key). "
                "Parity does not compare it."
            ),
            "duration_ms": 0,
            "details": {**details, "acknowledged": True},
        }
    return {
        "id": GATE_ID,
        "status": "block",
        "message": decision.message,
        "duration_ms": 0,
        "details": details,
        "suggested_actions": [
            f"Approve the disclosed DDL (adds {SURROGATE_KEY_COLUMN}) and re-validate",
            "Declare a source key and run upsert with it",
            f"Have the DBA set {decision.variable}=OFF for this account",
        ],
    }
