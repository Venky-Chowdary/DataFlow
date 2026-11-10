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


def _mysql_errno(exc: BaseException) -> int | None:
    args = getattr(exc, "args", None) or ()
    return args[0] if args and isinstance(args[0], int) else None


def server_requires_primary_key(cfg: Mapping[str, Any] | None) -> PrimaryKeyRequirement:
    """Ask the destination whether its CREATE TABLE demands a PRIMARY KEY.

    Reads ``@@<variable>`` (the session value where the variable has one, which is
    what this account's CREATE obeys). A variable this server does not have
    (1193) is not a requirement; any other failure is reported as unreadable.
    """
    if not cfg:
        return PrimaryKeyRequirement(unreadable="no destination connection settings")
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
                    return PrimaryKeyRequirement(variable=variable)
        return PrimaryKeyRequirement()
    finally:
        conn.close()


def planned_create_primary_key(
    *,
    sync_mode: str | None,
    mappings: list[dict[str, Any]] | None,
    operator_key_columns: list[str] | None,
    source_schema_catalog: Any = None,
    destination_config: Mapping[str, Any] | None = None,
) -> list[str]:
    """The PRIMARY KEY the writer's CREATE TABLE would declare on MySQL, or ``[]``.

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
        carry_source_keys=is_overwrite_sync(mode),
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


def keyless_create_refusal(
    *,
    dest_db_type: str,
    destination_config: Mapping[str, Any] | None,
    table_exists: bool | None,
    sync_mode: str | None,
    destination_table: str | None = None,
    key_hint: str | None = None,
    mappings: list[dict[str, Any]] | None = None,
    source: Mapping[str, Any] | None = None,
) -> str | None:
    """G6 issue text when this run would CREATE a keyless table the server refuses.

    Any write mode that creates the destination is asked the same question —
    will the writer's CREATE declare a PRIMARY KEY? — append (keys withheld on
    purpose), overwrite or upsert of a source without a key, a query source (no
    catalog key), SCD2 history. An existing table is not re-created.
    """
    from services.sync_cursor import normalize_sync_mode
    from src.transfer.engine_family import family_or_kind

    if table_exists is not False or not destination_config:
        return None
    if family_or_kind(dest_db_type or "") != "mysql":
        return None
    mode = normalize_sync_mode(str(sync_mode or ""), default="")
    keys = [k.strip() for k in str(key_hint or "").split(",") if k.strip()]
    requirement = server_requires_primary_key(destination_config)
    if not requirement.variable and not requirement.unreadable:
        return None
    catalog = source_catalog_for_create(source, mappings) if mode != "scd2" else None
    if planned_create_primary_key(
        sync_mode=mode, mappings=mappings, operator_key_columns=keys,
        source_schema_catalog=catalog,
        destination_config=destination_config,
    ):
        return None
    table = str(destination_table or "the destination table")
    if requirement.unreadable:
        # Not a pass: this CREATE declares no key, and whether the server
        # refuses that (3750) could not be read.
        return (
            f"Could not read whether the destination forces a PRIMARY KEY "
            f"(sql_require_primary_key / innodb_force_primary_key: "
            f"{requirement.unreadable}), and this {mode} CREATE of {table} declares "
            "none — if the server forces one, the first write fails with MySQL 3750. "
            "Fix: re-run Validate once the destination can be read, or run upsert "
            "with a key so Datawrap creates PRIMARY KEY on it."
        )
    variable = requirement.variable
    key = ",".join(keys)
    keyed = f" with the key `{key}`" if key else " with a key the source guarantees unique"
    from services.destination_key_collision_probe import (
        sync_mode_appends_without_key_resolution,
    )

    if mode == "scd2":
        why = ("An scd2 history table holds several versions of each key, so its "
               "CREATE declares no PRIMARY KEY.")
        fix = (f"create {table} yourself with a surrogate PRIMARY KEY (an AUTO_INCREMENT "
               f"column) and run scd2 into it; or have the DBA set {variable}=OFF for "
               "this account")
    else:
        if sync_mode_appends_without_key_resolution(mode):
            why = (f"A {mode} create-new declares no key on purpose — a second copy of "
                   "the same row is legal on append.")
        else:
            why = (f"The {mode} create declares no PRIMARY KEY: the source carries none "
                   "Datawrap can copy (a source table without a primary key, or a query "
                   "result, which has no catalog key).")
        fix = (f"run upsert{keyed} (Datawrap creates PRIMARY KEY on it); or create "
               f"{table} yourself with a primary key and load into it; or have the DBA "
               f"set {variable}=OFF for this account")
    return (
        f"The destination server has {variable}=ON: it refuses CREATE TABLE without "
        f"a PRIMARY KEY (MySQL error 3750). Fix: {fix}. {why} It would fail at the "
        "first write."
    )
