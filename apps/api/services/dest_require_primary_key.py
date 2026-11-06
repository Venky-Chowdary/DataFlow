"""A destination server that refuses tables without a PRIMARY KEY (QA MySQL D2).

MySQL 8.0.13+ ``sql_require_primary_key=ON`` (and MariaDB's
``innodb_force_primary_key=ON``) reject ``CREATE TABLE`` without a primary key
with error 3750 / 1173. DataFlow deliberately creates an append destination
without a key: a second copy of the same row is legal on append, and only
upsert/merge enforces the source key (services/schema_fidelity
``withhold_append_key_constraints``). So on such a server an append
create-new passed Validate and then failed at the first write.

The verdict belongs to Validate. This module reads the server variable over
the destination's own connection settings and names it, with the fixes, as a
target-DDL issue (G6) before anything is written. It never invents a key the
operator did not ask this sync to enforce.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)

#: (variable, server family) — read in this order; an unknown variable is
#: "not required" on that server (older MySQL, MariaDB lacks the MySQL name).
_REQUIRE_PK_VARIABLES = (
    ("sql_require_primary_key", "MySQL"),
    ("innodb_force_primary_key", "MariaDB"),
)


def server_requires_primary_key(cfg: Mapping[str, Any] | None) -> str:
    """The variable that forces a PRIMARY KEY on this MySQL-family server, or ``""``.

    Reads the session value, which is what this account's CREATE TABLE obeys.
    A probe that cannot connect answers ``""``: G2 owns connectivity.
    """
    if not cfg:
        return ""
    try:
        from connectors.mysql_conn import get_connection

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
    except Exception as exc:  # noqa: BLE001 — G2 reports the connection
        logger.debug("require-primary-key probe could not connect: %s", exc)
        return ""
    try:
        with conn.cursor() as cur:
            for variable, _family in _REQUIRE_PK_VARIABLES:
                try:
                    cur.execute(f"SELECT @@SESSION.{variable}")
                except Exception:  # noqa: BLE001 — variable absent on this server
                    try:
                        cur.execute(f"SELECT @@GLOBAL.{variable}")
                    except Exception:  # noqa: BLE001
                        continue
                row = cur.fetchone()
                if row and str(row[0]).strip().upper() in {"1", "ON", "TRUE"}:
                    return variable
        return ""
    except Exception as exc:  # noqa: BLE001 — a probe must not fail Validate
        logger.debug("require-primary-key probe failed: %s", exc)
        return ""
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("require-primary-key probe close failed", exc_info=True)


def keyless_create_refusal(
    *,
    dest_db_type: str,
    destination_config: Mapping[str, Any] | None,
    table_exists: bool | None,
    sync_mode: str | None,
    destination_table: str | None = None,
    key_hint: str | None = None,
) -> str | None:
    """G6 issue text when this run would CREATE a keyless table the server refuses.

    Only a create-new append is keyless by construction; upsert and overwrite
    create the source/operator key, and an existing table is not re-created.
    """
    from services.destination_key_collision_probe import (
        sync_mode_appends_without_key_resolution,
    )
    from src.transfer.engine_family import family_or_kind

    if table_exists is not False or not destination_config:
        return None
    if family_or_kind(dest_db_type or "") != "mysql":
        return None
    if not sync_mode_appends_without_key_resolution(str(sync_mode or "")):
        return None
    variable = server_requires_primary_key(destination_config)
    if not variable:
        return None
    table = str(destination_table or "the destination table")
    key = str(key_hint or "").strip()
    keyed = f" with the key `{key}`" if key else " with a key the source guarantees unique"
    return (
        f"The destination server has {variable}=ON: it refuses CREATE TABLE without "
        f"a PRIMARY KEY (MySQL error 3750). Fix: run upsert or full_refresh_overwrite"
        f"{keyed} (DataFlow creates PRIMARY KEY on it); or create {table} yourself with "
        f"a primary key and {sync_mode} into it; or have the DBA set {variable}=OFF for "
        f"this account. A {sync_mode} create-new declares no key on purpose — a second "
        "copy of the same row is legal on append — so it would fail at the first write."
    )
