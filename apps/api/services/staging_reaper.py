"""Canonical naming and reaping for internal staging tables.

Two engines need scratch tables inside the customer's destination schema: the
mirror key-staging table (``_df_mirrorkeys_*``) and the SCD2/mirror source
spool (``_dataflow_stg_*``). Both have the same two problems, so both use this
one owner rather than a private copy:

* A run killed mid-flight (deploy, OOM, API restart) leaves the table behind,
  and because operator listings hide internal prefixes the orphan is invisible
  and never cleaned. Accumulated orphans are also why a real table can sort out
  of a bounded object listing.
* A sweep must never drop a table a concurrent run is still filling. Age is
  therefore carried *in the name*, so the sweep needs no shared state.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)

#: An orphan older than this cannot belong to a live run.
STAGING_TTL_SECONDS = 6 * 3600

#: Epoch floor for a stamp to be read as a time. A legacy random suffix can be
#: all digits (``_df_mirrorkeys_255577532241``), and reading that as a stamp
#: dated it in the year 10069 — an orphan that could never age out.
_STAMP_EPOCH_FLOOR = 1_577_836_800  # 2020-01-01T00:00:00Z


def staging_table_name(prefix: str, discriminator: str = "") -> str:
    """``<prefix>[<discriminator>_]<epoch>_<rand>``.

    The stamp is what makes reaping safe; the random suffix is what keeps two
    concurrent runs of the same job from sharing one table.
    """
    stem = f"{discriminator}_" if discriminator else ""
    return f"{prefix}{stem}{int(time.time())}_{uuid.uuid4().hex[:8]}"


def staging_age_seconds(prefix: str, name: str) -> float | None:
    """Seconds since this staging table was named, or ``None`` if unstamped."""
    parts = [p for p in name[len(prefix):].split("_") if p]
    if len(parts) < 2 or not parts[-2].isdigit():
        return None
    stamp = float(parts[-2])
    now = time.time()
    if stamp < _STAMP_EPOCH_FLOOR or stamp > now + 86_400:
        # Not a clock this process can reason about — treat it as unstamped
        # rather than granting it an age that never exceeds the TTL.
        return None
    return max(now - stamp, 0.0)


def bound_lock_wait(conn: Any, dialect_name: str) -> None:
    """Cap how long a reap may wait for a lock — best effort, never fatal.

    An unstamped orphan has no age, so the only thing that distinguishes it
    from a table an older build is filling right now is the lock it holds.
    Waiting is what must not happen: this sweep runs inside a real transfer.
    """
    import sqlalchemy as sa

    from services.dialect_profiles import dialect_profile

    stmt = {
        "postgresql": "SET lock_timeout = '2s'",
        "redshift": "SET lock_timeout = '2s'",
        "mysql": "SET SESSION lock_wait_timeout = 2",
        "sqlserver": "SET LOCK_TIMEOUT 2000",
        "mssql": "SET LOCK_TIMEOUT 2000",
    }.get(dialect_profile(dialect_name).driver, "")
    if not stmt:
        return
    # Every catch below is deliberately broad: each driver raises its own DBAPI
    # error class, and no cleanup failure may become the transfer's failure.
    try:
        conn.execute(sa.text(stmt))
    except Exception as exc:  # noqa: BLE001 - best effort, never fatal
        logger.debug("staging reap lock bound skipped: %s", exc)
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - best effort, never fatal
            logger.debug("staging reap rollback after lock bound failed")


def drop_staging_table(conn: Any, qualified: str) -> None:
    """Drop one staging table, tolerating engines without ``IF EXISTS``."""
    import sqlalchemy as sa

    try:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {qualified}"))  # nosec B608
        conn.commit()
        return
    except Exception:  # noqa: BLE001 - engines without IF EXISTS retry below
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - best effort, never fatal
            logger.debug("staging drop rollback failed for %s", qualified)
    try:
        conn.execute(sa.text(f"DROP TABLE {qualified}"))  # nosec B608
        conn.commit()
    except Exception as exc:  # noqa: BLE001 - reported, never raised
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - best effort, never fatal
            logger.debug("staging drop rollback failed for %s", qualified)
        logger.warning("could not drop staging table %s: %s", qualified, exc)


def reap_orphan_staging(
    conn: Any,
    prefix: str,
    schema_name: str,
    dialect_name: str,
    *,
    keep: str = "",
) -> list[str]:
    """Drop staging tables no live run can own. Returns what was dropped.

    ``keep`` is the calling run's own table, which it drops itself. No catalog
    access is not a transfer failure — the sweep is skipped, not raised.
    """
    import sqlalchemy as sa

    from services.mirror_engine import _qualified_name

    sql = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE :pat"
    params: dict[str, Any] = {"pat": f"{prefix}%"}
    if schema_name:
        sql += " AND table_schema = :schema"
        params["schema"] = schema_name
    try:
        rows = conn.execute(sa.text(sql), params).fetchall()
    except Exception as exc:  # noqa: BLE001 - no catalog access is not a failure
        logger.debug("staging reap skipped: %s", exc)
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001 - best effort, never fatal
            logger.debug("staging reap rollback failed")
        return []

    dropped: list[str] = []
    bounded = False
    for row in rows:
        name = str(row[0])
        if keep and name == keep:
            continue
        age = staging_age_seconds(prefix, name)
        if age is not None and age < STAGING_TTL_SECONDS:
            # In TTL: a concurrent run may still be filling this one.
            continue
        if age is None and not bounded:
            # Unstamped names predate the stamp scheme, so age is unknowable —
            # a bounded lock wait skips one an older build still holds.
            bound_lock_wait(conn, dialect_name)
            bounded = True
        drop_staging_table(conn, _qualified_name(name, schema_name, dialect_name))
        dropped.append(name)
    if dropped:
        logger.info(
            "reaped %d orphaned staging table(s) for %s", len(dropped), prefix
        )
    return dropped
