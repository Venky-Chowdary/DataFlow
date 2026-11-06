"""One writer at a time per keyed destination table, for the keyed write witness.

Review M2 of the witness: two DataFlow jobs on the same table can regress it
with both reported green. A reads V1, B reads V2, B writes and verifies, A
writes V1 late; A's ladder then sees source V2, destination V1 == what A
wrote, and excuses the key as a concurrent source update.

The witness may only excuse drift when no other DataFlow job wrote the table
inside the job's [read, write, ladder] window. On PostgreSQL destinations a
session advisory lock, keyed on the server-side identity of the table
(``current_database().schema.table``, so host aliases cannot split it), is held
from before the read until the job ends:

* a keyed upsert job (the only kind that starts a witness) takes it
  **exclusive** when it is free; only then may its witness attribute;
* every other DataFlow job writing the table takes it **shared**, waiting
  (cancellable, bounded by ``DATAFLOW_DEST_WRITE_LOCK_WAIT_SECONDS``) while an
  exclusive holder runs; shared holders do not block one another, so writes
  that never attribute keep running concurrently as before;
* a keyed job that finds the table busy takes it shared and runs without a
  witness (unattributed, disclosed).

Destinations other than PostgreSQL get no lock, so their witness never
attributes (``no_exclusive_destination_lock``). A failure to take the lock for
any reason other than the wait deadline leaves the job unlocked and
unwitnessed; it never fails the job. The lock's session is closed when the job
ends (``release_destination_write_lock`` from the job scope), which releases it
on every outcome.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

EXCLUSIVE = "exclusive"
SHARED = "shared"
_POLL_SECONDS = 0.5
_LOCK = threading.Lock()
_HELD: dict[str, tuple[Any, str]] = {}


#: Off by default (review of f3a4c29b0): with it off no lock is taken, so no
#: witness attributes and re-read routes stay disclosed-unattributed.
FLAG = "DATAFLOW_DEST_WRITE_LOCK"


def lock_enabled() -> bool:
    from services.brand_env import getenv_brand_bool

    return getenv_brand_bool(FLAG, False)


class DestinationBusy(RuntimeError):
    """Another job held the destination table past the wait deadline."""


def _wait_seconds() -> float:
    from services.brand_env import getenv_brand

    try:
        return max(float(getenv_brand("DEST_WRITE_LOCK_WAIT_SECONDS", "3600")), 0.0)
    except ValueError:
        return 3600.0


def destination_lock_mode(job_id: Any) -> str:
    """``exclusive`` / ``shared`` / ``""`` for this job's destination table."""
    with _LOCK:
        held = _HELD.get(str(job_id or ""))
    return held[1] if held else ""


def _keyed_sync(request: Any) -> bool:
    from services.primary_key import sync_collapses_in_file_duplicates
    from services.sync_cursor import is_overwrite_sync

    modes = [str(getattr(request, "sync_mode", "") or "")] + [
        str(c.get("sync_mode") or "")
        for c in getattr(request, "stream_contracts", None) or []
        if isinstance(c, dict)
    ]
    modes = [m for m in modes if m]
    return bool(modes) and all(
        sync_collapses_in_file_duplicates(m) and not is_overwrite_sync(m) for m in modes
    )


def hold_destination_write_lock(request: Any, job_id: Any) -> str:
    """Take this job's destination-table lock (see module doc); returns the mode."""
    jid = str(job_id or "")
    if not lock_enabled():
        return ""
    dest = getattr(request, "destination", None)
    fmt = str(getattr(dest, "format", "") or "").strip().lower()
    if not jid or fmt not in {"postgresql", "postgres"} or destination_lock_mode(jid):
        return destination_lock_mode(jid)
    source = getattr(request, "source", None)
    table = str(getattr(dest, "table", "") or getattr(source, "table", "") or "")
    if not table:
        return ""
    try:
        from connectors.postgresql_conn import get_connection
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(dest)
        conn = get_connection(
            host=str(cfg.get("host") or ""), port=int(cfg.get("port") or 5432),
            database=str(cfg.get("database") or ""), username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""), ssl=bool(cfg.get("ssl", False)),
        )
        conn.autocommit = True
        cur = conn.cursor()
        schema = str(getattr(dest, "schema", "") or cfg.get("schema") or "public")
        cur.execute(
            "SELECT hashtextextended(current_database() || '.' || %s || '.' || %s, 0)",
            (schema, table),
        )
        key = int(cur.fetchone()[0])
    except Exception as exc:  # noqa: BLE001 - the lock guards attribution, never the run
        logger.warning("destination write lock unavailable for %s: %s", table, exc)
        return ""
    mode = ""
    if _keyed_sync(request):
        cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
        if cur.fetchone()[0]:
            mode = EXCLUSIVE
    if not mode:
        from services.cancel_check import cancel_checkpoint

        deadline = time.monotonic() + _wait_seconds()
        while True:
            cur.execute("SELECT pg_try_advisory_lock_shared(%s)", (key,))
            if cur.fetchone()[0]:
                mode = SHARED
                break
            try:
                cancel_checkpoint()
            except BaseException:
                conn.close()
                raise
            if time.monotonic() >= deadline:
                conn.close()
                raise DestinationBusy(
                    f"Another job is still writing {schema}.{table}: waited "
                    f"{_wait_seconds():.0f}s for it to finish before writing "
                    "(DATAFLOW_DEST_WRITE_LOCK_WAIT_SECONDS)."
                )
            time.sleep(_POLL_SECONDS)
    with _LOCK:
        _HELD[jid] = (conn, mode)
    return mode


def release_destination_write_lock(job_id: Any) -> None:
    with _LOCK:
        held = _HELD.pop(str(job_id or ""), None)
    if held is not None:
        try:
            held[0].close()  # ends the session: its advisory locks go with it
        except Exception:  # noqa: BLE001 - already gone is released
            logger.debug("destination write lock session close failed", exc_info=True)
