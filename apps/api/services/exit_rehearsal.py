"""Exit Rehearsal runner: a real, timed extract of every declared table, then its proof.

``start`` creates one transfer job per declared table on the existing engine
and scheduler (:func:`src.transfer.background.run_transfer_async`, exactly as
Parity Studio's re-sync does): the saved source connection into a file export
that lands in the private export bucket (PR 397) wherever the deployment uses
it. ``settle`` runs when every job has finished — from a job's completion
callback, the next read on any replica, or the scheduler beat — claims the
verification by compare-and-swap, measures completeness
(:mod:`services.exit_completeness`), seals the parts with a Merkle root and
records the measured duration against the system's window.

Durations come from the job store's own clocks (first job started to last job
completed) plus the verification's clock; a missing clock is "not measured",
never an estimate. A projection is labelled ``projection`` with its basis.
"""

from __future__ import annotations

import logging
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from services import exit_store
from services.exit_inventory import (
    ExitConflict,
    ExitInvalid,
    audit,
    definition_sha256,
    now_iso,
    parse_ts,
    rehearsals_of,
)
from services.exit_store import REHEARSALS, SYSTEMS

logger = logging.getLogger(__name__)

VERIFY_STALE = timedelta(hours=1)
SCHEDULE_CLAIM = "exit_rehearsal_schedule"
SCHEDULE_INTERVAL = timedelta(minutes=1)
SCHEDULE_ACTOR = "system:exit-rehearsal-schedule"
ERROR_EVENT = "exit.rehearsal.error"
TERMINAL = frozenset({"completed", "failed", "cancelled", "canceled", "error"})
_SAFE = re.compile(r"[^A-Za-z0-9_-]+")


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else None


def _job_store() -> Any:
    from services.parity_state import job_store

    return job_store()


# ------------------------------------------------- this process's background work

#: What this process started for rehearsals and has not finished: verifier threads, the progress of
#: running verifications (to stop them) and extract-job futures. Nothing here starts at import or on
#: a read; :func:`stop_background` ends it at app shutdown and :func:`join_background` waits for it.
_BG_LOCK = threading.Lock()
_VERIFIERS: dict[str, list[tuple[threading.Thread, float]]] = {}
_ACTIVE: dict[str, Any] = {}
_FUTURES: dict[str, list[Any]] = {}
_STOPPING = threading.Event()


class _Abandoned(Exception):
    """This verifier stopped without a result: its claim was taken over, or the process is shutting down."""


def _start_verifier(rehearsal_id: str) -> bool:
    """Verify ``rehearsal_id`` on a tracked background thread.

    One per rehearsal in this process: repeated beats do not stack verifiers.
    Only when the one here has been running longer than the stale window (its
    claim could have gone stale: it hangs) is another started beside it.
    """
    now = time.monotonic()
    with _BG_LOCK:
        running = [(t, at) for t, at in _VERIFIERS.get(rehearsal_id, []) if t.is_alive()]
        if _STOPPING.is_set() or any(now - at < VERIFY_STALE.total_seconds() for _t, at in running):
            return False
        thread = threading.Thread(target=_verifier_main, args=(rehearsal_id,), daemon=True,
                                  name=f"exit-verify-{rehearsal_id}")
        _VERIFIERS[rehearsal_id] = [*running, (thread, now)]
    thread.start()
    return True


def _verifier_main(rehearsal_id: str) -> None:
    try:
        settle_quietly(rehearsal_id)
    finally:
        with _BG_LOCK:
            left = [(t, at) for t, at in _VERIFIERS.get(rehearsal_id, []) if t is not threading.current_thread()]
            if left:
                _VERIFIERS[rehearsal_id] = left
            else:
                _VERIFIERS.pop(rehearsal_id, None)


def _track_future(rehearsal_id: str, future: Any) -> None:
    with _BG_LOCK:
        _FUTURES.setdefault(rehearsal_id, []).append(future)

    def done(_future: Any) -> None:
        settle_quietly(rehearsal_id)  # the last job to finish verifies (on the job's worker, joined with it)
        with _BG_LOCK:
            left = [f for f in _FUTURES.get(rehearsal_id, []) if f is not future]
            if left:
                _FUTURES[rehearsal_id] = left
            else:
                _FUTURES.pop(rehearsal_id, None)

    future.add_done_callback(done)


def background_work() -> list[str]:
    """Names of this process's unfinished rehearsal work (empty when idle)."""
    with _BG_LOCK:
        out = [t.name for group in _VERIFIERS.values() for t, _at in group if t.is_alive()]
        out += [f"exit-extract-{rid}" for rid, futures in _FUTURES.items() for f in futures if not f.done()]
        out += [f"exit-verify-callback-{rid}" for rid, futures in _FUTURES.items() if all(f.done() for f in futures)]
    return out


def join_background(timeout: float = 60.0, *, extracts: bool = True) -> bool:
    """Wait until this process's extract jobs and verifications have finished; ``False`` on timeout.

    ``extracts=False`` waits for running verifications only (extract jobs are the engine's to stop).
    """
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        with _BG_LOCK:
            threads = [t for group in _VERIFIERS.values() for t, _at in group if t.is_alive()]
            futures = [f for group in _FUTURES.values() for f in group] if extracts else []
            verifying = bool(_ACTIVE)
        if not threads and not futures and not (verifying and not extracts):
            return True
        left = deadline - time.monotonic()
        if left <= 0:
            return False
        for thread in threads:
            thread.join(min(left, 0.2))
        if futures and not threads:
            import concurrent.futures

            concurrent.futures.wait(futures, timeout=min(left, 0.2))
        if not threads:
            time.sleep(0.01)  # a done callback runs just after its future completes


def stop_background(timeout: float = 30.0) -> bool:
    """App shutdown: stop running verifications (their claims are released, nothing is closed) and join.

    A stopped rehearsal stays ``running``; the next start's scheduler beat verifies it.
    """
    _STOPPING.set()
    try:
        with _BG_LOCK:
            active = list(_ACTIVE.values())
        for progress in active:
            progress.abandoned = True
            progress.cancel()
        return join_background(timeout, extracts=False)
    finally:
        _STOPPING.clear()


# -------------------------------------------------------------------- start


def _source_endpoint(source: dict[str, Any], table: dict[str, Any], workspace_id: str) -> Any:
    from types import SimpleNamespace

    from services.schedule_runner import _endpoint_from_connector, _resolve_connector_for_schedule

    conn = _resolve_connector_for_schedule(str(source.get("connector_id") or ""),
                                           SimpleNamespace(workspace_id=workspace_id))
    if not conn:
        raise ExitInvalid("The system's source connection was not found in its workspace.")
    endpoint = _endpoint_from_connector(conn, table["name"])
    if table.get("schema"):
        endpoint.schema = table["schema"]
    return endpoint


def _keys(table: dict[str, Any], endpoint: Any) -> list[str]:
    if table.get("key_columns"):
        return list(table["key_columns"])
    try:
        from services.source_schema_authority import endpoint_primary_key_columns

        return endpoint_primary_key_columns(endpoint)
    except Exception:  # noqa: BLE001 - no catalog key: the key-set check does not apply
        return []


def extract_request(rehearsal: dict[str, Any], index: int, table: dict[str, Any], endpoint: Any) -> Any:
    """The extract job of one table: full read into a file export in the system's format."""
    from src.transfer.models import EndpointConfig, TransferRequest

    fmt = rehearsal["extract_format"]
    name = _SAFE.sub("_", table["name"])[:80] or "table"
    contracts = [{"name": table["name"], "selected": True, "sync_mode": "full_refresh_overwrite",
                  **({"primary_key": list(table["key_columns"])} if table["key_columns"] else {})}]
    return TransferRequest(
        source=endpoint,
        destination=EndpointConfig(kind="file_export", format=fmt,
                                   output_path=f"exit_{rehearsal['id']}_{index:04d}_{name}.{fmt}"),
        sync_mode="full_refresh_overwrite", stream_contracts=contracts, validation_mode="strict",
        skip_preflight=False, workspace_id=rehearsal["workspace_id"],
        triggered_by=f"exit rehearsal {rehearsal['id']}", idempotency_key=f"exit-{rehearsal['id']}-{index}",
        # The system's PII acknowledgement (services.exit_pii), never an implicit pass of the PII gate.
        **_acknowledgement(rehearsal),
    )


def _acknowledgement(rehearsal: dict[str, Any]) -> dict[str, Any]:
    """The extract's acknowledgement fields: the system's PII acknowledgement when it has a current one."""
    ack = rehearsal.get("pii_acknowledgement") if isinstance(rehearsal.get("pii_acknowledgement"), dict) else None
    if not ack:
        return {"acknowledgment_actor": rehearsal["actor"],
                "acknowledgment_reason": f"Exit rehearsal {rehearsal['id']} of {rehearsal['system_name']}."}
    return {"compliance_acknowledged": True, "acknowledgment_actor": str(ack.get("actor") or ""),
            "acknowledgment_reason": (f"{ack.get('reason')} (PII acknowledged {ack.get('at')}; exit rehearsal "
                                      f"{rehearsal['id']} of {rehearsal['system_name']})")}


def _claim_system(system_id: str, rehearsal_id: str) -> None:
    def claim(doc: dict[str, Any]) -> None:
        running = doc.get("running_rehearsal_id")
        if running:
            current = exit_store.get(REHEARSALS, running)
            if current and current.get("verdict") == "running":
                raise ExitConflict("A rehearsal of this system is already running.")
        doc["running_rehearsal_id"] = rehearsal_id

    try:
        exit_store.update(SYSTEMS, system_id, claim)
    except exit_store.StoreConflict as exc:
        raise ExitConflict("A rehearsal of this system is starting; try again.") from exc


def _release_system(system_id: str, rehearsal_id: str) -> None:
    def release(doc: dict[str, Any]) -> None:
        if doc.get("running_rehearsal_id") == rehearsal_id:
            doc["running_rehearsal_id"] = None

    for _ in range(3):
        try:
            exit_store.update(SYSTEMS, system_id, release)
            return
        except KeyError:
            return  # the system was removed; its rehearsals are kept
        except exit_store.StoreConflict:
            continue


def start(system: dict[str, Any], *, actor: str, trigger: str = "manual") -> dict[str, Any]:
    """Start a rehearsal of ``system``: one extract job per declared table (contract 4)."""
    from services.exit_inventory import default_roles
    from services.exit_pii import snapshot as pii_snapshot
    from services.export_paths import connector_export_scope
    from src.transfer.background import run_transfer_async
    from src.transfer.engine import get_transfer_engine

    rid = f"exr_{uuid.uuid4().hex[:12]}"
    _claim_system(system["id"], rid)
    previous = rehearsals_of(system["id"])
    started = now_iso()
    tables = [{"name": t["name"], "schema": t.get("schema"), "key_columns": list(t.get("key_columns") or []),
               "job_id": None, "status": "pending", "reason": "Waiting for the extract job."}
              for t in system.get("tables") or []]
    rehearsal = exit_store.insert(REHEARSALS, {
        "id": rid, "system_id": system["id"], "system_name": system.get("name"),
        "workspace_id": system.get("workspace_id") or "", "n": 1 + max((int(r.get("n") or 0) for r in previous),
                                                                       default=0),
        "trigger": trigger, "actor": actor or "anonymous", "started_at": started, "finished_at": None,
        "verdict": "running", "verdict_reason": "The rehearsal is still extracting or verifying.",
        "source": dict(system.get("source") or {}), "extract_format": system.get("extract_format") or "jsonl",
        "window_seconds": int(system.get("exit_window_hours") or 0) * 3600,
        "definition_sha256": definition_sha256(system),
        "pii_acknowledgement": pii_snapshot(system),
        "required_roles": list(system.get("required_roles") or default_roles()),
        "tables": tables, "approvals": [], "signed": None, "_verify": None,
    })
    taken = rehearsal.get("pii_acknowledgement") or None
    if taken is None:
        from services.exit_pii import note_lapse

        note_lapse(system)  # an acknowledgement of an earlier definition is recorded as lapsed, for good
    audit("exit.rehearsal.started", system=system, actor=actor,
          details={"rehearsal_id": rid, "trigger": trigger, "tables": len(tables),
                   # Whether this run's extracts carry a PII acknowledgement, and whose.
                   "pii_acknowledged": bool(taken), "pii_acknowledged_by": (taken or {}).get("actor"),
                   "pii_acknowledged_at": (taken or {}).get("at")})
    engine = get_transfer_engine()
    launches: list[tuple[int, Any, str]] = []
    # Reading the source's key columns opens it as the system's workspace, also on a scheduled run
    # with no request in play (its embedded file lives in that workspace's volume area).
    with connector_export_scope(str(rehearsal.get("workspace_id") or "")):
        for index, table in enumerate(tables):
            try:
                endpoint = _source_endpoint(rehearsal["source"], table, rehearsal["workspace_id"])
                table["key_columns"] = _keys(table, endpoint)
                req = extract_request(rehearsal, index, table, endpoint)
                job_id = engine._create_pending_job(req)
                _job_store().update_job_fields(job_id, {"exit_rehearsal_id": rid})
                table.update(job_id=job_id, status="running", reason="Extracting.")
                launches.append((index, req, job_id))
            except Exception as exc:  # noqa: BLE001 - this table failed to start; the others still run
                from services.parity_errors import safe_error

                table.update(status="failed", reason=safe_error(exc, what="Starting the extract job",
                                                                event=ERROR_EVENT, reference=True))

    def record(doc: dict[str, Any]) -> None:
        doc["tables"] = tables

    rehearsal, _ = exit_store.update(REHEARSALS, rid, record)
    for _index, req, job_id in launches:
        try:
            future = run_transfer_async(job_id, req, resume=False)
        except Exception as exc:  # noqa: BLE001 - the scheduler refused: that table reads failed, by name
            logger.warning("exit rehearsal %s: scheduling %s failed (%s)", rid, job_id, type(exc).__name__)
            _job_store().update_job_status(job_id, "failed", error="The scheduler did not accept the extract job.")
        else:
            if future is not None and hasattr(future, "add_done_callback"):
                _track_future(rid, future)
    return settle(rid, inline=False, start=False) or rehearsal


# ------------------------------------------------------------------- settle


def settle_quietly(rehearsal_id: str) -> None:
    try:
        settle(rehearsal_id)
    except Exception as exc:  # noqa: BLE001 - the next read settles it
        logger.info("exit rehearsal %s not settled yet (%s)", rehearsal_id, type(exc).__name__)


def _live_tables(doc: dict[str, Any], jobs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for table in doc["tables"]:
        table = dict(table)
        job = jobs.get(str(table.get("job_id") or ""))
        if table["status"] in {"pending", "running"} and job:
            status = str(job.get("status") or "").lower()
            table["status"] = "running"
            table["reason"] = "Verifying." if status in TERMINAL else f"Extracting ({status or 'pending'})."
        out.append(table)
    return out


def _update_retrying(rehearsal_id: str, mutate: Any, attempts: int = 5) -> dict[str, Any] | None:
    """``exit_store.update`` retried over concurrent writes; ``None`` when ``mutate`` refuses."""
    for _ in range(attempts):
        try:
            return exit_store.update(REHEARSALS, rehearsal_id, mutate)[0]
        except ExitConflict:
            return None
        except exit_store.StoreConflict:
            continue
    return None


class _Lease:
    """Keeps a verification claim fresh while the verifier makes progress (a heartbeat, not a timer).

    A claim is taken over only when its timestamp is older than ``VERIFY_STALE``;
    the lease checks every quarter of that window and refreshes the claim only
    when the verifier read rows since the last check (review 2, N4). A verifier
    that died, or hangs on a dead connection, stops refreshing and its claim
    goes stale; one that is merely slow keeps it and closes the rehearsal. Past
    the verification's total cap the lease stops it (:mod:`services.exit_verify_guard`).
    """

    def __init__(self, rehearsal_id: str, claim_id: str, progress: Any = None) -> None:
        self._stop = threading.Event()
        self._rid, self._claim = rehearsal_id, claim_id
        self._progress, self._seen = progress, -1
        self._thread = threading.Thread(target=self._beat, daemon=True, name=f"exit-verify-lease-{rehearsal_id}")
        self._thread.start()

    def _refresh(self, current: dict[str, Any]) -> None:
        held = current.get("_verify") or {}
        if current.get("verdict") != "running" or held.get("id") != self._claim:
            raise ExitConflict("the claim was lost")
        current["_verify"] = {**held, "at": now_iso()}

    def _lost(self) -> bool:
        """Another verifier holds the rehearsal now (or it is closed): this one must stop reading."""
        current = exit_store.get(REHEARSALS, self._rid) or {}
        return current.get("verdict") != "running" or (current.get("_verify") or {}).get("id") != self._claim

    def _stop_reading(self) -> None:
        progress = self._progress
        if progress is not None:
            progress.lost = True
            progress.cancel()

    def _beat(self) -> None:
        interval = max(VERIFY_STALE.total_seconds() / 4, 0.05)
        while not self._stop.wait(interval):
            progress = self._progress
            try:
                if progress is not None:
                    if progress.expired():
                        progress.cancel()  # the cap: stop the verifier and interrupt its blocked reads
                        return
                    if progress.rows == self._seen:
                        # No row read since the last check: let the claim go stale - and once it has been
                        # taken over, stop this verifier too (it must not read to the end for nothing).
                        if self._lost():
                            self._stop_reading()
                            return
                        continue
                    self._seen = progress.rows
                try:
                    exit_store.update(REHEARSALS, self._rid, self._refresh)
                except ExitConflict:
                    self._stop_reading()  # the claim was lost: a verifier without it stops reading
                    return
                except exit_store.StoreConflict:
                    logger.debug("exit rehearsal %s: claim refresh raced; next beat retries", self._rid)
            except Exception as exc:  # noqa: BLE001 - a missed beat is retried on the next one
                logger.info("exit rehearsal %s: claim refresh failed (%s)", self._rid, type(exc).__name__)

    def release(self) -> None:
        self._stop.set()
        self._thread.join(timeout=30)


def _verify_within_cap(doc: dict[str, Any], jobs: dict[str, dict[str, Any]], rehearsal_id: str, claim_id: str,
                       held_seconds: float) -> dict[str, Any]:
    """``verify`` under its lease, progress tracking and total cap (review 2, N4; review 3).

    ``held_seconds`` is how long earlier verifiers of this rehearsal held the
    claim: the cap counts verifying time only, never the time nobody was
    verifying (a replica that was down). Raises :class:`_Abandoned` when this
    verifier lost its claim or the process is stopping (no result to close with).
    """
    from services.exit_verify_guard import VerificationTimedOut, max_verify_seconds, tracking

    cap = max_verify_seconds()
    left = cap - max(0.0, held_seconds)
    if left <= 0:
        return verify(doc, jobs, timed_out_after=cap)
    with tracking(left) as progress:
        with _BG_LOCK:
            _ACTIVE[claim_id] = progress
            progress.abandoned = _STOPPING.is_set()
        lease = _Lease(rehearsal_id, claim_id, progress)
        try:
            if progress.abandoned:
                raise _Abandoned()
            result = verify(doc, jobs)
        except VerificationTimedOut:
            if progress.abandoned or progress.lost:
                raise _Abandoned() from None
            return verify(doc, jobs, timed_out_after=cap)
        finally:
            lease.release()
            with _BG_LOCK:
                _ACTIVE.pop(claim_id, None)
        if progress.abandoned or progress.lost:
            raise _Abandoned()
        # A read the cap interrupted (or the server cancelled at it) is a timeout, not a read failure. A
        # verification that read everything is kept, even when it finished a moment past the cap.
        interrupted = progress.cancelled.is_set() and any(t.get("_read_failed") for t in result["tables"])
        if interrupted:
            return verify(doc, jobs, timed_out_after=cap)
        for table in result["tables"]:
            table.pop("_read_failed", None)
        return result


def settle(rehearsal_id: str, *, inline: bool = True, start: bool = True) -> dict[str, Any] | None:
    """Verify, seal and close a rehearsal whose extract jobs have all finished.

    Returns the stored document (unchanged while jobs still run). Exactly one
    caller verifies: the verification is claimed by compare-and-swap and kept
    fresh by its verifier (:class:`_Lease`); only a claim nobody refreshed for
    ``VERIFY_STALE`` (its verifier died) is taken over. ``inline=False`` (the
    scheduler beat, a sign-off attempt) never runs the verification on the
    caller's thread — it re-reads both whole populations — but starts it on one
    tracked background thread and answers with the tables "Verifying.".
    ``start=False`` (a page read) only reports: a read never starts work.
    """
    doc = exit_store.get(REHEARSALS, rehearsal_id)
    if not doc or doc.get("verdict") != "running":
        return doc
    store = _job_store()
    jobs = {str(t["job_id"]): (store.get_job(str(t["job_id"])) or {}) for t in doc["tables"] if t.get("job_id")}
    if any(str(j.get("status") or "").lower() not in TERMINAL for j in jobs.values()):
        doc["tables"] = _live_tables(doc, jobs)
        return doc
    if not inline:
        held = parse_ts((doc.get("_verify") or {}).get("at"))
        if start and (not held or datetime.now(timezone.utc) - held >= VERIFY_STALE):
            _start_verifier(rehearsal_id)
        doc["tables"] = _live_tables(doc, jobs)
        return doc
    claim_id = uuid.uuid4().hex

    def claim(current: dict[str, Any]) -> None:
        held = current.get("_verify") or {}
        at = parse_ts(held.get("at"))
        if current.get("verdict") != "running" or (at and datetime.now(timezone.utc) - at < VERIFY_STALE):
            raise ExitConflict("verification already claimed")
        # The total cap counts the time verifiers held the claim (each one: its claim to its last
        # refresh), so a takeover does not reset it and time nobody was verifying does not count.
        current["_verify"] = {"id": claim_id, "at": now_iso(), "claimed_at": now_iso(),
                              "started": held.get("started") or now_iso(), "held_seconds": _held_seconds(held)}

    try:
        claimed, _ = exit_store.update(REHEARSALS, rehearsal_id, claim)
    except (ExitConflict, exit_store.StoreConflict):
        return exit_store.get(REHEARSALS, rehearsal_id)
    try:
        result = _verify_within_cap(doc, jobs, rehearsal_id, claim_id,
                                    float(claimed["_verify"].get("held_seconds") or 0.0))
    except _Abandoned:
        _release_claim(rehearsal_id, claim_id)
        return exit_store.get(REHEARSALS, rehearsal_id)

    def close(current: dict[str, Any]) -> None:
        if (current.get("_verify") or {}).get("id") != claim_id:
            raise ExitConflict("another verifier took over")
        current.update(result)
        current["_verify"] = None

    closed = _update_retrying(rehearsal_id, close)
    if closed is None:
        return exit_store.get(REHEARSALS, rehearsal_id)
    _release_system(doc["system_id"], rehearsal_id)
    system = {"id": doc["system_id"], "workspace_id": doc["workspace_id"]}
    action = "exit.rehearsal.failed" if closed["verdict"] == "failed" else "exit.rehearsal.completed"
    audit(action, system=system, actor="system:exit-rehearsal",
          details={"rehearsal_id": rehearsal_id, "verdict": closed["verdict"],
                   "measured_seconds": closed["duration"]["measured_seconds"],
                   "merkle_root": closed["seal"]["merkle_root"]},
          outcome="failed" if closed["verdict"] == "failed" else "succeeded")
    return closed


def _held_seconds(held: dict[str, Any]) -> float:
    """Seconds verifiers have held a rehearsal's claim so far: earlier attempts plus ``held``'s own.

    An attempt held the claim from when it took it until it last refreshed it,
    plus at most the stale window (a verifier that stopped is presumed gone
    once its claim is stale). The hours a rehearsal then waited for a verifier
    are nobody's verifying time.
    """
    total = float(held.get("held_seconds") or 0.0)
    claimed, last = parse_ts(held.get("claimed_at")), parse_ts(held.get("at"))
    if claimed and last:
        until = min(datetime.now(timezone.utc), last + VERIFY_STALE)
        total += max(0.0, (until - claimed).total_seconds())
    return total


def _release_claim(rehearsal_id: str, claim_id: str) -> None:
    """Give up a claim this verifier still holds (shutdown), keeping the time it was held."""
    def release(current: dict[str, Any]) -> None:
        held = current.get("_verify") or {}
        if current.get("verdict") != "running" or held.get("id") != claim_id:
            raise ExitConflict("the claim is not this verifier's")
        spent = float(held.get("held_seconds") or 0.0)
        claimed = parse_ts(held.get("claimed_at"))
        spent += max(0.0, (datetime.now(timezone.utc) - claimed).total_seconds()) if claimed else 0.0
        current["_verify"] = {"id": None, "at": None, "claimed_at": None, "started": held.get("started"),
                              "held_seconds": spent}

    _update_retrying(rehearsal_id, release)


# ------------------------------------------------------------------- verify


def _part(table: str, ds: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    from services.export_object_store import STORAGE_OBJECT

    if ds.get("storage") == STORAGE_OBJECT and ds.get("object_key"):
        return {"table": table, "name": str(ds.get("filename") or ""), "storage": "object_store",
                "object_key": ds["object_key"], "size_bytes": int(ds.get("size_bytes") or 0),
                "sha256": str(ds.get("sha256") or ""), "verified_on_read": False,
                "upload_parts": ds.get("upload_parts")}, ""
    path = str(ds.get("path") or "")
    if not path:
        return None, ""
    # The SHA-256 and size the export recorded when it wrote the file (empty on an older job).
    return {"table": table, "name": str(ds.get("filename") or ""), "storage": "local_file", "object_key": None,
            "size_bytes": int(ds.get("size_bytes") or 0), "sha256": str(ds.get("sha256") or ""),
            "verified_on_read": False, "upload_parts": None}, path


def _parity(job: dict[str, Any], job_id: str) -> dict[str, Any]:
    from services.parity_report import build_parity_report

    report = build_parity_report(job, reread=False, job_id=job_id)
    first = (report.get("tables") or [{}])[0]
    return {"check": first.get("check") or "none", "verdict": report.get("verdict"),
            "unexplained": int((report.get("parity") or {}).get("unexplained") or 0)}


def _failed_reason(job: dict[str, Any], job_id: str, schema: Any = ()) -> str:
    """Why the extract job failed, allowlisted: a child job's error quotes row values (contract 20)."""
    from services.exit_job_reasons import failed_reason

    return failed_reason(job, job_id, schema=schema)


def _table_columns(doc: dict[str, Any], table: dict[str, Any]) -> list[str]:
    """The source table's column names (schema), to name in a failure; [] when the catalog cannot be read."""
    try:
        return list(_declared_types(doc, table)[0])
    except Exception:  # noqa: BLE001 - the reason then names no column, never a value
        logger.info("exit rehearsal: column names unread for %s", table.get("name"), exc_info=True)
        return []


def _not_extracted(job: dict[str, Any], job_id: str, schema: Any = ()) -> dict[str, str]:
    """{status, reason} of a table whose extract job did not complete: held by the PII gate, or failed."""
    from services.exit_pii import NEEDS_PII_REVIEW, held_by_pii_gate, held_reason

    found = held_by_pii_gate(job)
    if found is not None:
        lapsed = (job.get("exit_pii_acknowledgement") or {}).get("lapsed")
        reason = held_reason(found, job_id)
        if lapsed:
            reason = (f"The extract job {job_id} started after {lapsed}, so the PII gate held it. "
                      + reason.split(": ", 1)[-1])
        return {"status": NEEDS_PII_REVIEW, "reason": reason, "pii_held": _held_columns(found)}
    return {"status": "failed", "reason": _failed_reason(job, job_id, schema)}


def _held_columns(found: dict[str, list[str]]) -> list[dict[str, Any]]:
    return [{"column": col, "categories": sorted({str(c) for c in cats or []})} for col, cats in sorted(found.items())]


def _acknowledgement_gap(doc: dict[str, Any], table: dict[str, Any], job: dict[str, Any],
                         job_id: str) -> dict[str, Any] | None:
    """{status, reason, pii_held} when a completed extract's PII acknowledgement does not stand at settle.

    The extract ran as an acknowledged transfer. It is proven only if that
    acknowledgement still holds now (not withdrawn, not lapsed: review 3, M1)
    and covers every column and category the gate found in this table (M2).
    """
    from services.exit_pii import NEEDS_PII_REVIEW, acknowledgement_lapsed, beyond_acknowledged, sensitive_fields

    found = sensitive_fields(job)
    request = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    if not found or not request.get("compliance_acknowledged"):
        return None
    action = ("A person acknowledges PII for this system again (who and why are audited and kept in the "
              "evidence), then rehearses it again.")
    lapsed = acknowledgement_lapsed(doc)
    if lapsed:
        return {"status": NEEDS_PII_REVIEW, "pii_held": _held_columns(found),
                "reason": (f"The extract job {job_id} ran under a PII acknowledgement that no longer stands "
                           f"({lapsed}), so what it wrote is not sealed as proven. {action}")}
    extra = beyond_acknowledged(found, (doc.get("pii_acknowledgement") or {}).get("covers"), str(table["name"]))
    if extra:
        cols = "; ".join(f"{c} ({', '.join(cats)})" if cats else c for c, cats in list(sorted(extra.items()))[:10])
        return {"status": NEEDS_PII_REVIEW, "pii_held": _held_columns(found),
                "reason": (f"The extract job {job_id} found PII beyond what was acknowledged: {cols}"
                           + (f" (+{len(extra) - 10} more)" if len(extra) > 10 else "")
                           + f". What it wrote is not sealed as proven. {action}")}
    return None


def _declared_types(doc: dict[str, Any], table: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    """(the source table's declared column types, {column: why it is not compared exactly}).

    The types steer the digest of both sides; a text column the engine read as
    JSON is decided by the catalog, never by its content (review 3, M7).
    """
    from services.exit_completeness import catalog_declared_types, pg_domain_base_types
    from services.source_schema_authority import endpoint_source_column_types
    from src.transfer.engine_family import family_or_kind

    endpoint = _source_endpoint(doc["source"], table, doc["workspace_id"])
    engine = family_or_kind(getattr(endpoint, "format", ""))
    types = endpoint_source_column_types(endpoint)
    domains: dict[str, str] = {}
    try:
        # A domain-typed column is its base type (introspection reports it as TEXT; review 5).
        domains = {c: t for c, t in pg_domain_base_types(endpoint, engine).items() if c in types}
    except Exception:  # noqa: BLE001 - an unread catalog keeps the introspected type: it fails closed
        logger.info("exit rehearsal: domain base types unread for %s", table.get("name"), exc_info=True)
    types.update(domains)
    types, unproven = catalog_declared_types(types, endpoint, engine)
    if str(doc.get("extract_format") or "").lower() not in {"csv", "tsv"}:
        from services.exit_completeness import domain_written_as_text

        unproven.update(domain_written_as_text(domains))
    return types, unproven


def verify_table(doc: dict[str, Any], table: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
    """One table's RehearsalTable (contract 4) from its finished job."""
    from services.exit_completeness import (
        json_null_documents,
        judge_table,
        mixed_storage_classes,
        profile_extract,
        profile_rows,
        resolve_keys,
        server_text_columns,
        source_columns_and_rows,
    )
    from services.parity_errors import safe_error
    from src.transfer.engine_family import family_or_kind

    out = {**table, "rows_source": None, "rows_extracted": None,
           "key_set": {"source_sha256": None, "extract_sha256": None, "match": None},
           "row_set": {"source_sha256": None, "extract_sha256": None, "match": None},
           "control_totals": [], "differences": [], "part": None, "extract_seconds": None,
           "value_parity": {"method": "none", "parity_verdict": None, "unexplained": None, "report": None}}
    if not job:
        return {**out, "status": "failed", "reason": "The extract job record is missing."}
    job_id = str(table["job_id"])
    out["value_parity"]["report"] = f"/api/v1/jobs/{job_id}/parity-report"
    ds = job.get("destination_summary") if isinstance(job.get("destination_summary"), dict) else {}
    out["extract_seconds"] = ds.get("elapsed_seconds")
    if str(job.get("status") or "").lower() != "completed":
        return {**out, **_not_extracted(job, job_id, _table_columns(doc, table))}
    gap = _acknowledgement_gap(doc, table, job, job_id)
    if gap is not None:
        return {**out, **gap}  # no part: an extract whose acknowledgement does not stand is not sealed
    part, local_path = _part(table["name"], ds)
    if part is None:
        return {**out, "status": "failed", "reason": "The extract job recorded no stored part."}
    gaps: list[str] = []
    try:
        parity = _parity(job, job_id)
    except Exception as exc:  # noqa: BLE001 - the report could not be built: say so
        parity = {}
        gaps.append(f"Parity Studio could not read the extract job's evidence ({type(exc).__name__})")
    keys = list(table.get("key_columns") or [])
    source = extract = None
    types: dict[str, str] = {}
    engine = ""
    try:
        endpoint = _source_endpoint(doc["source"], table, doc["workspace_id"])
        # The engine by its family: a hosted tile (supabase, azure_sql_database) is that engine (review 5, C-1).
        engine = family_or_kind(getattr(endpoint, "format", ""))
        types, unproven = _declared_types(doc, table)
        columns, rows, close = source_columns_and_rows(endpoint, types=types)
        keys, missing = resolve_keys(keys, columns)
        if missing:
            gaps.append(f"the declared key column(s) {', '.join(missing)} are not columns of the table, so the "
                        "key set was not measured")
        try:
            source = profile_rows(rows, columns, keys, None, types=types, engine=engine,
                                  extract_is_delimited=str(doc.get("extract_format") or "").lower() in {"csv", "tsv"},
                                  server_text=server_text_columns(columns, types, engine))
        finally:
            close()
        source.uncompared.update(unproven)
        for column, why in mixed_storage_classes(endpoint, columns, engine).items():
            source.uncompared.setdefault(column, why)  # SQLite: '1' next to 1 extracts alike (review 5, H-3)
        for column, count in json_null_documents(endpoint, types, engine).items():
            gaps.append(f"{column} holds {count} JSON null document(s), which the extract cannot tell from "
                        "SQL NULL, so its values were not compared exactly")
    except Exception as exc:  # noqa: BLE001 - not measured is a named gap, never a pass
        logger.info("exit rehearsal source read-back failed for %s", table["name"], exc_info=True)
        out["_read_failed"] = True
        gaps.append(f"the source could not be re-read for a population digest ({safe_error(exc, what='the read', event=ERROR_EVENT, reference=True)})")
    seal_ok: bool | None = None
    try:
        extract, digest, size = profile_extract(
            part, local_path=local_path, fmt=doc["extract_format"],
            columns=source.columns if source else [], key_columns=keys,
            numeric=sorted(source.totals) if source else [], types=types, engine=engine,
        )
        if part["sha256"]:
            seal_ok = digest == part["sha256"] and size == part["size_bytes"]
        else:
            # Nothing was recorded at write time: the bytes read are sealed, but not verified.
            part.update(sha256=digest, size_bytes=size)
            gaps.append("the stored part has no SHA-256 recorded when it was written, so it could not be "
                        "verified on read")
        part["verified_on_read"] = bool(seal_ok)
    except Exception as exc:  # noqa: BLE001 - an unreadable or expired part cannot be proven
        logger.info("exit rehearsal extract read-back failed for %s", table["name"], exc_info=True)
        out["_read_failed"] = True
        gaps.append(f"the stored part could not be read back ({safe_error(exc, what='the read', event=ERROR_EVENT, reference=True)})")
    if not part["sha256"]:
        part = None
    judged = judge_table(name=table["name"], source=source, extract=extract, key_columns=keys,
                         seal_ok=seal_ok, parity=parity, not_measured=gaps)
    out.update(
        status=judged["status"], reason=judged["reason"], differences=judged["differences"],
        control_totals=judged["control_totals"], part=part,
        rows_source=source.rows if source else None, rows_extracted=extract.rows if extract else None,
        key_set={"source_sha256": source.key_set_sha256 if source else None,
                 "extract_sha256": extract.key_set_sha256 if extract else None,
                 "match": (source.key_set_sha256 == extract.key_set_sha256)
                 if source and extract and source.key_set_sha256 else None},
        row_set={"source_sha256": source.row_set_sha256 if source else None,
                 "extract_sha256": extract.row_set_sha256 if extract else None,
                 "match": (source.row_set_sha256 == extract.row_set_sha256) if source and extract else None},
        value_parity={**out["value_parity"], "method": judged["value_method"],
                      "parity_verdict": parity.get("verdict"), "unexplained": parity.get("unexplained")},
    )
    return out


def _span(jobs: list[dict[str, Any]]) -> tuple[datetime | None, datetime | None]:
    starts = [parse_ts(j.get("started_at")) for j in jobs]
    ends = [parse_ts(j.get("completed_at")) for j in jobs]
    if not jobs or any(s is None for s in starts) or any(e is None for e in ends):
        return None, None
    return min(s for s in starts if s), max(e for e in ends if e)


def _hours(seconds: float) -> str:
    if seconds < 120:
        return f"{seconds:.0f} s"
    if seconds < 7200:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


def duration_block(extract_seconds: float | None, verify_seconds: float, window_seconds: int,
                   total_bytes: int, *, verdict: str) -> dict[str, Any]:
    """Duration (contract 4.2): measured clocks only.

    Only a ``proven`` rehearsal is compared with the window: a run that failed,
    left tables out or could not be proven did not extract the system, so how
    long it took says nothing about whether an exit fits.
    """
    measured = None if extract_seconds is None else int(round(extract_seconds + verify_seconds))
    if measured is None:
        statement = "The duration was not measured: an extract job carries no start or end time."
        return {"basis": "measured", "measured_seconds": None, "extract_seconds": None,
                "verify_seconds": int(round(verify_seconds)), "window_seconds": window_seconds,
                "fits_window": None, "margin_pct": None, "statement": statement}
    if verdict != "proven":
        statement = (f"Measured {_hours(measured)} (extract {_hours(extract_seconds or 0)}, verify "
                     f"{_hours(verify_seconds)}), but the rehearsal is not proven "
                     f"({str(verdict).replace('_', ' ')}), so it is not compared with the window.")
        return {"basis": "measured", "measured_seconds": measured, "extract_seconds": int(round(extract_seconds or 0)),
                "verify_seconds": int(round(verify_seconds)), "window_seconds": window_seconds,
                "fits_window": None, "margin_pct": None, "statement": statement}
    fits = measured <= window_seconds if window_seconds else None
    margin = round(100.0 * (window_seconds - measured) / window_seconds, 2) if window_seconds else None
    size = f" for {total_bytes:,} bytes" if total_bytes else ""
    verdict = ("fits the window" if fits else "does not fit the window") if fits is not None else "has no window"
    statement = (f"Measured {_hours(measured)}{size} (extract {_hours(extract_seconds or 0)}, verify "
                 f"{_hours(verify_seconds)}) against a window of {_hours(window_seconds)}: this exit {verdict}. "
                 "The figure is measured, not projected.")
    return {"basis": "measured", "measured_seconds": measured, "extract_seconds": int(round(extract_seconds or 0)),
            "verify_seconds": int(round(verify_seconds)), "window_seconds": window_seconds, "fits_window": fits,
            "margin_pct": margin, "statement": statement}


def _not_verified(table: dict[str, Any], job: dict[str, Any], timed_out_after: float) -> dict[str, Any]:
    """A table the verification did not measure: failed to start or extract, or the cap was reached."""
    from services.exit_verify_guard import timed_out_reason

    out = {**table, "rows_source": None, "rows_extracted": None, "part": None, "differences": [],
           "key_set": {"source_sha256": None, "extract_sha256": None, "match": None},
           "row_set": {"source_sha256": None, "extract_sha256": None, "match": None}, "control_totals": [],
           "value_parity": {"method": "none", "parity_verdict": None, "unexplained": None, "report": None},
           "extract_seconds": None}
    if table["status"] == "failed":
        return out
    if str(job.get("status") or "").lower() != "completed":
        return {**out, **_not_extracted(job, str(table.get("job_id") or ""))}
    return {**out, "status": "not_proven", "reason": timed_out_reason(timed_out_after)}


def verify(doc: dict[str, Any], jobs: dict[str, dict[str, Any]], *,
           timed_out_after: float | None = None) -> dict[str, Any]:
    """Every table's completeness, the seal, the measured duration and the verdict.

    ``timed_out_after`` (the cap, in seconds) closes the rehearsal without
    measuring: every extracted table reads not proven with that reason.
    """
    from services.exit_completeness import MERKLE_ALGORITHM, merkle_root, rehearsal_verdict
    from services.export_paths import connector_export_scope

    verify_started = datetime.now(timezone.utc)
    clock = time.monotonic()
    # The source is re-read as the system's workspace: its embedded SQLite/DuckDB file lives in that
    # workspace's area of the shared volume, which no unscoped caller may open (review round 3, H3).
    with connector_export_scope(str(doc.get("workspace_id") or "")):
        tables = [
            verify_table(doc, t, jobs.get(str(t.get("job_id") or ""), {}))
            if t["status"] != "failed" and timed_out_after is None else
            _not_verified(t, jobs.get(str(t.get("job_id") or ""), {}), float(timed_out_after or 0))
            for t in doc["tables"]
        ]
    verify_seconds = time.monotonic() - clock
    finished = datetime.now(timezone.utc)
    parts = [t["part"] for t in tables if t.get("part")]
    storages = {p["storage"] for p in parts}
    total_bytes = sum(int(p["size_bytes"]) for p in parts)
    seal = {"algorithm": MERKLE_ALGORITHM, "merkle_root": merkle_root(parts) if parts else None,
            "parts_count": len(parts), "total_bytes": total_bytes,
            "storage": (storages.pop() if len(storages) == 1 else "mixed") if storages else None, "parts": parts}
    first, last = _span([j for j in jobs.values() if j])
    extract_seconds = (last - first).total_seconds() if first and last else None
    verdict, reason = rehearsal_verdict(tables)
    methods = {m: 0 for m in ("full_checksum", "key_diff", "population_digest", "sampled", "count_only", "none")}
    for t in tables:
        methods[t["value_parity"]["method"]] = methods.get(t["value_parity"]["method"], 0) + 1
    count = {s: sum(1 for t in tables if t["status"] == s)
             for s in ("proven", "not_proven", "incomplete", "failed", "needs_pii_review")}
    extracted = sum(1 for t in tables if t.get("part"))
    statement = (f"{extracted} of {len(tables)} declared table(s) extracted. {count['proven']} proven complete "
                 f"(rows, key sets, every value and control totals). Value check: "
                 + ", ".join(f"{k.replace('_', ' ')} on {v}" for k, v in methods.items() if v) + ".")
    return {
        "verdict": verdict, "verdict_reason": reason, "finished_at": _iso(finished), "tables": tables,
        "completeness": {"tables_declared": len(tables), "tables_extracted": extracted,
                         "tables_proven": count["proven"], "tables_not_proven": count["not_proven"],
                         "tables_incomplete": count["incomplete"], "tables_failed": count["failed"],
                         "tables_needs_pii_review": count["needs_pii_review"],
                         "value_methods": methods, "statement": statement},
        "seal": seal,
        "duration": duration_block(extract_seconds, verify_seconds, int(doc.get("window_seconds") or 0),
                                   total_bytes, verdict=verdict),
        "_timeline": {"extract_started": _iso(first), "extract_finished": _iso(last),
                      "verify_started": _iso(verify_started), "verify_finished": _iso(finished)},
    }


# ------------------------------------------------------------- presentation


def timeline(doc: dict[str, Any]) -> list[dict[str, Any]]:
    marks = doc.get("_timeline") or {}
    running = doc.get("verdict") == "running"
    signed = (doc.get("signed") or {}).get("signed_at")

    def stage(name: str, start: Any, end: Any, status: str) -> dict[str, Any]:
        a, b = parse_ts(start), parse_ts(end)
        return {"stage": name, "status": status, "started_at": start, "finished_at": end,
                "seconds": round((b - a).total_seconds(), 3) if a and b else None}

    extracting = running and not marks
    return [
        stage("inventory", doc.get("started_at"), doc.get("started_at"), "done"),
        stage("extract", marks.get("extract_started"), marks.get("extract_finished"),
              "running" if extracting else ("failed" if doc.get("verdict") in {"failed", "needs_pii_review"}
                                            else "done")),
        stage("verify", marks.get("verify_started"), marks.get("verify_finished"),
              "running" if running and (doc.get("_verify") or {}) else ("pending" if running else "done")),
        stage("seal", marks.get("verify_finished"), marks.get("verify_finished"),
              "done" if (doc.get("seal") or {}).get("merkle_root") else "pending"),
        stage("sign", signed, signed, "done" if signed else "pending"),
    ]


def trend(doc: dict[str, Any]) -> dict[str, Any]:
    """Trend (contract 4): earlier finished rehearsals, measured figures only."""
    rows = [r for r in rehearsals_of(doc["system_id"]) if r.get("verdict") != "running"
            and str(r.get("started_at") or "") <= str(doc.get("started_at") or "")]
    rows = sorted(rows, key=lambda r: str(r.get("started_at") or ""))[-20:]
    points = [{"rehearsal_id": r["id"], "n": r.get("n"), "at": r.get("started_at"),
               "measured_seconds": (r.get("duration") or {}).get("measured_seconds"),
               "rows": sum(int(t.get("rows_extracted") or 0) for t in r.get("tables") or []) or None,
               "bytes": (r.get("seal") or {}).get("total_bytes"), "verdict": r.get("verdict")} for r in rows]
    sized = [p for p in points if p["rows"]]
    growth = (round(100.0 * (sized[-1]["rows"] - sized[-2]["rows"]) / sized[-2]["rows"], 2)
              if len(sized) >= 2 else None)
    # Only proven runs measured an exit: a failed or partial run's clock and rows project nothing.
    timed = [p for p in sized if p["measured_seconds"] is not None and p["verdict"] == "proven"]
    projection = None
    if len(timed) >= 2:
        latest, previous = timed[-1], timed[-2]
        seconds = int(round(latest["measured_seconds"] * latest["rows"] / previous["rows"]))
        projection = {"kind": "projection", "seconds": seconds,
                      "basis": (f"Projection, not a measurement: rehearsal {latest['n']} measured "
                                f"{latest['measured_seconds']} s for {latest['rows']} rows; if the data grows "
                                f"again as it did since rehearsal {previous['n']} ({previous['rows']} rows), "
                                f"the next exit takes about {seconds} s.")}
    return {"points": points, "growth_pct_rows": growth, "projection": projection}


#: RehearsalTable fields not measured yet (a pending / running / failed-to-start table).
_UNMEASURED_TABLE: dict[str, Any] = {
    "rows_source": None, "rows_extracted": None,
    "key_set": {"source_sha256": None, "extract_sha256": None, "match": None},
    "row_set": {"source_sha256": None, "extract_sha256": None, "match": None},
    "control_totals": [], "differences": [], "extract_seconds": None, "part": None,
    "value_parity": {"method": None, "parity_verdict": None, "unexplained": None, "report": None},
    # The columns (and categories) the PII gate named for a table that reads needs_pii_review.
    "pii_held": [],
}


def public(doc: dict[str, Any]) -> dict[str, Any]:
    """ExitRehearsal (contract 4): stored state plus the computed timeline, trend and sign-off."""
    from services.exit_evidence import evidence_block, signoff_block

    out = {k: v for k, v in doc.items() if not str(k).startswith("_")
           and k not in {"approvals", "signed", "source", "extract_format", "required_roles", "window_seconds"}}
    # A running rehearsal carries the full shape, with nothing measured yet.
    out["tables"] = [{**_UNMEASURED_TABLE, **{k: v for k, v in t.items() if not str(k).startswith("_")}}
                     for t in doc.get("tables") or []]
    n = len(doc.get("tables") or [])
    out.setdefault("completeness", {
        "tables_declared": n, "tables_extracted": 0, "tables_proven": 0, "tables_not_proven": 0,
        "tables_incomplete": 0, "tables_failed": sum(1 for t in doc.get("tables") or [] if t.get("status") == "failed"),
        "tables_needs_pii_review": 0,
        "value_methods": {}, "statement": "The rehearsal is still extracting or verifying."})
    out.setdefault("seal", {"algorithm": "sha256-merkle-rfc6962", "merkle_root": None, "parts_count": 0,
                            "total_bytes": 0, "storage": None, "parts": []})
    out.setdefault("duration", {"basis": "measured", "measured_seconds": None, "extract_seconds": None,
                                "verify_seconds": None, "window_seconds": int(doc.get("window_seconds") or 0),
                                "fits_window": None, "margin_pct": None,
                                "statement": "Not measured yet: the rehearsal is still running."})
    out["current"], out["stale_reason"] = _currency(doc)
    out["timeline"] = timeline(doc)
    out["trend"] = trend(doc)
    out["signoff"] = signoff_block(doc)
    out["evidence"] = evidence_block(doc)
    return out


def _currency(doc: dict[str, Any]) -> tuple[bool | None, str | None]:
    """(current, stale_reason): whether the system still has the definition this rehearsal ran against."""
    system = exit_store.get(SYSTEMS, str(doc.get("system_id") or ""))
    if not system:
        return None, "The system was removed; this rehearsal is kept as evidence of what it was."
    if doc.get("definition_sha256") and doc.get("definition_sha256") == definition_sha256(system):
        return True, None
    return False, ("The system's source connection, tables, keys, window or extract format changed since this "
                   "rehearsal; it no longer proves the system as it is defined now. Rehearse it again.")


# ------------------------------------------------------------ scheduler beat


def run_due_rehearsals(*, now: datetime | None = None) -> dict[str, int]:
    """Scheduler beat step: settle finished rehearsals, start scheduled ones that are due."""
    from services.cron_schedule import next_run
    from services.maintenance_claim import claim_local, claim_run

    current = now or datetime.now(timezone.utc)
    claimed = claim_run(SCHEDULE_CLAIM, SCHEDULE_INTERVAL, now=current)
    if claimed is None:
        claimed = claim_local(SCHEDULE_CLAIM, SCHEDULE_INTERVAL, now=current)
    if not claimed:
        return {"settled": 0, "started": 0}
    settled = started = 0
    for doc in exit_store.list_docs(REHEARSALS, query={"verdict": "running"}):
        closed = settle(doc["id"], inline=False)  # verification runs off the beat
        settled += int(bool(closed and closed.get("verdict") != "running"))
    for system in exit_store.list_docs(SYSTEMS, where=lambda d: bool(d.get("rehearsal_schedule"))):
        schedule = system["rehearsal_schedule"]
        after = parse_ts(system.get("schedule_last_fired_at")) or parse_ts(system.get("created_at")) or current
        try:
            due = next_run(schedule["cron"], after, schedule.get("timezone") or "UTC")
        except (ValueError, KeyError, TypeError) as exc:
            # A stored cron / zone that no longer parses (CronError is a ValueError, an unknown zone a
            # KeyError): this system's schedule cannot fire, the other systems still do. Named in the log.
            logger.warning("exit rehearsal schedule of %s cannot fire (%s)", system.get("id"), type(exc).__name__)
            continue
        if due > current:
            continue

        def mark(doc: dict[str, Any]) -> None:
            doc["schedule_last_fired_at"] = _iso(current)

        try:
            exit_store.update(SYSTEMS, system["id"], mark, expected_version=int(system.get("version") or 0))
            start(exit_store.get(SYSTEMS, system["id"]) or system, actor=SCHEDULE_ACTOR, trigger="schedule")
            started += 1
        except (ExitConflict, ExitInvalid, exit_store.StoreConflict, KeyError) as exc:
            logger.info("scheduled exit rehearsal of %s not started (%s)", system["id"], exc)
    return {"settled": settled, "started": started}
