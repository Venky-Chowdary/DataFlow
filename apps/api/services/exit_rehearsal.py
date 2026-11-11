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
        acknowledgment_actor=rehearsal["actor"],
        acknowledgment_reason=f"Exit rehearsal {rehearsal['id']} of {rehearsal['system_name']}.",
    )


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
        "required_roles": list(system.get("required_roles") or default_roles()),
        "tables": tables, "approvals": [], "signed": None, "_verify": None,
    })
    audit("exit.rehearsal.started", system=system, actor=actor,
          details={"rehearsal_id": rid, "trigger": trigger, "tables": len(tables)})
    engine = get_transfer_engine()
    launches: list[tuple[int, Any, str]] = []
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

            table.update(status="failed", reason=safe_error(exc, what="Starting the extract job", event=ERROR_EVENT))

    def record(doc: dict[str, Any]) -> None:
        doc["tables"] = tables

    rehearsal, _ = exit_store.update(REHEARSALS, rid, record)
    for _index, req, job_id in launches:
        try:
            future = run_transfer_async(job_id, req, resume=False)
        except Exception as exc:  # noqa: BLE001 - the scheduler refused: that table failed
            logger.warning("exit rehearsal %s: scheduling %s failed (%s)", rid, job_id, type(exc).__name__)
            _job_store().update_job_status(job_id, "failed", error="The scheduler did not accept the extract job.")
            continue
        if future is not None and hasattr(future, "add_done_callback"):
            future.add_done_callback(lambda _f: settle_quietly(rid))
    return settle(rid, inline=False) or rehearsal


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
    """Keeps a verification claim fresh while the verifier runs (a heartbeat, not a fixed age).

    A claim is taken over only when its timestamp is older than ``VERIFY_STALE``,
    so a live verifier refreshes it every quarter of that window. A verifier that
    died stops refreshing and its claim goes stale; one that is merely slow keeps
    it, and is the one that closes the rehearsal.
    """

    def __init__(self, rehearsal_id: str, claim_id: str) -> None:
        import threading

        self._stop = threading.Event()
        self._rid, self._claim = rehearsal_id, claim_id
        self._thread = threading.Thread(target=self._beat, daemon=True, name=f"exit-verify-lease-{rehearsal_id}")
        self._thread.start()

    def _refresh(self, current: dict[str, Any]) -> None:
        held = current.get("_verify") or {}
        if current.get("verdict") != "running" or held.get("id") != self._claim:
            raise ExitConflict("the claim was lost")
        current["_verify"] = {**held, "at": now_iso()}

    def _beat(self) -> None:
        interval = max(VERIFY_STALE.total_seconds() / 4, 0.05)
        while not self._stop.wait(interval):
            try:
                if _update_retrying(self._rid, self._refresh) is None:
                    return
            except Exception as exc:  # noqa: BLE001 - a missed beat is retried on the next one
                logger.info("exit rehearsal %s: claim refresh failed (%s)", self._rid, type(exc).__name__)

    def release(self) -> None:
        self._stop.set()
        self._thread.join(timeout=30)


def settle(rehearsal_id: str, *, inline: bool = True) -> dict[str, Any] | None:
    """Verify, seal and close a rehearsal whose extract jobs have all finished.

    Returns the stored document (unchanged while jobs still run). Exactly one
    caller verifies: the verification is claimed by compare-and-swap and kept
    fresh by its verifier (:class:`_Lease`); only a claim nobody refreshed for
    ``VERIFY_STALE`` (its verifier died) is taken over. ``inline=False`` (a page read, the
    scheduler beat, a sign-off attempt) never runs the verification on the
    caller's thread — it re-reads both whole populations — but starts it in
    the background and answers with the tables "Verifying.".
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
        import threading

        held = parse_ts((doc.get("_verify") or {}).get("at"))
        if not held or datetime.now(timezone.utc) - held >= VERIFY_STALE:
            threading.Thread(target=settle_quietly, args=(rehearsal_id,), daemon=True,
                             name=f"exit-verify-{rehearsal_id}").start()
        doc["tables"] = _live_tables(doc, jobs)
        return doc
    claim_id = uuid.uuid4().hex

    def claim(current: dict[str, Any]) -> None:
        held = current.get("_verify") or {}
        at = parse_ts(held.get("at"))
        if current.get("verdict") != "running" or (at and datetime.now(timezone.utc) - at < VERIFY_STALE):
            raise ExitConflict("verification already claimed")
        current["_verify"] = {"id": claim_id, "at": now_iso()}

    try:
        exit_store.update(REHEARSALS, rehearsal_id, claim)
    except (ExitConflict, exit_store.StoreConflict):
        return exit_store.get(REHEARSALS, rehearsal_id)
    lease = _Lease(rehearsal_id, claim_id)
    try:
        result = verify(doc, jobs)
    finally:
        lease.release()

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


def _failed_reason(job: dict[str, Any]) -> str:
    """Why the extract job failed, allowlisted: a child job's error quotes row values (contract 20)."""
    from services.parity_errors import safe_error

    status = str(job.get("status") or "without a status")
    error = job.get("error") or job.get("message")
    if not error:
        return f"The extract job ended {status}; no error was recorded."
    what = "The extract job" if status.lower() == "failed" else f"The extract job ({status})"
    return safe_error(error, what=what, event=ERROR_EVENT)


def _declared_types(doc: dict[str, Any], table: dict[str, Any]) -> dict[str, str]:
    """The source table's declared column types (they steer the digest of both sides)."""
    from services.source_schema_authority import endpoint_source_column_types

    return endpoint_source_column_types(_source_endpoint(doc["source"], table, doc["workspace_id"]))


def verify_table(doc: dict[str, Any], table: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
    """One table's RehearsalTable (contract 4) from its finished job."""
    from services.exit_completeness import (
        judge_table,
        profile_extract,
        profile_rows,
        resolve_keys,
        source_columns_and_rows,
    )
    from services.parity_errors import safe_error

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
        return {**out, "status": "failed", "reason": _failed_reason(job)}
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
        engine = str(getattr(endpoint, "format", "") or "").lower()
        types = _declared_types(doc, table)
        columns, rows, close = source_columns_and_rows(endpoint)
        keys, missing = resolve_keys(keys, columns)
        if missing:
            gaps.append(f"the declared key column(s) {', '.join(missing)} are not columns of the table, so the "
                        "key set was not measured")
        try:
            source = profile_rows(rows, columns, keys, None, types=types, engine=engine)
        finally:
            close()
    except Exception as exc:  # noqa: BLE001 - not measured is a named gap, never a pass
        logger.info("exit rehearsal source read-back failed for %s", table["name"], exc_info=True)
        gaps.append(f"the source could not be re-read for a population digest ({safe_error(exc, what='the read', event=ERROR_EVENT)})")
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
        gaps.append(f"the stored part could not be read back ({safe_error(exc, what='the read', event=ERROR_EVENT)})")
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


def verify(doc: dict[str, Any], jobs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Every table's completeness, the seal, the measured duration and the verdict."""
    from services.exit_completeness import MERKLE_ALGORITHM, merkle_root, rehearsal_verdict

    verify_started = datetime.now(timezone.utc)
    clock = time.monotonic()
    tables = [
        verify_table(doc, t, jobs.get(str(t.get("job_id") or ""), {})) if t["status"] != "failed" else
        {**t, "rows_source": None, "rows_extracted": None, "part": None, "differences": [],
         "key_set": {"source_sha256": None, "extract_sha256": None, "match": None},
         "row_set": {"source_sha256": None, "extract_sha256": None, "match": None}, "control_totals": [],
         "value_parity": {"method": "none", "parity_verdict": None, "unexplained": None, "report": None},
         "extract_seconds": None}
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
    count = {s: sum(1 for t in tables if t["status"] == s) for s in ("proven", "not_proven", "incomplete", "failed")}
    extracted = sum(1 for t in tables if t.get("part"))
    statement = (f"{extracted} of {len(tables)} declared table(s) extracted. {count['proven']} proven complete "
                 f"(rows, key sets, every value and control totals). Value check: "
                 + ", ".join(f"{k.replace('_', ' ')} on {v}" for k, v in methods.items() if v) + ".")
    return {
        "verdict": verdict, "verdict_reason": reason, "finished_at": _iso(finished), "tables": tables,
        "completeness": {"tables_declared": len(tables), "tables_extracted": extracted,
                         "tables_proven": count["proven"], "tables_not_proven": count["not_proven"],
                         "tables_incomplete": count["incomplete"], "tables_failed": count["failed"],
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
              "running" if extracting else ("failed" if doc.get("verdict") == "failed" else "done")),
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
    timed = [p for p in sized if p["measured_seconds"] is not None]
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
        "value_methods": {}, "statement": "The rehearsal is still extracting or verifying."})
    out.setdefault("seal", {"algorithm": "sha256-merkle-rfc6962", "merkle_root": None, "parts_count": 0,
                            "total_bytes": 0, "storage": None, "parts": []})
    out.setdefault("duration", {"basis": "measured", "measured_seconds": None, "extract_seconds": None,
                                "verify_seconds": None, "window_seconds": int(doc.get("window_seconds") or 0),
                                "fits_window": None, "margin_pct": None,
                                "statement": "Not measured yet: the rehearsal is still running."})
    out["timeline"] = timeline(doc)
    out["trend"] = trend(doc)
    out["signoff"] = signoff_block(doc)
    out["evidence"] = evidence_block(doc)
    return out


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
        except Exception:  # noqa: BLE001 - a schedule that cannot fire is skipped, not fatal
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
