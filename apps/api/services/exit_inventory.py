"""Exit inventory: the systems a workspace would have to take with it, and their readiness.

A system is a saved source connection, the tables to take, how critical it is
and the window an exit must fit in (a contract clause, the EU Data Act's
switching window, ...). Readiness is computed on every read from the stored
rehearsals (:mod:`services.exit_rehearsal`): the latest verdict, whether a
rehearsal is overdue (``rehearsal_every_days`` after the last one started), and
the measured duration against the window. Nothing here estimates a duration
or claims compliance; it reports what the rehearsals measured.
"""

from __future__ import annotations

import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from services import exit_store
from services.exit_store import REHEARSALS, SYSTEMS

CRITICALITY = ("critical", "important", "standard")
FORMATS = ("jsonl", "csv")
STATES = ("never_rehearsed", "running", "proven", "not_proven", "incomplete", "failed", "needs_pii_review")
DEFAULT_EVERY_DAYS = 90
MAX_TABLES = 2000
_TABLE_NAME = re.compile(r"^[^\x00-\x1f;'\"`]{1,256}$")
_WRITABLE = (
    "name", "description", "criticality", "source", "tables", "exit_window_hours", "window_basis",
    "regulatory_dates", "rehearsal_every_days", "rehearsal_schedule", "extract_format", "required_roles",
)


class ExitError(Exception):
    status_code = 400


class ExitNotFound(ExitError):
    status_code = 404


class ExitConflict(ExitError):
    status_code = 409


class ExitInvalid(ExitError):
    status_code = 422


class ExitForbidden(ExitError):
    status_code = 403


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else None


def default_roles() -> list[str]:
    from services.parity_state import empty_state

    return [str(r) for r in empty_state()["required_roles"]]


# ------------------------------------------------------------------ validation


def _text(value: Any, field: str, *, limit: int, required: bool = False) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ExitInvalid(f"{field} is required.")
    if len(text) > limit:
        raise ExitInvalid(f"{field} is longer than {limit} characters.")
    return text


def _int(value: Any, field: str, low: int, high: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ExitInvalid(f"{field} must be a whole number.") from exc
    if isinstance(value, bool) or not low <= number <= high:
        raise ExitInvalid(f"{field} must be between {low} and {high}.")
    return number


def _tables(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or not raw:
        raise ExitInvalid("tables must list at least one table to take.")
    if len(raw) > MAX_TABLES:
        raise ExitInvalid(f"A system declares at most {MAX_TABLES} tables.")
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        item = {"name": item} if isinstance(item, str) else item
        if not isinstance(item, dict):
            raise ExitInvalid("Each table is a name or an object with a name.")
        name = str(item.get("name") or "").strip()
        schema = str(item.get("schema") or "").strip() or None
        if not name or not _TABLE_NAME.match(name) or (schema and not _TABLE_NAME.match(schema)):
            raise ExitInvalid(f"{name or '(blank)'} is not a table name this system can extract.")
        ident = f"{(schema or '').lower()}.{name.lower()}"
        if ident in seen:
            raise ExitInvalid(f"{name} is declared twice.")
        seen.add(ident)
        keys = item.get("key_columns") or []
        if isinstance(keys, str):
            keys = [k for k in keys.split(",")]
        keys = [str(k).strip() for k in keys if str(k or "").strip()]
        out.append({"name": name, "schema": schema, "key_columns": keys})
    return out


def _dates(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            raise ExitInvalid("regulatory_dates holds {date, label} objects.")
        try:
            day = date.fromisoformat(str(item.get("date") or ""))
        except ValueError as exc:
            raise ExitInvalid(f"{item.get('date')!r} is not a YYYY-MM-DD date.") from exc
        out.append({"date": day.isoformat(), "label": _text(item.get("label"), "label", limit=200, required=True)})
    return sorted(out, key=lambda d: d["date"])


def _schedule(raw: Any) -> dict[str, str] | None:
    if raw in (None, {}, ""):
        return None
    if not isinstance(raw, dict):
        raise ExitInvalid("rehearsal_schedule is {cron, timezone} or null.")
    from services.cron_schedule import CronError, resolve_timezone, validate_cron

    cron = str(raw.get("cron") or "").strip()
    zone = str(raw.get("timezone") or "UTC").strip() or "UTC"
    try:
        validate_cron(cron)
        resolve_timezone(zone)
    except CronError as exc:
        raise ExitInvalid(f"rehearsal_schedule: {exc}") from exc
    return {"cron": cron, "timezone": zone}


def _roles(raw: Any) -> list[str]:
    from services.parity_authority import AuthorityConfigError, signoff_authority

    roles = [str(r).strip().lower() for r in (raw or default_roles()) if str(r or "").strip()]
    if not roles or len(set(roles)) != len(roles):
        raise ExitInvalid("required_roles must name each sign-off role once.")
    try:
        known = set(signoff_authority())
    except AuthorityConfigError as exc:
        raise ExitInvalid(str(exc)) from exc
    unknown = [r for r in roles if r not in known]
    if unknown:
        raise ExitInvalid(f"No sign-off authority is configured for {', '.join(unknown)}.")
    return roles


def validate(body: dict[str, Any], *, partial: bool = False) -> dict[str, Any]:
    """The writable fields of an ExitSystemInput, checked (contract 3); 422 on bad input."""
    out: dict[str, Any] = {}
    present = {k for k in _WRITABLE if k in body}
    if not partial:
        present |= {"name", "source", "tables", "exit_window_hours"}
    checks = {
        "name": lambda v: _text(v, "name", limit=120, required=True),
        "description": lambda v: _text(v, "description", limit=2000),
        "criticality": lambda v: _choice(v or "standard", CRITICALITY, "criticality"),
        "tables": _tables,
        "exit_window_hours": lambda v: _int(v, "exit_window_hours", 1, 87600),
        "window_basis": lambda v: _text(v, "window_basis", limit=300),
        "regulatory_dates": _dates,
        "rehearsal_every_days": lambda v: _int(v if v is not None else DEFAULT_EVERY_DAYS,
                                               "rehearsal_every_days", 1, 3650),
        "rehearsal_schedule": _schedule,
        "extract_format": lambda v: _choice(v or "jsonl", FORMATS, "extract_format"),
        "required_roles": _roles,
        "source": _source,
    }
    for name in present:
        out[name] = checks[name](body.get(name))
    return out


def _choice(value: Any, allowed: tuple[str, ...], field: str) -> str:
    text = str(value or "").strip().lower()
    if text not in allowed:
        raise ExitInvalid(f"{field} must be one of: {', '.join(allowed)}.")
    return text


def _source(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or not str(raw.get("connector_id") or "").strip():
        raise ExitInvalid("source.connector_id names the saved connection to take the data from.")
    return {"connector_id": str(raw["connector_id"]).strip()}


def resolve_source(source: dict[str, Any], workspace_id: str) -> dict[str, Any]:
    """Name and engine of the saved connector, in the system's workspace (else 422)."""
    from types import SimpleNamespace

    from services.schedule_runner import _resolve_connector_for_schedule

    conn = _resolve_connector_for_schedule(source["connector_id"], SimpleNamespace(workspace_id=workspace_id))
    if not conn:
        raise ExitInvalid("The source connection was not found in this workspace.")
    return {"connector_id": source["connector_id"], "connector_name": conn.get("name") or None,
            "engine": str(conn.get("type") or "").lower() or None}


# ----------------------------------------------------------------- operations


def audit(action: str, *, system: dict[str, Any], actor: str, details: dict[str, Any],
          outcome: str = "succeeded") -> None:
    """One audit-chain event per inventory / rehearsal write (``exit.*``)."""
    import logging

    from services.audit_log import append_audit_event

    try:
        append_audit_event(
            action=action, resource=f"exit_system:{system.get('id')}", actor=actor or "anonymous",
            level="info", details=details, workspace_id=str(system.get("workspace_id") or "") or None,
            outcome=outcome,
        )
    except Exception as exc:  # noqa: BLE001 - the write stands; a failed append is reported
        logging.getLogger(__name__).error("exit audit %s failed (%s)", action, type(exc).__name__, exc_info=exc)


def declare(body: dict[str, Any], *, workspace_id: str, actor: str) -> dict[str, Any]:
    fields = validate(body)
    fields["source"] = resolve_source(fields["source"], workspace_id)
    fields.setdefault("description", "")
    fields.setdefault("criticality", "standard")
    fields.setdefault("window_basis", "")
    fields.setdefault("regulatory_dates", [])
    fields.setdefault("rehearsal_every_days", DEFAULT_EVERY_DAYS)
    fields.setdefault("rehearsal_schedule", None)
    fields.setdefault("extract_format", "jsonl")
    fields.setdefault("required_roles", default_roles())
    at = now_iso()
    system = exit_store.insert(SYSTEMS, {
        "id": f"exs_{uuid.uuid4().hex[:12]}", "workspace_id": workspace_id, **fields,
        "created_at": at, "created_by": actor, "updated_at": at, "updated_by": actor,
        "schedule_last_fired_at": None,
    })
    audit("exit.system.declared", system=system, actor=actor,
          details={"name": system["name"], "tables": len(system["tables"]), "criticality": system["criticality"],
                   "exit_window_hours": system["exit_window_hours"]})
    return system


def get_system(system_id: str) -> dict[str, Any]:
    system = exit_store.get(SYSTEMS, system_id)
    if not system:
        raise ExitNotFound("Exit system not found")
    return system


def update(system_id: str, body: dict[str, Any], *, actor: str, expected_version: int | None) -> dict[str, Any]:
    system = get_system(system_id)
    fields = validate(body, partial=True)
    if "source" in fields:
        fields["source"] = resolve_source(fields["source"], str(system.get("workspace_id") or ""))
    if not fields:
        raise ExitInvalid("Nothing to change.")

    def apply(doc: dict[str, Any]) -> None:
        before = definition_sha256(doc)
        doc.update(fields)
        doc["updated_at"], doc["updated_by"] = now_iso(), actor
        if doc.get("pii_acknowledgement") and definition_sha256(doc) != before:
            # A PII acknowledgement is of one definition; once that changed it has lapsed for good
            # (changing the definition back does not bring it back).
            from services.exit_pii import lapse

            doc["pii_acknowledgement"] = lapse(doc["pii_acknowledgement"])

    try:
        updated, _ = exit_store.update(SYSTEMS, system_id, apply, expected_version=expected_version)
    except exit_store.StoreConflict as exc:
        raise ExitConflict(f"This system {exc}; reload it and try again.") from exc
    except KeyError as exc:
        raise ExitNotFound("Exit system not found") from exc
    audit("exit.system.updated", system=updated, actor=actor, details={"fields": sorted(fields)})
    return updated


def remove(system_id: str, *, actor: str) -> None:
    system = get_system(system_id)
    if running_rehearsal(system_id):
        raise ExitConflict("A rehearsal of this system is running; wait for it to finish.")
    exit_store.delete(SYSTEMS, system_id)
    audit("exit.system.deleted", system=system, actor=actor,
          details={"name": system.get("name"), "rehearsals_kept": True})


def rehearsals_of(system_id: str, *, limit: int = 0) -> list[dict[str, Any]]:
    return exit_store.list_docs(REHEARSALS, query={"system_id": system_id}, newest_first="started_at", limit=limit)


def running_rehearsal(system_id: str) -> dict[str, Any] | None:
    return next((r for r in rehearsals_of(system_id) if r.get("verdict") == "running"), None)


# ------------------------------------------------------------------ readiness


def connector_fingerprint(source: dict[str, Any], workspace_id: str) -> dict[str, Any]:
    """What the saved connection behind ``source.connector_id`` points at now (never a secret).

    The endpoint identity fields the job idempotency key reads (engine, host,
    port, database, schema, warehouse, path, ...) without the credential
    digest, so an edit to the connection behind the same id (another host or
    database) changes a system's definition and a password rotation does not.
    A connection that cannot be resolved reads ``{"resolved": False}``.
    """
    from types import SimpleNamespace

    from services.job_idempotency import _endpoint_identity
    from services.schedule_runner import _endpoint_from_connector, _resolve_connector_for_schedule

    connector_id = str((source or {}).get("connector_id") or "")
    try:
        conn = _resolve_connector_for_schedule(connector_id, SimpleNamespace(workspace_id=workspace_id))
        if not conn:
            return {"resolved": False}
        identity = _endpoint_identity(_endpoint_from_connector(conn, ""))
    except Exception:  # noqa: BLE001 - an unreadable connection is a changed definition, not a crash
        return {"resolved": False}
    return {k: v for k, v in identity.items() if k not in {"credential_digest", "table", "collection"}}


#: A read's own memo of the definition hash (never stored): one connector lookup per system per read.
_DEFINITION_MEMO = "_definition_sha256"


def with_definition(system: dict[str, Any]) -> dict[str, Any]:
    """``system`` with its definition hash computed once for this read (the board asks for it several times)."""
    if system.get(_DEFINITION_MEMO):
        return system
    return {**system, _DEFINITION_MEMO: definition_sha256(system)}


def definition_sha256(system: dict[str, Any]) -> str:
    """What a rehearsal proves about a system: its source, its tables and keys, its window and format.

    Stored on every rehearsal at start; readiness counts a proof only while the
    system still has this definition (a name, a criticality or a schedule may
    change without touching the proof). The source is the connector id *and*
    what that connection points at now (:func:`connector_fingerprint`), and the
    extract format is part of it (review 2, N9).
    """
    import hashlib
    import json

    if system.get(_DEFINITION_MEMO):
        return str(system[_DEFINITION_MEMO])
    tables = sorted(({"name": str(t.get("name") or ""), "schema": t.get("schema") or None,
                      "key_columns": [str(k) for k in t.get("key_columns") or []]}
                     for t in system.get("tables") or []), key=lambda t: (t["name"], str(t["schema"] or "")))
    source = system.get("source") or {}
    body = {"connector_id": str(source.get("connector_id") or ""), "tables": tables,
            "exit_window_hours": int(system.get("exit_window_hours") or 0),
            "extract_format": str(system.get("extract_format") or "jsonl"),
            "connector": connector_fingerprint(source, str(system.get("workspace_id") or ""))}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
                          .encode()).hexdigest()


def rehearsal_summary(rehearsal: dict[str, Any] | None, system: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """RehearsalSummary (contract 3); ``current`` when ``system`` still has the definition it rehearsed."""
    if not rehearsal:
        return None
    comp = rehearsal.get("completeness") or {}
    duration = rehearsal.get("duration") or {}
    seal = rehearsal.get("seal") or {}
    # A running rehearsal has no duration / completeness yet: read its stored definition.
    window = duration.get("window_seconds")
    window = int(rehearsal.get("window_seconds") or 0) if window is None else window
    return {
        "id": rehearsal["id"], "n": rehearsal.get("n"), "started_at": rehearsal.get("started_at"),
        "finished_at": rehearsal.get("finished_at"), "verdict": rehearsal.get("verdict"),
        "measured_seconds": duration.get("measured_seconds"), "window_seconds": window,
        # Only a proven run is compared with the window (contract 4.2).
        "fits_window": duration.get("fits_window") if rehearsal.get("verdict") == "proven" else None,
        "tables_declared": comp.get("tables_declared", len(rehearsal.get("tables") or [])),
        "tables_proven": comp.get("tables_proven", 0), "total_bytes": seal.get("total_bytes"),
        # The stored signature (``signoff`` is a computed view, never stored).
        "signed": isinstance(rehearsal.get("signed"), dict) and bool(rehearsal["signed"].get("signature")),
        "current": (None if system is None else
                    bool(rehearsal.get("definition_sha256")) and rehearsal.get("definition_sha256")
                    == definition_sha256(system)),
    }


def rehearsal_history(system: dict[str, Any], *, limit: int = 20) -> dict[str, Any]:
    """``GET /exit/systems/{id}/rehearsals``: summaries (``current`` against ``system``) and the trend."""
    from services.exit_rehearsal import trend

    rows = rehearsals_of(system["id"], limit=limit)
    return {"rehearsals": [rehearsal_summary(r, system) for r in rows],
            "trend": trend(rows[0]) if rows else {"points": [], "growth_pct_rows": None, "projection": None}}


def readiness(system: dict[str, Any], rehearsals: list[dict[str, Any]] | None = None,
              now: datetime | None = None) -> dict[str, Any]:
    """SystemReadiness (contract 3), deterministic from the stored rehearsals."""
    system = with_definition(system)
    rows = rehearsals if rehearsals is not None else rehearsals_of(system["id"])
    current = now or datetime.now(timezone.utc)
    running = next((r for r in rows if r.get("verdict") == "running"), None)
    finished = [r for r in rows if r.get("verdict") != "running"]
    last_started = rows[0] if rows else None
    basis = parse_ts((last_started or {}).get("started_at")) or parse_ts(system.get("created_at")) or current
    due = basis + timedelta(days=int(system.get("rehearsal_every_days") or DEFAULT_EVERY_DAYS))
    latest = finished[0] if finished else None
    stale = bool(latest) and (latest or {}).get("definition_sha256") != definition_sha256(system)
    state = "running" if running else ((latest or {}).get("verdict") if latest else "never_rehearsed")
    if state == "proven" and stale:
        state = "not_proven"
    return {
        "state": state if state in STATES else "failed",
        "overdue": current > due,
        "next_due_at": _iso(due),
        "running_rehearsal_id": (running or {}).get("id"),
        "stale": stale,
        "stale_reason": (f"The system's source connection, tables, keys, window or extract format changed since "
                         f"rehearsal {latest.get('n')}; rehearse it again to prove the current definition.")
        if stale and latest else None,
        "last_rehearsal": rehearsal_summary(latest or running, system),
        "last_proven_rehearsal": rehearsal_summary(next((r for r in finished if r.get("verdict") == "proven"), None),
                                                   system),
    }


def public_system(system: dict[str, Any], rehearsals: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    from services.exit_pii import pending_hold, view

    system = with_definition(system)
    rows = rehearsals if rehearsals is not None else rehearsals_of(system["id"])
    out = {k: v for k, v in system.items() if not str(k).startswith("_") and k != "schedule_last_fired_at"}
    out["pii_acknowledgement"] = view(system)
    # What the PII gate holds now (the tables, columns and categories an acknowledgement would cover).
    out["pii_hold"] = pending_hold(system, rows)
    out["readiness"] = readiness(system, rows)
    return out


def _label(state: str, tables: int, stale: bool = False) -> str:
    if stale and state == "not_proven":
        return "Not proven (changed since the last rehearsal)"
    return {
        "proven": f"Proven, {tables} table{'s' if tables != 1 else ''}",
        "never_rehearsed": "Never rehearsed",
        "running": "Rehearsal running",
        "not_proven": "Not proven",
        "incomplete": "Incomplete",
        "failed": "Rehearsal failed",
        "needs_pii_review": "Needs PII review",
    }.get(state, "Not proven")


def board(systems: list[dict[str, Any]], *, now: datetime | None = None) -> dict[str, Any]:
    """ExitReadiness (contract 3) for one workspace's systems."""
    current = now or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    upcoming: list[tuple[str, str, str]] = []
    for system in systems:
        system = with_definition(system)  # the connector behind the system is resolved once per row
        ready = readiness(system, now=current)
        last = ready["last_rehearsal"] or {}
        tables = len(system.get("tables") or [])
        rows.append({
            "id": system["id"], "name": system.get("name"), "criticality": system.get("criticality"),
            "engine": (system.get("source") or {}).get("engine"), "tables": tables, "state": ready["state"],
            "overdue": ready["overdue"], "next_due_at": ready["next_due_at"],
            "last_rehearsal_at": last.get("started_at"), "last_rehearsal_id": last.get("id"),
            "running_rehearsal_id": ready["running_rehearsal_id"],
            "measured_seconds": last.get("measured_seconds"),
            "window_seconds": int(system.get("exit_window_hours") or 0) * 3600,
            # Measured against the window the rehearsal ran under: shown only while that is still the window.
            "fits_window": last.get("fits_window") if last.get("current") else None,
            "stale": ready["stale"],
            "completeness_label": _label(ready["state"], int(last.get("tables_proven") or 0), ready["stale"]),
        })
        for item in system.get("regulatory_dates") or []:
            if item.get("date", "") >= current.date().isoformat():
                upcoming.append((item["date"], item.get("label") or "", system["id"]))
    # The longest *proven* exit: a failed or partial run's clock is not an exit's duration.
    measured = [r for r in rows if r["state"] == "proven" and isinstance(r["measured_seconds"], (int, float))]
    longest = max(measured, key=lambda r: r["measured_seconds"], default=None)
    upcoming.sort()
    return {
        "generated_at": _iso(current),
        "kpis": {
            "systems": len(rows),
            "systems_proven": sum(1 for r in rows if r["state"] == "proven"),
            "overdue": sum(1 for r in rows if r["overdue"]),
            "longest_measured_seconds": int(longest["measured_seconds"]) if longest else None,
            "longest_measured_system_id": longest["id"] if longest else None,
            "next_regulatory_date": ({"date": upcoming[0][0], "label": upcoming[0][1], "system_id": upcoming[0][2]}
                                     if upcoming else None),
        },
        "systems": rows,
    }
