"""Mapping Witness: bounded sample reads, the evidence store, and the snapshot a job runs with.

:mod:`services.mapping_witness` judges; this module feeds and remembers it.

* **Identity.** One canonical destination identity, computed the same way on
  the Map side and at job submit from the *resolved* endpoint (saved connector
  merged, default port, the engine's default schema, a host parsed out of a
  connection URI), so a blank schema on Map and ``public`` on the job are one
  table. The mapping digest covers that identity and the pairs actually written
  (omitted columns are left out on both sides).
* **Samples.** The source sample is the one Map shows (capped) or a bounded
  read. The destination is read by the server, keyed only: an ``IN`` read of
  the sample's keys selecting the mapped and candidate columns (never
  ``SELECT *``), at most 500 rows, under the workspace's egress allowlist. A
  warehouse (BigQuery, Snowflake, …) is read only on an explicit "Check
  mapping", never on every Map edit.
* **Store.** Append-only: reports per mapping digest (latest ten), and per
  destination the Map decisions (who approved or changed a pair, from the
  authenticated caller), the reviews of witness flags, and the model
  suggestions the Map step received. Nothing a job ran with is ever replaced.
* **Snapshot.** At submit, :func:`snapshot_for_request` copies onto the job
  the latest report for exactly the mapping and destination it runs, the Map
  decisions and reviews that apply to the targets it runs, and the model
  suggestions for its columns, with a content digest. The Trust Report and the
  AI decisions ledger read only that snapshot; a later Map session cannot
  change what a finished job says.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from services.mapping_witness import (
    MAX_ROWS,
    flagged_pairs,
    mapped_pairs,
    report_sha256,
    witness_mappings,
)
from services.platform_config import data_dir
from services.shared_doc_store import tracked
from services.shared_record_store import SharedRecordSet

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = "mapping_witness_snapshot.v1"
#: Reports kept per mapping digest; review decisions and Map decisions kept per destination.
MAX_REPORTS = 10
MAX_DECISIONS = 400
MAX_PER_SOURCE = 10
#: Destination columns one keyed read may select.
MAX_READ_COLUMNS = 64
REVIEW_DECISIONS = ("kept", "changed")
#: Engines where a read costs money or a scan: never read on a Map edit, only on request.
WAREHOUSES = frozenset({"bigquery", "snowflake", "redshift", "databricks", "synapse", "athena"})
TRIGGER_AUTO = "auto"
TRIGGER_EXPLICIT = "explicit"
_COLUMN_NAME = re.compile(r"^[\w .$#@-]{1,128}$")


def _store_path():
    return data_dir() / "mapping_witness.json"


def _file_load() -> dict[str, Any]:
    path = _store_path()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _file_save(data: dict[str, Any]) -> None:
    from services.atomic_file import write_json_atomic

    write_json_atomic(_store_path(), dict(data))


_DOC = SharedRecordSet(
    "mapping_witness",
    file_load=_file_load,
    file_save=_file_save,
    file_path=_store_path,
    partition_key="workspace_id",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha(body: Any) -> str:
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


# ── identity ─────────────────────────────────────────────────────────────────


def _endpoint_config(endpoint: Any) -> Any:
    """An ``EndpointConfig`` from a config, a DTO or a plain dict."""
    from src.transfer.models import EndpointConfig

    if endpoint is None or isinstance(endpoint, EndpointConfig):
        return endpoint
    if hasattr(endpoint, "model_dump"):
        data = endpoint.model_dump(by_alias=True)
    elif isinstance(endpoint, dict):
        data = dict(endpoint)
    else:
        return None
    return EndpointConfig.from_dict(str(data.get("kind") or "database"), data)


def destination_identity(endpoint: Any, workspace_id: str = "") -> str:
    """``engine|owner|database|schema|object`` for the resolved destination, lower-case.

    ``owner`` is the saved connector when there is one, else ``host:port`` with
    the engine's default port and the host of a connection URI.
    """
    ep = _endpoint_config(endpoint)
    if ep is None:
        return ""
    from services.dialect_profiles import schema_from_cfg
    from src.transfer.connector_capabilities import effective_port, resolve_driver_type

    try:
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(ep, workspace_id or None)
    except Exception as exc:  # noqa: BLE001 - an unresolvable endpoint still has its own fields
        logger.info("witness identity: endpoint not resolved (%s)", type(exc).__name__)
        cfg = {"host": ep.host or "", "port": ep.port or 0, "database": ep.database or "",
               "schema": ep.schema or "", "table": ep.table or "", "type": ep.format or "",
               "connection_string": ep.connection_string or ""}
    driver = resolve_driver_type(str(cfg.get("type") or ep.format or "")).lower()
    host = str(cfg.get("host") or "").strip()
    port: Any = cfg.get("port") or 0
    database = str(cfg.get("database") or "").strip()
    uri = str(cfg.get("connection_string") or "").strip()
    if uri and "://" in uri:
        parsed = urlparse(uri)
        host = host or (parsed.hostname or "")
        try:
            port = port or (parsed.port or 0)
        except ValueError:
            port = port or 0
        database = database or parsed.path.lstrip("/").split("/")[0]
    owner = str(getattr(ep, "connector_id", "") or "").strip()
    owner = f"connector:{owner}" if owner else f"{host}:{effective_port(driver, port) if host else ''}"
    schema = schema_from_cfg(driver, cfg)
    obj = str(cfg.get("table") or ep.table or ep.collection or "")
    return "|".join(str(p or "").strip().casefold() for p in (driver, owner, database, schema, obj))


def mapping_digest(mappings: list[dict[str, Any]] | None, identity: str) -> str:
    """One id for "these written pairs onto this destination", from names only."""
    pairs = sorted({(p["source"].casefold(), p["target"].casefold()) for p in mapped_pairs(list(mappings or []))})
    return _sha({"destination": identity, "pairs": [list(p) for p in pairs]})


def _report_key(workspace_id: str, digest: str) -> str:
    return f"witness::{workspace_id}::{digest}"


def _activity_key(workspace_id: str, identity: str) -> str:
    return f"activity::{workspace_id}::{_sha(identity)[:32]}"


def _load(key: str, workspace_id: str) -> tuple[Any, dict[str, Any] | None]:
    data = _DOC.load(keys=[key])
    record = data.get(key)
    if not isinstance(record, dict) or str(record.get("workspace_id") or "") != workspace_id:
        return data, None
    return data, record


def _save(data: Any, key: str, record: dict[str, Any]) -> None:
    data[key] = record
    _DOC.save(tracked(data, getattr(data, "rev", None)))


# ── writes (append-only) ─────────────────────────────────────────────────────


def save_report(report: dict[str, Any], *, workspace_id: str, identity: str, digest: str,
                actor: str = "") -> dict[str, Any]:
    """Append a report for this mapping. Earlier reports stay; nothing a job ran with is replaced."""
    key = _report_key(workspace_id, digest)
    data, record = _load(key, workspace_id)
    record = record or {"workspace_id": workspace_id, "mapping_digest": digest,
                        "destination_identity": identity, "reports": []}
    entry = {"report": report, "report_sha256": report_sha256(report),
             "requested_by": str(actor or "")[:256], "recorded_at": _now()}
    record = {**record, "reports": [*list(record.get("reports") or []), entry][-MAX_REPORTS:],
              "updated_at": entry["recorded_at"]}
    _save(data, key, record)
    return {"mapping_digest": digest, **entry}


def latest_report(*, workspace_id: str, digest: str, not_after: str | None = None) -> dict[str, Any] | None:
    _data, record = _load(_report_key(workspace_id, digest), workspace_id)
    entries = [e for e in (record or {}).get("reports") or [] if isinstance(e, dict)]
    if not_after:
        entries = [e for e in entries if str(e.get("recorded_at") or "") <= not_after]
    return entries[-1] if entries else None


def _activity(workspace_id: str, identity: str) -> tuple[Any, str, dict[str, Any]]:
    key = _activity_key(workspace_id, identity)
    data, record = _load(key, workspace_id)
    record = record or {"workspace_id": workspace_id, "destination_identity": identity,
                        "map_decisions": {}, "flag_reviews": [], "model_suggestions": {}}
    return data, key, record


def _clean_name(value: Any) -> str:
    text = str(value or "").strip()
    return text if _COLUMN_NAME.match(text) else ""


def record_review(*, workspace_id: str, identity: str, digest: str, source: str, decision: str, actor: str,
                  new_target: str = "") -> dict[str, Any]:
    """A person's answer to one flag of the latest report for this mapping: kept, or changed to a column."""
    if decision not in REVIEW_DECISIONS:
        raise ValueError(f"decision must be one of: {', '.join(REVIEW_DECISIONS)}")
    latest = latest_report(workspace_id=workspace_id, digest=digest)
    if not latest:
        raise LookupError("No witness report for this mapping")
    flagged = {p["source"].casefold(): p for p in flagged_pairs(latest["report"])}
    pair = flagged.get(str(source or "").casefold())
    if not pair:
        raise LookupError("This column has no witness flag to review")
    target = _clean_name(new_target)
    if decision == "changed" and (not target or target.casefold() == pair["target"].casefold()):
        raise ValueError("A changed decision names the new destination column (a column name).")
    entry = {
        "source": pair["source"],
        "flagged_target": pair["target"],
        "flag_kind": (pair.get("flag") or {}).get("kind"),
        "report_sha256": latest["report_sha256"],
        "mapping_digest": digest,
        "decision": decision,
        "new_target": target if decision == "changed" else None,
        "by": str(actor or "anonymous")[:256],
        "at": _now(),
    }
    data, key, record = _activity(workspace_id, identity)
    reviews = [*list(record.get("flag_reviews") or []), entry][-MAX_DECISIONS:]
    _save(data, key, {**record, "flag_reviews": reviews, "updated_at": entry["at"]})
    return entry


def record_map_decisions(*, workspace_id: str, identity: str, mappings: list[dict[str, Any]],
                         actor: str) -> int:
    """Who approved or changed each pair in Map (the caller is authenticated; the body names no person)."""
    data, key, record = _activity(workspace_id, identity)
    decisions: dict[str, list[dict[str, Any]]] = dict(record.get("map_decisions") or {})
    at = _now()
    recorded = 0
    for m in mappings[:400]:
        if not isinstance(m, dict) or not (m.get("approved") or m.get("changed")):
            continue
        source, target = _clean_name(m.get("source")), _clean_name(m.get("target"))
        if not source or not target:
            continue
        entry = {"target": target, "approved": bool(m.get("approved")), "changed": bool(m.get("changed")),
                 "by": str(actor or "anonymous")[:256], "at": at}
        history = [e for e in decisions.get(source.casefold(), []) if isinstance(e, dict)]
        decisions[source.casefold()] = [*history, entry][-MAX_PER_SOURCE:]
        recorded += 1
    if recorded:
        _save(data, key, {**record, "map_decisions": decisions, "updated_at": at})
    return recorded


def record_model_suggestions(*, workspace_id: str, identity: str, mappings: list[dict[str, Any]],
                             llm: dict[str, Any] | None) -> int:
    """Model suggestions Map received (engine_suggestion from a named provider), recorded server-side."""
    egress = (llm or {}).get("ai_egress") if isinstance((llm or {}).get("ai_egress"), dict) else {}
    rows = []
    for m in mappings or []:
        sug = m.get("engine_suggestion") if isinstance(m, dict) else None
        if not isinstance(sug, dict) or not sug.get("llm_provider"):
            continue
        rows.append((str(m.get("source") or ""), {
            "suggested_target": str(sug.get("target") or m.get("target") or ""),
            "suggested_transform": sug.get("transform"),
            "deterministic_target": str(m.get("target") or ""),
            "provider": str(sug.get("llm_provider")),
            "model": str(sug.get("llm_model") or "unrecorded"),
            "payload_sha256": egress.get("payload_sha256"),
            "event_hash": egress.get("event_hash"),
            "at": _now(),
        }))
    if not rows:
        return 0
    data, key, record = _activity(workspace_id, identity)
    suggestions: dict[str, list[dict[str, Any]]] = dict(record.get("model_suggestions") or {})
    for source, entry in rows:
        history = [e for e in suggestions.get(source.casefold(), []) if isinstance(e, dict)]
        suggestions[source.casefold()] = [*history, entry][-MAX_PER_SOURCE:]
    _save(data, key, {**record, "model_suggestions": suggestions, "updated_at": _now()})
    return len(rows)


# ── the snapshot a job runs with ─────────────────────────────────────────────


def build_snapshot(*, workspace_id: str, identity: str, mappings: list[dict[str, Any]],
                   taken_at: str | None = None) -> dict[str, Any]:
    """What the job runs with: the report for its exact mapping, and the decisions that apply to it."""
    taken_at = taken_at or _now()
    pairs = {p["source"].casefold(): p for p in mapped_pairs(mappings)}
    digest = mapping_digest(mappings, identity)
    latest = latest_report(workspace_id=workspace_id, digest=digest, not_after=taken_at)
    _data, _key, activity = _activity(workspace_id, identity)
    map_decisions: dict[str, dict[str, Any]] = {}
    for source, history in (activity.get("map_decisions") or {}).items():
        pair = pairs.get(source)
        valid = [e for e in history or [] if isinstance(e, dict) and pair
                 and str(e.get("target") or "").casefold() == pair["target"].casefold()
                 and str(e.get("at") or "") <= taken_at]
        if valid:
            map_decisions[pair["source"]] = valid[-1]
    reviews = []
    report_digest = (latest or {}).get("report_sha256")
    for r in activity.get("flag_reviews") or []:
        if not isinstance(r, dict) or str(r.get("at") or "") > taken_at:
            continue
        pair = pairs.get(str(r.get("source") or "").casefold())
        if not pair:
            continue
        ran = pair["target"].casefold()
        if r.get("decision") == "kept" and r.get("report_sha256") == report_digest \
                and str(r.get("flagged_target") or "").casefold() == ran:
            reviews.append(r)  # the flag on what ran was looked at and kept
        elif r.get("decision") == "changed" and str(r.get("new_target") or "").casefold() == ran:
            reviews.append(r)  # an earlier flag was resolved by changing to what ran
    suggestions = {}
    for source, history in (activity.get("model_suggestions") or {}).items():
        pair = pairs.get(source)
        valid = [e for e in history or [] if isinstance(e, dict) and pair and str(e.get("at") or "") <= taken_at]
        if valid:
            suggestions[pair["source"]] = valid[-1]
    body = {
        "version": SNAPSHOT_VERSION,
        "status": "ok",
        "taken_at": taken_at,
        "mapping_digest": digest,
        "destination_identity": identity,
        "report": (latest or {}).get("report"),
        "report_sha256": report_digest,
        "report_recorded_at": (latest or {}).get("recorded_at"),
        "requested_by": (latest or {}).get("requested_by"),
        "map_decisions": map_decisions,
        "flag_reviews": reviews,
        "model_suggestions": suggestions,
    }
    return {**body, "sha256": _sha(body)}


def snapshot_sha256(snapshot: dict[str, Any]) -> str:
    return _sha({k: v for k, v in snapshot.items() if k != "sha256"})


def snapshot_for_request(request: Any) -> dict[str, Any]:
    """The witness snapshot stamped onto a job at submit. Never raises: a failure is stated."""
    workspace_id = str(getattr(request, "workspace_id", "") or "")
    try:
        identity = destination_identity(getattr(request, "destination", None), workspace_id)
        return build_snapshot(workspace_id=workspace_id, identity=identity,
                              mappings=list(getattr(request, "mappings", None) or []))
    except Exception as exc:  # noqa: BLE001 - submitting a job never depends on evidence
        logger.warning("mapping witness snapshot unavailable: %s", type(exc).__name__)
        body = {"version": SNAPSHOT_VERSION, "status": "unavailable", "taken_at": _now(),
                "reason": "The mapping evidence store could not be read when the job was submitted."}
        return {**body, "sha256": _sha(body)}


def snapshot_of(job: dict[str, Any]) -> dict[str, Any] | None:
    snap = job.get("mapping_witness")
    return snap if isinstance(snap, dict) else None


# ── bounded sample reads ─────────────────────────────────────────────────────


def _endpoint_reader(endpoint: Any, workspace_id: str) -> tuple[str, dict[str, Any], str, str]:
    from services.dialect_profiles import schema_from_cfg
    from src.transfer.adapters import resolve_connector_config, resolve_endpoint
    from src.transfer.connector_capabilities import resolve_driver_type

    resolved = resolve_endpoint(_endpoint_config(endpoint), workspace_id=workspace_id or None)
    cfg = resolve_connector_config(resolved, workspace_id or None)
    db_type = resolve_driver_type(str(cfg.get("type") or resolved.format or "")).lower()
    table = str(resolved.table or resolved.collection or "")
    return db_type, cfg, schema_from_cfg(db_type, cfg), table


def engine_of(endpoint: Any) -> str:
    ep = _endpoint_config(endpoint)
    if ep is None:
        return ""
    from src.transfer.connector_capabilities import resolve_driver_type

    return resolve_driver_type(str(ep.format or "")).lower()


def read_rows(endpoint: Any, *, workspace_id: str, key_column: str, keys: list[Any] | None,
              columns: list[str]) -> list[dict[str, Any]]:
    """A keyed, column-bounded read: ``SELECT <columns> … WHERE key IN (…)``, at most :data:`MAX_ROWS`.

    Runs under the workspace's egress allowlist, like every other connector dial.
    """
    from services.egress_guard import egress_workspace
    from services.target_sample import read_target_sample

    if not key_column or not columns:
        raise LookupError("a witness read needs a key and the columns to select")
    with egress_workspace(workspace_id):
        db_type, cfg, schema, table = _endpoint_reader(endpoint, workspace_id)
        if not table:
            raise LookupError("the endpoint names no table")
        wanted = [k for k in (keys or []) if k is not None][:MAX_ROWS]
        if keys is not None and not wanted:
            return []
        rows = read_target_sample(
            db_type, cfg, schema=schema, table_name=table, columns=list(columns),
            limit=max(1, len(wanted) if keys is not None else MAX_ROWS),
            sort_key=key_column, key_values=wanted if keys is not None else None,
        )
    return [r for r in rows if isinstance(r, dict)][:MAX_ROWS]


def _read_columns(mappings: list[dict[str, Any]], key_target: str, candidates: list[str] | None) -> list[str]:
    """Mapped existing targets, the key and the candidate columns, deduplicated, bounded."""
    out: list[str] = []
    seen: set[str] = set()
    names = [key_target]
    names += [p["target"] for p in mapped_pairs(mappings) if not p["create_new"]]
    names += [str(c) for c in candidates or []]
    for name in names:
        clean = _clean_name(name)
        if clean and clean.casefold() not in seen:
            seen.add(clean.casefold())
            out.append(clean)
        if len(out) >= MAX_READ_COLUMNS:
            break
    return out


def run_witness(
    *,
    mappings: list[dict[str, Any]],
    source_rows: list[dict[str, Any]] | None,
    source_endpoint: Any = None,
    destination_endpoint: Any = None,
    key: tuple[str, str] | None = None,
    candidate_columns: list[str] | None = None,
    destination_table_exists: bool | None = None,
    date_locale: str = "",
    workspace_id: str = "",
    trigger: str = TRIGGER_AUTO,
) -> dict[str, Any]:
    """Gather both samples (bounded) and judge. Read problems become notes, never errors."""
    notes: list[str] = []
    rows = [r for r in (source_rows or []) if isinstance(r, dict)][:MAX_ROWS]
    if not rows and source_endpoint is not None and getattr(source_endpoint, "kind", "") == "database" and key:
        try:
            cols = [key[0], *[p["source"] for p in mapped_pairs(mappings)]]
            rows = read_rows(source_endpoint, workspace_id=workspace_id, key_column=key[0], keys=None,
                             columns=_read_columns([], key[0], cols))
        except Exception as exc:  # noqa: BLE001
            logger.info("mapping witness: source sample unreadable (%s)", type(exc).__name__)
            notes.append("The source sample could not be read.")
    dest_rows: list[dict[str, Any]] | None = None
    engine = engine_of(destination_endpoint)
    is_db = destination_endpoint is not None and getattr(destination_endpoint, "kind", "") == "database"
    if destination_table_exists is False:
        notes.append("The destination table does not exist yet, so only source values were checked.")
    elif is_db and not (key and key[0] and key[1]):
        notes.append("No key joins the sample to the destination, so only source values were checked.")
    elif is_db and engine in WAREHOUSES and trigger != TRIGGER_EXPLICIT:
        notes.append("Reading a warehouse runs only when you choose Check mapping; source values were checked.")
    elif is_db and rows and key:
        try:
            dest_rows = read_rows(destination_endpoint, workspace_id=workspace_id, key_column=key[1],
                                  keys=[r.get(key[0]) for r in rows],
                                  columns=_read_columns(mappings, key[1], candidate_columns))
        except Exception as exc:  # noqa: BLE001
            logger.info("mapping witness: destination sample unreadable (%s)", type(exc).__name__)
            notes.append("The destination could not be read, so only source values were checked.")
            dest_rows = None
    report = witness_mappings(mappings, rows, dest_rows, key=key, date_locale=date_locale)
    report["notes"] = notes
    report["trigger"] = trigger if trigger in (TRIGGER_AUTO, TRIGGER_EXPLICIT) else TRIGGER_AUTO
    return report


def key_pair(mappings: list[dict[str, Any]], target_key_columns: list[str] | None,
             explicit: dict[str, Any] | None = None) -> tuple[str, str] | None:
    """The (source, destination) column pair the samples join on."""
    if explicit and str(explicit.get("source") or "").strip() and str(explicit.get("target") or "").strip():
        return str(explicit["source"]).strip(), str(explicit["target"]).strip()
    wanted = [str(k).strip().casefold() for k in target_key_columns or [] if str(k).strip()]
    for k in wanted:
        for p in mapped_pairs(mappings):
            if p["target"].casefold() == k:
                return p["source"], p["target"]
    return None
