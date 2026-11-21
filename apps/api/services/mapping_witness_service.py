"""Mapping Witness: reading the bounded samples, storing reports, linking them to jobs.

:mod:`services.mapping_witness` judges; this module feeds and remembers it.

* **Samples.** The source sample is the one Map already shows (sent by the
  client, capped) or, for a database source, a bounded read. The destination
  sample is read by the server, never taken from the client: a keyed ``IN``
  read of the source sample's keys through the same reader Gate-8 uses
  (:func:`services.target_sample.read_target_sample`), or, with no key, a
  bounded unordered read for the profile check. A destination that cannot be
  read leaves the report in source-only mode and says so; it never fails Map.
* **Store.** One record per workspace, destination table and mapping
  (``mapping_digest``): the latest report and the review decisions people made
  on its flags. Jobs carry no witness id; a job's mapping and destination hash
  to the same digest, so the Trust Report and the AI decisions ledger find the
  evidence for exactly the mapping that ran — and nothing for any other.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from services.mapping_witness import MAX_ROWS, flagged_pairs, witness_mappings
from services.platform_config import data_dir
from services.shared_doc_store import tracked
from services.shared_record_store import SharedRecordSet

logger = logging.getLogger(__name__)

#: Review decisions kept per witness record.
MAX_DECISIONS = 400
REVIEW_DECISIONS = ("kept", "changed")


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


def mapping_digest(mappings: list[dict[str, Any]] | None, destination: dict[str, Any] | None) -> str:
    """One id for "this mapping onto this destination table", from names only."""
    from services.mapping_memory import destination_scope

    pairs = sorted(
        {
            (str(m.get("source") or "").strip().lower(), str(m.get("target") or "").strip().lower())
            for m in mappings or []
            if isinstance(m, dict) and str(m.get("source") or "").strip() and str(m.get("target") or "").strip()
            and not (m.get("intentional_omit") or m.get("intentionalOmit"))
        }
    )
    body = {"destination": destination_scope(destination), "pairs": [list(p) for p in pairs]}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _key(workspace_id: str, digest: str) -> str:
    return f"witness::{workspace_id}::{digest}"


def save_report(report: dict[str, Any], *, workspace_id: str, digest: str, actor: str = "") -> dict[str, Any]:
    """Keep the latest report for this mapping; earlier review decisions stay with it."""
    key = _key(workspace_id, digest)
    data = _DOC.load(keys=[key])
    prior = data.get(key) if isinstance(data.get(key), dict) else {}
    record = {
        "workspace_id": workspace_id,
        "mapping_digest": digest,
        "report": report,
        "requested_by": str(actor or "")[:256],
        "updated_at": _now(),
        "decisions": list(prior.get("decisions") or [])[-MAX_DECISIONS:],
    }
    data[key] = record
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return record


def load_record(*, workspace_id: str, digest: str) -> dict[str, Any] | None:
    key = _key(workspace_id, digest)
    record = _DOC.load(keys=[key]).get(key)
    if not isinstance(record, dict) or str(record.get("workspace_id") or "") != workspace_id:
        return None
    return record


def record_review(
    *, workspace_id: str, digest: str, source: str, decision: str, actor: str, new_target: str = "",
) -> dict[str, Any]:
    """A person's answer to one flag: kept the mapping, or changed its target."""
    if decision not in REVIEW_DECISIONS:
        raise ValueError(f"decision must be one of: {', '.join(REVIEW_DECISIONS)}")
    key = _key(workspace_id, digest)
    data = _DOC.load(keys=[key])
    record = data.get(key)
    if not isinstance(record, dict) or str(record.get("workspace_id") or "") != workspace_id:
        raise LookupError("No witness report for this mapping")
    flagged = {p["source"]: p for p in flagged_pairs(record.get("report"))}
    if source not in flagged:
        raise LookupError("This column has no witness flag to review")
    pair = flagged[source]
    entry = {
        "source": source,
        "target": pair["target"],
        "flag_kind": (pair.get("flag") or {}).get("kind"),
        "decision": decision,
        "new_target": str(new_target or "")[:256] or None,
        "by": str(actor or "anonymous")[:256],
        "at": _now(),
        "report_sha256": record["report"].get("inputs_sha256"),
    }
    decisions = [d for d in record.get("decisions") or [] if d.get("source") != source]
    decisions.append(entry)
    record = {**record, "decisions": decisions[-MAX_DECISIONS:], "updated_at": _now()}
    data[key] = record
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return entry


def record_for_job(job: dict[str, Any]) -> dict[str, Any] | None:
    """The witness record for the mapping and destination this job ran with, if one was made."""
    from services.parity_evidence import mappings_of, request_of

    req = request_of(job)
    workspace_id = str(req.get("workspace_id") or job.get("workspace_id") or "")
    digest = mapping_digest(mappings_of(job), req.get("destination") if isinstance(req.get("destination"), dict) else {})
    try:
        return load_record(workspace_id=workspace_id, digest=digest)
    except Exception as exc:  # noqa: BLE001 - evidence lookup must never break a report read
        logger.info("mapping witness lookup failed: %s", type(exc).__name__)
        return None


# ── bounded sample reads ─────────────────────────────────────────────────────


def _endpoint_reader(endpoint: Any, workspace_id: str) -> tuple[str, dict[str, Any], str, str]:
    from services.dialect_profiles import schema_from_cfg
    from src.transfer.adapters import resolve_connector_config, resolve_endpoint
    from src.transfer.connector_capabilities import resolve_driver_type

    resolved = resolve_endpoint(endpoint, workspace_id=workspace_id or None)
    cfg = resolve_connector_config(resolved)
    db_type = resolve_driver_type(str(cfg.get("type") or resolved.format or "")).lower()
    table = str(resolved.table or resolved.collection or "")
    return db_type, cfg, schema_from_cfg(db_type, cfg), table


def read_rows(endpoint: Any, *, workspace_id: str, key_column: str = "", keys: list[Any] | None = None,
              limit: int = MAX_ROWS) -> list[dict[str, Any]]:
    """A bounded read of a database table: keyed (``IN`` on ``key_column``) or the first ``limit`` rows."""
    from services.target_sample import read_target_sample

    db_type, cfg, schema, table = _endpoint_reader(endpoint, workspace_id)
    if not table:
        raise LookupError("the endpoint names no table")
    if keys is not None:
        keys = [k for k in keys if k is not None][:limit]
        if not keys:
            return []
    rows = read_target_sample(
        db_type, cfg, schema=schema, table_name=table, columns=None,
        limit=max(1, len(keys) if keys is not None else limit),
        sort_key=key_column or None, key_values=list(keys) if keys is not None else None,
    )
    return [r for r in rows if isinstance(r, dict)][:limit]


def run_witness(
    *,
    mappings: list[dict[str, Any]],
    source_rows: list[dict[str, Any]] | None,
    source_endpoint: Any = None,
    destination_endpoint: Any = None,
    key: tuple[str, str] | None = None,
    destination_table_exists: bool | None = None,
    date_locale: str = "",
    workspace_id: str = "",
) -> dict[str, Any]:
    """Gather both samples (bounded) and judge. Read problems become notes, never errors."""
    notes: list[str] = []
    rows = [r for r in (source_rows or []) if isinstance(r, dict)][:MAX_ROWS]
    if not rows and source_endpoint is not None and getattr(source_endpoint, "kind", "") == "database":
        try:
            rows = read_rows(source_endpoint, workspace_id=workspace_id, key_column=(key or ("", ""))[0])
        except Exception as exc:  # noqa: BLE001
            logger.info("mapping witness: source sample unreadable (%s)", type(exc).__name__)
            notes.append("The source sample could not be read.")
    dest_rows: list[dict[str, Any]] | None = None
    can_read_dest = (
        destination_endpoint is not None
        and getattr(destination_endpoint, "kind", "") == "database"
        and destination_table_exists is not False
    )
    if can_read_dest and rows:
        try:
            if key and key[0] and key[1]:
                dest_rows = read_rows(destination_endpoint, workspace_id=workspace_id, key_column=key[1],
                                      keys=[r.get(key[0]) for r in rows])
            else:
                dest_rows = read_rows(destination_endpoint, workspace_id=workspace_id)
        except Exception as exc:  # noqa: BLE001
            logger.info("mapping witness: destination sample unreadable (%s)", type(exc).__name__)
            notes.append("The destination could not be read, so only source values were checked.")
            dest_rows = None
    elif destination_table_exists is False:
        notes.append("The destination table does not exist yet, so only source values were checked.")
    report = witness_mappings(mappings, rows, dest_rows, key=key, date_locale=date_locale)
    report["notes"] = notes
    return report


def key_pair(mappings: list[dict[str, Any]], target_key_columns: list[str] | None,
             explicit: dict[str, Any] | None = None) -> tuple[str, str] | None:
    """The (source, destination) column pair the samples join on."""
    if explicit and str(explicit.get("source") or "").strip() and str(explicit.get("target") or "").strip():
        return str(explicit["source"]).strip(), str(explicit["target"]).strip()
    wanted = [str(k).strip().lower() for k in target_key_columns or [] if str(k).strip()]
    for k in wanted:
        for m in mappings or []:
            if isinstance(m, dict) and str(m.get("target") or "").strip().lower() == k and m.get("source"):
                return str(m["source"]).strip(), str(m["target"]).strip()
    return None
