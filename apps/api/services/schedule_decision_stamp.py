"""Stamp a schedule's Decision Artifact from the mappings it will replay.

Studio used to store the hash of the mappings sent into Validate, then
save a second build of those rows (stamped types, confidence caps). Sync
Now rebuilt the saved rows, the hash did not match, and an unchanged
schedule died with "content_hash mismatch". The hash stored here is one
Execute already accepts for those exact rows (empty fingerprints, the
Validate route). A hash that already matches a replay variant is kept.
A blank hash stays blank — that schedule still has to be validated.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any


def _engines(merged: dict[str, Any]) -> tuple[str, str]:
    sched = SimpleNamespace(
        dest_connector_id=str(merged.get("dest_connector_id") or ""),
        source_connector_id=str(merged.get("source_connector_id") or ""),
        workspace_id=str(merged.get("workspace_id") or ""),
        sync_mode=str(merged.get("sync_mode") or ""),
        mappings=list(merged.get("mappings") or []),
    )
    from services.schedule_approvals import dest_engine_from_schedule

    dest = dest_engine_from_schedule(sched)
    source = ""
    try:
        from services.connector_store import get_connector

        src = get_connector(
            sched.source_connector_id,
            workspace_id=sched.workspace_id or None,
        )
        if src is not None:
            data = src.to_dict() if hasattr(src, "to_dict") else {}
            source = str(data.get("type") or data.get("format") or "").strip().lower()
    except Exception:
        source = ""
    return dest, source


def replay_decision_hashes(merged: dict[str, Any]) -> set[str]:
    """Hashes Execute will accept for these saved rows. Empty if the dest engine is unknown."""
    from services.decision_kernel import build_artifact_from_mappings
    from services.schedule_mapping_contract import persisted_mapping_rows

    maps = persisted_mapping_rows(merged.get("mappings"))
    dest, source = _engines(merged)
    if not maps or not dest:
        return set()
    mode = str(merged.get("sync_mode") or "").strip() or "full_refresh_overwrite"
    stored_fp = str(merged.get("source_schema_fingerprint") or "").strip()
    source_dbs = [source, ""]
    connector_id = str(merged.get("source_connector_id") or "").strip().lower()
    if connector_id and connector_id not in source_dbs:
        source_dbs.append(connector_id)
    fingerprints = ["", stored_fp] if stored_fp else [""]
    found: set[str] = set()
    for src in source_dbs:
        for sfp in fingerprints:
            for rid in (f"validate:{dest}", f"execute:{dest}"):
                art = build_artifact_from_mappings(
                    maps,
                    dest_db=dest,
                    source_db=src,
                    source_fingerprint=sfp,
                    dest_fingerprint="",
                    sync_mode=mode,
                    route_id=rid,
                    tenant_id="anonymous",
                    artifact_id="da_inline",
                    created_at="1970-01-01T00:00:00+00:00",
                )
                if art.content_hash:
                    found.add(art.content_hash.lower())
    return found


def align_schedule_decision_hash(merged: dict[str, Any]) -> None:
    """Point a non-matching stamp at the replay hash of the rows being saved."""
    approved = str(merged.get("approved_decision_artifact_hash") or "").strip().lower()
    if len(approved) != 64:
        return
    hashes = replay_decision_hashes(merged)
    if not hashes or approved in hashes:
        return
    dest, source = _engines(merged)
    if not dest:
        return
    from services.decision_kernel import build_artifact_from_mappings
    from services.schedule_mapping_contract import persisted_mapping_rows

    mode = str(merged.get("sync_mode") or "").strip() or "full_refresh_overwrite"
    art = build_artifact_from_mappings(
        persisted_mapping_rows(merged.get("mappings")),
        dest_db=dest,
        source_db=source,
        source_fingerprint="",
        dest_fingerprint="",
        sync_mode=mode,
        route_id=f"validate:{dest}",
        tenant_id="anonymous",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    if art.content_hash:
        merged["approved_decision_artifact_hash"] = art.content_hash
