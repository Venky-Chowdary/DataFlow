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

from collections.abc import Iterator
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


def _replay_artifacts(merged: dict[str, Any]) -> Iterator[tuple[bool, Any]]:
    """``(legacy, artifact)`` for every variant Execute replays these saved rows as."""
    from services.decision_kernel import build_artifact_from_mappings
    from services.schedule_mapping_contract import persisted_mapping_rows

    maps = persisted_mapping_rows(merged.get("mappings"))
    dest, source = _engines(merged)
    if not maps or not dest:
        return
    from services.conversion_contract import bind_stream_maps

    # Each stream's own rows are part of the stamp, as at Execute (F1c).
    maps = bind_stream_maps(maps, list(merged.get("stream_contracts") or []), dest_db=dest)
    mode = str(merged.get("sync_mode") or "").strip() or "full_refresh_overwrite"
    stored_fp = str(merged.get("source_schema_fingerprint") or "").strip()
    source_dbs = [source, ""]
    connector_id = str(merged.get("source_connector_id") or "").strip().lower()
    if connector_id and connector_id not in source_dbs:
        source_dbs.append(connector_id)
    fingerprints = ["", stored_fp] if stored_fp else [""]
    from contextlib import nullcontext

    from services.decision_kernel.legacy_rules import legacy_plan_rules

    # Review Q1: also the previous release's type rules (its stamps).
    for legacy in (False, True):
        with legacy_plan_rules() if legacy else nullcontext():
            for src in source_dbs:
                for sfp in fingerprints:
                    for rid in (f"validate:{dest}", f"execute:{dest}"):
                        yield legacy, build_artifact_from_mappings(
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


def replay_decision_hashes(merged: dict[str, Any]) -> set[str]:
    """Hashes Execute will accept for these saved rows. Empty if the dest engine is unknown."""
    from services.decision_kernel.models import accepted_stamps, previous_release_stamps

    found: set[str] = set()
    for legacy, art in _replay_artifacts(merged):
        if art.content_hash and not legacy:
            found.add(art.content_hash.lower())
        if art.content_hash:
            # The same decision under every accepted digest (the
            # feature/Cleanup and PR #435 interim stamps).
            found.update(previous_release_stamps(art) if legacy else accepted_stamps(art))
    return found


#: Schedule fields a Decision Artifact stamp was taken over: an edit of any of
#: them drops a key-blind stamp (review 3, round 2 item 1).
_DECISION_FIELDS = (
    "source_connector_id", "source_table", "dest_connector_id", "dest_table",
    "dest_schema", "dest_database", "mappings", "stream_contracts", "sync_mode",
    "primary_key", "source_primary_key", "validation_mode", "duplicate_policy",
    "error_policy",
)

KEY_BLIND_STAMP_DROPPED = (
    "This pipeline's approval was made by an earlier release and does not record "
    "the key columns, so it does not carry over an edit of the route, mappings, key, "
    "sync mode or error handling. Run Validate again to approve the edited pipeline."
)


def _stamp_is_key_blind(merged: dict[str, Any]) -> bool:
    """The schedule's stamp is a feature/Cleanup or PR #435 interim digest of its rows.

    True also when that cannot be told (the destination engine is unknown):
    only a stamp known to cover key flags is kept across a decision edit.
    """
    from services.decision_kernel.models import stamp_format

    approved = str(merged.get("approved_decision_artifact_hash") or "").strip().lower()
    if len(approved) != 64:
        return False
    seen = blind = False
    for _legacy, art in _replay_artifacts(merged):
        seen = True
        fmt = stamp_format(approved, art)
        if fmt in ("current", "scored"):
            return False
        blind = blind or fmt in ("base", "interim")
    return blind or not seen


def drop_key_blind_stamp_on_edit(before: dict[str, Any], merged: dict[str, Any]) -> str:
    """Drop a key-blind stamp when this edit touches what it was taken over; the note, or ''."""
    if not any(merged.get(key) != before.get(key) for key in _DECISION_FIELDS):
        return ""
    if not _stamp_is_key_blind(merged):
        return ""
    merged["approved_decision_artifact_hash"] = ""
    merged["approved_ddl_identity_hash"] = ""
    return KEY_BLIND_STAMP_DROPPED


def decision_hash_for_sync(merged: dict[str, Any]) -> str:
    """The hash Sync Now must send: the one Validate stored.

    Rebuilding it with empty fingerprints approved a source-type change, a
    sync_mode change, and an error_policy change. Dest-exists is relaxed in
    the execute gate, which still requires this hash, the source fingerprint,
    the sync mode, and the error policy to match. This function does not
    mint a new hash.
    """
    return str(merged.get("approved_decision_artifact_hash") or "").strip().lower()


def align_schedule_decision_hash(merged: dict[str, Any]) -> None:
    """Point a non-matching stamp at the replay hash of the rows being saved.

    Only when the stamp is a recorded Validate of exactly these rows
    (SEC-APPROVAL-1 P3): re-stamping whatever rows were saved turned any
    64-character hash into an approval. A stamp that names other rows is
    dropped; the schedule is then unvalidated, as before any Validate.
    """
    approved = str(merged.get("approved_decision_artifact_hash") or "").strip().lower()
    if len(approved) != 64:
        return
    hashes = replay_decision_hashes(merged)
    if not hashes or approved in hashes:
        return
    dest, source = _engines(merged)
    if not dest:
        return
    from services.validate_approvals import schedule_stamp_verified

    if not schedule_stamp_verified(merged, dest_db=dest):
        merged["approved_decision_artifact_hash"] = ""
        merged["approved_ddl_identity_hash"] = ""
        return
    from services.decision_kernel import build_artifact_from_mappings
    from services.schedule_mapping_contract import persisted_mapping_rows

    mode = str(merged.get("sync_mode") or "").strip() or "full_refresh_overwrite"
    from services.conversion_contract import bind_stream_maps

    art = build_artifact_from_mappings(
        bind_stream_maps(persisted_mapping_rows(merged.get("mappings")),
                         list(merged.get("stream_contracts") or []), dest_db=dest),
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


UNRECORDED_STAMP_WARNING = (
    "The Validate approval sent with this pipeline is not a Validate of these rows and this "
    "destination, so it was not saved. Run Validate, then save the pipeline again."
)


def drop_unrecorded_stamp(payload: dict[str, Any], existing: dict[str, Any] | None = None) -> str | None:
    """Remove a client-sent stamp that no recorded Validate approved (P3).

    A schedule save over the API keeps ``approved_decision_artifact_hash``
    only when a real Validate in this workspace returned it for exactly the
    rows (and destination) being saved. Returns the warning when it dropped one.
    """
    sent = str(payload.get("approved_decision_artifact_hash") or "").strip()
    if not sent:
        return None
    merged = {**(existing or {}), **payload}
    dest, _source = _engines(merged)
    from services.validate_approvals import schedule_stamp_verified

    if dest and schedule_stamp_verified(merged, dest_db=dest):
        return None
    payload.pop("approved_decision_artifact_hash", None)
    payload.pop("approved_ddl_identity_hash", None)
    return UNRECORDED_STAMP_WARNING

