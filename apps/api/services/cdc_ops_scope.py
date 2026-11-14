"""Tenant scope for CDC/sync watermarks and CDC leases (SEC-1).

A cursor key names a route (source database and table, destination and its
connector id), and a lease names the source host it fences. Neither store
had a workspace column, so the ``/ops/cdc-cursors*`` and ``/ops/cdc-leases*``
routes let any workspace admin list, read, clear and force-release every
workspace's bookmarks and leases.

The owner of a key is resolved from what the stores already record, in this
order: the ``workspace_id`` stamped into the watermark metadata or the lease
meta (new writes stamp it), the workspace of the job named there, then the
workspace of the saved connector the key's ``id:<connector>`` names. A key
with no resolvable owner is an unscoped legacy row: with workspace isolation
on, only a platform administrator sees it.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable

from fastapi import HTTPException, Request

_logger = logging.getLogger(__name__)

_CONNECTOR_IN_KEY = re.compile(r"\|id:([A-Za-z0-9._-]{1,128})")

NOT_FOUND = "Cursor not found"

#: job id -> workspace, for stamping writes (a job never changes workspace).
_STAMP_CACHE: dict[str, str] = {}


def _system_scope():  # type: ignore[no-untyped-def]
    from services.workspace_filter import bound_workspace_filter

    return bound_workspace_filter(None)


def job_workspace(job_id: str, cache: dict[Any, str | None] | None = None) -> str | None:
    """The workspace a job belongs to, or ``None`` when the job is unknown."""
    jid = str(job_id or "").strip()
    if not jid:
        return None
    if cache is not None and ("job", jid) in cache:
        return cache[("job", jid)]
    found: str | None = None
    try:
        from services.mongodb_service import get_mongodb_service

        with _system_scope():
            doc = get_mongodb_service().get_job(jid)
        if isinstance(doc, dict):
            found = str(doc.get("workspace_id") or "")
    except Exception as exc:  # noqa: BLE001 - an unreadable job is an unknown owner
        _logger.warning("Could not read job %s for cursor ownership: %s", jid, exc)
    if cache is not None:
        cache[("job", jid)] = found
    return found


def _connector_workspace(cursor_key: str, cache: dict[Any, str | None] | None) -> str | None:
    for cid in _CONNECTOR_IN_KEY.findall(cursor_key or ""):
        if cache is not None and ("conn", cid) in cache:
            hit = cache[("conn", cid)]
        else:
            try:
                from services.connector_store import connector_owner_workspace

                exists, owner = connector_owner_workspace(cid)
                hit = owner if exists else None
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Could not read connector %s for cursor ownership: %s", cid, exc)
                hit = None
            if cache is not None:
                cache[("conn", cid)] = hit
        if hit is not None:
            return hit
    return None


def owner_from_records(
    cursor_key: str,
    metadata: dict[str, Any] | None = None,
    lease: dict[str, Any] | None = None,
    *,
    cache: dict[Any, str | None] | None = None,
) -> str:
    """The workspace that owns a cursor key ("" = unscoped legacy)."""
    meta = metadata if isinstance(metadata, dict) else {}
    lease_meta = (lease or {}).get("meta") if isinstance((lease or {}).get("meta"), dict) else {}
    for source in (meta, lease_meta):
        ws = str(source.get("workspace_id") or "").strip()
        if ws:
            return ws
    job_ids = [meta.get("job_id"), lease_meta.get("job_id"), (lease or {}).get("holder_job_id")]
    if lease and lease.get("holder_id"):
        from services.cdc_lease import parse_holder_job_id

        job_ids.append(parse_holder_job_id(str(lease.get("holder_id") or "")))
    for jid in job_ids:
        ws = job_workspace(str(jid or ""), cache)
        if ws:
            return ws
    return _connector_workspace(cursor_key, cache) or ""


def cursor_owner(cursor_key: str) -> tuple[bool, str]:
    """``(exists, workspace_id)`` for a cursor key: a watermark or a lease."""
    from services.cdc_lease import lease_view
    from services.sync_cursor import get_watermark_record

    key = (cursor_key or "").strip()
    if not key:
        return False, ""
    watermark, metadata = get_watermark_record(key)
    lease = lease_view(key)
    if watermark is None and lease is None:
        return False, ""
    return True, owner_from_records(key, metadata, lease, cache={})


def _scope(request: Request, workspace_id: str | None = None):  # type: ignore[no-untyped-def]
    from services.workspace_access import tenant_scope

    return tenant_scope(request, workspace_id) if workspace_id else tenant_scope(request)


def cursor_visible(request: Request, cursor_key: str, *, workspace_id: str | None = None) -> bool:
    """Whether the caller may read this key (a missing key is not visible)."""
    exists, owner = cursor_owner(cursor_key)
    return exists and _scope(request, workspace_id).allows(owner)


def assert_cursor_writable(request: Request, cursor_key: str, *, workspace_id: str | None = None) -> None:
    """404 a key the caller cannot see (or that does not exist); 403 without write."""
    exists, owner = cursor_owner(cursor_key)
    scope = _scope(request, workspace_id)
    if not exists or not scope.allows(owner):
        raise HTTPException(status_code=404, detail=NOT_FOUND)
    if not scope.can_write(owner):
        raise HTTPException(status_code=403, detail="Write access to workspace denied")


def visible_cursor_keys(request: Request, records: Iterable[tuple[str, dict[str, Any]]]) -> list[str]:
    """Keys of ``(key, metadata)`` records the caller's tenant scope covers."""
    from services.cdc_lease import lease_view

    scope = _scope(request)
    if scope.unrestricted and not scope.header_workspace_id:
        return [key for key, _meta in records]
    cache: dict[Any, str | None] = {}
    out: list[str] = []
    for key, meta in records:
        owner = owner_from_records(key, meta, None, cache=cache)
        if not owner:
            owner = owner_from_records(key, meta, lease_view(key), cache=cache)
        if scope.allows(owner):
            out.append(key)
    return out


def visible_leases(request: Request, leases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Lease views the caller's tenant scope covers."""
    from services.sync_cursor import get_watermark_record

    scope = _scope(request)
    if scope.unrestricted and not scope.header_workspace_id:
        return leases
    cache: dict[Any, str | None] = {}
    out: list[dict[str, Any]] = []
    for view in leases:
        key = str(view.get("cursor_key") or "")
        owner = owner_from_records(key, None, view, cache=cache)
        if not owner:
            _wm, meta = get_watermark_record(key)
            owner = owner_from_records(key, meta, view, cache=cache)
        if scope.allows(owner):
            out.append(view)
    return out


def stamp_owner(meta: dict[str, Any] | None) -> dict[str, Any] | None:
    """Add ``workspace_id`` to watermark/lease metadata when it is knowable.

    From the job the metadata names, else the workspace bound to the request
    being served. Never fails the write it decorates.
    """
    if meta is None or str(meta.get("workspace_id") or "").strip():
        return meta
    try:
        jid = str(meta.get("job_id") or "").strip()
        ws = _STAMP_CACHE.get(jid) or "" if jid else ""
        if jid and not ws:
            ws = job_workspace(jid) or ""
            if ws:
                if len(_STAMP_CACHE) >= 1024:
                    _STAMP_CACHE.clear()
                _STAMP_CACHE[jid] = ws
        if not ws:
            from services.workspace_filter import current_workspace_stamp

            ws = current_workspace_stamp(for_write=False) or ""
        if ws:
            return {**meta, "workspace_id": ws}
    except Exception as exc:  # noqa: BLE001 - ownership is resolved again on read
        _logger.warning("Could not stamp a workspace on cursor metadata: %s", exc)
    return meta
