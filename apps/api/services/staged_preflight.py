"""Confirm reuses the preflight staging already ran (D308).

Staging opens the source once and saves the gate result under a run id.
Confirm carries that id. When the inputs Confirm will check are the same
inputs staging checked, the second catalog walk does not run. A changed
mapping, mode, table, filter, or recipe is a different check, and it runs.
"""

from __future__ import annotations

import contextvars
import copy
import hashlib
import json
import threading
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

_LOCK = threading.Lock()
_RUNS: OrderedDict[str, dict[str, Any]] = OrderedDict()
_MAX = 50
_BOUND: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "df_staged_preflight", default=None
)


def _canonical_mappings(mappings: Any) -> list[dict[str, str]]:
    rows = []
    for item in mappings or []:
        if not isinstance(item, dict):
            continue
        rows.append({
            "source": str(item.get("source") or ""),
            "target": str(item.get("target") or item.get("source") or ""),
            "transform": str(item.get("transform") or ""),
        })
    rows.sort(key=lambda row: (row["source"], row["target"], row["transform"]))
    return rows


def preflight_input_fingerprint(**parts: Any) -> str:
    """Identity of a preflight. Sample rows and row counts are not part of it.

    Confirm has loaded the batch staging only sampled. Those differ on every
    run and must not force a second catalog walk.
    """
    payload = {
        "source_connector_id": str(parts.get("source_connector_id") or ""),
        "source_table": str(parts.get("source_table") or ""),
        "source_format": str(parts.get("source_format") or "").strip().lower(),
        "destination_connector_id": str(parts.get("destination_connector_id") or ""),
        "destination_table": str(parts.get("destination_table") or ""),
        "destination_db_type": str(parts.get("destination_db_type") or "").strip().lower(),
        "sync_mode": str(parts.get("sync_mode") or ""),
        "schema_policy": str(parts.get("schema_policy") or ""),
        "validation_mode": str(parts.get("validation_mode") or ""),
        "source_filter": parts.get("source_filter") or {},
        "shape_recipe": parts.get("shape_recipe") or {},
        "mappings": _canonical_mappings(parts.get("mappings")),
        "stream_contracts": parts.get("stream_contracts") or [],
    }
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def remember_staged_preflight(run_id: str, fingerprint: str, result: dict[str, Any]) -> None:
    rid = str(run_id or "").strip()
    if not rid or not isinstance(result, dict):
        return
    with _LOCK:
        _RUNS[rid] = {"fingerprint": fingerprint, "result": copy.deepcopy(result)}
        _RUNS.move_to_end(rid)
        while len(_RUNS) > _MAX:
            _RUNS.popitem(last=False)


def staged_preflight_for_bound() -> dict[str, Any] | None:
    """The bound run, already checked against this request's inputs."""
    row = _BOUND.get()
    if not row:
        return None
    out = copy.deepcopy(row["result"])
    out["reused_staged_preflight"] = True
    return out


@contextmanager
def bind_staged_preflight(run_id: str, fingerprint: str = "") -> Iterator[None]:
    """Bind the staged run only when ``fingerprint`` is the one it was saved with."""
    rid = str(run_id or "").strip()
    with _LOCK:
        row = _RUNS.get(rid) if rid else None
    if row is not None and row.get("fingerprint") != fingerprint:
        row = None
    token = _BOUND.set(row)
    try:
        yield
    finally:
        _BOUND.reset(token)


def request_preflight_fingerprint(request: Any) -> str:
    source = getattr(request, "source", None)
    dest = getattr(request, "destination", None)
    return preflight_input_fingerprint(
        source_connector_id=getattr(source, "connector_id", "") or "",
        source_table=getattr(source, "table", "") or getattr(source, "collection", "") or "",
        source_format=getattr(source, "format", "") or "",
        destination_connector_id=getattr(dest, "connector_id", "") or "",
        destination_table=getattr(dest, "table", "") or getattr(dest, "collection", "") or "",
        destination_db_type=getattr(dest, "format", "") or "",
        mappings=getattr(request, "mappings", None) or [],
        sync_mode=getattr(request, "sync_mode", "") or "",
        schema_policy=getattr(request, "schema_policy", "") or "",
        validation_mode=getattr(request, "validation_mode", "") or "",
        source_filter=getattr(request, "source_filter", None) or {},
        shape_recipe=getattr(request, "shape_recipe", None) or {},
        stream_contracts=getattr(request, "stream_contracts", None) or [],
    )
