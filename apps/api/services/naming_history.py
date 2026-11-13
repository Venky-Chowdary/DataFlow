"""What earlier runs did to a destination object's names (G11 final review B2).

Every job created after the G11 upgrade carries ``naming_scheme``
(``create_transfer_job``), and each run stamps its naming decision per
destination object it writes::

    naming: [{"object": <writer_object_name>, "mode": "exact" | "legacy",
              "scheme_version": 1}, ...]

``object_history`` answers, for one object, what the last runs did:

* only jobs that wrote rows, to the SAME resolved writer object, in the run's
  OWN workspace (passed explicitly; a scheduler beat is not bound to one),
  on the SAME cluster (connector id, else host:port; an empty host proves
  nothing);
* the most recent stamped run decides (``exact`` / ``legacy``);
* else a job that predates the scheme (no ``naming_scheme``) means
  ``legacy``: it wrote the pre-G11 names;
* else ``none``: a new object.

Kafka follows it for its default names; the MongoDB / Elasticsearch / Redis /
DynamoDB old-field probes run only on ``legacy`` history.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

logger = logging.getLogger(__name__)

SCHEME_VERSION = 1
LEGACY = "legacy"
EXACT = "exact"
NONE = "none"

#: (job id, workspace id) of the run in progress (``naming_job_scope``).
_RUN: ContextVar[tuple[str, str] | None] = ContextVar("g11_naming_run", default=None)
#: The stamps this run has written, per job id.
_STAMPS: dict[str, list[dict[str, Any]]] = {}
#: Objects whose old fields this run checked or acknowledged, per job id.
_CHECKED: dict[str, set[str]] = {}


@contextmanager
def naming_job_scope(job_id: str, workspace_id: str) -> Iterator[None]:
    """Bind the run's job and workspace while it executes (``tracked_job_scope``)."""
    token = _RUN.set((str(job_id or ""), str(workspace_id or "")))
    try:
        yield
    finally:
        _RUN.reset(token)
        _STAMPS.pop(str(job_id or ""), None)
        _CHECKED.pop(str(job_id or ""), None)


def run_workspace(request: Any = None) -> str:
    """The workspace a run (or Validate) acts in, never a bound "every workspace" filter."""
    if request is not None and getattr(request, "workspace_id", None) is not None:
        return str(getattr(request, "workspace_id") or "")
    bound = _RUN.get()
    if bound is not None:
        return bound[1]
    from services.workspace_filter import current_workspace_stamp

    return str(current_workspace_stamp(for_write=False) or "")


def _hosts(host: Any, port: Any, connection_string: Any = "") -> frozenset[str]:
    out: set[str] = set()
    h = str(host or "").strip().lower()
    if h:
        out.add(f"{h}:{int(port)}" if str(port or "").strip().isdigit() and int(port) else h)
    conn = str(connection_string or "").strip()
    if conn and not conn.startswith(("enc:", "gAAAA")):  # an encrypted secret proves nothing
        body = conn.split("://", 1)[-1].split("/", 1)[0].rsplit("@", 1)[-1]
        out.update(p.strip().lower() for p in body.split(",") if re.match(r"^[\w.-]+:\d+$", p.strip()))
    return frozenset(out)


def cluster_of(cfg: Mapping[str, Any] | None, endpoint: Any = None) -> tuple[str, frozenset[str]]:
    """``(connector id, host:port set)`` of the destination a run writes."""
    cfg = dict(cfg or {})
    if isinstance(endpoint, Mapping):
        endpoint = None  # Validate's options dict names no cluster
    cid = str(getattr(endpoint, "connector_id", "") or cfg.get("connector_id") or "")
    host = getattr(endpoint, "host", "") or cfg.get("host") or ""
    port = getattr(endpoint, "port", 0) or cfg.get("port") or 0
    conn = getattr(endpoint, "connection_string", "") or cfg.get("connection_string") or ""
    return cid, _hosts(host, port, conn)


def _job_cluster(job: Mapping[str, Any]) -> tuple[str, frozenset[str]]:
    dest = dict((job.get("transfer_request") or {}).get("destination") or {})
    cid = str(job.get("dest_connector_id") or dest.get("connector_id") or "")
    return cid, _hosts(dest.get("host"), dest.get("port"), dest.get("connection_string"))


def same_cluster(a: tuple[str, frozenset[str]], b: tuple[str, frozenset[str]]) -> bool:
    """Same connector, or a shared normalized host (a port on one side only still
    matches): another connector to the same server is the same cluster (re-check F3)."""
    if a[0] and b[0] and a[0] == b[0]:
        return True
    if not (a[1] and b[1]):
        return False  # an empty host proves nothing

    def bare(hosts: frozenset[str]) -> set[str]:
        return {h.rsplit(":", 1)[0] if re.search(r":\d+$", h) else h for h in hosts}

    if a[1] & b[1]:
        return True
    portless = {h for h in a[1] | b[1] if not re.search(r":\d+$", h)}
    return bool(portless & bare(a[1]) & bare(b[1]))


def scope_of(kind: str, cfg: Mapping[str, Any] | None, endpoint: Any = None) -> str:
    """The namespace an object name lives in (security check M1).

    MongoDB: the database; Redis: the db index; SQL: database/schema. Kafka
    topics, Elasticsearch indexes and DynamoDB tables are cluster-wide ("").
    The same collection name in another database is another object.
    """
    from connectors.destination_naming import (
        DOCUMENT_STORES,
        SQL_NAME_LIMITS,
        SQL_UNLIMITED,
    )

    cfg = dict(cfg or {})
    if isinstance(endpoint, Mapping):
        endpoint = None

    def part(name: str) -> str:
        return str(getattr(endpoint, name, "") or cfg.get(name) or "").strip().lower()

    if kind in DOCUMENT_STORES:
        return part("database")
    if kind == "redis":
        index = part("database").removeprefix("db")
        return index if index.isdigit() else "0"
    if kind in SQL_NAME_LIMITS or kind in SQL_UNLIMITED:
        return f"{part('database')}/{part('schema')}"
    return ""


def _job_scope(kind: str, job: Mapping[str, Any]) -> str | None:
    """A job's namespace, or None when the job document does not name one."""
    dest = dict((job.get("transfer_request") or {}).get("destination") or {})
    if not (dest.get("database") or dest.get("schema") or job.get("destination_database")):
        return None
    return scope_of(kind, {"database": dest.get("database") or job.get("destination_database"),
                           "schema": dest.get("schema")})


def object_history(
    kind: str, obj: str, cfg: Mapping[str, Any] | None, endpoint: Any, workspace_id: str,
) -> str:
    """``legacy`` / ``exact`` / ``none`` for ``obj`` (see the module docstring).

    Raises when the job history cannot be read: callers fail closed.
    """
    from connectors.destination_naming import STREAM_SINKS, dest_kind
    from services.mongodb_service import get_mongodb_service

    if not obj:
        return NONE
    family = STREAM_SINKS if kind in STREAM_SINKS else frozenset({kind})
    here = cluster_of(cfg, endpoint)
    namespace = scope_of(kind, cfg, endpoint)
    current = (_RUN.get() or ("", ""))[0]
    rows = get_mongodb_service().naming_history_jobs(obj, workspace_id=str(workspace_id or ""))
    legacy = False
    for job in rows:  # newest first
        if str(job.get("_id") or "") == current:
            continue
        if dest_kind(str(job.get("destination_type") or "")) not in family:
            continue
        if not same_cluster(here, _job_cluster(job)):
            continue
        stamps = job.get("naming")
        if isinstance(stamps, list):
            for stamp in stamps:
                # A stamp decides only for the same object in the same namespace
                # (database / schema / db index): M1.
                if (isinstance(stamp, Mapping) and str(stamp.get("object") or "") == obj
                        and str(stamp.get("scope") or "") == namespace):
                    return EXACT if str(stamp.get("mode") or "") == EXACT else LEGACY
        job_scope = _job_scope(kind, job)
        if not job.get("naming_scheme") and stamps is None and job_scope in (None, namespace):
            legacy = True  # written before the upgrade (here, or not provably elsewhere)
    return LEGACY if legacy else NONE


def ensure_naming_history_indexes(svc: Any = None, *, max_time_ms: int = 30000) -> bool:
    """Build the job store's naming-history indexes (API startup, in the background)."""
    try:
        if svc is None:
            from services.mongodb_service import get_mongodb_service

            svc = get_mongodb_service()
        build = getattr(svc, "ensure_naming_history_indexes", None)
        return bool(build(max_time_ms=max_time_ms)) if callable(build) else False
    except Exception as exc:  # noqa: BLE001 - an index is an optimisation; the query is bounded
        logger.warning("G11: naming-history indexes not built: %s", exc)
        return False


def record_naming(obj: str, mode: str, scope: str = "") -> None:
    """Stamp this run's naming decision for ``obj`` (in ``scope``) on its job."""
    bound = _RUN.get()
    if bound is None or not bound[0] or not obj:
        return
    job_id = bound[0]
    stamps = [s for s in _STAMPS.get(job_id, [])
              if (s.get("object"), s.get("scope", "")) != (obj, scope)]
    stamps.append({"object": obj, "scope": scope, "mode": EXACT if mode == EXACT else LEGACY,
                   "scheme_version": SCHEME_VERSION})
    _STAMPS[job_id] = stamps
    try:
        from services.mongodb_service import get_mongodb_service

        get_mongodb_service().update_job_fields(
            job_id, {"naming": list(stamps)}, touch_updated_at=False)
    except Exception as exc:  # noqa: BLE001 - a stamp must never stop the run
        logger.warning("G11: naming decision not stamped on %s: %s", job_id, exc)


def note_checked(obj: str) -> None:
    """This run checked (or acknowledged) ``obj``'s old field names (no-op outside a run)."""
    bound = _RUN.get()
    if bound is not None and bound[0] and obj:
        _CHECKED.setdefault(bound[0], set()).add(obj)


def record_request_naming(request: Any, obj: str) -> None:
    """Stamp what this run decided for ``obj``, and only that (re-check F3).

    ``legacy`` when a row kept an old name (a server adoption); ``exact`` only
    when the run checked or acknowledged the object's old field names. A run
    whose Map renamed nothing decided nothing and stamps nothing, so it can
    never mask an earlier pre-upgrade job.
    """
    from connectors.destination_naming import dest_kind
    from services.target_name_legacy import MAP_TARGET

    dest = getattr(request, "destination", None)
    try:
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(dest) if dest is not None else {}
    except Exception:  # noqa: BLE001 - the endpoint's own fields still name the namespace
        cfg = {}
    scope = scope_of(dest_kind(str(getattr(dest, "format", "") or "")), cfg, dest)
    adopted = any(isinstance(m, dict) and m.get(MAP_TARGET)
                  for m in (getattr(request, "mappings", None) or []))
    if adopted:
        record_naming(obj, LEGACY, scope)
        return
    bound = _RUN.get()
    if bound is not None and obj in _CHECKED.get(bound[0], set()):
        record_naming(obj, EXACT, scope)


__all__ = [
    "EXACT",
    "LEGACY",
    "NONE",
    "SCHEME_VERSION",
    "cluster_of",
    "ensure_naming_history_indexes",
    "naming_job_scope",
    "note_checked",
    "object_history",
    "record_naming",
    "record_request_naming",
    "run_workspace",
    "same_cluster",
    "scope_of",
]
