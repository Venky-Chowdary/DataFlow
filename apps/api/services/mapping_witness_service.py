"""Mapping Witness: bounded sample reads, the session-bound evidence store, the snapshot a job runs with.

:mod:`services.mapping_witness` judges; this module feeds and remembers it.

* **Session.** Every piece of evidence — a witness report, a Map approval, a
  flag review, a model suggestion — belongs to one Map session: the transfer
  plan id when Studio has one, else the id Map minted for the session, which
  the run then submits (``witness_session_id``). A job takes only evidence of
  its own session, for its own source and destination, at most
  :data:`MAX_EVIDENCE_AGE` old. Evidence with no session, or from another
  session, user or file, never reaches a job: the pair reads "not checked".
* **Identity.** Destination identity from the *resolved* endpoint (driver,
  host and default port — never a connector id, which a fallback lookup can
  supply on one side only; the BigQuery project; the file path or object key
  of a file destination), with table and schema case kept where the engine is
  case-sensitive. The mapping digest covers it and the pairs actually written.
* **Samples.** The destination is read by key only, in batches of named
  columns (never ``SELECT *``). A target the catalog does not have yet, a name
  the sampler cannot address, or one past the read bound is "not read". A
  warehouse is read only on an explicit Check mapping. Reads run under the
  workspace's egress allowlist.
* **Writes.** Every read-modify-write of the store runs under one lock (and, in
  file mode, an ``flock`` across processes, then an atomic replace), so two
  writers never lose each other's update. A request past its deadline (the
  route already answered 504) writes nothing: :class:`WriteGuard` is checked
  under that same lock at save time.
* **Bounds.** Records are per workspace and session; each is capped. A new pair
  set checked in an open session supersedes that session's earlier record for
  the same source and destination unless a job was submitted with it; a record
  keeps its latest report whole and earlier ones by hash only. Past
  :data:`MAX_RECORDS_PER_WORKSPACE`, records older than the evidence window go
  first; a record inside the window (an open Map session) goes only past the
  hard bound :data:`MAX_LIVE_RECORDS_PER_WORKSPACE`, and its session then reads
  "evidence expired", never "no check matches".
"""

from __future__ import annotations

import contextlib
import functools
import hashlib
import json
import logging
import re
import secrets
import threading
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from services.mapping_witness import (
    MAX_ROWS,
    flagged_pairs,
    mapped_pairs,
    report_sha256,
    source_sample_digest,
    witness_mappings,
)
from services.platform_config import data_dir
from services.shared_doc_store import tracked
from services.shared_record_store import SharedRecordSet

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = "mapping_witness_snapshot.v2"
#: Reports kept per session and mapping digest.
MAX_REPORTS = 3
#: Per record: flag reviews, sources with Map decisions or suggestions, entries kept per source.
MAX_REVIEWS = 400
MAX_SOURCES = 400
MAX_PER_SOURCE = 5
#: Witness and activity records one workspace keeps before records past the evidence window are pruned.
MAX_RECORDS_PER_WORKSPACE = 300
#: The hard bound: only past it is a record inside the evidence window pruned (oldest first).
MAX_LIVE_RECORDS_PER_WORKSPACE = 3000
#: Sessions remembered as pruned, so their runs read "evidence expired".
MAX_EXPIRED_SESSIONS = 1000
#: Evidence older than this at submit is not used.
MAX_EVIDENCE_AGE = timedelta(hours=24)
CLOCK_SKEW = timedelta(minutes=5)
#: Columns per keyed read (the key is added), and reads per check.
READ_BATCH_COLUMNS = 60
MAX_READ_BATCHES = 4
REVIEW_DECISIONS = ("kept", "changed")
CONTINUED = "continued"
#: Engines where a read costs money or a scan: read only on an explicit Check mapping.
WAREHOUSES = frozenset({"bigquery", "snowflake", "redshift", "databricks", "synapse", "athena"})
#: Engines whose quoted identifiers keep their case (``"Orders"`` is not ``orders``).
CASE_SENSITIVE = frozenset({"postgresql", "postgres", "cockroachdb", "timescaledb", "yugabytedb"})
TRIGGER_AUTO = "auto"
TRIGGER_EXPLICIT = "explicit"
_COLUMN_NAME = re.compile(r"^[^\x00-\x1f;]{1,128}$")
_SESSION = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")


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

    write_json_atomic(_store_path(), dict(data), indent=None)


_DOC = SharedRecordSet(
    "mapping_witness",
    file_load=_file_load,
    file_save=_file_save,
    file_path=_store_path,
    partition_key="workspace_id",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── one writer at a time ─────────────────────────────────────────────────────

_WRITE_LOCK = threading.RLock()
_WRITING = threading.local()


@contextlib.contextmanager
def _writing() -> Iterator[None]:
    """Hold the store for one read-modify-write: the process lock, and in file mode an ``flock``."""
    with _WRITE_LOCK:
        depth = getattr(_WRITING, "depth", 0)
        _WRITING.depth = depth + 1
        try:
            from services.shared_doc_store import control_store_backend

            if depth or control_store_backend() != "file":
                yield
                return
            path = _store_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(f"{path}.lock", "a+") as handle:
                try:
                    import fcntl

                    fcntl.flock(handle, fcntl.LOCK_EX)
                except (ImportError, OSError):  # no flock here: the process lock still serialises
                    logger.debug("mapping witness: file lock unavailable")
                yield
        finally:
            _WRITING.depth = depth


def _serialised(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Run a writer under :func:`_writing`, read again and re-applied when another node's write won the swap."""
    retried = _DOC.retrying(fn)

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with _writing():
            return retried(*args, **kwargs)

    return wrapper


class WriteGuard:
    """A request's leave to write: gone once its deadline passed or the route gave up on it (504).

    :meth:`open` is read under the store's write lock, at save time, and :meth:`cancel`
    takes the same lock, so a check the caller was told "did not finish" is never recorded.
    The cutoff sits a little before the deadline, so a save never lands as the 504 goes out.
    """

    def __init__(self, seconds: float) -> None:
        self._cutoff = time.monotonic() + max(0.0, seconds - min(1.0, seconds / 10))
        self._cancelled = False

    def cancel(self) -> None:
        with _WRITE_LOCK:
            self._cancelled = True

    def open(self) -> bool:
        return not self._cancelled and time.monotonic() < self._cutoff


def _closed(guard: WriteGuard | None) -> bool:
    return guard is not None and not guard.open()


def _sha(body: Any) -> str:
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def clean_session(value: Any) -> str:
    """A usable session id, or "" (evidence without a session never reaches a job)."""
    text = str(value or "").strip()
    return text if _SESSION.match(text) else ""


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


def _resolved_cfg(ep: Any, workspace_id: str) -> dict[str, Any]:
    try:
        from src.transfer.adapters import resolve_connector_config

        return resolve_connector_config(ep, workspace_id or None)
    except Exception as exc:  # noqa: BLE001 - an unresolvable endpoint still has its own fields
        logger.info("witness identity: endpoint not resolved (%s)", type(exc).__name__)
        return {"host": ep.host or "", "port": ep.port or 0, "database": ep.database or "",
                "schema": ep.schema or "", "table": ep.table or "", "type": ep.format or "",
                "connection_string": ep.connection_string or "", "service_account": ep.service_account or ""}


def _bigquery_project(cfg: dict[str, Any], ep: Any) -> str:
    extra = dict(getattr(ep, "extra", None) or {})
    for value in (cfg.get("project"), cfg.get("project_id"), extra.get("project"), extra.get("project_id")):
        if str(value or "").strip():
            return str(value).strip()
    account = cfg.get("service_account") or getattr(ep, "service_account", "") or ""
    try:
        parsed = json.loads(account) if isinstance(account, str) and account.strip().startswith("{") else {}
    except ValueError:
        parsed = {}
    return str((parsed or {}).get("project_id") or "")


def destination_identity(endpoint: Any, workspace_id: str = "") -> str:
    """``driver|host:port|database|schema|object`` for the resolved endpoint.

    Files name their path or object key; BigQuery its project; table and schema
    keep their case on engines where a quoted identifier is case-sensitive.
    """
    ep = _endpoint_config(endpoint)
    if ep is None:
        return ""
    from services.dialect_profiles import schema_from_cfg
    from src.transfer.connector_capabilities import effective_port, resolve_driver_type

    if str(ep.kind or "") in ("file", "file_export"):
        extra = dict(ep.extra or {})
        where = ep.output_path or extra.get("object_key") or extra.get("key") or extra.get("path") or ep.table
        bucket = extra.get("bucket") or ep.database or ""
        return "|".join(["file", str(ep.format or "").casefold(), str(bucket), str(where or "")])
    cfg = _resolved_cfg(ep, workspace_id)
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
    owner = f"{host.casefold()}:{effective_port(driver, port) if host else ''}"
    if driver == "bigquery":
        owner = f"project:{_bigquery_project(cfg, ep)}"
    schema = schema_from_cfg(driver, cfg)
    obj = str(cfg.get("table") or ep.table or ep.collection or "")
    if driver not in CASE_SENSITIVE:
        schema, obj = schema.casefold(), obj.casefold()
    return "|".join([driver, owner, database.casefold(), schema, obj])


#: Text a statement keeps verbatim: string literals, quoted identifiers, dollar quotes, comments.
_VERBATIM = re.compile(
    r"'(?:[^']|'')*'?|\"(?:[^\"]|\"\")*\"?|`(?:[^`]|``)*`?|\[[^\]]*\]?"
    r"|\$(\w*)\$.*?(?:\$\1\$|\Z)|--[^\n]*|/\*.*?(?:\*/|\Z)",
    re.S,
)
_SPACE = re.compile(r"\s+")
_AROUND_PUNCT = re.compile(r" ?([,();=]) ?")


def statement_form(statement: Any) -> str:
    """A statement in the form two spellings of one query share.

    Case and whitespace are folded only outside literals, quoted identifiers and
    comments: ``'ACME'`` is not ``'acme'``, ``"Orders"`` is not ``"orders"``, and
    ``'a  b'`` is not ``'a b'``; ``SELECT a,b`` and ``select a , b`` are one query.
    """
    text = str(statement or "")
    out: list[str] = []
    at = 0
    for m in _VERBATIM.finditer(text):
        out.append(_AROUND_PUNCT.sub(r"\1", _SPACE.sub(" ", text[at:m.start()]).casefold()))
        out.append(m.group(0))
        at = m.end()
    out.append(_AROUND_PUNCT.sub(r"\1", _SPACE.sub(" ", text[at:]).casefold()))
    return "".join(out).strip().rstrip(";").strip()


def _object_path(name: str) -> str:
    """A file or object by its whole path (``2023/customers.csv`` is not ``2024/customers.csv``)."""
    path = re.sub(r"/{2,}", "/", name.replace("\\", "/"))
    while path.startswith("./"):
        path = path[2:]
    return path


def source_identity(source: dict[str, Any] | None) -> str:
    """What the sample came from, the one normal form both Map and the job are compared in.

    ``file|<path>``, or ``database|<table or collection>`` for every other source
    kind (a cloud object is named by its path). Paths are kept whole. A query or
    procedure source adds ``#`` and a digest of its :func:`statement_form`, so two
    different extracts of one stream are two sources.
    """
    src = dict(source or {})
    raw_kind = str(src.get("kind") or "").strip().casefold()
    name = _object_path(str(src.get("name") or "").strip())
    kind = "file" if raw_kind == "file" else ("database" if raw_kind or name else "")
    text = statement_form(src.get("statement"))
    ident = f"{kind}|{name.casefold()}" if kind or name else ""
    return f"{ident}#{hashlib.sha256(text.encode()).hexdigest()[:16]}" if text else ident


def _callable_statement(extra: Any) -> str:
    extra = extra if isinstance(extra, dict) else {}
    mode = str(extra.get("source_read_mode") or "").strip().casefold()
    if mode == "query":
        return str(extra.get("source_query") or "")
    if mode == "procedure":
        return str(extra.get("procedure_call") or "")
    return ""


def request_source_identity(request: Any) -> str:
    """The job's side of :func:`source_identity`, from the submitted transfer request."""
    src = getattr(request, "source", None)
    kind = str(getattr(src, "kind", "") or "")
    name = (getattr(request, "source_filename", "") or "") if kind == "file" else (
        getattr(src, "table", "") or getattr(src, "collection", "") or "")
    return source_identity({"kind": kind, "name": name, "statement": _callable_statement(getattr(src, "extra", None))})


def mapping_digest(mappings: list[dict[str, Any]] | None, identity: str) -> str:
    """One id for "these written pairs onto this destination", from names only."""
    pairs = sorted({(p["source"].casefold(), p["target"].casefold()) for p in mapped_pairs(list(mappings or []))})
    return _sha({"destination": identity, "pairs": [list(p) for p in pairs]})


def _report_key(workspace_id: str, session: str, digest: str) -> str:
    return f"witness::{workspace_id}::{session}::{digest}"


def _activity_key(workspace_id: str, session: str, identity: str) -> str:
    return f"activity::{workspace_id}::{session}::{_sha(identity)[:32]}"


def _load(key: str, workspace_id: str) -> tuple[Any, dict[str, Any] | None]:
    data = _DOC.load(keys=[key])
    record = data.get(key)
    if not isinstance(record, dict) or str(record.get("workspace_id") or "") != workspace_id:
        return data, None
    return data, record


def _commit(data: Any, guard: WriteGuard | None = None) -> bool:
    """Write ``data`` back unless the request's guard has closed (checked under the write lock)."""
    if _closed(guard):
        return False
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return True


def _save(data: Any, key: str, record: dict[str, Any], *, new: bool, guard: WriteGuard | None = None) -> bool:
    data[key] = record
    if not _commit(data, guard):
        return False
    if new:
        prune_workspace(str(record.get("workspace_id") or ""))
    return True


def _expired_key(workspace_id: str) -> str:
    return f"expired::{workspace_id}"


def _is_old(updated_at: str, now: datetime) -> bool:
    try:
        return datetime.fromisoformat(updated_at) < now - MAX_EVIDENCE_AGE - CLOCK_SKEW
    except ValueError:
        return True


def _prune_in(data: Any, workspace_id: str, keep: int | None = None, live_keep: int | None = None) -> int:
    """Drop past-bound records from loaded ``data`` (see :func:`prune_workspace`); returns how many went."""
    keep = MAX_RECORDS_PER_WORKSPACE if keep is None else keep
    live_keep = MAX_LIVE_RECORDS_PER_WORKSPACE if live_keep is None else live_keep
    mine = sorted((str((r or {}).get("updated_at") or ""), k) for k, r in data.items()
                  if isinstance(r, dict) and str(r.get("workspace_id") or "") == workspace_id
                  and str(k).startswith(("witness::", "activity::")))
    if len(mine) <= keep:
        return 0
    now = datetime.now(timezone.utc)
    old = [k for at, k in mine if _is_old(at, now)]
    drop = old[: len(mine) - keep]
    live = [k for at, k in mine if not _is_old(at, now)]
    drop += live[: max(0, len(live) - live_keep)]
    if not drop:
        return 0
    expired_key = _expired_key(workspace_id)
    expired = dict((data.get(expired_key) or {}).get("sessions") or {})
    stamp = now.isoformat()
    for k in drop:
        expired[str(data[k].get("session") or "")] = stamp
        del data[k]
    expired.pop("", None)
    newest = sorted(expired.items(), key=lambda kv: kv[1])[-MAX_EXPIRED_SESSIONS:]
    data[expired_key] = {"workspace_id": workspace_id, "sessions": dict(newest), "updated_at": stamp}
    return len(drop)


@_serialised
def prune_workspace(workspace_id: str, keep: int | None = None, live_keep: int | None = None) -> int:
    """Past ``keep`` records, drop those older than the evidence window (oldest first); drop a record
    inside the window only past ``live_keep``. Pruned sessions are remembered as expired. Returns how many went."""
    try:
        data = _DOC.load(part=workspace_id)
        dropped = _prune_in(data, workspace_id, keep, live_keep)
        if dropped:
            _DOC.save(tracked(data, getattr(data, "rev", None)))
        return dropped
    except Exception as exc:  # noqa: BLE001 - pruning is housekeeping; the evidence is already stored
        logger.info("mapping witness prune skipped: %s", type(exc).__name__)
        return 0


# ── writes ───────────────────────────────────────────────────────────────────

_SALT_KEY = "salt::"


@_serialised
def _new_salt(workspace_id: str) -> str:
    key = _SALT_KEY + workspace_id
    data, record = _load(key, workspace_id)
    if record and record.get("salt"):
        return str(record["salt"])
    salt = secrets.token_hex(32)
    data[key] = {"workspace_id": workspace_id, "salt": salt, "updated_at": _now()}
    _DOC.save(tracked(data, getattr(data, "rev", None)))
    return salt


def workspace_sample_key(workspace_id: str) -> bytes:
    """The workspace's server-held secret for :func:`source_sample_digest` (never leaves the store)."""
    _data, record = _load(_SALT_KEY + workspace_id, workspace_id)
    salt = str((record or {}).get("salt") or "") or _new_salt(workspace_id)
    return bytes.fromhex(salt)


def _compact(entry: dict[str, Any]) -> dict[str, Any]:
    """An earlier report kept by reference: its hash and who/when, not the report again."""
    return {k: entry[k] for k in ("report_sha256", "source_identity", "requested_by", "recorded_at") if k in entry}


def _supersede(data: Any, keep_key: str, *, workspace_id: str, session: str, identity: str,
               source: str) -> list[dict[str, Any]]:
    """Drop this session's earlier records for the same source and destination that no job was submitted with.

    Returns what a late review of one of them still needs (its digest, report hash and flagged
    pairs, nothing else), newest last: a "changed" answer sent just before the edit's check landed.
    """
    gone: list[tuple[str, list[dict[str, Any]]]] = []
    for k in [k for k in data.keys() if str(k).startswith("witness::") and k != keep_key]:
        r = data.get(k)
        if not isinstance(r, dict) or str(r.get("workspace_id") or "") != workspace_id or r.get("submitted_at"):
            continue
        reports = [e for e in r.get("reports") or [] if isinstance(e, dict)]
        if (r.get("session") == session and r.get("destination_identity") == identity and reports
                and all(e.get("source_identity") == source for e in reports)):
            stub = {"digest": r.get("mapping_digest"), "report_sha256": reports[-1].get("report_sha256"),
                    "source_identity": source,
                    "flagged": [{"source": f["source"], "target": f["target"], "status": f["status"],
                                 "flag": {"id": (f.get("flag") or {}).get("id"),
                                          "kind": (f.get("flag") or {}).get("kind")}}
                                for f in flagged_pairs(reports[-1].get("report") or {})]}
            gone.append((str(r.get("updated_at") or ""), [*(r.get("previous") or []), stub]))
            del data[k]
    return [p for _at, chain in sorted(gone, key=lambda g: g[0]) for p in chain if isinstance(p, dict)]


def _superseded_report(workspace_id: str, session: str, digest: str) -> dict[str, Any] | None:
    """The flagged pairs of a report an edit in this session superseded, for a review sent before it landed."""
    data = _DOC.load(part=workspace_id)
    for k, r in data.items():
        if not (str(k).startswith("witness::") and isinstance(r, dict) and r.get("session") == session
                and str(r.get("workspace_id") or "") == workspace_id):
            continue
        for p in r.get("previous") or []:
            if isinstance(p, dict) and p.get("digest") == digest:
                return {"report": {"pairs": list(p.get("flagged") or [])}, "report_sha256": p.get("report_sha256"),
                        "source_identity": p.get("source_identity") or ""}
    return None


@_serialised
def save_report(report: dict[str, Any], *, workspace_id: str, session: str, identity: str, digest: str,
                source: str = "", actor: str = "", guard: WriteGuard | None = None) -> dict[str, Any] | None:
    """Record a report for this session and mapping; None when ``guard`` closed first (nothing written).

    The latest report is kept whole, the ones before it (up to :data:`MAX_REPORTS`) by hash. A new pair
    set in an open session supersedes that session's unsubmitted record for this source and destination.
    """
    session = clean_session(session)
    key = _report_key(workspace_id, session, digest)
    data, record = _load(key, workspace_id)
    new = record is None
    if new:
        from services.shared_doc_store import control_store_backend

        if control_store_backend() != "file":
            data = _DOC.load(part=workspace_id)  # one read for the session's siblings and the bound
        previous = _supersede(data, key, workspace_id=workspace_id, session=session, identity=identity,
                              source=source) if session else []
    record = record or {"workspace_id": workspace_id, "session": session, "mapping_digest": digest,
                        "destination_identity": identity, "reports": []}
    if new and previous:
        record["previous"] = previous[-MAX_REPORTS:]
    entry = {"report": report, "report_sha256": report_sha256(report), "source_identity": source,
             "requested_by": str(actor or "")[:256], "recorded_at": _now()}
    earlier = [_compact(e) for e in record.get("reports") or [] if isinstance(e, dict)]
    data[key] = {**record, "reports": [*earlier, entry][-MAX_REPORTS:], "updated_at": entry["recorded_at"]}
    if new:
        _prune_in(data, workspace_id)
    if not _commit(data, guard):
        return None
    return {"mapping_digest": digest, "session": session, **entry}


@_serialised
def mark_submitted(*, workspace_id: str, session: str, digest: str, report_digest: str) -> bool:
    """A job was submitted with this record's report: a later pair-set edit no longer supersedes it."""
    key = _report_key(workspace_id, clean_session(session), digest)
    data, record = _load(key, workspace_id)
    if not record or record.get("submitted_at"):
        return False
    if not any(isinstance(e, dict) and e.get("report_sha256") == report_digest for e in record.get("reports") or []):
        return False
    data[key] = {**record, "submitted_at": _now()}
    return _commit(data)


def latest_report(*, workspace_id: str, session: str, digest: str) -> dict[str, Any] | None:
    _data, record = _load(_report_key(workspace_id, clean_session(session), digest), workspace_id)
    entries = [e for e in (record or {}).get("reports") or [] if isinstance(e, dict)]
    return entries[-1] if entries else None


def _activity(workspace_id: str, session: str, identity: str) -> tuple[Any, str, dict[str, Any], bool]:
    key = _activity_key(workspace_id, session, identity)
    data, record = _load(key, workspace_id)
    new = record is None
    record = record or {"workspace_id": workspace_id, "session": session, "destination_identity": identity,
                        "map_decisions": {}, "flag_reviews": [], "model_suggestions": {}}
    return data, key, record, new


def _clean_name(value: Any) -> str:
    text = str(value or "").strip()
    return text if _COLUMN_NAME.match(text) else ""


def _bounded_sources(table: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    """At most :data:`MAX_SOURCES` sources, the most recently touched kept."""
    if len(table) <= MAX_SOURCES:
        return table
    ranked = sorted(table.items(), key=lambda kv: str((kv[1] or [{}])[-1].get("at") or ""))
    return dict(ranked[-MAX_SOURCES:])


@_serialised
def record_review(*, workspace_id: str, session: str, identity: str, digest: str, source: str, decision: str,
                  actor: str, flag_id: str = "", new_target: str = "", guard: WriteGuard | None = None,
                  ) -> dict[str, Any] | None:
    """A person's answer to one flag of this session's latest report.

    ``kept`` is an explicit "Keep this mapping" on the flag named by ``flag_id``;
    ``changed`` names the column the person changed it to; ``continued`` records
    that they went on past a flag nobody reviewed.
    """
    session = clean_session(session)
    if not session:
        raise ValueError("A review belongs to a Map session.")
    if decision not in (*REVIEW_DECISIONS, CONTINUED):
        raise ValueError("decision must be kept, changed or continued")
    latest = (latest_report(workspace_id=workspace_id, session=session, digest=digest)
              or _superseded_report(workspace_id, session, digest))
    if not latest:
        raise LookupError("No witness report for this mapping in this session")
    flagged = {p["source"].casefold(): p for p in flagged_pairs(latest["report"])}
    pair = flagged.get(str(source or "").casefold())
    if not pair:
        raise LookupError("This column has no witness flag to review")
    pair_flag = pair.get("flag") or {}
    if decision in ("kept", CONTINUED) and flag_id != pair_flag.get("id"):
        raise LookupError("That flag is not on the latest check; review the flag that is shown")
    target = _clean_name(new_target)
    if decision == "changed" and (not target or target.casefold() == pair["target"].casefold()):
        raise ValueError("A changed decision names the new destination column (a column name).")
    entry = {
        "source": pair["source"],
        "flagged_target": pair["target"],
        "flag_kind": pair_flag.get("kind"),
        "flag_id": pair_flag.get("id"),
        "report_sha256": latest["report_sha256"],
        "source_identity": latest.get("source_identity") or "",
        "mapping_digest": digest,
        "decision": decision,
        "new_target": target if decision == "changed" else None,
        "by": str(actor or "anonymous")[:256],
        "at": _now(),
    }
    data, key, record, new = _activity(workspace_id, session, identity)
    reviews = [*list(record.get("flag_reviews") or []), entry][-MAX_REVIEWS:]
    if not _save(data, key, {**record, "flag_reviews": reviews, "updated_at": entry["at"]}, new=new, guard=guard):
        return None
    return entry


@_serialised
def record_map_decisions(*, workspace_id: str, session: str, identity: str, mappings: list[dict[str, Any]],
                         actor: str, source: str = "", guard: WriteGuard | None = None) -> int | None:
    """Who approved or changed each pair in Map, in this session, for this source (the caller is authenticated)."""
    session = clean_session(session)
    if not session:
        return 0
    data, key, record, new = _activity(workspace_id, session, identity)
    decisions: dict[str, list[dict[str, Any]]] = dict(record.get("map_decisions") or {})
    at = _now()
    recorded = 0
    for m in mappings[:MAX_SOURCES]:
        if not isinstance(m, dict) or not (m.get("approved") or m.get("changed")):
            continue
        column, target = _clean_name(m.get("source")), _clean_name(m.get("target"))
        if not column or not target:
            continue
        entry = {"target": target, "approved": bool(m.get("approved")), "changed": bool(m.get("changed")),
                 "source_identity": source, "by": str(actor or "anonymous")[:256], "at": at}
        history = [e for e in decisions.get(column.casefold(), []) if isinstance(e, dict)]
        decisions[column.casefold()] = [*history, entry][-MAX_PER_SOURCE:]
        recorded += 1
    if recorded and not _save(data, key, {**record, "map_decisions": _bounded_sources(decisions), "updated_at": at},
                              new=new, guard=guard):
        return None
    return recorded


@_serialised
def record_model_suggestions(*, workspace_id: str, session: str, identity: str, mappings: list[dict[str, Any]],
                             llm: dict[str, Any] | None) -> int:
    """Model suggestions Map received (engine_suggestion from a named provider), in this session."""
    session = clean_session(session)
    if not session:
        return 0
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
    data, key, record, new = _activity(workspace_id, session, identity)
    suggestions: dict[str, list[dict[str, Any]]] = dict(record.get("model_suggestions") or {})
    for source, entry in rows[:MAX_SOURCES]:
        history = [e for e in suggestions.get(source.casefold(), []) if isinstance(e, dict)]
        suggestions[source.casefold()] = [*history, entry][-MAX_PER_SOURCE:]
    _save(data, key, {**record, "model_suggestions": _bounded_sources(suggestions), "updated_at": _now()}, new=new)
    return len(rows)


# ── the snapshot a job runs with ─────────────────────────────────────────────


def _fresh(at: Any, taken: datetime) -> bool:
    try:
        when = datetime.fromisoformat(str(at))
    except ValueError:
        return False
    # A check saved by another API node a moment "after" this submit is the same
    # session's evidence: allow for clock skew between nodes, never for days.
    return taken - MAX_EVIDENCE_AGE <= when <= taken + CLOCK_SKEW


def build_snapshot(*, workspace_id: str, session: str, identity: str, source: str,
                   mappings: list[dict[str, Any]], taken_at: str | None = None) -> dict[str, Any]:
    """What the job runs with: its own session's evidence for its source, destination and pairs."""
    taken_at = taken_at or _now()
    taken = datetime.fromisoformat(taken_at)
    session = clean_session(session)
    pairs = {p["source"].casefold(): p for p in mapped_pairs(mappings)}
    digest = mapping_digest(mappings, identity)
    base = {"version": SNAPSHOT_VERSION, "status": "ok", "taken_at": taken_at, "session": session or None,
            "mapping_digest": digest, "destination_identity": identity, "source_identity": source,
            "report": None, "report_sha256": None, "report_recorded_at": None, "requested_by": None,
            "map_decisions": {}, "flag_reviews": [], "model_suggestions": {}}
    if not session:
        body = {**base, "not_checked_reason": "The run was not submitted from a Map session."}
        return {**body, "sha256": _sha(body)}
    latest = latest_report(workspace_id=workspace_id, session=session, digest=digest)
    expired = False
    if latest and latest.get("source_identity") == source and not _fresh(latest.get("recorded_at"), taken):
        latest, expired = None, True  # this run's check, but too old to stand
    if latest and latest.get("source_identity") != source:
        latest = None  # another file or table: not this run's evidence
    if not latest and not expired:
        _d, gone = _load(_expired_key(workspace_id), workspace_id)
        expired = session in ((gone or {}).get("sessions") or {})
    _data, _key, activity, _new = _activity(workspace_id, session, identity)
    map_decisions: dict[str, dict[str, Any]] = {}
    for src_key, history in (activity.get("map_decisions") or {}).items():
        pair = pairs.get(src_key)
        valid = [e for e in history or [] if isinstance(e, dict) and pair
                 and str(e.get("target") or "").casefold() == pair["target"].casefold()
                 and e.get("source_identity") == source and _fresh(e.get("at"), taken)]
        if valid:
            map_decisions[pair["source"]] = valid[-1]
    reviews = []
    report_digest = (latest or {}).get("report_sha256")
    # A flag id covers the check's inputs, source, target and kind: the same flag on a re-check keeps its answer.
    flag_ids = {p["source"].casefold(): (p.get("flag") or {}).get("id")
                for p in flagged_pairs((latest or {}).get("report") or {})}
    for r in activity.get("flag_reviews") or []:
        if not isinstance(r, dict) or not _fresh(r.get("at"), taken) or r.get("source_identity") != source:
            continue
        pair = pairs.get(str(r.get("source") or "").casefold())
        if not pair:
            continue
        ran = pair["target"].casefold()
        on_what_ran = (bool(r.get("flag_id")) and r.get("flag_id") == flag_ids.get(pair["source"].casefold())
                       and str(r.get("flagged_target") or "").casefold() == ran)
        if r.get("decision") in ("kept", CONTINUED) and on_what_ran:
            reviews.append(r)  # the flag on what ran: kept explicitly, or continued past unreviewed
        elif r.get("decision") == "changed" and str(r.get("new_target") or "").casefold() == ran:
            reviews.append(r)  # an earlier flag in this session, resolved by changing to what ran
    suggestions = {}
    for src_key, history in (activity.get("model_suggestions") or {}).items():
        pair = pairs.get(src_key)
        valid = [e for e in history or [] if isinstance(e, dict) and pair and _fresh(e.get("at"), taken)]
        if valid:
            suggestions[pair["source"]] = valid[-1]
    body = {
        **base,
        "report": (latest or {}).get("report"),
        "report_sha256": report_digest,
        "report_recorded_at": (latest or {}).get("recorded_at"),
        "requested_by": (latest or {}).get("requested_by"),
        "map_decisions": map_decisions,
        "flag_reviews": reviews,
        "model_suggestions": suggestions,
    }
    if not latest:
        body["not_checked_reason"] = (
            "Evidence expired: this session's mapping check is older than 24 hours or was removed; check the "
            "mapping again." if expired else "No mapping check of this session matches this run's source and mapping.")
    return {**body, "sha256": _sha(body)}


def snapshot_sha256(snapshot: dict[str, Any]) -> str:
    return _sha({k: v for k, v in snapshot.items() if k != "sha256"})


def snapshot_for_request(request: Any) -> dict[str, Any]:
    """The witness snapshot stamped onto a job at submit. Never raises: a failure is stated."""
    workspace_id = str(getattr(request, "workspace_id", "") or "")
    try:
        identity = destination_identity(getattr(request, "destination", None), workspace_id)
        snap = build_snapshot(workspace_id=workspace_id, session=getattr(request, "witness_session_id", "") or "",
                              identity=identity, source=request_source_identity(request),
                              mappings=list(getattr(request, "mappings", None) or []))
    except Exception as exc:  # noqa: BLE001 - submitting a job never depends on evidence
        logger.warning("mapping witness snapshot unavailable: %s", type(exc).__name__)
        body = {"version": SNAPSHOT_VERSION, "status": "unavailable", "taken_at": _now(),
                "reason": "The mapping evidence store could not be read when the job was submitted."}
        return {**body, "sha256": _sha(body)}
    if snap.get("report_sha256"):
        try:  # the job holds its own copy; this only stops a later edit superseding the record
            mark_submitted(workspace_id=workspace_id, session=snap.get("session") or "",
                           digest=snap["mapping_digest"], report_digest=snap["report_sha256"])
        except Exception as exc:  # noqa: BLE001 - submitting a job never depends on evidence housekeeping
            logger.info("mapping witness: record not marked submitted (%s)", type(exc).__name__)
    return snap


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


def engine_of(endpoint: Any, workspace_id: str = "") -> str:
    """The engine actually dialed: a saved connector's own type, not the request's label."""
    ep = _endpoint_config(endpoint)
    if ep is None:
        return ""
    from src.transfer.connector_capabilities import resolve_driver_type

    cfg = _resolved_cfg(ep, workspace_id)
    return resolve_driver_type(str(cfg.get("type") or ep.format or "")).lower()


def _addressable(name: str) -> bool:
    """A name the shared sampler reads as written (it sanitizes names it cannot quote verbatim)."""
    from connectors.sql_identifiers import sanitize_identifier

    return sanitize_identifier(name, preserve_case=True, max_len=128) == name


def read_rows(endpoint: Any, *, workspace_id: str, key_column: str, keys: list[Any] | None,
              columns: list[str]) -> list[dict[str, Any]]:
    """One keyed, column-bounded read: ``SELECT <columns> … WHERE key IN (…)``, at most :data:`MAX_ROWS`.

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


def plan_read(mappings: list[dict[str, Any]], key_target: str,
              candidates: list[str] | None) -> tuple[list[list[str]], dict[str, str]]:
    """(batches of columns to read, unread target → why). Targets first, then candidates."""
    known = {str(c).casefold() for c in candidates or []}
    unread: dict[str, str] = {}
    wanted: list[str] = []
    seen: set[str] = {key_target.casefold()}
    for p in mapped_pairs(mappings):
        t = p["target"]
        if t.casefold() in seen:
            continue
        seen.add(t.casefold())
        if p["create_new"] or (known and t.casefold() not in known):
            unread[t] = "the column does not exist in the destination yet."
        elif not _clean_name(t) or not _addressable(t):
            unread[t] = "the sampler cannot address this column name."
        else:
            wanted.append(t)
    budget = READ_BATCH_COLUMNS * MAX_READ_BATCHES
    for t in wanted[budget:]:
        unread[t] = "past the number of columns one check reads."
    wanted = wanted[:budget]
    for c in candidates or []:
        name = str(c)
        if len(wanted) >= budget or name.casefold() in seen:
            continue
        seen.add(name.casefold())
        if _clean_name(name) and _addressable(name):
            wanted.append(name)
    batches = [[key_target, *wanted[i:i + READ_BATCH_COLUMNS]] for i in range(0, len(wanted), READ_BATCH_COLUMNS)]
    return batches or [[key_target]], unread


def read_destination(endpoint: Any, *, workspace_id: str, key: tuple[str, str], source_rows: list[dict],
                     mappings: list[dict[str, Any]], candidates: list[str] | None,
                     ) -> tuple[list[dict[str, Any]] | None, dict[str, str]]:
    """Batched keyed reads merged by key; a failed batch leaves its targets "not read"."""
    from services.mapping_witness import norm

    batches, unread = plan_read(mappings, key[1], candidates)
    keys = [r.get(key[0]) for r in source_rows]
    merged: dict[str, dict[str, Any]] = {}
    targets = {p["target"].casefold() for p in mapped_pairs(mappings)}
    read_any = False
    for batch in batches:
        rows: list[dict[str, Any]] | None = None
        try:
            rows = read_rows(endpoint, workspace_id=workspace_id, key_column=key[1], keys=keys, columns=batch)
        except Exception as exc:  # noqa: BLE001 - not silent: every target of the batch is reported "not read"
            logger.info("mapping witness: a destination read batch failed (%s)", type(exc).__name__)
            unread.update({col: "the destination could not be read for this column." for col in batch[1:]
                           if col.casefold() in targets})
        if rows is None:
            continue
        read_any = True
        for row in rows:
            k = norm(row.get(key[1]))
            if k is not None:
                merged.setdefault(k, {}).update(row)
    return (list(merged.values()) if read_any else None), unread


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
    """Gather both samples (bounded) and judge. Read problems become notes and "not read", never errors."""
    notes: list[str] = []
    rows = [r for r in (source_rows or []) if isinstance(r, dict)][:MAX_ROWS]
    if not rows and source_endpoint is not None and getattr(source_endpoint, "kind", "") == "database" and key:
        try:
            cols = [p["source"] for p in mapped_pairs(mappings) if _clean_name(p["source"])]
            rows = read_rows(source_endpoint, workspace_id=workspace_id, key_column=key[0], keys=None,
                             columns=[key[0], *[c for c in cols if c.casefold() != key[0].casefold()]][:64])
        except Exception as exc:  # noqa: BLE001
            logger.info("mapping witness: source sample unreadable (%s)", type(exc).__name__)
            notes.append("The source sample could not be read.")
    dest_rows: list[dict[str, Any]] | None = None
    unread: dict[str, str] = {}
    is_db = destination_endpoint is not None and getattr(destination_endpoint, "kind", "") == "database"
    if destination_table_exists is False:
        notes.append("The destination table does not exist yet, so only source values were checked.")
    elif is_db and not (key and key[0] and key[1]):
        notes.append("No key joins the sample to the destination, so only source values were checked.")
    elif is_db and trigger != TRIGGER_EXPLICIT and engine_of(destination_endpoint, workspace_id) in WAREHOUSES:
        notes.append("Reading a warehouse runs only when you choose Check mapping; source values were checked.")
    elif is_db and rows and key:
        dest_rows, unread = read_destination(destination_endpoint, workspace_id=workspace_id, key=key,
                                             source_rows=rows, mappings=mappings, candidates=candidate_columns)
        if dest_rows is None:
            notes.append("The destination could not be read, so only source values were checked.")
    digest = source_sample_digest(mappings, rows, key, workspace_sample_key(workspace_id)) if rows else ""
    report = witness_mappings(mappings, rows, dest_rows, key=key, date_locale=date_locale, unread=unread,
                              source_digest=digest)
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
