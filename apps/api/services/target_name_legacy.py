"""Existing SQL tables keep the column names earlier runs gave them (G11 compatibility).

``connectors.destination_naming`` stops renaming targets: SQL engines keep a
name up to their real limit instead of PostgreSQL's 63 and BigQuery keeps
case. A table an earlier run created still holds the old spelling, so a
target whose new spelling is not a column there, but whose legacy spelling
(sanitized, cut at 63, lowercased on BigQuery) is exactly one column, keeps
writing to that column. Validate shows an info note
(``g23d_existing_target_columns``). The rename is part of the canonical Map on
both sides: Validate judges and hashes the adopted rows (``run_file_preflight``)
and Execute retargets the mapping the same way before anything reads or
writes, so the approved Decision Artifact / Map->DDL identity, the writer,
Gate-8 and the read-back all name that column. A new table gets the full name. A name past the engine's limit is
refused only when no existing column matches.

MongoDB / Elasticsearch: a target whose old renamed spelling (``a.b`` ->
``a_b``, ``@timestamp`` -> ``_timestamp``, spaces, hyphens, accents, names
past 63) is already a field of the existing collection or index would leave
the old field stale beside the new one. Validate (``g23e``) and Execute
(``legacy_fields_refusal``, before any write, for every caller) refuse it with
both spellings and the remedies, unless the run overwrites or the operator
acknowledged ``legacy_field_policy=write_new`` (recorded on the run).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Mapping
from typing import Any

from connectors.destination_naming import (
    DOCUMENT_STORES,
    KEY_VALUE_STORES,
    SAAS_APPS,
    SEARCH_STORES,
    SQL_NAME_LIMITS,
    SQL_UNLIMITED,
    STREAM_SINKS,
    _NO_CUT,
    _as_dict,
    dest_kind,
)
from connectors.sql_identifiers import sanitize_identifier

logger = logging.getLogger(__name__)

#: Engines whose column names compare case-insensitively.
_CASE_INSENSITIVE = frozenset({"mysql", "sqlserver", "synapse", "sqlite", "bigquery", "snowflake"})
_LEGACY_CUT = 63


#: Key on an adopted Map row holding the operator's original target.
MAP_TARGET = "map_target"


def legacy_spelling(dest_type: str | None, name: Any) -> str:
    """The column name a writer produced for ``name`` before G11."""
    kind = dest_kind(dest_type)
    return sanitize_identifier(
        str(name or ""), preserve_case=kind != "bigquery", max_len=_LEGACY_CUT
    )


def _full_spelling(name: Any, kind: str = "") -> str:
    """The name without the old cut (what Validate measures against the limit).

    A SaaS app's field name / a Kafka field is kept exactly as mapped (G11)."""
    if kind in SAAS_APPS or kind in STREAM_SINKS:
        return str(name or "").strip()
    return sanitize_identifier(str(name or ""), preserve_case=True, max_len=_NO_CUT)


def _applies(kind: str) -> bool:
    """Destinations whose existing columns / fields a target may adopt.

    Kafka: a Schema Registry subject's fields, or ``key_format=legacy``
    (``services.kafka_field_names``)."""
    return (kind in SQL_NAME_LIMITS or kind in SQL_UNLIMITED or kind in SAAS_APPS
            or kind in STREAM_SINKS)


def existing_column_for(
    dest_type: str | None, target: str, existing: Iterable[str]
) -> str | None:
    """The existing legacy column ``target`` should keep writing to, or ``None``.

    ``None`` when the new spelling is itself a column (it is used as is), when
    the legacy spelling is the new one, or when no / several columns match it.
    """
    kind = dest_kind(dest_type)
    if not _applies(kind):
        return None
    cols = [str(c) for c in existing if str(c or "")]
    if not cols:
        return None
    fold = kind in _CASE_INSENSITIVE
    full = _full_spelling(target, kind)
    if full in cols:
        return None
    if fold and kind != "bigquery" and any(c.casefold() == full.casefold() for c in cols):
        return None  # same column, already matched by the engine
    legacy = legacy_spelling(kind, target)
    hits = [c for c in cols if c == legacy or (fold and c.casefold() == legacy.casefold())]
    if kind == "bigquery" and not hits:
        hits = [c for c in cols if c.casefold() == full.casefold()]
    if len(hits) != 1 or hits[0] == full:
        return None
    return hits[0]


def strip_client_map_targets(mappings: Any) -> Any:
    """Drop ``map_target`` from inbound Map rows (final review B1).

    Only ``adopt_for_request`` / ``adopt_existing_columns`` may set it; a row
    arriving with one would otherwise steer ``operator_rows`` and the
    approval fallback.
    """
    if not isinstance(mappings, list):
        return mappings
    return [
        {k: v for k, v in m.items() if k != MAP_TARGET}
        if isinstance(m, dict) and MAP_TARGET in m else m
        for m in mappings
    ]


def operator_rows(mappings: Iterable[Any] | None) -> list[dict[str, Any]]:
    """Map rows with any earlier adoption undone (the operator's own targets)."""
    rows = [dict(_as_dict(m)) for m in (mappings or [])]
    for row in rows:
        if row.get(MAP_TARGET):
            row["target"] = row.pop(MAP_TARGET)
    return rows


def recreated_by_overwrite(dest_type: str | None, sync_mode: str | None) -> bool:
    """True when this run drops and recreates the table (BigQuery, Snowflake ...).

    The earlier run's legacy column goes with the dropped table, so the full
    name is used, in single-stream and multi-stream alike (re-review L3).
    """
    kind = dest_kind(dest_type)
    if not _applies(kind) or kind in SAAS_APPS or kind in STREAM_SINKS:
        return False
    from services.db_type_utils import dest_schema_is_recreated_on_overwrite
    from services.sync_cursor import is_overwrite_sync

    return is_overwrite_sync(str(sync_mode or "")) and dest_schema_is_recreated_on_overwrite(kind)


def adopt_rows_for_run(
    dest_type: str | None, mappings: Iterable[Any] | None, existing: Iterable[str] | None,
    sync_mode: str | None, *, destination_config: Mapping[str, Any] | None = None,
    destination_table: str = "",
) -> list[dict[str, Any]]:
    """The Map rows a run writes: adopted onto existing columns unless it recreates the table.

    Kafka (no live columns at Validate): the registry subject's fields or
    ``key_format=legacy``, read the way Execute reads them.
    """
    if recreated_by_overwrite(dest_type, sync_mode):
        return operator_rows(mappings)
    existing = list(existing or [])
    if not existing and dest_kind(dest_type) in STREAM_SINKS and destination_config:
        try:
            existing = _kafka_fields(destination_config, None, destination_table, mappings)
        except Exception as exc:  # noqa: BLE001 - Validate's g23e reports it
            logger.info("Kafka field names unread at Validate: %s", exc)
    return adopt_existing_columns(dest_type, mappings, existing)[0]


def _kafka_naming(cfg: Any, endpoint: Any, topic: str, mappings: Any, run_scope: Any = None
                  ) -> dict[str, Any]:
    """``kafka_field_names.kafka_naming`` on the operator's rows, once per run and topic."""
    from services.kafka_field_names import kafka_naming, renamed_targets

    rows = operator_rows(mappings)
    key = (_run_key(run_scope, "kafka-naming", topic, tuple(renamed_targets(rows)))
           if run_scope is not None else None)
    hit = _INTROSPECT_MEMO.get(key) if key else None
    if hit and time.monotonic() - hit[0] < _PROBE_MEMO_SECONDS:
        return hit[1]
    naming = kafka_naming(cfg, endpoint, topic, rows, timeout=_probe_seconds())
    if key:
        _INTROSPECT_MEMO[key] = (time.monotonic(), naming)
    return naming


def _kafka_fields(cfg: Any, endpoint: Any, topic: str, mappings: Any, run_scope: Any = None
                  ) -> list[str]:
    """The fields a Kafka run's targets adopt (registry subject / old spellings)."""
    from services.kafka_field_names import adopted_fields

    return adopted_fields(_kafka_naming(cfg, endpoint, topic, mappings, run_scope))


def adopted_collision_issues(dest_type: str | None, rows: Iterable[Any]) -> list[dict[str, str]]:
    """Map rows that would all write one existing column after adoption.

    Judged over every final row - adopted and literal - so an adopted
    ``Name`` and a literal ``name`` on BigQuery, or an adopted 65-character
    MySQL target and a literal row on its 63-character column, are refused
    (re-review L1). Validate and Execute call this same function.
    """
    from services.mapping_constraints import write_mappings

    kind = dest_kind(dest_type)
    fold = kind in _CASE_INSENSITIVE
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in write_mappings([dict(_as_dict(r)) for r in rows or []]):
        target = str(row.get("target") or "").strip()
        if target:
            groups.setdefault(target.casefold() if fold else target, []).append(row)
    issues: list[dict[str, str]] = []
    for members in groups.values():
        if len(members) < 2 or not any(m.get(MAP_TARGET) for m in members):
            continue
        names = [str(m.get(MAP_TARGET) or m.get("target")) for m in members]
        column = str(members[0].get("target"))
        issues.append({
            "code": "target_name_collision",
            "target": names[-1],
            "other": names[0],
            "message": (
                "Map targets " + ", ".join(repr(n) for n in names)
                + f" would all write the existing column {column!r}. Rename one in Map."
            ),
        })
    return issues


def adopt_existing_columns(
    dest_type: str | None, mappings: Iterable[Any] | None, existing: Iterable[str] | None
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Mappings retargeted to the legacy columns an existing table holds, and notes."""
    rows = [dict(_as_dict(m)) for m in (mappings or [])]
    cols = list(existing or [])
    notes: list[dict[str, str]] = []
    if not cols or not _applies(dest_kind(dest_type)):
        return rows, notes
    rows = operator_rows(rows)  # re-resolve from the operator's spelling
    for row in rows:
        target = str(row.get("target") or "")
        if not target.strip():
            continue
        column = existing_column_for(dest_type, target, cols)
        if column is None:
            continue
        row["target"] = column
        # The Map's own spelling travels with the row, so a multi-stream run
        # can resolve it again against each stream's table.
        row[MAP_TARGET] = target
        notes.append({
            "code": "target_uses_existing_column",
            "target": target,
            "column": column,
            "message": (
                f"Map target {target!r} uses the existing column {column!r} created by "
                "an earlier run (Datawrap now keeps the full name only on new tables)."
            ),
        })
    return rows, notes


def _needs_probe(kind: str, mappings: list[dict[str, Any]]) -> bool:
    for m in mappings:
        target = str(m.get("target") or "")
        if target and legacy_spelling(kind, target) != _full_spelling(target, kind):
            return True
    return False


def _bounded_probe(dest: Any) -> dict[str, Any]:
    """The destination's introspection, under the engine's connect deadline."""
    import copy

    from services.connector_executor import get_executor
    from services.workspace_filter import current_workspace_stamp
    from src.ai.copilot.tool_deadline import _Progress, deadline_scope, interrupt_connections
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.engine_destination import _destination_connect_deadline

    probe = copy.deepcopy(dest)
    probe.extra = {**(probe.extra or {}), "introspect_purpose": "destination"}
    seconds = _destination_connect_deadline()
    progress = _Progress(limit=seconds)

    def _run() -> dict[str, Any]:
        with deadline_scope(seconds, progress):
            return dict(introspect_endpoint(probe) or {})

    return get_executor().run(
        "destination_connect",
        _run,
        workspace=current_workspace_stamp(for_write=False),
        seconds=seconds,
        describe_timeout=lambda: "existing-column probe timed out",
        label=lambda: "destination_connect: existing target columns",
        interrupt=lambda: interrupt_connections(progress),
    )


_INTROSPECT_MEMO: dict[tuple, tuple[float, dict[str, Any]]] = {}


#: Attribute on a run's destination endpoint naming that run, so the memo of
#: destination probes is shared by the run's own checks only (deepcopies of
#: the endpoint for each stream keep it); other runs and requests never hit it.
_RUN_TOKEN = "_g11_run_token"


def begin_run(dest: Any) -> None:
    """Mark ``dest`` as one run (idempotent)."""
    import uuid

    if dest is not None and not getattr(dest, _RUN_TOKEN, ""):
        try:
            setattr(dest, _RUN_TOKEN, uuid.uuid4().hex)
        except (AttributeError, TypeError):
            pass


def _run_key(dest: Any, *parts: Any) -> tuple | None:
    token = str(getattr(dest, _RUN_TOKEN, "") or "")
    return (token, *parts) if token else None


def _memo_probe(dest: Any) -> dict[str, Any]:
    """``_bounded_probe``, once per run and table: a multi-stream Execute checks
    each stream up front and again in its stream loop (re-review M1)."""
    key = _run_key(dest, *(str(getattr(dest, f, "") or "") for f in (
        "format", "table", "collection", "database", "schema")))
    if key is None:
        return _bounded_probe(dest)
    hit = _INTROSPECT_MEMO.get(key)
    if hit and time.monotonic() - hit[0] < _PROBE_MEMO_SECONDS:
        return hit[1]
    info = _bounded_probe(dest)
    _INTROSPECT_MEMO[key] = (time.monotonic(), info)
    return info


def adopted_collision_refusal(dest_type: str | None, rows: Iterable[Any]) -> str:
    """Execute's form of :func:`adopted_collision_issues` (``""`` when none)."""
    issues = adopted_collision_issues(dest_type, rows)
    return " ".join(i["message"] for i in issues[:3])


def adopt_for_request(request: Any) -> list[dict[str, str]]:
    """Execute: retarget ``request.mappings`` onto an existing table's legacy columns.

    Probes the destination (bounded by the connect deadline) only when some
    target's spelling changed (a long name, a mixed-case BigQuery target) and
    adoption could matter: a refusal adoption cannot lift (a case collision)
    is left to Execute's own check. A failed probe raises ``AdoptionUnverified``.
    """
    from connectors.destination_naming import _mapped_targets, target_name_issues

    dest = getattr(request, "destination", None)
    begin_run(dest)
    kind = dest_kind(str(getattr(dest, "format", "") or ""))
    mappings = [dict(_as_dict(m)) for m in (getattr(request, "mappings", None) or [])]
    if dest is None or not _applies(kind) or not _needs_probe(kind, mappings):
        return []
    if recreated_by_overwrite(kind, getattr(request, "sync_mode", "")):
        request.mappings = operator_rows(mappings)
        return []
    raw_issues = target_name_issues(kind, _mapped_targets(mappings))
    if any(i["code"] != "target_name_too_long" for i in raw_issues):
        return []
    try:
        if kind in STREAM_SINKS:
            from src.transfer.adapters import resolve_connector_config

            existing = _kafka_fields(
                resolve_connector_config(dest), dest,
                writer_object_name(dest, getattr(request, "source", None)), mappings, dest)
            info: dict[str, Any] = {}
        else:
            info = _memo_probe(dest)
            existing = list(dict(info.get("schema") or {}).keys())
    except Exception as exc:  # noqa: BLE001 - fail closed (re-review L2)
        # Adopting nothing would write the full name beside an earlier run's
        # column, or fail later as a misleading "DDL identity mismatch".
        raise AdoptionUnverified(
            f"Could not read the destination's existing columns ({exc}), so Datawrap "
            "cannot tell whether an earlier run created these columns under their old "
            "names. Check the destination connection and run again."
        ) from exc
    if info.get("table_exists") is False:
        return []
    rows, notes = adopt_existing_columns(kind, mappings, existing)
    if notes:
        request.mappings = rows
        try:  # the server's proof of this adoption (engine_identity.verified_identity_maps)
            request._g11_adoption = {"dest": kind, "live": list(existing)}
        except (AttributeError, TypeError):
            pass
        for note in notes:
            logger.info("G11: %s", note["message"])
    return notes


# -- multi-stream: each stream against its own destination object -----------


def selected_streams(request: Any) -> list[dict[str, Any]]:
    """Selected stream contracts of a multi-stream request (empty for one stream)."""
    contracts = [
        dict(_as_dict(c)) for c in (getattr(request, "stream_contracts", None) or [])
    ]
    picked = [c for c in contracts if c.get("selected", True) and str(c.get("name") or "")]
    # One stream writes the request's own object (stream_multi runs only for N > 1).
    return picked if len(picked) > 1 else []


def prepare_stream_targets(
    destination: Any,
    mappings: Iterable[Any] | None,
    sync_mode: str | None,
    *,
    stream_name: str = "",
) -> tuple[list[dict[str, Any]], list[str]]:
    """One stream's Map rows adopted onto its own table, and its write_new
    acknowledgement (nothing is stamped on the shared endpoint), or ``ValueError``.

    ``destination`` is already bound to the stream, or is bound here (on a
    copy) when ``stream_name`` is given. Applies the same rules as a
    single-stream Execute: legacy-column adoption, the name refusal, and the
    MongoDB / Elasticsearch old-field rule.
    """
    import copy
    from types import SimpleNamespace

    from connectors.destination_naming import target_names_refusal

    dest = destination
    if stream_name:
        from services.multi_stream_targets import bind_endpoint_to_stream

        dest = copy.deepcopy(destination)
        bind_endpoint_to_stream(dest, stream_name)
    rows = operator_rows(mappings)
    req = SimpleNamespace(destination=dest, mappings=rows, sync_mode=sync_mode)
    label = stream_name or str(getattr(dest, "table", "") or getattr(dest, "collection", ""))
    try:
        adopt_for_request(req)
    except AdoptionUnverified as exc:
        raise ValueError(f"Stream {label!r}: {exc}") from exc
    refusal = target_names_refusal(str(getattr(dest, "format", "") or ""), req.mappings) or (
        adopted_collision_refusal(getattr(dest, "format", ""), req.mappings))
    ack: list[str] = []
    if not refusal:
        refusal, ack = legacy_fields_check(req)
    if refusal:
        raise ValueError(f"Stream {label!r}: {refusal}")
    return list(req.mappings), ack


def stream_existing_columns(
    dest_type: str | None, cfg: Mapping[str, Any] | None, table: str, endpoint: Any = None
) -> list[str]:
    """The columns a stream's existing destination table holds ([] if new).

    Validate passes the probe config; Execute's own preflight passes the
    request's destination endpoint, bound here (on a copy) to the stream.
    """
    if not table or not _applies(dest_kind(dest_type)):
        return []
    if endpoint is not None and not isinstance(endpoint, Mapping):
        import copy

        from services.multi_stream_targets import bind_endpoint_to_stream

        endpoint = copy.deepcopy(endpoint)
        bind_endpoint_to_stream(endpoint, table)
    elif cfg:
        from src.transfer.models import EndpointConfig

        endpoint = EndpointConfig(
            kind="database", format=str(dest_type or ""), host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0), database=str(cfg.get("database") or ""),
            schema=str(cfg.get("schema") or ""), username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""), table=table,
        )
    else:
        return []
    try:
        info = _bounded_probe(endpoint)
    except Exception as exc:  # noqa: BLE001 - Execute re-checks each stream before writing
        logger.info("stream column probe skipped for %s: %s", table, exc)
        return []
    if info.get("table_exists") is False:
        return []
    return list(dict(info.get("schema") or {}).keys())


def stream_target_gates(
    dest_type: str | None,
    mappings: Iterable[Any] | None,
    contracts: Iterable[Mapping[str, Any]],
    *,
    destination_config: Mapping[str, Any] | None,
    sync_mode: str | None,
    legacy_field_policy: str = "",
    destination_endpoint: Any = None,
) -> list[dict[str, Any]]:
    """Validate's target-name gates for every stream of a multi-stream route.

    Each stream is judged against its own destination object (named after the
    stream) with its own Map rows; issues and notes carry the stream name.
    """
    merged: dict[str, dict[str, Any]] = {}
    for contract in contracts:
        name = str(contract.get("name") or "")
        own = contract.get("mappings")
        rows = own if isinstance(own, list) and own else list(mappings or [])
        gates = target_name_gates(
            dest_type, rows,
            existing_columns=stream_existing_columns(
                dest_type, destination_config, name, destination_endpoint),
            destination_config=destination_config, destination_table=name,
            sync_mode=str(contract.get("sync_mode") or sync_mode or ""),
            legacy_field_policy=legacy_field_policy,
            destination_endpoint=destination_endpoint,
        )
        for gate in gates:
            into = merged.setdefault(gate["id"], {**gate, "message": "", "details": {
                **gate["details"], "issues": [], "notes": []}})
            into["message"] = (into["message"] + " " if into["message"] else "") + (
                f"Stream {name!r}: {gate['message']}")
            if gate["status"] == "block":
                into["status"] = "block"
            for key in ("issues", "notes"):
                into["details"][key].extend(
                    {**item, "stream": name} for item in gate["details"].get(key) or [])
    return list(merged.values())


def streams_refusal(request: Any) -> str:
    """Execute, before any write: every selected stream against its own object."""
    mappings = list(getattr(request, "mappings", None) or [])
    for contract in selected_streams(request):
        own = contract.get("mappings")
        try:
            prepare_stream_targets(
                request.destination,
                own if isinstance(own, list) and own else mappings,
                str(contract.get("sync_mode") or getattr(request, "sync_mode", "") or ""),
                stream_name=str(contract["name"]),
            )
        except ValueError as exc:
            return str(exc)
    return ""


# -- document / search stores: the old renamed field ------------------------

#: Operator acknowledgement (destination option): write the new field even
#: though the existing collection / index holds the old renamed one.
LEGACY_FIELD_POLICY = "legacy_field_policy"
WRITE_NEW = "write_new"
LEGACY_FIELD_ACK = "legacy_field_ack"


def old_field_spellings(target: str) -> list[str]:
    """Names writers before G11 gave ``target`` in a document/search store."""
    from connectors.sql_identifiers import legacy_sanitized_identifier

    out: list[str] = []
    for old in (
        sanitize_identifier(target, preserve_case=True),
        legacy_sanitized_identifier(target, preserve_case=True),
    ):
        if old and old != target and old not in out:
            out.append(old)
    return out


def _renamed_pairs(mappings: Iterable[Any] | None) -> list[tuple[str, str]]:
    from connectors.destination_naming import _mapped_targets

    return [(t, old) for t in _mapped_targets(mappings) for old in old_field_spellings(t)]


#: Stores holding named fields per record, where an old renamed field can sit
#: stale beside the new one.
_FIELD_STORES = DOCUMENT_STORES | SEARCH_STORES | KEY_VALUE_STORES
_SAMPLE_RECORDS = 200
_REDIS_SCAN_CALLS = 50


class AdoptionUnverified(RuntimeError):
    """The destination's existing columns could not be read for adoption."""


class LegacyFieldsUnverified(RuntimeError):
    """The old-field check could not finish within its budget."""


def _probe_seconds() -> float:
    from services.brand_env import getenv_brand

    try:
        return max(0.1, float(getenv_brand("LEGACY_FIELD_PROBE_SECONDS", "5") or 5))
    except ValueError:
        return 5.0


#: Short-lived memo of old-field probes: Execute's up-front multi-stream check
#: and the stream loop ask the same question seconds apart (re-review M1).
_PROBE_MEMO: dict[tuple, tuple[float, list[tuple[str, str]]]] = {}
_PROBE_MEMO_SECONDS = 30.0


def mongo_fields_present(cfg: Mapping[str, Any], collection: str, names: Iterable[str]) -> set[str]:
    """Which of ``names`` at least one stored document holds (``$exists``)."""
    from connectors.mongodb_common import _mongo_client, normalize_mongodb_connection_string

    conn = normalize_mongodb_connection_string(
        str(cfg.get("connection_string") or ""),
        database=str(cfg.get("database") or ""),
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 27017),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        ssl=bool(cfg.get("ssl", False)),
        auth_source=str(cfg.get("auth_source") or ""),
    )
    coll = _mongo_client(conn)[str(cfg.get("database") or "test")][
        sanitize_identifier(collection, preserve_case=True)
    ]
    # One bounded query per collection (re-review M1): a single $or over every
    # old name, a projection of just those names, a document cap and a server
    # time limit. Per-name find_one scanned the whole collection once per target.
    wanted = list(dict.fromkeys(names))
    found: set[str] = set()
    from pymongo.errors import ExecutionTimeout

    try:
        cursor = (
            coll.find({"$or": [{n: {"$exists": True}} for n in wanted]}, {n: 1 for n in wanted})
            .limit(_SAMPLE_RECORDS)
            .max_time_ms(int(_probe_seconds() * 1000))
        )
        for doc in cursor:
            found.update(n for n in wanted if n in doc)
            if len(found) == len(wanted):
                break
    except ExecutionTimeout as exc:
        raise LegacyFieldsUnverified(
            f"MongoDB did not answer the old-field check within {_probe_seconds():g}s"
        ) from exc
    return found




def redis_fields_present(cfg: Mapping[str, Any], prefix: str, names: Iterable[str]) -> set[str]:
    """Which of ``names`` a sample of the prefix's stored documents holds."""
    from itertools import islice

    from connectors.redis_reader import _redis_client, keys_for_prefix, redis_json_row

    client = _redis_client(dict(cfg))
    wanted = set(names)
    # A SCAN budget (re-review M1): scan_iter walked the whole keyspace when the
    # prefix held fewer than the sample size.
    raw: list[Any] = []
    cursor, calls = 0, 0
    deadline = time.monotonic() + _probe_seconds()
    while True:
        cursor, batch = client.scan(cursor=cursor, match=f"{prefix}:*", count=1000)
        raw.extend(batch)
        calls += 1
        if not cursor or len(raw) >= _SAMPLE_RECORDS:
            break
        if calls >= _REDIS_SCAN_CALLS or time.monotonic() > deadline:
            if not raw:
                raise LegacyFieldsUnverified(
                    f"Redis SCAN found no {prefix}:* key within its budget "
                    f"({calls} calls, {_probe_seconds():g}s)"
                )
            break
    keys = keys_for_prefix(
        [k.decode() if isinstance(k, bytes) else str(k) for k in islice(raw, _SAMPLE_RECORDS)],
        prefix,
    )
    found: set[str] = set()
    for key in keys:
        found.update(wanted & set(redis_json_row(client.get(key))))
        if found == wanted:
            break
    return found


def dynamodb_fields_present(cfg: Mapping[str, Any], table: str, names: Iterable[str]) -> set[str]:
    """Which of ``names`` a sample of the table's items holds."""
    from connectors.aws_common import boto3_client
    from connectors.dynamodb_writer import dynamodb_client_cfg

    client = boto3_client("dynamodb", dynamodb_client_cfg(dict(cfg)))
    wanted = list(dict.fromkeys(names))
    try:
        page = client.scan(
            TableName=table, Limit=_SAMPLE_RECORDS,
            ProjectionExpression=", ".join(f"#n{i}" for i in range(len(wanted))),
            ExpressionAttributeNames={f"#n{i}": n for i, n in enumerate(wanted)},
        )
    except Exception as exc:  # noqa: BLE001 - a missing table holds no old fields
        if "ResourceNotFoundException" in type(exc).__name__ + str(exc):
            return set()
        raise
    found: set[str] = set()
    for item in page.get("Items") or []:
        found.update(set(item) & set(wanted))
    return found


def es_mapping_fields(cfg: Mapping[str, Any], index: str) -> set[str]:
    """Dotted field paths an existing Elasticsearch index mapping declares."""
    from connectors.elasticsearch_reader import _client

    raw = _client(dict(cfg)).indices.get_mapping(index=index)
    body = raw.body if hasattr(raw, "body") else raw
    fields: set[str] = set()

    def walk(props: Mapping[str, Any], prefix: str) -> None:
        for name, spec in (props or {}).items():
            path = f"{prefix}.{name}" if prefix else str(name)
            fields.add(path)
            if isinstance(spec, Mapping) and isinstance(spec.get("properties"), Mapping):
                walk(spec["properties"], path)

    for entry in (body or {}).values():
        walk(((entry or {}).get("mappings") or {}).get("properties") or {}, "")
    return fields


def existing_legacy_fields(
    dest_type: str | None, cfg: Mapping[str, Any] | None, table: str, mappings: Iterable[Any] | None,
    run_scope: Any = None,
) -> list[tuple[str, str]]:
    """``(target, old field)`` pairs whose old field the destination already holds.

    MongoDB: any stored document holding the old name (``$exists``);
    Elasticsearch: the index mapping declares it. Empty for a new or empty
    collection / index, or when nothing was renamed.
    """
    kind = dest_kind(dest_type)
    if kind not in _FIELD_STORES:
        return []
    pairs = _renamed_pairs(mappings)
    if not pairs or not cfg or not table:
        return []
    olds = [old for _t, old in pairs]
    key = _run_key(run_scope, kind, table, tuple(sorted(set(olds)))) if run_scope else None
    hit = _PROBE_MEMO.get(key) if key else None
    if hit and time.monotonic() - hit[0] < _PROBE_MEMO_SECONDS:
        return [p for p in hit[1] if p in pairs]
    if kind in DOCUMENT_STORES:
        present = mongo_fields_present(cfg, table, olds)
    elif kind == "redis":
        present = redis_fields_present(cfg, table, olds)
    elif kind == "dynamodb":
        present = dynamodb_fields_present(cfg, table, olds)
    else:
        present = es_mapping_fields(cfg, table)
    found = [(t, old) for t, old in pairs if old in present]
    if key:
        _PROBE_MEMO[key] = (time.monotonic(), found)
    return found


def legacy_field_issues(
    dest_type: str | None, found: list[tuple[str, str]], *, blocking: bool
) -> list[dict[str, str]]:
    """``target_path_legacy_flat_field`` per target whose old field exists."""
    kind = dest_kind(dest_type)
    store = {"mongodb": "collection", "redis": "key prefix", "dynamodb": "table"}.get(
        kind, "index")
    out: list[dict[str, str]] = []
    for target, old in found:
        rename = {
            "mongodb": f"updateMany({{}}, {{$rename: {{{old!r}: {target!r}}}}})",
            "redis": "a rewrite of the stored documents under the new field name",
            "dynamodb": "an UpdateItem pass that renames the attribute",
        }.get(kind, "a reindex that renames the field")
        ack = (
            f" To write {target!r} anyway, set {LEGACY_FIELD_POLICY}={WRITE_NEW!r} on the "
            "destination; the run records it."
            if blocking else ""
        )
        out.append({
            "code": "target_path_legacy_flat_field",
            "target": target,
            "other": old,
            "message": (
                f"The existing {store} already holds {old!r}, the name earlier runs "
                f"wrote for {target!r}; this run writes {target!r}, so the {store} "
                f"would mix both shapes and {old!r} would go stale. Rename the Map "
                f"target to {old!r} to keep the old layout, run one overwrite, or "
                f"migrate first with {rename}.{ack}"
            ),
        })
    return out


def _policy_of(extra: Mapping[str, Any] | None) -> str:
    extra = extra or {}
    nested = extra.get("extra") if isinstance(extra.get("extra"), Mapping) else {}
    return str(extra.get(LEGACY_FIELD_POLICY) or nested.get(LEGACY_FIELD_POLICY) or "").strip()


def legacy_field_policy_of(endpoint: Any) -> str:
    """``legacy_field_policy`` an endpoint (dict or EndpointConfig) carries."""
    if isinstance(endpoint, Mapping):
        return _policy_of(endpoint)
    return _policy_of(getattr(endpoint, "extra", None) or {})


def legacy_field_verdict(
    dest_type: str | None,
    mappings: Iterable[Any] | None,
    cfg: Mapping[str, Any] | None,
    table: str,
    *,
    sync_mode: str | None,
    policy: str = "",
    run_scope: Any = None,
) -> tuple[list[dict[str, str]], bool]:
    """The one rule Validate and Execute share: ``(issues, blocking)``.

    Blocking unless the run overwrites (the collection / index is replaced,
    nothing mixes) or the operator set ``legacy_field_policy=write_new``.
    """
    from services.sync_cursor import is_overwrite_sync

    found = existing_legacy_fields(dest_type, cfg, table, mappings, run_scope)
    blocking = bool(found) and not is_overwrite_sync(str(sync_mode or "")) and policy != WRITE_NEW
    return legacy_field_issues(dest_type, found, blocking=blocking), blocking


def writer_object_name(destination: Any, source: Any = None) -> str:
    """The collection / index / key prefix / table the writer writes.

    ``resolve_dest_table`` with the stream's own fallback (the source object,
    ``import`` for a file): a destination that names its object only through
    ``database`` (an Elasticsearch index) or not at all (``dt_<source>``) is
    probed where the writer will write, never skipped.
    """
    if destination is None:
        return ""
    from types import SimpleNamespace

    def _endpoint(ep: Any) -> Any:
        if not isinstance(ep, Mapping):
            return ep
        return SimpleNamespace(**{k: str(ep.get(k) or "") for k in (
            "kind", "format", "table", "collection", "database", "schema")})

    from src.transfer.adapters import resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type

    dest, src = _endpoint(destination), _endpoint(source)
    fallback = "import"
    if src is not None and str(getattr(src, "kind", "") or "") != "file":
        from src.transfer.stream import _source_name

        fallback = _source_name(src) if getattr(src, "format", "") else str(
            getattr(src, "table", "") or getattr(src, "collection", "") or "")
    return str(resolve_dest_table(
        resolve_driver_type(str(getattr(dest, "format", "") or "")), dest, fallback) or "")


def legacy_fields_check(request: Any) -> tuple[str, list[str]]:
    """``(refusal, acknowledgement)`` for one request; nothing is mutated.

    The refusal is ``""`` when the run may write. The acknowledgement lists,
    for an operator's ``legacy_field_policy=write_new``, what the run keeps as
    stored; the caller records it on the run (or on one stream of it).
    """
    dest = getattr(request, "destination", None)
    kind = dest_kind(str(getattr(dest, "format", "") or ""))
    if dest is not None and kind in STREAM_SINKS:
        return _kafka_check(request)
    if dest is None or kind not in _FIELD_STORES:
        return "", []
    mappings = list(getattr(request, "mappings", None) or [])
    if not _renamed_pairs(mappings):
        return "", []
    policy = _policy_of(dict(getattr(dest, "extra", None) or {}))
    from services.sync_cursor import is_overwrite_sync

    sync_mode = str(getattr(request, "sync_mode", "") or "")
    if is_overwrite_sync(sync_mode):
        return "", []  # the collection / index is replaced: nothing can mix
    try:
        from src.transfer.adapters import resolve_connector_config

        cfg = resolve_connector_config(dest)
        table = writer_object_name(dest, getattr(request, "source", None))
        issues, blocking = legacy_field_verdict(
            kind, mappings, cfg, table, sync_mode=sync_mode, policy=policy, run_scope=dest,
        )
    except Exception as exc:  # noqa: BLE001 - fail closed: an unchecked store is not "no old fields"
        if policy == WRITE_NEW:
            return "", [f"old field names not verified ({exc}); "
                        f"{LEGACY_FIELD_POLICY}={WRITE_NEW}"]
        return (
            f"Could not verify the destination's old field names ({exc}). Set "
            f"{LEGACY_FIELD_POLICY}={WRITE_NEW!r} on the destination to write the new "
            "names anyway (recorded on the run), or run an overwrite.", [])
    if blocking:
        return "Destination holds the old field names: " + " ".join(
            i["message"] for i in issues[:3]), []
    if issues and policy == WRITE_NEW:
        return "", [
            f"{i['other']} kept as stored; this run writes {i['target']} "
            f"({LEGACY_FIELD_POLICY}={WRITE_NEW})"
            for i in issues
        ]
    return "", []


def _kafka_check(request: Any) -> tuple[str, list[str]]:
    """Execute: the Kafka naming decision's run evidence (never a refusal once adopted).

    The upgrade default (``key_format=legacy`` on a topic with earlier runs)
    and exact names chosen on such a topic are recorded on the run; history
    that cannot be read with no ``key_format`` set refuses it.
    """
    from services.kafka_field_names import KafkaHistoryUnverified, kafka_evidence

    dest = request.destination
    mappings = list(getattr(request, "mappings", None) or [])
    try:
        from src.transfer.adapters import resolve_connector_config

        topic = writer_object_name(dest, getattr(request, "source", None))
        naming = _kafka_naming(resolve_connector_config(dest), dest, topic, mappings, dest)
    except KafkaHistoryUnverified as exc:
        return str(exc), []
    except Exception as exc:  # noqa: BLE001 - fail closed: the registry could not be read
        return f"Could not read the topic's field names ({exc}).", []
    return "", kafka_evidence(naming, topic)[1]


def set_run_ack(dest: Any, ack: list[str]) -> None:
    """Stamp (or clear) the acknowledgement the writer records for this run / stream.

    Cleared when empty, so one stream's acknowledgement never carries over to
    the next stream on the shared destination endpoint.
    """
    if dest is None:
        return
    extra = getattr(dest, "extra", None)
    if not isinstance(extra, dict):
        if ack:
            dest.extra = {LEGACY_FIELD_ACK: list(ack)}
        return
    # In place: callers patch and restore the same extra dict around a stream.
    if ack:
        extra[LEGACY_FIELD_ACK] = list(ack)
    else:
        extra.pop(LEGACY_FIELD_ACK, None)


def legacy_fields_refusal(request: Any) -> str:
    """Execute: refuse to write a new field beside its stale old spelling.

    Runs before any write, for every caller (Studio, API, schedules, CDC). An
    acknowledged run (``legacy_field_policy=write_new``) proceeds; the
    acknowledgement is stamped on the destination (``legacy_field_ack``) and
    the writer records it in the run's warnings.
    """
    refusal, ack = legacy_fields_check(request)
    if not refusal:
        set_run_ack(getattr(request, "destination", None), ack)
    return refusal


#: Accepted ``legacy_field_policy`` values ("" = not set: refuse old fields).
LEGACY_FIELD_POLICIES = frozenset({"", WRITE_NEW})


def legacy_field_policy_refusal(endpoint: Any) -> str:
    """Why an endpoint's ``legacy_field_policy`` is not a known value, or ``""``."""
    value = legacy_field_policy_of(endpoint)
    if value in LEGACY_FIELD_POLICIES:
        return ""
    return (f"Unknown {LEGACY_FIELD_POLICY} {value!r}: use {WRITE_NEW!r} "
            "(or leave it unset to refuse a run beside old field names).")


def target_name_gates(
    dest_type: str | None,
    mappings: Iterable[Any] | None,
    *,
    existing_columns: Iterable[str] | None = None,
    destination_config: Mapping[str, Any] | None = None,
    destination_table: str = "",
    sync_mode: str | None = None,
    legacy_field_policy: str = "",
    destination_endpoint: Any = None,
) -> list[dict[str, Any]]:
    """Validate's target-name gates.

    ``g23c_target_names`` (block) holds names the destination refuses, judged
    after targets adopt an existing table's legacy columns;
    ``g23d_existing_target_columns`` (info) lists those adoptions;
    ``g23e_target_legacy_fields`` lists MongoDB / Elasticsearch targets whose
    old renamed spelling the existing collection or index holds: it blocks
    unless the run overwrites or the operator set ``legacy_field_policy``
    to ``write_new`` (the same rule Execute applies, ``legacy_fields_refusal``).
    """
    from connectors.destination_naming import _mapped_targets, target_name_issues

    kind = dest_kind(dest_type)
    unread: Exception | None = None
    naming: dict[str, Any] = {}
    if kind in STREAM_SINKS and not list(existing_columns or []):
        try:  # the topic's Schema Registry subject / key_format / its earlier runs
            from services.kafka_field_names import adopted_fields

            naming = _kafka_naming(
                destination_config, destination_endpoint, destination_table, mappings)
            existing_columns = adopted_fields(naming)
        except Exception as exc:  # noqa: BLE001 - reported in g23e, never "no fields"
            unread = exc
    rows, notes = adopt_existing_columns(dest_type, mappings, existing_columns)
    if naming.get("default"):
        from services.kafka_field_names import default_legacy_note

        # One note for the upgrade default, naming the exact-name option.
        notes = [{"code": "kafka_key_format_legacy_default", "target": "", "column": "",
                  "message": default_legacy_note(naming, destination_table)}] + notes
    if recreated_by_overwrite(dest_type, sync_mode):
        rows = operator_rows(rows)  # the table is recreated: full names (re-review L3)
        notes = [{**n, "code": "target_replaces_existing_column", "message": (
            f"Map target {n['target']!r} replaces the existing column {n['column']!r} an "
            "earlier run created: this overwrite recreates the table with the full name."
        )} for n in notes]
    issues = target_name_issues(dest_type, _mapped_targets(rows))
    issues.extend(adopted_collision_issues(dest_type, rows))
    gates: list[dict[str, Any]] = []
    if issues:
        head = "; ".join(i["message"] for i in issues[:3])
        more = f" (and {len(issues) - 3} more)" if len(issues) > 3 else ""
        gates.append({
            "id": "g23c_target_names",
            "status": "block",
            "message": f"Destination field names refused: {head}{more}",
            "duration_ms": 0,
            "details": {"dest_type": kind, "issues": issues},
        })
    if notes:
        gates.append({
            "id": "g23d_existing_target_columns",
            "status": "pass",
            "severity": "info",
            "message": " ".join(n["message"] for n in notes[:3]),
            "duration_ms": 0,
            "details": {"dest_type": kind, "notes": notes},
        })
    if legacy_field_policy not in LEGACY_FIELD_POLICIES:
        gates.append({
            "id": "g23e_target_legacy_fields", "status": "block", "duration_ms": 0,
            "message": legacy_field_policy_refusal({LEGACY_FIELD_POLICY: legacy_field_policy}),
            "details": {"dest_type": kind, "issues": [{
                "code": "legacy_field_policy_unknown", "target": "",
                "message": legacy_field_policy_refusal(
                    {LEGACY_FIELD_POLICY: legacy_field_policy})}]},
        })
        return gates
    try:
        if unread is not None:
            raise unread
        if kind in STREAM_SINKS:
            from services.kafka_field_names import kafka_evidence

            warnings, blocking = kafka_evidence(naming, destination_table)[0], False
        else:
            warnings, blocking = legacy_field_verdict(
                dest_type, mappings, destination_config, destination_table,
                sync_mode=sync_mode, policy=legacy_field_policy,
            )
    except Exception as exc:  # noqa: BLE001 - reported, never read as "no old fields"
        from services.sync_cursor import is_overwrite_sync

        blocking = not is_overwrite_sync(str(sync_mode or "")) and legacy_field_policy != WRITE_NEW
        warnings = [{
            "code": "target_path_legacy_fields_unchecked",
            "target": "",
            "message": (
                f"Could not verify the destination's old field names ({exc}). Set "
                f"{LEGACY_FIELD_POLICY}={WRITE_NEW!r} to write the new names anyway, or "
                "run an overwrite."
            ),
        }]
    if warnings:
        gates.append({
            "id": "g23e_target_legacy_fields",
            "status": "block" if blocking else "warn",
            "message": " ".join(w["message"] for w in warnings[:3]),
            "duration_ms": 0,
            "details": {"dest_type": kind, "issues": warnings,
                        LEGACY_FIELD_POLICY: legacy_field_policy or None},
        })
    return gates


__all__ = [
    "AdoptionUnverified",
    "MAP_TARGET",
    "LegacyFieldsUnverified",
    "operator_rows",
    "strip_client_map_targets",
    "prepare_stream_targets",
    "selected_streams",
    "stream_existing_columns",
    "stream_target_gates",
    "streams_refusal",
    "adopt_existing_columns",
    "adopt_for_request",
    "adopt_rows_for_run",
    "writer_object_name",
    "recreated_by_overwrite",
    "adopted_collision_issues",
    "existing_column_for",
    "es_mapping_fields",
    "existing_legacy_fields",
    "legacy_field_issues",
    "legacy_field_policy_of",
    "legacy_field_verdict",
    "legacy_field_policy_refusal",
    "legacy_fields_check",
    "legacy_fields_refusal",
    "set_run_ack",
    "legacy_spelling",
    "dynamodb_fields_present",
    "mongo_fields_present",
    "redis_fields_present",
    "old_field_spellings",
    "target_name_gates",
]
