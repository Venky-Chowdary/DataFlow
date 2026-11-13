"""Per-transfer duplicate policy.

Three outcomes, one owner:

* ``upsert`` — MERGE / ON CONFLICT DO UPDATE. Default when a business key
  exists and the operator did not ask for duplicates.
* ``dedupe_skip`` — insert-if-not-exists (ON CONFLICT DO NOTHING /
  WHEN NOT MATCHED THEN INSERT). Existing rows stay.
* ``append`` — plain INSERT, duplicates allowed. Only when the operator
  sets ``duplicate_policy=append`` or ``allow_duplicates``.

``full_refresh_append`` stays an insert: two runs land 2N. A schedule that
sets ``duplicate_policy=upsert`` (the schedule default when a key exists)
MERGEs instead. ``skip_duplicates`` inserts only new keys.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any

_MATCH = contextvars.ContextVar("df_duplicate_match_action", default="update")
_ORDER = contextvars.ContextVar("df_duplicate_order_column", default="")
# True while a write runs under an explicit append policy (duplicate_policy=append
# or allow_duplicates): repeated keys are the operator's choice, so the writer's
# CREATE must not turn the declared key into a unique constraint.
_ROWS_ALLOWED = contextvars.ContextVar("df_duplicate_rows_allowed", default=False)

# incremental_append is cursor-bounded: only rows after the watermark are
# written. Rewriting it to a full-file upsert reloaded the history (the
# file incremental tests). full_refresh_append / append with a key is the
# path that turned 25 rows into 50.
_APPEND_SYNC = frozenset(
    {
        "full_refresh_append",
        "append",
    }
)


class DuplicatePolicyError(ValueError):
    """The requested policy cannot be applied on this route."""


@dataclass(frozen=True)
class DuplicateDecision:
    policy: str
    write_mode: str
    match_action: str
    explicit: bool
    reason: str


def validate_schedule_file_policy(
    duplicate_policy: str | None,
    in_file_dedupe: str | None = "block",
    *,
    primary_key: str = "",
) -> tuple[str, str]:
    """Reject an unknown schedule policy. Empty policy stays empty.

    Upsert and skip-duplicates need a key. Append is the explicit opt-in
    that keeps duplicate rows. In-file dedupe is ``block`` or ``keep_last``.
    """
    policy = normalize_duplicate_policy(duplicate_policy)
    if policy and policy not in {"upsert", "dedupe_skip", "append"}:
        raise DuplicatePolicyError(
            f"Unknown duplicate_policy {duplicate_policy!r}. "
            "Use upsert, skip_duplicates, or append."
        )
    dedupe = (in_file_dedupe or "block").strip().lower().replace("-", "_")
    if dedupe in {"last", "last_wins"}:
        dedupe = "keep_last"
    if dedupe not in {"block", "keep_last"}:
        raise DuplicatePolicyError(
            f"Unknown in_file_dedupe {in_file_dedupe!r}. Use block or keep_last."
        )
    if policy in {"upsert", "dedupe_skip"} and not str(primary_key or "").strip():
        raise DuplicatePolicyError(
            "Upsert and skip_duplicates need a primary key. "
            "Declare one, or choose append to allow duplicate rows."
        )
    return policy, dedupe


def route_duplicate_options(*extras: Any) -> dict[str, str]:
    """``in_file_dedupe`` / ``duplicate_policy`` the route chose, from the first
    extra (source, then destination) that names each.

    Validate reads them through ``preflight_file_plan`` and the file writer
    reads ``destination.extra``; the engine stamps this answer on the
    destination so both see one choice. A ``keep_last`` set on the source alone
    used to clear Validate while the writer loaded every copy.
    """
    from services.preflight_file_plan import _plan_duplicate_policy, _plan_in_file_dedupe

    configs = [e for e in extras if isinstance(e, dict)]
    out: dict[str, str] = {}
    dedupe = _plan_in_file_dedupe(*configs)
    if dedupe:
        out["in_file_dedupe"] = dedupe
    policy = _plan_duplicate_policy(*configs)
    if policy:
        out["duplicate_policy"] = policy
    return out


def request_duplicate_option(request: Any) -> dict[str, str]:
    """The request-level ``duplicate_policy`` / ``allow_duplicates`` as a route extra."""
    policy = normalize_duplicate_policy(str(getattr(request, "duplicate_policy", "") or ""))
    if not policy and bool(getattr(request, "allow_duplicates", False)):
        policy = "append"
    return {"duplicate_policy": policy} if policy else {}


def route_keyed_duplicate_policy(request: Any) -> bool:
    """The route chose ``dedupe_skip`` / ``upsert``: a stored key resolves on the key."""
    extra = getattr(getattr(request, "destination", None), "extra", None) or {}
    policy = normalize_duplicate_policy(
        str(extra.get("duplicate_policy") or getattr(request, "duplicate_policy", "") or "")
    )
    return policy in {"dedupe_skip", "upsert"}


def summary_keyed_duplicate_policy(summary: Any) -> bool:
    """The write resolved stored keys by the route's dedupe_skip / upsert choice."""
    raw = (summary or {}).get("duplicate_policy") if isinstance(summary, dict) else ""
    return normalize_duplicate_policy(str(raw or "")) in {"dedupe_skip", "upsert"}


def keyed_policy_key_columns(
    *,
    contract_primary_key: str | None,
    destination_pk_columns: list[str] | None,
    mappings: list[dict] | None,
) -> list[str]:
    """Destination columns a dedupe_skip / upsert append matches stored rows on.

    The engine's order: the stream contract's declared key (source names, mapped
    to their targets), else the key the destination table itself declares. A
    name-inferred or whole-row identity is not a key the writer can match on.
    """
    targets = {
        str(m.get("source") or "").strip().casefold(): str(m.get("target") or "").strip()
        for m in (mappings or [])
        if isinstance(m, dict) and m.get("source") and m.get("target")
    }
    declared = [
        part.strip()
        for part in str(contract_primary_key or "").replace(";", ",").split(",")
        if part.strip()
    ]
    if declared:
        mapped = [targets.get(col.casefold(), "") for col in declared]
        return mapped if all(mapped) else []
    mapped_targets = set(targets.values())
    dest_keys = [str(c).strip() for c in (destination_pk_columns or []) if str(c).strip()]
    return dest_keys if dest_keys and all(c in mapped_targets for c in dest_keys) else []


SKIPPED_IDENTITY_KEYS_CAP = 100_000


def merge_skipped_identity_keys(prev: Any, incoming: Any) -> dict[str, Any]:
    """Every batch's kept keys (dedupe_skip), not only the last batch's.

    Past the cap the list is marked truncated: reconcile then cannot exclude
    every kept key and grades the run on what it has (fails closed).
    """
    prev = prev if isinstance(prev, dict) else {}
    incoming = incoming if isinstance(incoming, dict) else {}
    keys = [*(prev.get("skipped_identity_keys") or []), *(incoming.get("skipped_identity_keys") or [])]
    if not keys:
        return {}
    out: dict[str, Any] = {"skipped_identity_keys": [str(k) for k in keys[:SKIPPED_IDENTITY_KEYS_CAP]]}
    if len(keys) > SKIPPED_IDENTITY_KEYS_CAP or prev.get("skipped_identity_keys_truncated"):
        out["skipped_identity_keys_truncated"] = True
    return out


def rows_without_kept_keys(
    rows: list, target_cols: list[str], key_cols: list[str] | None, kept: Any
) -> list:
    """Drop rows whose key a dedupe_skip write left stored (writer key markers)."""
    kept_set = kept if isinstance(kept, (set, frozenset)) else {str(k) for k in (kept or [])}
    keys = list(key_cols or [])
    if not kept_set or not keys or any(c not in target_cols for c in keys):
        return rows
    indexes = [target_cols.index(c) for c in keys]
    out = []
    for row in rows:
        if isinstance(row, dict):
            values = [row.get(c) for c in keys]
        else:
            values = [row[i] if i < len(row) else None for i in indexes]
        parts = ["" if v is None else str(v).strip() for v in values]
        marker = parts[0] if len(parts) == 1 else "\x1f".join(parts)
        if marker not in kept_set:
            out.append(row)
    return out


def with_duplicate_route_option(probe_cfg: Any, *extras: Any) -> Any:
    """Destination probe config carrying the route's duplicate choice.

    Validate reads the plan's duplicate policy from this config, so the choice
    Studio/MCP sent in the destination options must ride on it, exactly as the
    engine stamps it before Execute's own preflight.
    """
    if not probe_cfg:
        return probe_cfg or None
    options = route_duplicate_options(*[dict(e) for e in extras if isinstance(e, dict)])
    if not options:
        return probe_cfg
    from services.secret_config import RedactedConfig

    merged = {**dict(probe_cfg), **options}
    return RedactedConfig(merged) if isinstance(probe_cfg, RedactedConfig) else merged


def keep_last_reads_whole(request: Any) -> bool:
    """A record-batch source (file, S3/GCS/ADLS/SFTP object) asked for keep_last.

    Object-store sources are ``kind=database`` and record-batch
    (``preflight_source_kind.source_is_record_batch``), so Validate accepts
    keep_last for them; the table stream does not collapse, so such a run
    reads the batch whole on the buffered route, where
    :func:`keep_last_records` collapses it (QA C2 review 3).
    """
    from services.preflight_source_kind import source_is_record_batch

    source = getattr(request, "source", None)
    if not source_is_record_batch(
        str(getattr(source, "kind", "") or ""), str(getattr(source, "format", "") or "")
    ):
        return False
    if multi_stream_keep_last_refusal(
        str(getattr(source, "format", "") or ""), getattr(request, "stream_contracts", None)
    ):
        return False  # the multi-stream runner refuses it (fail closed)
    options = route_duplicate_options(
        dict(getattr(source, "extra", None) or {}),
        dict(getattr(getattr(request, "destination", None), "extra", None) or {}),
    )
    return options.get("in_file_dedupe") == "keep_last"


def multi_stream_keep_last_refusal(source_format: str, stream_contracts: Any) -> str:
    """Why keep_last cannot run on a multi-object record-batch read, or "".

    keep_last on an object store collapses one object read whole (the
    buffered route, :func:`keep_last_reads_whole`). A run that selects two or
    more objects is streamed object by object without that collapse, and the
    buffered route reads only one — so it is refused at Validate and at
    schedule save, and the runner fails closed (QA C2 review 3, A2).
    """
    from services.preflight_source_kind import source_is_record_batch
    from services.sync_cursor import resolve_selected_sync_contracts

    if not source_is_record_batch("database", source_format):
        return ""
    contracts = [c for c in stream_contracts or [] if isinstance(c, dict)]
    selected = resolve_selected_sync_contracts(contracts)
    if len(selected) < 2:
        return ""
    return (
        f"in_file_dedupe=keep_last collapses one object read whole; this run "
        f"selects {len(selected)} objects, which are streamed one by one without "
        "that collapse, so it is not applied. Run each object as its own transfer "
        "(one selected stream) with keep_last, or remove the duplicate keys at the "
        "source."
    )


def keep_last_records(request: Any, records: list[dict]) -> list[dict]:
    """The buffered route's keep_last: the same collapse the file stream runs.

    ``file_stream`` collapses through :func:`iter_keep_last`; a file the engine
    reads whole (priority/limit, staging, mirror) is collapsed here, before
    the write and before Gate-8 hashes ``records``. A table is never collapsed
    here — Validate refuses keep_last on one (``KEEP_LAST_TABLE_REFUSAL``).
    """
    if not records or not keep_last_reads_whole(request):
        return records
    from services.sync_cursor import resolve_sync_contract

    contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
    keys = list(contract.primary_key_columns()) if contract else []
    if not keys:
        return records
    collapsed, _dropped = collapse_keep_last(list(records), keys)
    return collapsed


def schedule_keep_last_refusal(payload: Any) -> str:
    """Why a schedule's keep_last can never run, or "" — the same rule as Validate.

    A schedule always stamps ``in_file_dedupe`` (``block`` / ``keep_last``) on
    every run. keep_last on a table read that does not define a last copy
    (``data_integrity.keep_last_table_refusal``) failed every beat; it is
    refused at save instead (REST, MCP create_schedule and GitOps all save
    through ``schedule_store``). A record-batch source, or one whose connector
    cannot be resolved here, is left to Validate.
    """
    data = dict(payload or {})
    dedupe = str(data.get("in_file_dedupe") or "").strip().lower().replace("-", "_")
    if dedupe not in {"keep_last", "last", "last_wins"}:
        return ""
    from services.preflight_source_kind import source_is_record_batch

    src_type = str(data.get("source_type") or "")
    if not src_type and data.get("source_connector_id"):
        try:
            from services.connector_store import get_connector

            found = get_connector(str(data["source_connector_id"]))
            src_type = str(getattr(found, "type", "") or (found or {}).get("type", "") if found else "")
        except Exception:  # noqa: BLE001 — unresolved: Validate decides
            return ""
    if not src_type:
        return ""
    if source_is_record_batch("database", src_type):
        return multi_stream_keep_last_refusal(src_type, data.get("stream_contracts"))
    table = str(data.get("source_table") or "")
    contracts = [c for c in data.get("stream_contracts") or [] if isinstance(c, dict)] or [{
        "name": table, "selected": True, "cursor_field": str(data.get("cursor_column") or ""),
    }]
    from services.data_integrity import keep_last_table_refusal

    return keep_last_table_refusal(
        str(data.get("sync_mode") or "full_refresh_overwrite"),
        stream_contracts=contracts, source_type=src_type, stream_name=table,
    )


def normalize_duplicate_policy(raw: str | None) -> str:
    text = (raw or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "merge": "upsert",
        "update": "upsert",
        "on_conflict_update": "upsert",
        "skip": "dedupe_skip",
        "skip_duplicates": "dedupe_skip",
        "insert_if_not_exists": "dedupe_skip",
        "do_nothing": "dedupe_skip",
        "ignore": "dedupe_skip",
        "allow_duplicates": "append",
        "insert": "append",
    }
    return aliases.get(text, text)


def resolve_duplicate_policy(
    *,
    duplicate_policy: str = "",
    allow_duplicates: bool = False,
    sync_mode: str = "",
    business_key: bool = False,
    write_mode: str = "",
) -> DuplicateDecision:
    """Choose write mode and match action for this transfer."""
    explicit = normalize_duplicate_policy(duplicate_policy)
    if allow_duplicates and not explicit:
        explicit = "append"
    caller = (write_mode or "").strip().lower()

    if explicit == "append":
        return DuplicateDecision(
            "append",
            "insert",
            "update",
            True,
            "Append was chosen explicitly — duplicate rows are allowed.",
        )
    if explicit == "dedupe_skip":
        if not business_key:
            raise DuplicatePolicyError(
                "Skip duplicates needs a business key. Declare a primary key, "
                "or choose append to allow duplicate rows."
            )
        return DuplicateDecision(
            "dedupe_skip",
            "upsert",
            "skip",
            True,
            "Skip duplicates — existing keys stay, new keys insert.",
        )
    if explicit == "upsert":
        if not business_key:
            raise DuplicatePolicyError(
                "Upsert needs a business key. Declare a primary key, "
                "or choose append to allow duplicate rows."
            )
        return DuplicateDecision(
            "upsert",
            "upsert",
            "update",
            True,
            "Upsert on the business key — existing rows update, new rows insert.",
        )
    if explicit and explicit not in {"upsert", "dedupe_skip", "append"}:
        raise DuplicatePolicyError(
            f"Unknown duplicate_policy {duplicate_policy!r}. "
            "Use upsert, dedupe_skip, or append."
        )

    # full_refresh_append is already an explicit sync mode: two runs land 2N.
    # Rewriting it to upsert failed Gate-8 (dest Δ 0) and the sync-mode matrix.
    # Upsert and skip-duplicates apply when duplicate_policy names them.
    if caller == "upsert" and business_key:
        return DuplicateDecision(
            "upsert",
            "upsert",
            "update",
            False,
            "Upsert on the business key.",
        )
    return DuplicateDecision(
        "append" if caller != "upsert" else "upsert",
        caller or "insert",
        "update",
        False,
        "",
    )


def set_match_action(action: str, *, duplicates_allowed: bool = False):
    return (
        _MATCH.set("skip" if (action or "").strip().lower() == "skip" else "update"),
        _ROWS_ALLOWED.set(bool(duplicates_allowed)),
    )


def reset_match_action(token) -> None:
    match_token, rows_token = token
    _ROWS_ALLOWED.reset(rows_token)
    _MATCH.reset(match_token)


def duplicates_allowed(decision: "DuplicateDecision | None") -> bool:
    """The operator explicitly allowed repeated keys (append / allow_duplicates)."""
    return bool(decision is not None and decision.explicit and decision.policy == "append")


def current_duplicates_allowed() -> bool:
    return _ROWS_ALLOWED.get()


def current_match_action() -> str:
    return _MATCH.get()


def set_order_column(name: str):
    return _ORDER.set((name or "").strip())


def reset_order_column(token) -> None:
    _ORDER.reset(token)


def current_order_column() -> str:
    return _ORDER.get()


def latest_version_per_key(
    rows: list[dict],
    *,
    cursor_column: str,
    key_columns: list[str],
) -> list[dict]:
    """One row per key — the one an ordered upsert keeps.

    The writer reads ``ORDER BY cursor, key`` and keeps the last copy of a key
    it sees. Ranking a population the same way needs the watermark's typed
    comparison (``services.sync_cursor.compare_cursor_values``: ``999`` before
    ``1000``, instants by value, NULL lowest) with the key as the tie-break;
    sorting the wire text ranked ``"999"`` above ``"1000"`` and let a NULL
    cursor win. Equal ``(cursor, key)`` keep arrival order (stable sort): two
    versions with the same cursor and key are not provably ordered on either
    side, so the survivor here is the last one *this* read saw, which matches
    the writer only when both reads returned them in the same order. Then
    :func:`collapse_keep_last`.
    """
    from functools import cmp_to_key

    from services.sync_cursor import compare_cursor_values
    from services.value_serializer import is_reader_null_cell

    keys = [str(c) for c in key_columns if str(c or "").strip()]
    if not rows or not keys:
        return list(rows)

    def _typed(value: Any) -> str | None:
        return None if value is None or is_reader_null_cell(value) else str(value)

    ordered = list(rows)
    if cursor_column:
        def _writer_order(a: dict, b: dict) -> int:
            by_cursor = compare_cursor_values(_typed(a.get(cursor_column)), _typed(b.get(cursor_column)))
            if by_cursor:
                return by_cursor
            for col in keys:
                by_key = compare_cursor_values(_typed(a.get(col)), _typed(b.get(col)))
                if by_key:
                    return by_key
            return 0

        ordered = sorted(ordered, key=cmp_to_key(_writer_order))
    kept, _dropped = collapse_keep_last(ordered, keys)
    return kept


def collapse_keep_last(
    rows: list[dict],
    key_columns: list[str],
    key_fn: Any = None,
) -> tuple[list[dict], int]:
    """Keep the last row for each key. Earlier copies are not a second insert.

    ``block`` is the other in-file choice: the caller refuses the file instead
    of calling this. Rows with a NULL / missing key are kept in order: ``''`` is
    a key like any other, so two ``''`` rows collapse (D53).
    """
    from services.value_serializer import is_reader_null_cell

    keys = [str(col) for col in key_columns if str(col).strip()]
    if not keys:
        return list(rows), 0
    index: dict[tuple, int] = {}
    kept: list[dict] = []
    dropped = 0
    for row in rows:
        if not isinstance(row, dict):
            kept.append(row)
            continue
        identity = key_fn(row) if key_fn is not None else tuple(row.get(col) for col in keys)
        if not isinstance(identity, tuple):
            identity = (identity,)
        if any(value is None or is_reader_null_cell(value) for value in identity):
            kept.append(row)
            continue
        previous = index.get(identity)
        if previous is None:
            index[identity] = len(kept)
            kept.append(row)
            continue
        kept[previous] = row
        dropped += 1
    return kept, dropped


def iter_keep_last(batches, key_columns: list[str]):
    """Collapse one file to last-wins, then yield it in the original chunk sizes."""
    rows: list[dict] = []
    width = 0
    for batch in batches:
        chunk = list(batch or [])
        if chunk and width == 0:
            width = len(chunk)
        rows.extend(chunk)
    collapsed, _dropped = collapse_keep_last(rows, key_columns)
    step = width or 1000
    for start in range(0, len(collapsed), step):
        yield collapsed[start : start + step]


def order_wins(incoming: object, kept: object) -> bool:
    """True when ``incoming`` should replace ``kept`` (last-wins on ties)."""
    if incoming is None and kept is None:
        return True
    if incoming is None:
        return False
    if kept is None:
        return True
    try:
        return float(incoming) >= float(kept)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return str(incoming) >= str(kept)


def keyed_policy_no_key_gate(policy: str) -> dict[str, Any]:
    """Validate blocker: a skip/update append that has no key to match on."""
    label = "Skip keys already stored" if policy == "dedupe_skip" else "Update keys already stored"
    return {
        "id": "g_upsert_dest_key",
        "status": "block",
        "message": (
            f"Duplicate rows is set to '{label}', but no key is declared "
            "to match stored rows on. Declare a primary key for this "
            "stream, or set Duplicate rows to 'Insert every row'."
        ),
        "duration_ms": 0,
        "details": {
            "rule_id": "g_upsert_dest_key.keyed_policy_no_key",
            "remediation_kind": "declare_key",
            "duplicate_policy": policy,
        },
        "suggested_actions": [
            "Declare a primary key for this stream",
            "Set Duplicate rows to Insert every row",
        ],
    }


#: Destinations whose writer keeps a stored key under ``dedupe_skip``
#: (``current_match_action() == "skip"``): the SQL writers (ON CONFLICT DO
#: NOTHING / MERGE insert-only / stored-key hold-back), the keyed object-store
#: merge, and the non-SQL writers that insert only absent keys (MongoDB
#: ``$setOnInsert``, Redis ``SET NX``, DynamoDB conditional PutItem,
#: Elasticsearch bulk ``create``, BigQuery MERGE without WHEN MATCHED, Iceberg
#: ``upsert(when_matched_update_all=False)``). Keys are resolved driver ids.
DEDUPE_SKIP_DRIVERS = frozenset({
    "postgresql", "redshift", "mysql", "sqlite", "snowflake", "bigquery",
    "generic_sql", "sqlserver", "oracle",
    "s3", "gcs", "adls", "azure", "sftp",
    "mongodb", "redis", "dynamodb", "elasticsearch", "iceberg",
})


def dedupe_skip_refusal(dest_type: str) -> str:
    """Why ``skip_duplicates`` cannot run into this destination, or "".

    Every other writer upserts — it would overwrite the stored row the policy
    promises to leave alone, so the option is refused rather than ignored.
    """
    raw = str(dest_type or "").strip().lower()
    if not raw:
        return ""
    from src.transfer.connector_capabilities import resolve_driver_type

    driver = resolve_driver_type(raw)
    if raw in DEDUPE_SKIP_DRIVERS or driver in DEDUPE_SKIP_DRIVERS:
        return ""
    return (
        f"Duplicate rows 'Skip keys already stored' (skip_duplicates) is not "
        f"available for a {raw} destination: its writer cannot leave a stored key "
        "unchanged and would overwrite it. Choose 'Update keys already stored' "
        "(upsert) or 'Insert every row' (append)."
    )


def dedupe_skip_validate_gate(dest_type: str) -> dict[str, Any] | None:
    """Validate blocker for ``skip_duplicates`` into an unsupported destination."""
    message = dedupe_skip_refusal(dest_type)
    if not message:
        return None
    return {
        "id": "g_duplicate_policy_destination",
        "status": "block",
        "message": message,
        "duration_ms": 0,
        "details": {
            "rule_id": "g_duplicate_policy_destination.dedupe_skip_unsupported",
            "remediation_kind": "change_duplicate_policy",
            "duplicate_policy": "dedupe_skip",
            "destination_type": str(dest_type or ""),
        },
        "suggested_actions": [
            "Set Duplicate rows to Update keys already stored",
            "Set Duplicate rows to Insert every row",
        ],
    }


def schedule_dedupe_skip_refusal(payload: Any) -> str:
    """Save-time twin of :func:`dedupe_skip_validate_gate` for a schedule."""
    data = dict(payload or {})
    if normalize_duplicate_policy(str(data.get("duplicate_policy") or "")) != "dedupe_skip":
        return ""
    dest_type = str(data.get("dest_type") or "")
    if not dest_type and data.get("dest_connector_id"):
        try:
            from services.connector_store import get_connector

            found = get_connector(str(data["dest_connector_id"]))
        except Exception:  # noqa: BLE001 — unresolved here: Validate decides
            return ""
        dest_type = str(getattr(found, "type", "") or "") if found else ""
    return dedupe_skip_refusal(dest_type)
