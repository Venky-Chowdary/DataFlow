"""Validate's sync contract gate: read scope, cursor meaning, cursor/key presence.

Kept beside the gate that uses it rather than inside it, so the transfer's read
side and Validate's checks resolve a route's watermark through one call. A gate
that rebuilt the key itself found no watermark and silently fell back to
reasoning about the whole table.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from services.append_continuity import REBASELINE_REMEDY
from services.cursor_semantics import evaluate_cursor_semantics
from services.sync_cursor import contract_cursor_field
from services.sync_cursor import IncrementalReadScope, resolve_incremental_read_scope

logger = logging.getLogger(__name__)

#: Destinations that honor SCD2 / mirror streaming paths (must match Studio gating).
SQL_HISTORY_SYNC_DESTS = frozenset({
    "postgresql",
    "mysql",
    "sqlite",
    "snowflake",
    "bigquery",
    "redshift",
    "generic_sql",
    "sqlserver",
    "mssql",
    "oracle",
    "duckdb",
})

#: Sources that can drive CDC (log / change-stream) in production.
CDC_CAPABLE_SOURCES = frozenset({
    "postgresql",
    "mysql",
    "sqlserver",
    "mssql",
    "oracle",
    "mongodb",
    "azure_sql_database",
    "microsoft_sql_server",
    "amazon_rds_sql_server",
    "amazon_rds_postgresql",
    "amazon_rds_mysql",
    "amazon_aurora_postgresql",
    "amazon_aurora_mysql",
})

MODES_REQUIRING_CURSOR = frozenset(
    {"incremental_append", "incremental_deduped", "cdc"}
)
#: A file has no log cursor. Keyed upsert re-reads the file and MERGEs on the
#: business key. Demanding updated_at here is what blocked file schedules.
_FILE_UPSERT_SOURCES = frozenset(
    {
        "file",
        "csv",
        "excel",
        "json",
        "parquet",
        "sftp",
        "s3",
        "gcs",
        "adls",
        "local",
        "upload",
    }
)
_FILE_UPSERT_MODES = frozenset(
    {"upsert", "incremental_deduped", "incremental_upsert"}
)
MODES_REQUIRING_PRIMARY_KEY = frozenset(
    {"upsert", "incremental_deduped", "cdc", "scd2", "mirror"}
)


def _kafka_source(source_name: str) -> bool:
    from services.copy_kafka_common import kafka_family_name

    return kafka_family_name(source_name) == "kafka"


def _declared_primary_key_columns(contract: dict[str, Any]) -> list[str]:
    raw = contract.get("primary_key")
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        raw = contract.get("primary_keys")
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [part.strip() for part in str(raw or "").replace(";", ",").split(",") if part.strip()]


def catalog_identity_applies(
    contract: dict[str, Any], source_table: str, stream_count: int
) -> bool:
    """A catalog key belongs to one table, not to every stream on the route."""
    if stream_count <= 1:
        return True
    name = str(contract.get("name") or contract.get("stream") or "").strip().lower()
    table = (source_table or "").strip().lower()
    if not name or not table:
        return False
    return name in {table, table.split(".")[-1]}


def _mapping_rows(mappings: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in mappings or []:
        if isinstance(item, dict):
            rows.append(item)
            continue
        dump = getattr(item, "model_dump", None)
        if callable(dump):
            dumped = dump()
            if isinstance(dumped, dict):
                rows.append(dumped)
    return rows


def resolved_stream_identity_columns(
    contract: dict[str, Any],
    *,
    catalog_primary_key_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
) -> list[str]:
    """Contract key, otherwise the fully mapped source-catalog key.

    Empty when neither exists. Does not invent ``id``. An unmapped catalog
    key is empty too — upserting on part of the key would match the wrong rows.
    """
    declared = _declared_primary_key_columns(contract)
    if declared:
        return declared
    if catalog_primary_key_columns is None:
        return []
    cols = [
        str(col).strip()
        for col in catalog_primary_key_columns
        if str(col or "").strip()
    ]
    if not cols:
        return []
    mapping_rows = _mapping_rows(mappings)
    if mapping_rows:
        from services.primary_key import mapped_catalog_upsert_key

        sources, _targets = mapped_catalog_upsert_key(cols, mapping_rows)
        return sources
    return cols


def contract_declares_primary_key(contract: dict[str, Any]) -> bool:
    """True when the stream contract names at least one identity column.

    A list of blanks (``primary_keys: [""]``) is not a key. Execute splits
    that list and then refuses CDC, so Validate must refuse it too.
    """
    raw = contract.get("primary_key")
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        raw = contract.get("primary_keys")
    if isinstance(raw, (list, tuple)):
        return any(str(item).strip() for item in raw)
    return bool(str(raw or "").strip())


def _transfer_destination_config(
    destination_db_type: str, destination_config: Mapping[str, Any] | None
) -> dict[str, Any]:
    """The destination as the transfer resolves it, so both build one cursor key.

    The transfer keys its watermark on ``route_endpoint_identity`` of the
    endpoint after ``resolve_connector_config`` (saved connector merged, host
    defaulted). Validate received the raw/redacted probe config, built the
    unscoped key, found no watermark and judged the whole table on every rerun
    (QA MX3-03: "Append would store a second copy of 10 row(s)").
    """
    raw = dict(destination_config or {})
    try:
        from src.transfer.adapters import resolve_connector_config
        from src.transfer.models import EndpointConfig

        endpoint = EndpointConfig.from_dict(
            "database", {"format": destination_db_type, **raw}
        )
        return {**raw, **resolve_connector_config(endpoint)}
    except (ImportError, KeyError, TypeError, ValueError, AttributeError) as exc:
        logger.warning(
            "destination identity for the incremental read scope unresolved "
            "(type=%s): %s — the gate judges the whole sample",
            destination_db_type,
            exc,
        )
        return raw


def _source_identity_config(
    source_config: Mapping[str, Any] | None, source_connector_id: str
) -> Mapping[str, Any]:
    """Source config with the database the run will key its bookmark on.

    Validate may name the source by connector id alone; the run resolves the
    saved connector's database, so the bookmark key must be built from it too
    (round 5, D203 prov.: REST Validate read ``postgresql::t`` and saw none).
    """
    cfg = dict(source_config or {})
    cid = (source_connector_id or "").strip()
    if cfg.get("database") or not cid:
        return cfg
    try:
        from services.connector_store import get_connector

        conn = get_connector(cid)
        database = str(getattr(conn, "database", "") or "") if conn is not None else ""
    except Exception as exc:  # noqa: BLE001 - unresolved stays the whole-sample view
        logger.warning("source identity for the read scope unresolved (%s): %s", cid, exc)
        database = ""
    if database:
        cfg["database"] = database
    return cfg


def resolve_read_scope(
    *,
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None,
    source_format: str,
    source_config: Mapping[str, Any] | None,
    source_table: str,
    destination_db_type: str,
    destination_config: Mapping[str, Any] | None,
    destination_table: str,
    source_connector_id: str = "",
) -> IncrementalReadScope:
    """The cursor state of this route, as the transfer's read side sees it.

    Built from the same identifiers the transfer uses for its watermark, so the
    delta a gate reasons about is the delta the reader will deliver. A route
    whose watermark cannot be resolved reports no watermark, which keeps checks
    on the whole sample rather than silently narrowing them.
    """
    try:
        from src.transfer.connector_capabilities import resolve_driver_type

        source_type = resolve_driver_type(source_format or "")
    except Exception:
        source_type = (source_format or "").strip().lower()
    src_cfg = _source_identity_config(source_config, source_connector_id)
    dst_cfg = _transfer_destination_config(destination_db_type, destination_config)
    try:
        return resolve_incremental_read_scope(
            sync_mode=sync_mode,
            stream_contracts=stream_contracts,
            source_type=source_type,
            source_database=str(src_cfg.get("database") or ""),
            source_object=source_table,
            source=src_cfg,
            dest_type=(destination_db_type or "").strip().lower(),
            dest_database=str(dst_cfg.get("database") or ""),
            dest_object=destination_table,
            destination=dst_cfg,
        )
    except Exception as exc:
        logger.warning("incremental read scope unresolved: %s", exc, exc_info=exc)
        return IncrementalReadScope()


def _contract_lookback(contract: dict[str, Any]) -> int:
    try:
        days = int(
            contract.get("cursor_lookback_days")
            or contract.get("lookback_days")
            or 0
        )
    except (TypeError, ValueError):
        return 0
    return days if days > 0 else 0


def evaluate_stream_cursor_semantics(
    contracts: list[dict[str, Any]],
    *,
    sync_mode: str,
    validation_mode: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Judge each stream's cursor declaration; return verdicts and blocking issues.

    Never inferred from the column's name: a name cannot establish whether the
    source moves the value when a row changes.
    """
    verdicts: list[dict[str, Any]] = []
    issues: list[str] = []
    for c in contracts:
        cursor = contract_cursor_field(c)
        if not cursor:
            continue
        verdict = evaluate_cursor_semantics(
            sync_mode=sync_mode,
            cursor_field=cursor,
            declared=str(c.get("cursor_semantics") or ""),
            validation_mode=validation_mode,
            lookback_days=_contract_lookback(c),
        )
        if verdict.status == "not_applicable":
            continue
        stream = c.get("name") or c.get("stream") or "stream"
        entry = verdict.to_dict()
        entry["stream"] = stream
        verdicts.append(entry)
        if verdict.blocks:
            issues.append(f"{stream}.{cursor}: {verdict.reason}")
    return verdicts, issues


CLEAR_CURSOR_ENDPOINT = "POST /api/v1/ops/cdc-cursors/clear"


def clear_cursor_remedy(cursor_key: str = "") -> str:
    """The cursor-reset action by both doors: MCP tool and REST route (D100)."""
    key = f" with cursor_key '{cursor_key}'" if cursor_key else ""
    return f"MCP tool clear_cursor_resnapshot{key}, or {CLEAR_CURSOR_ENDPOINT}"


def cursor_identity_issue(scope: IncrementalReadScope | None) -> str:
    """Refusal text when the stored watermark belongs to a different column.

    The cursor column of a route is part of the meaning of its watermark. When
    an operator repoints the cursor (``id`` → ``updated_at``), the stored value
    can neither bound the new read nor be translated into it: applying it either
    aborts the read or, when both types compare, silently skips the rows between
    the two columns' orderings. The only sound answers are reset-and-resnapshot
    or restore the previous cursor, and both are the operator's call.
    """
    if scope is None or not scope.cursor_column_changed:
        return ""
    return (
        f"Cursor changed for this route: the stored watermark "
        f"'{scope.watermark}' was measured on '{scope.watermark_cursor_column}', "
        f"not on '{scope.cursor_column}' — it cannot bound a read on a different "
        f"column. Reset the cursor ({clear_cursor_remedy(scope.cursor_key)}) "
        f"to re-snapshot, or restore "
        f"'{scope.watermark_cursor_column}' as the cursor field."
    )


def dest_object_missing(
    dest_type: str, dest_cfg: dict[str, Any], dest_table: str
) -> bool | None:
    """Whether the destination object is absent from its catalog — or ``None``.

    "The table is gone" and "the table has zero rows" are different states
    with different recovery advice; a count of 0 conflates them (D100).
    ``None`` means the probe could not decide — never a guess either way.
    """
    try:
        from connectors.generic_sql import _engine
        from connectors.sql_identifiers import split_qualified_table
        from services.sql_object_identity import resolve_object_identity

        engine = _engine(dest_cfg)
        schema, table = split_qualified_table(
            dest_table, (dest_cfg.get("schema") or "").strip() or None
        )
        ident = resolve_object_identity(engine, table, schema)
        return not ident.exists
    except Exception:  # noqa: BLE001 — unknown stays unknown
        return None


def _dest_state_text(dest_missing: bool | None) -> str:
    if dest_missing is True:
        return "the destination table is missing"
    return "the destination is empty"


def cursor_destination_reset_issue(
    scope: IncrementalReadScope | None,
    dest_rows: int | None,
    *,
    dest_missing: bool | None = None,
) -> str:
    """Refusal text when the watermark outlived the destination it delivered to.

    A watermark asserts "everything at or below this value is already at rest in
    that destination". Dropping, recreating or truncating the destination voids
    that assertion, but the cursor survives it, so the next incremental run reads
    only what changed since and reports a green, near-empty load — the whole
    history is silently missing. Fivetran and Airbyte answer this with an
    operator-triggered state reset; so do we, and we refuse until it happens.

    ``dest_rows`` is ``None`` when the destination cannot be counted (an engine
    with no cheap count, or an unreadable probe). Unknown is not evidence of a
    reset, so it never refuses — only a measured empty destination does.
    """
    if scope is None or not scope.bounded or dest_rows is None or dest_rows > 0:
        return ""
    return (
        f"Destination reset since the last incremental run: the stored watermark "
        f"'{scope.watermark}' on '{scope.cursor_column}' claims those rows are "
        f"already at rest, but {_dest_state_text(dest_missing)} — resuming would skip "
        f"the whole history and report success. Clear the cursor "
        f"({clear_cursor_remedy(scope.cursor_key)}) to re-snapshot, or "
        f"rebaseline the route ({REBASELINE_REMEDY}), or point the run at the "
        f"destination that holds the history."
    )


def build_sync_contract_gate(
    contracts: list[dict[str, Any]],
    *,
    sync: str,
    validation: str,
    dest: str,
    src: str,
    kind: str,
    source_columns: list[str] | None,
    pass_status: str,
    block_status: str,
    source_read_mode: str = "",
    read_scope: IncrementalReadScope | None = None,
    catalog_primary_key_columns: list[str] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    source_table: str = "",
) -> dict[str, Any]:
    """The g9 gate: is this route's sync contract complete and semantically sound?

    Presence of a cursor and a key is necessary but not sufficient — what the
    cursor *means* decides whether the read can lose rows, so both are judged
    here and reported as one gate with one primary action per cause.
    """
    from services.sync_cursor import seed_sdk_contract_defaults

    contracts = list(seed_sdk_contract_defaults(src, source_table, contracts) or [])
    multi_stream = len(contracts) > 1
    requires_cursor = sync in MODES_REQUIRING_CURSOR
    requires_primary_key = sync in MODES_REQUIRING_PRIMARY_KEY
    source_name = (src or "").strip().lower()
    source_kind = (kind or "").strip().lower()
    if sync in _FILE_UPSERT_MODES and (
        source_name in _FILE_UPSERT_SOURCES
        or (source_kind == "file" and source_name in {"", "file", * _FILE_UPSERT_SOURCES})
    ):
        # Plain keyed upsert. The file is the snapshot; a cursor column is not
        # how a corrected drop is re-read.
        requires_cursor = False
        requires_primary_key = True

    missing_cursor = [
        c.get("name") or c.get("stream") or "stream"
        for c in contracts
        if requires_cursor
        and not contract_cursor_field(c)
        # CDC's cursor is the log position, not a table column. A contract
        # that names no cursor_field is that log (cdc_position). Inventing
        # updated_at would skip rows the log had already captured. Incremental
        # append still requires a table cursor.
        and sync != "cdc"
        and str(c.get("cursor_semantics") or "").strip().lower() != "cdc_position"
        # Kafka's incremental position is the consumer group's committed
        # offset per partition, not a column: an upsert keyed on id needs no
        # cursor column, and a row watermark would drop messages.
        and not _kafka_source(source_name)
    ]
    missing_primary_key: list[str] = []
    catalog_identity: list[tuple[str, list[str]]] = []
    if requires_primary_key and not contracts:
        ident = resolved_stream_identity_columns(
            {},
            catalog_primary_key_columns=catalog_primary_key_columns,
            mappings=mappings,
        )
        if ident:
            catalog_identity.append(("stream", ident))
        else:
            # Recorded below as the no-contract sentence. An empty list here
            # keeps that sentence from also saying "Missing primary key for stream".
            pass
    for c in contracts:
        if not requires_primary_key:
            continue
        catalog_for_stream = (
            catalog_primary_key_columns
            if catalog_identity_applies(c, source_table, len(contracts))
            else None
        )
        ident = resolved_stream_identity_columns(
            c,
            catalog_primary_key_columns=catalog_for_stream,
            mappings=mappings,
        )
        stream = str(c.get("name") or c.get("stream") or "stream")
        if not ident:
            missing_primary_key.append(stream)
        else:
            catalog_identity.append((stream, ident))

    # Live column check — typo'd cursor/PK names must fail at Validate, not mid-run.
    source_col_set = {
        str(c).strip().lower() for c in (source_columns or []) if str(c).strip()
    }
    unknown_cursor: list[str] = []
    unknown_pk: list[str] = []
    if source_col_set:
        for c in contracts:
            stream = c.get("name") or c.get("stream") or "stream"
            if requires_cursor:
                cursor = contract_cursor_field(c)
                if cursor and cursor.lower() not in source_col_set:
                    unknown_cursor.append(f"{stream}.{cursor}")
    if source_col_set and requires_primary_key:
        for stream, ident in catalog_identity:
            for name in ident:
                if name.lower() not in source_col_set:
                    unknown_pk.append(f"{stream}.{name}")

    issues: list[str] = []
    from services.procedure_source import callable_sync_refusal

    refused = callable_sync_refusal(sync, source_read_mode=source_read_mode)
    if refused:
        issues.append(refused)
    if sync in {"scd2", "mirror"} and not refused:
        if multi_stream:
            issues.append(
                f"{sync.upper()} is not supported for multi-stream transfers"
            )
        elif not dest:
            issues.append(f"{sync.upper()} requires a SQL table destination")
        elif dest not in SQL_HISTORY_SYNC_DESTS:
            issues.append(
                f"{sync.upper()} requires a SQL table destination (not '{dest}')"
            )
    if sync == "cdc" and not refused:
        if kind in {"file", "cloud"}:
            issues.append("CDC requires a database source (not file/cloud)")
        elif src and src not in CDC_CAPABLE_SOURCES:
            issues.append(f"CDC is not supported for source type '{src}'")
        from services.type_system import destination_is_file_export

        if destination_is_file_export(dest):
            issues.append(
                "Object stores have no write-ahead log or replication slot, "
                "so CDC cannot land there. Use full refresh or incremental append."
            )
    if missing_cursor:
        issues.append(
            f"Missing cursor field for {', '.join(missing_cursor[:5])} — declare "
            "stream_contracts[].cursor_field (and cursor_semantics) for that stream"
        )
    if requires_primary_key and not contracts and not catalog_identity:
        issues.append(
            f"Missing primary key — no stream contract carries one; {sync} requires "
            "stream_contracts entries naming primary_key (plus cursor_field and "
            "cursor_semantics for the incremental read)"
        )
    if missing_primary_key:
        issues.append(
            f"Missing primary key for {', '.join(missing_primary_key[:5])} — declare "
            "stream_contracts[].primary_key for that stream"
        )
    if unknown_cursor:
        issues.append(
            f"Cursor field not in source schema: {', '.join(unknown_cursor[:5])}"
        )
    if unknown_pk:
        issues.append(f"Primary key not in source schema: {', '.join(unknown_pk[:5])}")

    verdicts, semantics_issues = evaluate_stream_cursor_semantics(
        contracts, sync_mode=sync, validation_mode=validation
    )
    issues.extend(semantics_issues)

    cursor_conflict = cursor_identity_issue(read_scope)
    if cursor_conflict:
        issues.append(cursor_conflict)

    if issues:
        return {
            "id": "g9_sync_contract",
            "status": block_status,
            "message": issues[0],
            "duration_ms": 0,
            "details": {
                "issues": issues,
                "sync_mode": sync,
                "cursor_semantics": verdicts or None,
            },
        }
    return {
        "id": "g9_sync_contract",
        "status": pass_status,
        "message": f"Sync contract valid for {sync.replace('_', ' ')}",
        "duration_ms": 0,
        "details": {
            "sync_mode": sync,
            "streams": len(contracts),
            "requires_cursor": requires_cursor,
            "requires_primary_key": requires_primary_key,
            "cursor_semantics": verdicts or None,
        },
    }


def _endpoint_identity(endpoint: Any) -> dict[str, Any]:
    """Connector id / host / port of an endpoint — what keys its bookmark."""
    out: dict[str, Any] = {}
    for field in ("connector_id", "host", "port"):
        value = getattr(endpoint, field, None)
        if value not in (None, "", 0):
            out[field] = value
    return out


def read_scope_for_transfer_request(request: Any) -> IncrementalReadScope:
    """:func:`resolve_read_scope` addressed by a ``TransferRequest``.

    Execute and the read side must ask the same question about the same cursor
    key, so the endpoint-shaped translation lives beside the resolver rather
    than being restated by each caller.
    """
    src = getattr(request, "source", None)
    dest = getattr(request, "destination", None)
    src_extra = getattr(src, "extra", None) or {}
    dest_extra = getattr(dest, "extra", None) or {}
    return resolve_read_scope(
        sync_mode=str(getattr(request, "sync_mode", "") or ""),
        stream_contracts=list(getattr(request, "stream_contracts", None) or []),
        source_format=str(getattr(src, "format", None) or ""),
        source_config={"database": str(getattr(src, "database", "") or ""), **src_extra},
        source_table=str(getattr(src, "table", "") or getattr(src, "collection", "") or ""),
        destination_db_type=str(getattr(dest, "format", None) or ""),
        destination_config={
            "database": str(getattr(dest, "database", "") or ""),
            **dest_extra,
            # The route identity the run writes its watermark under. Without
            # it the endpoint defaulted to host=localhost and Execute's gates
            # read a third, empty bookmark (round 5, D203 prov.).
            **_endpoint_identity(dest),
        },
        destination_table=str(
            getattr(dest, "table", "") or getattr(dest, "collection", "") or ""
        ),
    )


def countable_dest_rows(dest_type: str, dest_cfg: dict[str, Any], dest_table: str) -> int | None:
    """Rows at rest in the destination, or ``None`` when it cannot be counted.

    Unknown must stay distinguishable from empty: a probe that failed is not
    evidence the destination was reset, and treating it as one would block a
    healthy incremental run.
    """
    from services.dest_precount import precount_table

    try:
        return precount_table(dest_type, dest_cfg, dest_table)
    except Exception as exc:
        logger.debug("destination pre-count for cursor check failed: %s", exc, exc_info=exc)
        return None


def refuse_unusable_cursor_state(
    scope: IncrementalReadScope,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
) -> None:
    """Raise before a bounded read that its own watermark cannot justify.

    Two states make a bounded read unsafe: the watermark was measured on a
    different column (either aborts mid-run or skips the rows between the two
    orderings), and a destination measured empty while the watermark claims the
    history already landed (the bounded read would skip it for good).
    """
    if scope.cursor_column_changed:
        raise ValueError(cursor_identity_issue(scope))
    if not scope.bounded:
        return
    issue = cursor_destination_reset_issue(
        scope,
        countable_dest_rows(dest_type, dest_cfg, dest_table),
        dest_missing=dest_object_missing(dest_type, dest_cfg, dest_table),
    )
    if issue:
        raise ValueError(issue)
