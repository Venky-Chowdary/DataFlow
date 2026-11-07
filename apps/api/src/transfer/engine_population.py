"""Preflight design-sample and population helpers (re-exported by src.transfer.engine)."""

from __future__ import annotations

from typing import Any
from typing import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.read_options import ReadOptions
    from services.shape_apply import ShapeRunner
    from src.transfer.models import EndpointConfig
    from src.transfer.models import TransferRequest
    import os

import logging

logger = logging.getLogger("src.transfer.engine")


def _widen_design_sample(
    source: "EndpointConfig",
    columns: list[str],
    source_filter: dict[str, Any] | None,
    runner: "ShapeRunner | None",
    *,
    wanted: int,
    scan_budget: int = 50_000,
) -> list[dict[str, Any]]:
    """Read further into the source until ``wanted`` rows survive the read rewrites.

    A declared source filter and an approved recipe both remove rows *before*
    Map, the type inference and the gates ever see them, so a selective one can
    empty the first page and leave preflight with no rows to judge — which reads
    as "no sample" and blocks a run that is in fact correct. Reading on is the
    only honest answer: the rows exist, they are just deeper than one page.

    Bounded on purpose. A filter that keeps one row in a million must not turn
    design time into a full table scan, so the walk stops at ``scan_budget`` raw
    rows and returns what survived — the caller then judges a thinner sample,
    which is what it would have done for a small table anyway.
    """
    from src.transfer.engine import (  # lazy: honours monkeypatches on the original module
        Any,
    )
    from services.row_filter import apply_row_filter

    from .source_peek import iter_stream_source_column_rows

    survivors: list[dict[str, Any]] = []
    scanned = 0
    for row in iter_stream_source_column_rows(source, columns):
        scanned += 1
        kept = [row]
        if source_filter:
            kept = apply_row_filter(kept, source_filter)
        if kept and runner is not None:
            kept = runner.records(kept)
        survivors.extend(kept)
        if len(survivors) >= wanted or scanned >= scan_budget:
            break
    return survivors[:wanted]


def _rekey_to_read_columns(
    schema: dict[str, str], columns: list[str] | None
) -> dict[str, str]:
    """Re-key declared types onto the spelling the reader gave the rows.

    Oracle/DB2/Snowflake introspection reports the folded catalog name
    (``AMOUNT``) while the same read hands rows back as ``amount``. Every
    consumer keyed on the row's own column name then missed the declaration and
    fell back to a sampled guess — ``NUMBER(12,2)`` money was scored as
    ``DECIMAL(8,4)``. Only an unambiguous single fold match is renamed.
    """
    if not schema or not columns:
        return schema
    out = dict(schema)
    for col in columns:
        name = str(col)
        if name in out:
            continue
        folded = name.casefold()
        hits = [k for k in out if str(k).casefold() == folded]
        if len(hits) == 1:
            out[name] = out.pop(hits[0])
    return out


def _source_nullability_probe(source: EndpointConfig) -> dict[str, bool]:
    """Real NOT NULL facts for a database source.

    G3 enforces the destination's NOT NULL contract, and a source column with
    unknown nullability is assumed nullable. That is right for files, but for
    an introspected database it made every NOT NULL destination column look
    unsafe — copying a table onto an identical table with a PRIMARY KEY blocked
    as a "type coercion issue". Returning {} keeps the old assume-nullable
    behaviour whenever the probe cannot speak.
    """
    if source.kind != "database":
        return {}
    try:
        from .endpoint_intelligence import introspect_endpoint

        source.extra = {**(source.extra or {}), "introspect_purpose": "source", "introspect_strict": True}
        info = introspect_endpoint(source)
        return {
            str(k): bool(v)
            for k, v in dict(info.get("schema_nullability") or {}).items()
        }
    except Exception as exc:
        logger.debug("source nullability probe failed: %s", exc, exc_info=exc)
        return {}


def _apply_sampled_dest_profile(
    destination: EndpointConfig,
    mappings: list[dict[str, Any]],
    dest_schema_types: dict[str, str],
) -> dict[str, str]:
    """Widen a sampled dest profile so writers and preflight share one shape.

    Object-store / Redis rereads measure DECIMAL(2,2) from the first page.
    Treating that as DDL made run 2 refuse (and quarantine) the source's
    declared DECIMAL(12,2). Declared catalogs (SQL, Elasticsearch mappings)
    are left untouched.
    """
    from services.dest_schema_authority import apply_sampled_profile_to_dest_types

    extra = dict(getattr(destination, "extra", None) or {})
    authority = extra.get("schema_carrier_authority")
    widened = apply_sampled_profile_to_dest_types(
        dest_schema_types,
        mappings,
        dest_db=str(getattr(destination, "format", "") or ""),
        authority_map=authority if isinstance(authority, dict) else None,
    )
    extra["schema_types"] = dict(widened)
    destination.extra = extra
    return widened


def _destination_filler_metadata(extra: dict[str, Any] | None) -> dict[str, Any]:
    """Catalog facts about who fills a destination column when no mapping does (G14)."""
    meta = dict(extra or {})
    return {
        "destination_column_defaults": dict(meta.get("schema_defaults") or {}),
        "destination_identity_columns": list(meta.get("identity_columns") or []),
        "destination_generated_columns": list(meta.get("generated_columns") or []),
        "destination_dynamo_key_schema": [
            dict(row)
            for row in (meta.get("dynamo_key_schema") or [])
            if isinstance(row, dict)
        ],
        "destination_dynamo_index_attributes": dict(
            meta.get("dynamo_index_attributes") or {}
        ),
        "destination_live_column_types": dict(
            meta.get("schema_types")
            or meta.get("overwrite_replaced_column_types")
            or {}
        ),
    }


def _preflight_sample_rows(records: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Execute must use the same sample cap as Validate (not a thinner head)."""
    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT

    rows = list(records or [])
    if len(rows) > PREFLIGHT_SAMPLE_LIMIT:
        from services.sample_strategy import position_reservoir_sample

        return position_reservoir_sample(rows, PREFLIGHT_SAMPLE_LIMIT)
    return rows


def _file_population_rows(
    content: bytes | str | os.PathLike[str],
    filename: str,
    read_options: ReadOptions | None = None,
) -> Iterator[dict[str, Any]]:
    """Every source row of a file, for the pre-write population fit scan.

    Read-only and lazy: the preflight scan pulls rows only for columns whose
    declared source type can exceed a bounded destination carrier, so a file
    with no narrowing mapping is never re-read. A source that cannot be
    re-streamed yields nothing, which the scan reports as unmeasured evidence
    rather than as proof of fit.
    """
    from .file_stream import iter_source_rows

    try:
        yield from iter_source_rows(content, filename, read_options=read_options)
    except Exception as exc:  # noqa: BLE001 - unreadable source is unmeasured, never "fits"
        logger.warning(
            "population fit scan could not re-read %s: %s", filename, exc
        )


def _table_population_rows(
    request: TransferRequest,
    mappings: list[dict[str, Any]],
    *,
    column_types: dict[str, str],
    dest_types: dict[str, str],
    dest_db: str,
    shape_runner: "ShapeRunner | None" = None,
) -> Iterator[dict[str, Any]]:
    """Every source row of a table, projected to the bounded columns only.

    Same pre-write question as the file path, asked of a relational/object
    source: which declared narrowing can this population actually not survive.
    The projection is resolved from the mappings themselves, so a table with no
    bounded destination carrier issues no query at all. A reader that cannot be
    paged independently yields nothing, which the scan reports as preview
    evidence rather than as proof of fit.
    """
    from services.preflight_cursor_gate import read_scope_for_transfer_request
    from services.sync_cursor import incremental_read_narrows, normalize_sync_mode
    from .source_peek import iter_bounded_table_population_rows

    if normalize_sync_mode(getattr(request, "sync_mode", "") or "", default="") == "cdc":
        return
    scope = None
    if incremental_read_narrows(getattr(request, "sync_mode", "") or ""):
        try:
            scope = read_scope_for_transfer_request(request)
        except Exception:
            scope = None

    try:
        rows = iter_bounded_table_population_rows(
            request.source,
            mappings,
            column_types=column_types,
            dest_types=dest_types,
            dest_db=dest_db,
            source_kind=getattr(request.source, "kind", "") or "",
            source_format=getattr(request.source, "format", "") or "",
            limit=int(request.limit or 0),
            shape_runner=shape_runner,
            read_scope=scope if scope is not None and getattr(scope, "bounded", False) else None,
            source_filter=getattr(request, "source_filter", None) or None,
        )
        if rows is None:
            return
        yield from rows
    except Exception as exc:  # noqa: BLE001 - unreadable source is unmeasured, never "fits"
        logger.warning("population fit scan could not re-read source table: %s", exc)
