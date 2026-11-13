"""One reversibility table per selected stream of a multi-table run (review 3, C1).

A run that selects several tables writes each with its own mappings, its own
read (a ``source_query`` / CALL in place of the table) and its own columns;
all of that lives on ``stream_contracts[i]`` (:mod:`services.multi_stream_plan`),
not on the endpoint or the run's shared mapping list. The gate used to analyse
the first table only. Here every selected stream gets its own
ReversibilityTable:

* **Execute** reads each stream the way the sequential writer will: the source
  remapped to the stream and patched with its statement, peeked for its
  columns, types, sample rows and measured count; the stream's own mappings,
  else the inherited map adopted exactly as the writer adopts it
  (``adopt_inherited_mappings``); the destination probed for its types and the
  mappings stamped by the same Decision Kernel step the engine runs.
* **Validate, the plan tab and a job view** read what is declared: the stream's
  own mappings (with their type stamps), its read mode, and its column list
  from the source catalog when the source names a saved connection.

A stream that cannot be analysed is never assumed fine: it becomes an open
table that names the stream and why, and the gate blocks on it.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Mapping
from typing import Any

from services.reversibility import UNAVAILABLE, analyze_table, not_analysed_table, population_entries

logger = logging.getLogger(__name__)


def named_contracts(contracts: Any) -> list[dict[str, Any]]:
    """The contracts with the name the writer gives each (``name``, else ``stream``: ``SyncContract``)."""
    return [{**raw, "name": str(raw.get("name") or raw.get("stream") or "stream").strip()}
            for raw in contracts or [] if isinstance(raw, Mapping)]


def selected_stream_names(contracts: Any) -> list[str]:
    """Names of the selected streams, in contract order (one name: a single-table run).

    Exactly the writer's selection (``resolve_selected_sync_contracts``): a
    truthy ``selected``, a stream named by ``name`` or ``stream`` (review 4).
    """
    from services.sync_cursor import resolve_selected_sync_contracts

    return [c.name for c in resolve_selected_sync_contracts(named_contracts(contracts)) if c.name]


def stream_keys(raw: Mapping[str, Any]) -> list[str]:
    pk = raw.get("primary_key")
    cols = pk.split(",") if isinstance(pk, str) else list(pk or [])
    return [str(c).strip() for c in cols if str(c or "").strip()]


def own_mappings(raw: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The mappings a stream declares for itself (``[]``: it inherits the run's)."""
    maps = raw.get("mappings")
    return [m for m in maps if isinstance(m, dict)] if isinstance(maps, list) else []


def stream_read_mode(raw: Mapping[str, Any]) -> str:
    """``table``, ``query`` or ``procedure`` for one stream's contract (an unreadable mode fails closed)."""
    from services.multi_stream_plan import patches_for_stream

    try:
        patch, _dest = patches_for_stream(raw)
    except Exception:  # noqa: BLE001 - an ambiguous or unknown read mode is not the table: fail closed
        return "query"
    return str((patch or {}).get("source_read_mode") or "table")


def _window(raw: Mapping[str, Any], shared_options: Any, mode: str) -> dict[str, Any]:
    options = raw.get("read_options") if isinstance(raw.get("read_options"), Mapping) else shared_options
    options = options if isinstance(options, Mapping) else {}
    return {"read_mode": mode, "skip_rows": options.get("skip_rows"), "skip_footer": options.get("skip_footer"),
            "header_row": options.get("header_row")}


def _why(exc: BaseException) -> str:
    return f"it could not be read for the analysis ({type(exc).__name__})"


def declared_stream_table(
    name: str,
    raw: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    dest_db: str,
    source_db: str,
    row_limit: int,
    source_filter: Mapping[str, Any] | None,
    read_options: Any,
    recipe_steps: list[dict[str, Any]],
    enforce: str | None,
) -> dict[str, Any]:
    """A non-primary stream's table from what is declared (plan tab, Validate, job view): no rows are read."""
    mode = stream_read_mode(raw)
    window = _window(raw, read_options, mode)
    maps = own_mappings(raw)
    if not maps:
        table = not_analysed_table(
            name, "it declares no mappings of its own, so its columns and types are not known before Execute "
                  "reads it. Declare the stream's mappings, or run it as its own transfer")
        table["columns"].extend(population_entries(
            row_limit, source_filter, read_mode=mode, skip_rows=int(window["skip_rows"] or 0),
            skip_footer=int(window["skip_footer"] or 0), header_row=int(window["header_row"] or 0)))
        return _retitled(table, name, stream_keys(raw))
    source_types: dict[str, str] = {}
    connector_id = str(source.get("connector_id") or "").strip()
    catalog_read = False
    if connector_id and mode == "table":
        try:
            from services.source_schema_authority import live_source_column_types

            source_types = {str(k): str(v) for k, v in (live_source_column_types(
                source_connector_id=connector_id, source_table=name,
                source_schema=str(source.get("schema") or "")) or {}).items()}
            catalog_read = bool(source_types)
        except Exception as exc:  # noqa: BLE001 - an unread catalog is a named gap below, never a pass
            logger.info("reversibility: catalog of stream %s unread (%s)", name, type(exc).__name__)
    table = analyze_table(
        table=name, mappings=maps, source_columns=list(source_types), source_types=source_types,
        target_types={}, recipe_steps=recipe_steps, recipe_input_columns=list(source_types) or None,
        key_columns=stream_keys(raw), dest_db=dest_db, source_db=source_db, row_limit=row_limit,
        source_filter=source_filter, read_window=window, enforce=enforce,
    )
    if not catalog_read and mode == "table":
        # Without the stream's column list a source column its mappings leave out cannot be ruled out.
        gap = not_analysed_table(name, "its source columns could not be read here, so a column its mappings "
                                       "leave out cannot be ruled out before Execute reads the table")
        table["columns"].extend(gap["columns"])
        table = _retitled(table, name, stream_keys(raw))
    return table


def _retitled(table: dict[str, Any], name: str, keys: list[str]) -> dict[str, Any]:
    from services.reversibility import table_of

    return table_of(name, table["columns"], keys)


# ------------------------------------------------------------------- Execute


def _remapped(endpoint: Any, name: str, *, keep_none: bool = False) -> Any:
    """A copy of ``endpoint`` pointed at the stream ``name`` (as the sequential writer remaps it)."""
    out = copy.deepcopy(endpoint)
    is_collection = getattr(out, "format", "") == "mongodb" or bool(getattr(endpoint, "collection", None))
    if keep_none and getattr(endpoint, "table", None) is None and getattr(endpoint, "collection", None) is None:
        return out
    if is_collection:
        out.collection = name
    else:
        out.table = name
    return out


def execute_stream_design(request: Any, name: str, raw: Mapping[str, Any],
                          inherited: list[dict[str, Any]]) -> dict[str, Any]:
    """What Execute will write for one non-primary stream: its mappings, types, sample and measured count.

    Mirrors ``run_non_cdc_multi_stream_sequential``: remapped endpoints, the
    stream's statement patched onto the source, its own mappings or the
    inherited map adopted for its columns. ``stamped`` are the dicts the
    writer will read (the contract's own), or ``None`` for an inherited map.
    """
    from services.execute_shape_route import peek_declared_source
    from services.multi_stream_plan import adopt_inherited_mappings, patches_for_stream
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.mapping_write_stamp import enrich_mappings_with_types, stamp_additive_mappings_for_write

    source = _remapped(request.source, name)
    destination = _remapped(request.destination, name, keep_none=True)
    patch, _dest_patch = patches_for_stream(raw)
    if patch:
        source.extra = {**(getattr(source, "extra", None) or {}), **patch}
    single = [{**dict(raw), "name": name, "selected": True}]
    columns, schema, total_rows, sample_rows = peek_declared_source(source, single)[:4]
    declared = own_mappings(raw)
    maps = declared or adopt_inherited_mappings(inherited, list(columns))[0]
    dest_types, exists = _destination_schema_probe(
        destination, sync_mode=str(raw.get("sync_mode") or getattr(request, "sync_mode", "") or ""))
    sub = copy.copy(request)
    sub.source, sub.destination, sub.stream_contracts, sub.mappings = source, destination, single, maps
    planned = enrich_mappings_with_types([dict(m) for m in maps], column_types=schema, dest_types=dest_types)
    planned = stamp_additive_mappings_for_write(
        sub, planned, column_types=schema, dest_types=dest_types,
        sample_rows=[r for r in (sample_rows or [])[:100] if isinstance(r, dict)], dest_table_exists=exists)
    return {"mappings": planned, "stamped": declared or None, "column_types": dict(schema or {}),
            "dest_types": dict(dest_types or {}), "sample_rows": list(sample_rows or []),
            "total_rows": total_rows, "destination": destination, "source": source}


def execute_stream_table(request: Any, name: str, raw: Mapping[str, Any], inherited: list[dict[str, Any]],
                         analyse: Any) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """(table, design) of one non-primary stream at Execute; an unreadable stream is an open, named table."""
    try:
        design = execute_stream_design(request, name, raw, inherited)
    except Exception as exc:  # noqa: BLE001 - not analysed is a named block, never a pass
        logger.warning("reversibility: stream %s not analysed (%s)", name, type(exc).__name__, exc_info=True)
        return not_analysed_table(name, _why(exc)), None
    return analyse(design), design


__all__ = ["UNAVAILABLE", "declared_stream_table", "execute_stream_design", "execute_stream_table",
           "own_mappings", "selected_stream_names", "stream_keys", "stream_read_mode"]
