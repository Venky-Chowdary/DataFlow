"""Iceberg catalog reader for REST / Glue / SQL / Nessie catalogs.

Wraps ``pyiceberg`` to read real Iceberg tables and return Datawrap ``ReadBatch``
objects. The reader expects either a ``cfg`` dict (the canonical
``resolve_connector_config`` output) or individual connection fields.
"""

from __future__ import annotations

from typing import Any

from connectors.base import ReadBatch
from services.value_serializer import cell_to_string


def _stringify(value: Any) -> str:
    """One Iceberg cell. Same wire as PostgreSQL / SQL readers.

    Nested STRUCT/LIST used to ``json.dumps(..., default=str)`` — nested
    BINARY became a Python ``b'...'`` repr, Decimal used ``str()`` (scientific
    invent), and timestamps used a space instead of ISO ``T``. Leaf datetime /
    Decimal used ``str(value)`` for the same invent. ``cell_to_string`` is the
    one reader algorithm.
    """
    return cell_to_string(value, preserve_sql_null=True)


def _endpoint_from_cfg_or_kwargs(
    cfg: dict[str, Any] | None,
    table: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Build a normalized endpoint dict from ``cfg`` or legacy kwargs."""
    if cfg is not None:
        endpoint = dict(cfg)
    else:
        endpoint = dict(kwargs)
    # ``read_via_registry`` passes the table name as ``object`` for SaaS-style
    # readers; accept either form.
    table = table or kwargs.get("object", "")
    if table:
        endpoint["table"] = table
    endpoint.setdefault("schema", endpoint.get("schema", ""))
    return endpoint


def _read_table(
    endpoint: dict[str, Any],
    *,
    limit: int,
    offset: int,
    columns: list[str] | None,
) -> ReadBatch:
    """Read an Iceberg table and return a Datawrap ``ReadBatch``."""
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

    config = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = config["namespace"] + (config["table_name"],)
    tbl = catalog.load_table(identifier)

    # Use scan(limit=...) then slice for offset. pyiceberg scan does not support
    # offset directly, so we over-read and discard leading rows for now.
    fetch_limit = limit + offset if offset else limit
    all_columns = [f.name for f in tbl.schema().fields]
    selected = columns if columns else all_columns
    arrow = tbl.scan(limit=fetch_limit, selected_fields=selected).to_arrow()
    if offset:
        arrow = arrow.slice(offset)
    if limit and len(arrow) > limit:
        arrow = arrow.slice(0, limit)

    headers = list(arrow.column_names)
    rows = []
    for row in arrow.to_pylist():
        rows.append([_stringify(row.get(c)) for c in headers])

    return ReadBatch(
        headers=headers,
        rows=rows,
        offset=offset,
        total_rows=None,  # Could be expensive to count; engine handles truncation guard elsewhere.
    )


def read_table_batch(
    *,
    cfg: dict[str, Any] | None = None,
    table: str = "",
    limit: int = 100_000,
    offset: int = 0,
    columns: list[str] | None = None,
    **kwargs: Any,
) -> ReadBatch:
    """Entry point used by ``connector_dispatch.read_via_registry``.

    ``read_via_registry`` first tries positional/legacy kwargs; on mismatch it
    falls back to passing ``cfg`` as a dict. This function accepts both forms.
    """
    endpoint = _endpoint_from_cfg_or_kwargs(cfg, table, **kwargs)
    return _read_table(endpoint, limit=limit, offset=offset, columns=columns)
