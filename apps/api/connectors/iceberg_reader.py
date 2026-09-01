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


def _filesystem_schema_columns(endpoint: dict[str, Any]) -> list[str]:
    """Current filesystem CoW schema field names from Iceberg metadata.

    Dest COUNT parquet projection returns ``[]`` when ``cols`` is empty, so a
    source peek that passes ``columns=None`` would invent an empty table.
    """
    from connectors.iceberg_writer import _load_metadata, _resolve_iceberg_table_dir

    table = str(endpoint.get("table") or endpoint.get("table_name") or "")
    schema = str(endpoint.get("schema") or "")
    if endpoint.get("warehouse") and not endpoint.get("database"):
        endpoint = {**endpoint, "database": endpoint.get("warehouse")}
    table_dir = _resolve_iceberg_table_dir(endpoint, table, schema or None)
    versions = sorted((table_dir / "metadata").glob("v*.metadata.json"))
    if not versions:
        return []
    meta = _load_metadata(versions[-1]) or {}
    schema_json = (meta.get("schemas") or [{}])[-1] or meta.get("schema") or {}
    if not isinstance(schema_json, dict) or not schema_json.get("fields"):
        schema_json = meta.get("schema") or {}
    if not isinstance(schema_json, dict):
        return []
    return [
        str(field.get("name"))
        for field in (schema_json.get("fields") or [])
        if field.get("name")
    ]


def _read_filesystem_table(
    endpoint: dict[str, Any],
    *,
    limit: int,
    offset: int,
    columns: list[str] | None,
) -> ReadBatch:
    """Read legacy filesystem CoW tables from snapshot data files.

    ``load_catalog`` used to fall through to SqlCatalog for filesystem
    warehouses and raise ``SQL connection URI is required``. Writer/COUNT
    already use the parquet snapshot; the reader must too.
    """
    from services.dest_precount import _iceberg_snapshot_rows

    table = str(endpoint.get("table") or endpoint.get("table_name") or "")
    schema = str(endpoint.get("schema") or "")
    wanted = [str(c) for c in (columns or []) if str(c).strip()]
    if not wanted:
        wanted = _filesystem_schema_columns(endpoint)
    rows = _iceberg_snapshot_rows(
        endpoint, schema=schema, table_name=table, cols=wanted
    )
    if rows is None:
        raise RuntimeError(
            f"Iceberg filesystem snapshot unreadable for {schema}.{table}"
        )
    if not wanted:
        wanted = list(rows[0].keys()) if rows else []
    if offset:
        rows = rows[int(offset) :]
    if limit:
        rows = rows[: int(limit)]
    headers = wanted
    data = [[_stringify(row.get(col)) for col in headers] for row in rows]
    return ReadBatch(
        headers=headers,
        rows=data,
        offset=offset,
        total_rows=None,
    )


def _read_table(
    endpoint: dict[str, Any],
    *,
    limit: int,
    offset: int,
    columns: list[str] | None,
) -> ReadBatch:
    """Read an Iceberg table and return a Datawrap ``ReadBatch``."""
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config
    from connectors.iceberg_writer import resolve_iceberg_write_path

    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except Exception:
        write_path = ""
    config = parse_iceberg_catalog_config(endpoint)
    catalog_type = str(config.get("catalog_type") or "").lower()
    if catalog_type == "filesystem" or write_path == "filesystem":
        return _read_filesystem_table(
            endpoint, limit=limit, offset=offset, columns=columns
        )

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
