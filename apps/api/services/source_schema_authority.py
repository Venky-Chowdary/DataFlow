"""Live source schema authority for Validate.

Validate used to score mappings against the ``column_types`` the browser
posted, while Execute re-derived the source schema from the live connector and
re-decided. When the two disagreed the operator got 13/13 green at Validate and
a hard fidelity failure at write — the Mongo ``_id`` case (``VARCHAR`` in the
Map payload, ``OBJECTID`` from live BSON introspection) is one instance, but the
divergence is connector-agnostic: any source whose declared types are refined
server-side (BSON, Parquet logical types, SaaS describes, PG domains) can drift.

This module gives Validate the same source facts Execute will use. Live
introspection wins over the posted declaration; whatever it cannot answer keeps
the declared type. Columns that changed are returned as drift so the response
can show *why* a gate now blocks instead of silently rescoring.
"""

from __future__ import annotations

import logging
from typing import Any

from services.column_case import column_type_or_none

logger = logging.getLogger("datawrap.preflight")


#: ``EndpointConfig.extra`` key listing source columns introspected as a bare
#: ``numeric`` whose DECIMAL(p,s) in the schema is measured, not declared.
UNCONSTRAINED_DECIMALS_KEY = "unconstrained_decimal_columns"


def declared_decimal_for_dest(
    schema: dict[str, Any], source: Any, dest_format: str
) -> dict[str, Any]:
    """Source schema with measured-decimal columns restored to a bare DECIMAL.

    A bare ``numeric`` is measured to a DECIMAL(p,s) so a comparison against a
    bounded destination can be decided from the data (see
    :mod:`services.decimal_capacity_probe`). That measurement describes the rows
    present, so it must not become the DDL of an engine that has the same
    unbounded type: create-new on PostgreSQL would invent NUMERIC(29,9) and the
    next larger value would fail (D68).

    Every destination with column DDL creates from the declaration: a bare
    ``numeric`` becomes the engine's unbounded/widest exact decimal
    (``invent_dest_type("DECIMAL")``), not the DECIMAL(9,6) run 1's rows
    measured, which run 2's larger values then overflowed on MySQL and SQL
    Server (D68).
    """
    measured = (getattr(source, "extra", None) or {}).get(UNCONSTRAINED_DECIMALS_KEY)
    return declare_measured_decimals(schema, measured, dest_format)


def declare_measured_decimals(
    schema: dict[str, Any], measured: Any, dest_format: str
) -> dict[str, Any]:
    """``schema`` with each measured bare numeric restored to bare ``DECIMAL``.

    One rule for every create-new typing path (Execute's auto-map, the
    overwrite typed mapping, the MCP plan's Map).
    """
    from services.db_type_utils import dest_declares_column_ddl

    if not measured or not dest_declares_column_ddl(str(dest_format or "")):
        return schema
    folded = {str(c).casefold() for c in measured}
    return {
        col: ("DECIMAL" if str(col).casefold() in folded else typ)
        for col, typ in schema.items()
    }


def measured_decimal_columns(columns: Any) -> list[str]:
    """Names of introspected columns whose DECIMAL(p,s) is a row measurement."""
    return [
        str(c.get("name"))
        for c in (columns or [])
        if isinstance(c, dict) and c.get("decimal_capacity_measured") and c.get("name")
    ]


def endpoint_source_column_types(endpoint: Any) -> dict[str, str]:
    """Declared column types for an already-built source endpoint.

    The reader reports the shape it decoded, which for a SQL source is the
    Python value space (``uid`` arrives as ``str`` and is reported ``VARCHAR``)
    rather than the column's declaration (``CHAR(36)``). Mapping off the decoded
    shape while the coercion validator later sees the declaration is the same
    Validate/Execute split as the Mongo ``_id`` case, so both must read this.
    """
    if endpoint is None or getattr(endpoint, "kind", "") != "database":
        return {}
    try:
        from src.transfer.endpoint_intelligence import introspect_endpoint

        endpoint.extra = {**(endpoint.extra or {}), "introspect_purpose": "source", "introspect_strict": True}
        info = introspect_endpoint(endpoint)
    except Exception:
        logger.debug("live source introspect failed for endpoint", exc_info=True)
        return {}
    schema = info.get("schema") or {}
    if not isinstance(schema, dict):
        return {}
    measured = [str(c) for c in (info.get("unconstrained_decimals") or [])]
    if measured:
        # Map reads these to keep a bare ``numeric`` bare on PostgreSQL (D68).
        endpoint.extra = {**(endpoint.extra or {}), UNCONSTRAINED_DECIMALS_KEY: measured}
    return {str(k): str(v) for k, v in schema.items() if str(v or "").strip()}


def _endpoint_catalog_info(endpoint: Any) -> dict[str, Any]:
    """The source endpoint's catalog read, or ``{}`` when it cannot be read."""
    if endpoint is None:
        return {}
    try:
        from src.transfer.endpoint_intelligence import introspect_endpoint

        extra = dict(getattr(endpoint, "extra", None) or {})
        extra["introspect_purpose"] = "source"
        extra["introspect_strict"] = True
        endpoint.extra = extra
        info = introspect_endpoint(endpoint)
    except Exception:  # noqa: BLE001 — unread catalog blocks CDC; it does not approve it
        logger.debug("catalog primary key introspect failed", exc_info=True)
        return {}
    return info if isinstance(info, dict) else {}


def endpoint_primary_key_columns(endpoint: Any) -> list[str]:
    """Source-catalog primary key. Empty when the catalog has none or cannot be read.

    A failed introspect is empty on purpose: CDC must not be approved on a
    guessed ``id`` when the catalog could not be read.
    """
    info = _endpoint_catalog_info(endpoint)
    return [
        str(col).strip()
        for col in (info.get("primary_key_columns") or [])
        if str(col or "").strip()
    ]


def endpoint_key_groups(endpoint: Any) -> list[list[str]]:
    """Source-catalog primary key, then unique keys, as column groups (``[]``: none / unread).

    The same normaliser the destination key groups use
    (``services.schema_drift.dest_key_groups``).
    """
    from services.schema_drift import dest_key_groups

    info = _endpoint_catalog_info(endpoint)
    return dest_key_groups(info.get("primary_key_columns"), info.get("unique_keys"))


def append_declares_no_key(
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None,
    contract_primary_key: str | None = None,
) -> bool:
    """True for an append whose contract names no identity key."""
    from services.destination_key_collision_probe import (
        sync_mode_appends_without_key_resolution,
    )
    from services.preflight_cursor_gate import contract_declares_primary_key

    if not sync_mode_appends_without_key_resolution(sync_mode):
        return False
    selected = [c for c in stream_contracts or [] if isinstance(c, dict) and c.get("selected", True)]
    return not (
        str(contract_primary_key or "").strip()
        or (selected and all(contract_declares_primary_key(c) for c in selected))
    )


def append_identity_source_key(
    *,
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None,
    contract_primary_key: str | None = None,
    source_key_groups: list[list[str]] | None = None,
    source_endpoint: Any = None,
    source_connector_id: str = "",
    source_table: str = "",
    source_config: dict[str, Any] | None = None,
) -> list[str]:
    """The source catalog key an append's collision probe keys on, or ``[]``.

    An append declares no key of its own, so the probe used to fall back to a
    column *named* ``id`` (Rfam ``clan``: ``id`` is a label, ``clan_acc`` is the
    key) and refused a new row whose label repeated an old one. The real
    identity is the contract key (the probe reads it itself), else the source
    catalog's primary or unique key — this — else no key. ``source_key_groups``
    is what Execute already read; otherwise ``source_endpoint`` (or the saved
    source connector Validate names) is introspected.
    """
    if not append_declares_no_key(sync_mode, stream_contracts, contract_primary_key):
        return []
    groups = source_key_groups
    if groups is None:
        endpoint = source_endpoint
        cfg = source_config if isinstance(source_config, dict) else {}
        if endpoint is None and str(source_connector_id or "").strip() and source_table:
            endpoint = _saved_source_endpoint(
                str(source_connector_id).strip(), source_table=source_table,
                source_collection=str(cfg.get("collection") or ""), stream=source_table,
                source_schema=str(cfg.get("schema") or ""),
                source_database=str(cfg.get("database") or ""), source_config=cfg or None,
            )
        groups = endpoint_key_groups(endpoint) if endpoint is not None else []
    return list(groups[0]) if groups else []


def live_source_primary_key_columns(
    *,
    source_connector_id: str = "",
    source_table: str = "",
    source_collection: str = "",
    source_schema: str = "",
    source_database: str = "",
    source_config: Any = None,
) -> list[str]:
    """Catalog primary key of a saved source, or empty when it has none.

    A query / procedure result set has no catalog key: the stream label names
    the extract's first relation, not the rows the extract returns.
    """
    connector_id = (source_connector_id or "").strip()
    stream = (source_table or source_collection or "").strip()
    if not connector_id or not stream:
        return []
    endpoint = _saved_source_endpoint(
        connector_id,
        source_table=source_table,
        source_collection=source_collection,
        stream=stream,
        source_schema=source_schema,
        source_database=source_database,
        source_config=source_config,
    )
    if endpoint is None:
        return []
    try:
        from services.procedure_source import is_callable_source

        if is_callable_source(endpoint):
            return []
    except Exception:  # noqa: BLE001 — unread mode blocks, it does not approve
        return []
    return endpoint_primary_key_columns(endpoint)


def live_source_column_types(
    *,
    source_connector_id: str = "",
    source_table: str = "",
    source_collection: str = "",
    source_schema: str = "",
    source_database: str = "",
    source_config: Any = None,
) -> dict[str, str]:
    """Introspect the saved source connector for its declared column types.

    Returns ``{}`` when there is no saved connector or introspection fails —
    Validate then keeps the posted declaration rather than inventing types.

    A query / procedure source (``source_config`` carries the extract) is
    described by its result set, the shape Execute reads. Its stream label is
    the extract's first relation (``SELECT … FROM orders`` → ``orders``), so a
    catalog lookup by that name described the base *table* and merged every
    table column into the query's types: Validate then fingerprinted a schema
    Execute never reads ("source schema drifted since Validate").
    """
    connector_id = (source_connector_id or "").strip()
    stream = (source_table or source_collection or "").strip()
    if not connector_id or not stream:
        return {}
    endpoint = _saved_source_endpoint(
        connector_id,
        source_table=source_table,
        source_collection=source_collection,
        stream=stream,
        source_schema=source_schema,
        source_database=source_database,
        source_config=source_config,
    )
    if endpoint is None:
        return {}
    return endpoint_source_column_types(endpoint)


def _saved_source_endpoint(
    connector_id: str,
    *,
    source_table: str,
    source_collection: str,
    stream: str,
    source_schema: str,
    source_database: str,
    source_config: Any,
):
    """The saved source as Execute builds it, extract (query/CALL + binds) included."""
    try:
        from services.connector_probe import endpoint_from_saved_connector

        endpoint = endpoint_from_saved_connector(
            connector_id,
            table=source_table or stream,
            collection=source_collection or stream,
            schema=source_schema or "",
            database=source_database or "",
        )
    except Exception:
        logger.debug("live source introspect failed for %s", connector_id, exc_info=True)
        return None
    if endpoint is None or source_config is None:
        return endpoint
    try:
        from services.procedure_source import (
            is_callable_source,
            merge_callable_source_extra,
        )

        if is_callable_source(source_config):
            endpoint.extra = merge_callable_source_extra(
                endpoint.extra, source_config
            )
    except Exception:
        logger.debug("callable source extract merge failed", exc_info=True)
        return None
    return endpoint


def reconcile_source_types(
    declared: dict[str, str] | None,
    live: dict[str, str] | None,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Return ``(types, drift)`` where live introspection wins over ``declared``.

    ``drift`` lists only columns whose *logical* type changed — a declared
    ``VARCHAR(64)`` refined to ``VARCHAR(24)`` is the same decision and is not
    worth an operator note, but ``VARCHAR`` → ``OBJECTID`` changes the create-new
    invent and the fidelity verdict, so it must be visible.
    """
    from services.type_system import normalize_logical_type

    merged = dict(declared or {})
    # Oracle/Snowflake catalogs fold identifiers to upper case while the reader
    # decodes rows under the case the operator sees. Keying the live type under
    # the folded name left *both* spellings in the map, and every consumer that
    # looked up the row's own column name kept reading the decoded guess
    # (``amount`` sampled as DECIMAL(8,4)) instead of the live NUMBER(12,2).
    declared_by_fold = {str(k).casefold(): str(k) for k in (declared or {})}
    drift: list[dict[str, str]] = []
    for col, live_type in (live or {}).items():
        key = col
        if col not in merged:
            folded = declared_by_fold.get(str(col).casefold())
            if folded is not None:
                key = folded
        col = key
        prior = str(merged.get(col) or "").strip()
        merged[col] = live_type
        if not prior:
            continue
        try:
            changed = normalize_logical_type(prior) != normalize_logical_type(live_type)
        except (ValueError, TypeError, KeyError, AttributeError):
            changed = prior.upper() != str(live_type).upper()
        if changed:
            drift.append({"column": col, "declared": prior, "live": str(live_type)})
    return merged, drift


def restamp_mapping_source_types(
    mappings: list[dict[str, Any]] | None,
    source_types: dict[str, str] | None,
) -> list[dict[str, Any]]:
    """Re-stamp ``source_type`` on Map rows from the authoritative schema.

    The Decision Artifact hashes each mapping's ``source_type``; leaving the
    browser's stale stamp there would sign an artifact Execute cannot reproduce.
    """
    types = source_types or {}
    out: list[dict[str, Any]] = []
    for m in mappings or []:
        row = dict(m)
        live = str(column_type_or_none(types, str(row.get("source") or "")) or "").strip()
        if live:
            row["source_type"] = live
        out.append(row)
    return out


def empty_source_column_types(
    column_types: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    *,
    declared: dict[str, str] | None = None,
    previous: dict[str, str] | None = None,
    destination: dict[str, str] | None = None,
) -> dict[str, str]:
    """Column types for a measured-empty source whose types were only inferred.

    A header-only file has names and no values, so ``string`` on every column
    is a placeholder, not evidence. Judging it as evidence paused the route for
    a ``type_change`` and refused ``string → INTEGER`` into the table it loads
    every day. With no value to contradict it, the known contract stands: the
    declared type, else the last run's, else the live destination column the
    mapping writes by name. A column none of them knows keeps its placeholder.
    """
    out = dict(column_types or {})
    targets = {
        str(m.get("source") or ""): str(m.get("target") or "")
        for m in mappings or []
        if isinstance(m, dict)
    }
    for col in list(out):
        known = (
            column_type_or_none(declared or {}, col)
            or column_type_or_none(previous or {}, col)
            or column_type_or_none(destination or {}, targets.get(col) or col)
        )
        if known and str(known).strip():
            out[col] = str(known).strip()
    return out
