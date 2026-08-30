"""Destination/source schema introspection for the transfer adapters.

Extracted from ``src.transfer.adapters`` (Phase F8 size freeze) with no
behaviour change: the shape of a table is read from the destination catalog
first and only falls back to inference when the catalog cannot answer, so a
destination that exists is never described from source guesses. ``adapters``
re-exports these names, so no call site changes.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

def _columns_type_and_nullability(
    columns: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, bool]]:
    """Split introspect column dicts into type map + nullable map."""
    types, nulls, _defaults, _ident, _gen, _coll = _columns_schema_meta(columns)
    return types, nulls


def _columns_schema_meta(
    columns: list[dict[str, Any]],
) -> tuple[
    dict[str, str],
    dict[str, bool],
    dict[str, str],
    list[str],
    list[str],
    dict[str, str],
]:
    """Type / nullability / defaults / identity / generated / collation maps."""
    types: dict[str, str] = {}
    nulls: dict[str, bool] = {}
    defaults: dict[str, str] = {}
    identity: list[str] = []
    generated: list[str] = []
    collations: dict[str, str] = {}
    for col in columns:
        name = col.get("name")
        if not name:
            continue
        key = str(name)
        types[key] = str(col.get("inferred_type") or "TEXT")
        if "nullable" in col:
            nulls[key] = bool(col["nullable"])
        dflt = col.get("default")
        if dflt is not None and str(dflt).strip() != "":
            defaults[key] = str(dflt)
        if col.get("is_identity"):
            identity.append(key)
        inferred_u = str(col.get("inferred_type") or "").upper()
        if "GENERATED ALWAYS" in inferred_u or str(col.get("generation") or "").lower() == "always":
            if key not in generated:
                generated.append(key)
        coll = str(col.get("collation") or "").strip()
        if coll:
            collations[key] = coll
    return types, nulls, defaults, identity, generated, collations


def _introspect_table_schema_rich(
    db_type: str,
    cfg: dict[str, Any],
    table: str,
    headers: list[str],
    records: list[dict] | None = None,
    *,
    strict_namespace: bool = False,
) -> tuple[dict[str, str], dict[str, bool], dict[str, Any]]:
    """Load column types + nullability + unique keys from INFORMATION_SCHEMA.

    ``strict_namespace`` is required for destination probes so missing tables in
    the chosen DB/schema are not "healed" from another namespace on the host.
    Nullability feeds G3 NOT NULL contracts — never invent nullable=True when
    the catalog says otherwise.
    Third return value carries ``primary_key_columns`` / ``unique_keys`` when the
    catalog exposes them (PG/MySQL today), plus Property 6 defaults/identity.
    """
    empty_keys: dict[str, Any] = {
        "primary_key_columns": [],
        "unique_keys": [],
        "defaults": {},
        "identity_columns": [],
        "generated_columns": [],
        "collations": {},
        "charsets": {},
        "physical_storage": None,
        "check_constraints_meta": None,
        "indexes_meta": None,
        "warnings": [],
    }

    def _keys_from_info(
        payload: dict[str, Any],
        *,
        defaults: dict[str, str] | None = None,
        identity_columns: list[str] | None = None,
        generated_columns: list[str] | None = None,
        collations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        warnings = [str(w) for w in (payload.get("warnings") or []) if w]
        # Catalog message often carries the advisory-key honesty note when
        # ``warnings`` was folded into ``message`` by introspect.
        msg = str(payload.get("message") or "").strip()
        if msg and any(
            token in msg.lower()
            for token in ("not enforced", "informational", "advisory")
        ):
            if msg not in warnings:
                warnings.append(msg)
        charsets: dict[str, str] = {}
        for col in payload.get("columns") or []:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name") or "").strip()
            cs = str(col.get("charset") or "").strip()
            if name and cs:
                charsets[name] = cs
        return {
            "primary_key_columns": list(payload.get("primary_key_columns") or []),
            "unique_keys": list(payload.get("unique_keys") or []),
            "defaults": dict(defaults or {}),
            "identity_columns": list(identity_columns or []),
            "generated_columns": list(generated_columns or []),
            "collations": dict(collations or {}),
            "charsets": charsets,
            "foreign_keys": list(payload.get("foreign_keys") or []),
            "check_constraints": list(payload.get("check_constraints") or []),
            # None when the dialect/probe never measured placement — the
            # certificate reports "unknown", never "no partitioning".
            "physical_storage": payload.get("physical_storage"),
            # None when the CHECK catalog was never read — the certificate says
            # "unmeasured", never "no CHECK constraints".
            "check_constraints_meta": payload.get("check_constraints_meta"),
            # None when the index catalog was never read — the certificate says
            # "unmeasured", never "no secondary indexes".
            "indexes_meta": payload.get("indexes_meta"),
            "warnings": warnings,
        }

    if db_type == "generic_sql":
        try:
            from connectors.generic_sql import introspect_table_schema

            info = introspect_table_schema(cfg, table)
            if info.get("ok") and info.get("columns"):
                types, nulls, defaults, ident, gen, coll = _columns_schema_meta(
                    info["columns"]
                )
                return types, nulls, _keys_from_info(
                    info,
                    defaults=defaults,
                    identity_columns=ident,
                    generated_columns=gen,
                    collations=coll,
                )
        except Exception as exc:
            logger.debug("table schema introspection failed: %s", exc, exc_info=exc)

    from connectors.generic_sql import connection_options
    from services.dialect_profiles import schema_from_cfg
    from services.schema_introspect import introspect_schema

    info = introspect_schema(
        db_type,
        host=cfg.get("host", ""),
        port=int(
            cfg.get("port")
            or (
                3306
                if db_type == "mysql"
                else 1433
                if db_type == "sqlserver"
                else 1521
                if db_type == "oracle"
                else 5439
                if db_type == "redshift"
                else 5432
            )
        ),
        database=cfg.get("database", ""),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        schema=schema_from_cfg(db_type, cfg),
        connection_string=cfg.get("connection_string", ""),
        # Prefer connector ssl exactly as list/probe use it. Do not default True
        # when the key is missing — that caused managed vs local mismatch where
        # SHOW TABLES succeeded (ssl=False) but INFORMATION_SCHEMA failed (ssl=True),
        # leaving Map with "destination schema unavailable" for an existing table.
        ssl=bool(cfg.get("ssl", False)),
        warehouse=cfg.get("warehouse", ""),
        table=table,
        catalog_type=cfg.get("type", ""),
        auth_source=cfg.get("auth_source", ""),
        api_key=cfg.get("api_key", ""),
        role=str(cfg.get("role") or ""),
        auth_role=str(cfg.get("auth_role") or ""),
        private_key=str(cfg.get("private_key") or ""),
        strict_namespace=strict_namespace,
        # TLS / service-name / driver keywords the writer will use.
        **connection_options(cfg),
    )
    if info.get("ok") and info.get("columns"):
        types, nulls, defaults, ident, gen, coll = _columns_schema_meta(info["columns"])
        return types, nulls, _keys_from_info(
            info,
            defaults=defaults,
            identity_columns=ident,
            generated_columns=gen,
            collations=coll,
        )

    # Retry once with flipped SSL when the first probe failed (common when the
    # connector ssl flag does not match the host's TLS requirement).
    if not info.get("ok") and db_type in (
        "postgresql",
        "redshift",
        "mysql",
        "sqlserver",
    ):
        flipped = not bool(cfg.get("ssl", False))
        info_retry = introspect_schema(
            db_type,
            host=cfg.get("host", ""),
            port=int(
                cfg.get("port")
                or (
                    3306
                    if db_type == "mysql"
                    else 1433
                    if db_type == "sqlserver"
                    else 1521
                    if db_type == "oracle"
                    else 5439
                    if db_type == "redshift"
                    else 5432
                )
            ),
            database=cfg.get("database", ""),
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=schema_from_cfg(db_type, cfg),
            connection_string=cfg.get("connection_string", ""),
            ssl=flipped,
            warehouse=cfg.get("warehouse", ""),
            table=table,
            catalog_type=cfg.get("type", ""),
            auth_source=cfg.get("auth_source", ""),
            api_key=cfg.get("api_key", ""),
            strict_namespace=strict_namespace,
            **connection_options(cfg),
        )
        if info_retry.get("ok") and info_retry.get("columns"):
            types, nulls, defaults, ident, gen, coll = _columns_schema_meta(
                info_retry["columns"]
            )
            return types, nulls, _keys_from_info(
                info_retry,
                defaults=defaults,
                identity_columns=ident,
                generated_columns=gen,
                collations=coll,
            )

    # Fallback: infer logical types from the sample records we already have in hand.
    # This is essential for schemaless sources (MongoDB, DynamoDB, Redis) whose
    # stored values may be strings but whose content is numeric, boolean, JSON, etc.
    keys = dict(empty_keys)
    if not info.get("ok"):
        err = str(info.get("error") or "").strip()
        if err:
            keys["probe_error"] = err
    if records:
        try:
            from services.file_parser import FileParser

            inferred = FileParser.infer_schema(records)
            if inferred:
                return {h: inferred.get(h, "TEXT") for h in headers}, {}, keys
        except Exception as exc:
            logger.debug("record schema inference failed: %s", exc, exc_info=exc)
    if headers:
        return {h: "TEXT" for h in headers}, {}, keys
    return {}, {}, keys


def _introspect_table_schema(
    db_type: str,
    cfg: dict[str, Any],
    table: str,
    headers: list[str],
    records: list[dict] | None = None,
    *,
    strict_namespace: bool = False,
) -> dict[str, str]:
    """Load column types from INFORMATION_SCHEMA or infer from sample records."""
    types, _nulls, _keys = _introspect_table_schema_rich(
        db_type,
        cfg,
        table,
        headers,
        records=records,
        strict_namespace=strict_namespace,
    )
    return types
