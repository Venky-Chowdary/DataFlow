"""Live database schema tools for Data Pilot — reuse transfer introspectors.

Do not invent parallel SQL. Every probe goes through:
  connector resolve → EndpointConfig → introspect_endpoint / schema_introspect.
"""

from __future__ import annotations

from typing import Any


class AmbiguousConnectorError(Exception):
    """More than one saved connector matches — ask the user which one."""

    def __init__(self, message: str, candidates: list[str] | None = None):
        super().__init__(message)
        self.message = message
        self.candidates = candidates or []


def _tool_result(name: str, *, success: bool, output: Any = None, error: str = ""):
    from .tools import ToolResult

    return ToolResult(name=name, success=success, output=output, error=error)


_TYPE_ALIASES = {
    "postgres": "postgresql",
    "pg": "postgresql",
    "psql": "postgresql",
    "mongo": "mongodb",
    "bq": "bigquery",
    "snowflake": "snowflake",
    "mysql": "mysql",
    "sqlite": "sqlite",
}


def _match_score(needle: str, label: str, ctype: str = "") -> float:
    """Rank how well a saved connector matches a user phrase. Higher is better."""
    n = (needle or "").strip().lower()
    label_l = (label or "").strip().lower()
    type_l = (ctype or "").strip().lower()
    if not n:
        return 0.0
    if label_l == n:
        return 100.0
    if label_l.startswith(n) or n.startswith(label_l):
        return 85.0 - abs(len(label_l) - len(n)) * 0.2
    if n in label_l:
        return 60.0 - max(0, len(label_l) - len(n)) * 0.4
    alias = _TYPE_ALIASES.get(n, n)
    if alias and (alias == type_l or alias in type_l or n == type_l or n in type_l):
        return 35.0
    # Token overlap: "local postgres" vs "Local Postgres Prod"
    n_toks = {t for t in n.replace("-", " ").split() if t}
    l_toks = {t for t in label_l.replace("-", " ").split() if t}
    if n_toks and n_toks <= l_toks:
        return 70.0 + len(n_toks) * 2.0
    if n_toks and l_toks:
        overlap = len(n_toks & l_toks) / len(n_toks)
        if overlap >= 0.5:
            return 45.0 + overlap * 20.0
    return 0.0


def _pick_connector(needle: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    """Pick a clear winner or raise AmbiguousConnectorError — never silent first-match."""
    scored: list[tuple[float, dict[str, Any]]] = []
    for d in candidates:
        score = _match_score(
            needle,
            str(d.get("name") or ""),
            str(d.get("type") or d.get("format") or ""),
        )
        if score > 0:
            scored.append((score, d))
    if not scored:
        raise AmbiguousConnectorError(
            f'No connector matched “{needle}”. Name a saved connector from Connectors.'
        )
    scored.sort(key=lambda x: (-x[0], str(x[1].get("name") or "").lower()))
    best_score, best = scored[0]
    # Exact / near-exact name wins alone
    if best_score >= 95.0:
        return best
    # Clear margin over runner-up
    if len(scored) == 1 or best_score >= scored[1][0] + 12.0:
        return best
    # Ambiguous — list top names in plain language
    names: list[str] = []
    for _, d in scored[:5]:
        name = str(d.get("name") or "").strip()
        if name and name not in names:
            names.append(name)
    listed = ", ".join(f"**{n}**" for n in names)
    raise AmbiguousConnectorError(
        f"Which connector did you mean? {listed}",
        candidates=names,
    )


def _connector_dict(connector_id: str = "", name: str = "") -> dict[str, Any] | None:
    """Resolve a saved connector by id or fuzzy name.

    Raises AmbiguousConnectorError when multiple connectors match equally.
    """
    cid = (connector_id or "").strip()
    needle = (name or "").strip().lower()

    try:
        from services.connector_store import get_connector, list_connectors

        if cid:
            c = get_connector(cid)
            if c:
                return c.to_dict() if hasattr(c, "to_dict") else dict(c.__dict__)
        if needle:
            pool: list[dict[str, Any]] = []
            for c in list_connectors():
                d = c.to_dict() if hasattr(c, "to_dict") else dict(c.__dict__)
                pool.append(d)
            return _pick_connector(needle, pool)
    except AmbiguousConnectorError:
        raise
    except Exception:
        pass

    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if cid:
            found = mongo.get_connector(cid)
            if found:
                return found
        if needle:
            pool = list(mongo.list_connectors() or [])
            return _pick_connector(needle, [c for c in pool if isinstance(c, dict)])
    except AmbiguousConnectorError:
        raise
    except Exception:
        pass
    return None


def _safe_connector(connector_id: str = "", name: str = "", tool: str = "schema"):
    """Resolve connector or return a failed ToolResult (plain-language errors)."""
    try:
        conn = _connector_dict(connector_id, name)
    except AmbiguousConnectorError as exc:
        return None, _tool_result(tool, success=False, error=exc.message)
    if not conn:
        return None, _tool_result(
            tool,
            success=False,
            error=(
                "Connector not found. Name a saved connector, e.g. "
                '"columns on airports in Local Postgres".'
            ),
        )
    return conn, None


def _endpoint_from_connector(conn: dict[str, Any], table: str = "") -> Any:
    from src.transfer.models import EndpointConfig

    fmt = str(conn.get("type") or conn.get("format") or "").lower()
    return EndpointConfig(
        kind="database",
        format=fmt,
        connector_id=str(conn.get("id") or conn.get("_id") or ""),
        host=str(conn.get("host") or ""),
        port=int(conn.get("port") or 0),
        database=str(conn.get("database") or ""),
        schema=str(conn.get("schema") or ""),
        table=table or "",
        collection=table or "",
        username=str(conn.get("username") or ""),
        password=str(conn.get("password") or ""),
        connection_string=str(conn.get("connection_string") or ""),
        warehouse=str(conn.get("warehouse") or ""),
        ssl=bool(conn.get("ssl")),
        auth_mode=str(conn.get("auth_mode") or ""),
        auth_role=str(conn.get("auth_role") or ""),
        auth_source=str(conn.get("auth_source") or ""),
    )


def _normalize_columns(info: dict[str, Any]) -> list[dict[str, Any]]:
    cols = info.get("columns") or []
    if isinstance(cols, dict):
        return [
            {"name": str(k), "inferred_type": str(v), "nullable": True}
            for k, v in cols.items()
        ]
    out: list[dict[str, Any]] = []
    for c in cols:
        if isinstance(c, dict):
            out.append({
                "name": str(c.get("name") or ""),
                "inferred_type": str(
                    c.get("inferred_type") or c.get("type") or c.get("data_type") or "TEXT"
                ),
                "nullable": bool(c.get("nullable", True)),
                "data_type": str(c.get("data_type") or c.get("column_type") or ""),
            })
        else:
            out.append({"name": str(c), "inferred_type": "TEXT", "nullable": True})
    return [c for c in out if c.get("name")]


def _schema_map(columns: list[dict[str, Any]]) -> dict[str, str]:
    return {c["name"]: c.get("inferred_type") or "TEXT" for c in columns}


def list_connector_objects(
    connector_id: str = "",
    connector_name: str = "",
    limit: int = 100,
):
    conn, err = _safe_connector(connector_id, connector_name, "list_connector_objects")
    if err:
        return err
    try:
        from src.transfer.endpoint_intelligence import introspect_endpoint

        endpoint = _endpoint_from_connector(conn)
        info = introspect_endpoint(endpoint)
        raw_objects = list(info.get("objects") or [])
        objects: list[str] = []
        for obj in raw_objects:
            if isinstance(obj, dict):
                name = str(obj.get("name") or obj.get("table") or obj.get("id") or "").strip()
            else:
                name = str(obj).strip()
            if name:
                objects.append(name)
        objects = objects[: max(1, min(int(limit or 100), 500))]
        return _tool_result(
            "list_connector_objects",
            success=True,
            output={
                "connector_id": str(conn.get("id") or conn.get("_id") or ""),
                "connector_name": conn.get("name"),
                "type": conn.get("type") or conn.get("format"),
                "connected": bool(info.get("connected")),
                "objects": objects,
                "count": len(objects),
                "message": info.get("message") or "",
                "database": conn.get("database"),
                "schema": conn.get("schema"),
            },
        )
    except Exception as e:
        return _tool_result(
            "list_connector_objects",
            success=False,
            error=f"Failed to list objects: {e}",
        )


def introspect_connector_schema(
    connector_id: str = "",
    connector_name: str = "",
    table: str = "",
):
    table = (table or "").strip()
    if not table:
        return _tool_result(
            "introspect_connector_schema",
            success=False,
            error='Which table or collection? Example: "schema of airports on Local Postgres".',
        )
    conn, err = _safe_connector(connector_id, connector_name, "introspect_connector_schema")
    if err:
        return err
    try:
        from src.transfer.adapters import resolve_connector_config
        from src.transfer.connector_capabilities import resolve_driver_type
        from services.schema_introspect import introspect_schema

        endpoint = _endpoint_from_connector(conn, table=table)
        cfg = resolve_connector_config(endpoint)
        db_type = resolve_driver_type(cfg.get("type") or endpoint.format or "")
        info = introspect_schema(
            db_type,
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 0) or 5432,
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            schema=str(cfg.get("schema") or "public"),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl")),
            warehouse=str(cfg.get("warehouse") or ""),
            table=table,
            catalog_type=str(cfg.get("type") or ""),
            auth_source=str(cfg.get("auth_source") or ""),
        )
        if not info.get("ok"):
            return _tool_result(
                "introspect_connector_schema",
                success=False,
                output={
                    "connector_name": conn.get("name"),
                    "table": table,
                    "type": db_type,
                },
                error=str(info.get("error") or f"Could not introspect {table}"),
            )
        columns = _normalize_columns(info)
        if not columns:
            # Some drivers return ok=True with empty columns when the table is missing.
            available = []
            for t in info.get("tables") or []:
                if isinstance(t, dict):
                    available.append(str(t.get("name") or ""))
                else:
                    available.append(str(t))
            available = [a for a in available if a][:20]
            hint = (
                f" Known tables include: {', '.join(available)}."
                if available
                else " Ask me to list the tables on that connector."
            )
            return _tool_result(
                "introspect_connector_schema",
                success=False,
                output={
                    "connector_name": conn.get("name"),
                    "table": table,
                    "type": db_type,
                    "tables": available,
                },
                error=f"No columns found for `{table}` on {conn.get('name')}.{hint}",
            )
        return _tool_result(
            "introspect_connector_schema",
            success=True,
            output={
                "connector_id": str(conn.get("id") or conn.get("_id") or ""),
                "connector_name": conn.get("name"),
                "type": db_type,
                "table": table,
                "schema": info.get("schema") or cfg.get("schema"),
                "database": cfg.get("database"),
                "columns": columns,
                "column_count": len(columns),
                "tables": info.get("tables") or [],
                "warnings": info.get("warnings") or [],
                "schema_map": _schema_map(columns),
            },
        )
    except Exception as e:
        return _tool_result(
            "introspect_connector_schema",
            success=False,
            error=f"Schema introspect failed: {e}",
        )


def diff_schemas(
    source_connector_id: str = "",
    source_connector_name: str = "",
    source_table: str = "",
    dest_connector_id: str = "",
    dest_connector_name: str = "",
    dest_table: str = "",
):
    src_table = (source_table or "").strip()
    dst_table = (dest_table or src_table).strip()
    if not src_table:
        return _tool_result(
            "diff_schemas",
            success=False,
            error=(
                "Need a source table. Example: "
                '"diff airports on Local Postgres vs data on LocalMongoDB".'
            ),
        )
    src = introspect_connector_schema(source_connector_id, source_connector_name, src_table)
    if not src.success:
        return _tool_result("diff_schemas", success=False, error=src.error)
    dst = introspect_connector_schema(dest_connector_id, dest_connector_name, dst_table)
    if not dst.success:
        return _tool_result("diff_schemas", success=False, error=dst.error)

    from services.schema_drift import classify_schema_change

    src_map = (src.output or {}).get("schema_map") or {}
    dst_map = (dst.output or {}).get("schema_map") or {}
    src_null = {
        c["name"]: bool(c.get("nullable", True))
        for c in (src.output or {}).get("columns") or []
    }
    dst_null = {
        c["name"]: bool(c.get("nullable", True))
        for c in (dst.output or {}).get("columns") or []
    }
    classification = classify_schema_change(
        {"columns": src_map, "nullable": src_null},
        {"columns": dst_map, "nullable": dst_null},
    )
    only_src = sorted(set(src_map) - set(dst_map))
    only_dst = sorted(set(dst_map) - set(src_map))
    shared = sorted(set(src_map) & set(dst_map))
    type_mismatches = [
        {"column": col, "source_type": src_map[col], "dest_type": dst_map[col]}
        for col in shared
        if str(src_map[col]).upper() != str(dst_map[col]).upper()
    ]
    return _tool_result(
        "diff_schemas",
        success=True,
        output={
            "source": {
                "connector": (src.output or {}).get("connector_name"),
                "table": src_table,
                "column_count": len(src_map),
            },
            "destination": {
                "connector": (dst.output or {}).get("connector_name"),
                "table": dst_table,
                "column_count": len(dst_map),
            },
            "shared_columns": shared,
            "only_in_source": only_src,
            "only_in_destination": only_dst,
            "type_mismatches": type_mismatches,
            "severity": classification.get("severity"),
            "additive": classification.get("additive") or [],
            "breaking": classification.get("breaking") or [],
        },
    )


def map_connector_schemas(
    source_connector_id: str = "",
    source_connector_name: str = "",
    source_table: str = "",
    dest_connector_id: str = "",
    dest_connector_name: str = "",
    dest_table: str = "",
    threshold: float = 0.85,
):
    """Live introspect both sides, then run the real semantic mapper."""
    src_table = (source_table or "").strip()
    dst_table = (dest_table or src_table).strip()
    if not src_table:
        return _tool_result(
            "map_connector_schemas",
            success=False,
            error=(
                "Need source table. Example: "
                '"map e2e_customers on Local Postgres to data on LocalMongoDB".'
            ),
        )

    src = introspect_connector_schema(source_connector_id, source_connector_name, src_table)
    if not src.success:
        return _tool_result("map_connector_schemas", success=False, error=src.error)

    dst_cols: list[str] = []
    dst_schemas: list[dict] = []
    dst_name = dest_connector_name
    if dest_connector_id or dest_connector_name:
        dst = introspect_connector_schema(dest_connector_id, dest_connector_name, dst_table)
        if dst.success:
            dst_cols = [c["name"] for c in (dst.output or {}).get("columns") or []]
            dst_schemas = [
                {
                    "name": c["name"],
                    "inferred_type": c.get("inferred_type") or "VARCHAR",
                    "samples": [],
                }
                for c in (dst.output or {}).get("columns") or []
            ]
            dst_name = (dst.output or {}).get("connector_name") or dest_connector_name
        elif not dst_table:
            return _tool_result("map_connector_schemas", success=False, error=dst.error)

    src_cols = [c["name"] for c in (src.output or {}).get("columns") or []]
    src_schemas = [
        {
            "name": c["name"],
            "inferred_type": c.get("inferred_type") or "VARCHAR",
            "samples": [],
        }
        for c in (src.output or {}).get("columns") or []
    ]
    if not src_cols:
        return _tool_result(
            "map_connector_schemas",
            success=False,
            error=f"Source `{src_table}` has no columns to map.",
        )

    from services.semantic_mapper import map_columns

    mappings = map_columns(
        src_cols,
        dst_cols,
        source_schemas=src_schemas,
        target_schemas=dst_schemas or None,
        threshold=float(threshold or 0.85),
    )
    mapped_src = {m.get("source") for m in mappings if m.get("source")}
    mapped_tgt = {m.get("target") for m in mappings if m.get("target")}
    unmapped_source = [c for c in src_cols if c not in mapped_src]
    unmapped_target = [c for c in dst_cols if c not in mapped_tgt]
    low_confidence = [
        {
            "source": m.get("source"),
            "target": m.get("target"),
            "confidence": m.get("confidence"),
            "reasoning": m.get("reasoning"),
        }
        for m in mappings
        if float(m.get("confidence") or 0) < 0.85
    ]
    type_risks = [
        {
            "source": m.get("source"),
            "target": m.get("target"),
            "source_type": m.get("source_type"),
            "target_type": m.get("target_type"),
        }
        for m in mappings
        if m.get("source_type") and m.get("target_type")
        and str(m.get("source_type")).upper() != str(m.get("target_type")).upper()
    ]
    return _tool_result(
        "map_connector_schemas",
        success=True,
        output={
            "source": {
                "connector": (src.output or {}).get("connector_name"),
                "table": src_table,
                "columns": src_cols,
            },
            "destination": {
                "connector": dst_name,
                "table": dst_table if dst_cols else None,
                "columns": dst_cols,
                "passthrough": not bool(dst_cols),
            },
            "mappings": [
                {
                    "source": m.get("source"),
                    "target": m.get("target"),
                    "confidence": round(float(m.get("confidence") or 0), 3),
                    "reasoning": m.get("reasoning"),
                    "source_type": m.get("source_type"),
                    "target_type": m.get("target_type"),
                }
                for m in mappings
            ],
            "mapping_count": len(mappings),
            "unmapped_source": unmapped_source,
            "unmapped_target": unmapped_target,
            "low_confidence": low_confidence,
            "type_risks": type_risks,
            "threshold": float(threshold or 0.85),
        },
    )
