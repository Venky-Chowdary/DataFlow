"""Schema introspection for document and time-series stores (Couchbase, InfluxDB).

Dispatched from ``services.schema_introspect.introspect_schema`` by ``db_type``.
"""

from __future__ import annotations

from typing import Any


def introspect_couchbase(**kwargs: Any) -> dict[str, Any]:
    """List Couchbase keyspaces, and sample one keyspace's document keys."""
    from connectors.couchbase import list_keyspaces, read_object

    keyspace = str(kwargs.get("table") or kwargs.get("database") or "").strip()
    common = {
        "host": str(kwargs.get("host") or ""),
        "port": int(kwargs.get("port") or 8093),
        "username": str(kwargs.get("username") or ""),
        "password": str(kwargs.get("password") or ""),
        "ssl": bool(kwargs.get("ssl")),
    }
    try:
        keyspaces = list_keyspaces(**common)
    except Exception as exc:  # noqa: BLE001 - a driver error is the introspection result
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    if not keyspace:
        return {"ok": True, "columns": [], "tables": keyspaces, "schema": ""}
    try:
        batch = read_object(cfg=common, object=keyspace, limit=20)
    except Exception as exc:  # noqa: BLE001 - a driver error is the introspection result
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "columns": [],
            "tables": keyspaces or [keyspace],
        }
    from services.schema_inference import infer_columns_from_rows

    # Documents are schemaless: any key may be absent on a document.
    columns = [
        {**col, "nullable": True}
        for col in infer_columns_from_rows(list(batch.headers or []), list(batch.rows or []))
    ]
    return {"ok": True, "columns": columns, "tables": keyspaces or [keyspace], "schema": keyspace}


# InfluxDB declares one storage type per field; a sample cannot see wider
# values later in the series, so the declaration is the column contract.
_INFLUX_FIELD_TYPES = {
    "float": "DOUBLE PRECISION",
    "integer": "BIGINT",
    "unsigned": "DECIMAL(20,0)",
    "string": "TEXT",
    "boolean": "BOOLEAN",
}


def introspect_influxdb(**kwargs: Any) -> dict[str, Any]:
    """List measurements, and type one measurement's time, tags and fields."""
    from connectors.influxdb import list_measurements, measurement_schema, read_object

    measurement = str(kwargs.get("table") or "").strip()
    common = {
        "host": str(kwargs.get("host") or ""),
        "port": int(kwargs.get("port") or 8086),
        "database": str(kwargs.get("database") or ""),
        "username": str(kwargs.get("username") or ""),
        "password": str(kwargs.get("password") or ""),
        "ssl": bool(kwargs.get("ssl")),
    }
    try:
        measurements = list_measurements(**common)
    except Exception as exc:  # noqa: BLE001 - a driver error is the introspection result
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    if not measurement:
        return {"ok": True, "columns": [], "tables": measurements, "schema": common["database"]}
    try:
        fields, tags = measurement_schema(measurement=measurement, **common)
        batch = read_object(cfg=common, object=measurement, limit=20)
    except Exception as exc:  # noqa: BLE001 - a driver error is the introspection result
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "columns": [],
            "tables": measurements or [measurement],
        }
    from services.schema_inference import infer_columns_from_rows

    tag_set = set(tags)
    columns: list[dict[str, Any]] = []
    for col in infer_columns_from_rows(list(batch.headers or []), list(batch.rows or [])):
        name = col.get("name")
        declared = _INFLUX_FIELD_TYPES.get(fields.get(name, ""))
        if declared:
            col = {
                **col,
                "inferred_type": declared,
                "confidence": 1.0,
                "notes": [*(col.get("notes") or []), f"declared InfluxDB field type {fields[name]}"],
            }
        elif name in tag_set:
            col = {**col, "inferred_type": "TEXT", "notes": [*(col.get("notes") or []), "InfluxDB tag (string)"]}
        # Every point carries ``time``; a field or tag may be absent on a point.
        columns.append({**col, "nullable": name != "time"})
    return {"ok": True, "columns": columns, "tables": measurements or [measurement], "schema": measurement}


STORE_INTROSPECTORS = {
    "couchbase": introspect_couchbase,
    "influxdb": introspect_influxdb,
}
