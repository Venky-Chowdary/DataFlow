"""Versioned schema registry and automated column-level lineage.

The registry records schemas observed from source/destination connectors and
maintains a directed graph of source-column -> target-column mappings for every
transfer job. This enables impact analysis, schema drift detection, and
automated downstream propagation.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir

_SCHEMAS_PATH = data_dir() / "schema_registry_schemas.json"
_LINEAGE_PATH = data_dir() / "schema_registry_lineage.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(path: Path, data: dict[str, Any]) -> None:
    write_json_atomic(path, data, indent=2)


@dataclass
class SchemaVersion:
    connector_type: str
    connector_id: str
    object_name: str
    version: int
    columns: list[dict[str, Any]]
    discovered_at: str = field(default_factory=_now)
    job_id: str = ""
    source_of_truth: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "connector_type": self.connector_type,
            "connector_id": self.connector_id,
            "object_name": self.object_name,
            "version": self.version,
            "columns": self.columns,
            "discovered_at": self.discovered_at,
            "job_id": self.job_id,
            "source_of_truth": self.source_of_truth,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SchemaVersion":
        return cls(
            connector_type=str(data.get("connector_type", "")),
            connector_id=str(data.get("connector_id", "")),
            object_name=str(data.get("object_name", "")),
            version=int(data.get("version", 0)),
            columns=list(data.get("columns", [])),
            discovered_at=str(data.get("discovered_at", _now())),
            job_id=str(data.get("job_id", "")),
            source_of_truth=bool(data.get("source_of_truth", False)),
        )


@dataclass
class LineageEdge:
    source_connector: str
    source_object: str
    source_column: str
    target_connector: str
    target_object: str
    target_column: str
    transform: str
    job_id: str
    created_at: str = field(default_factory=_now)
    edge_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "source_connector": self.source_connector,
            "source_object": self.source_object,
            "source_column": self.source_column,
            "target_connector": self.target_connector,
            "target_object": self.target_object,
            "target_column": self.target_column,
            "transform": self.transform,
            "job_id": self.job_id,
            "created_at": self.created_at,
        }


def _schema_key(connector_id: str, object_name: str, connector_type: str = "") -> str:
    return f"{connector_type}:{connector_id}:{object_name}".lower()


def _latest_version(connector_id: str, object_name: str, connector_type: str = "") -> int:
    data = _load(_SCHEMAS_PATH)
    key = _schema_key(connector_id, object_name, connector_type)
    versions = [int(v.get("version", 0)) for v in data.get(key, [])]
    return max(versions) if versions else 0


def register_schema(
    columns: list[dict[str, Any]],
    *,
    connector_type: str,
    connector_id: str,
    object_name: str,
    job_id: str = "",
    source_of_truth: bool = False,
) -> SchemaVersion:
    """Store a new schema version if it differs from the previous one."""
    data = _load(_SCHEMAS_PATH)
    key = _schema_key(connector_id, object_name, connector_type)
    prior = data.get(key, [])
    last = SchemaVersion.from_dict(prior[-1]) if prior else None

    # Normalize columns for comparison
    norm_cols = _normalize_columns(columns)
    last_norm = _normalize_columns(last.columns) if last else None

    if last_norm == norm_cols:
        # No change; optionally update source_of_truth and job_id
        if source_of_truth and last:
            last.source_of_truth = True
            last.job_id = job_id or last.job_id
            prior[-1] = last.to_dict()
            _save(_SCHEMAS_PATH, data)
        return last or SchemaVersion(connector_type, connector_id, object_name, 0, columns)

    version = (last.version if last else 0) + 1
    entry = SchemaVersion(
        connector_type=connector_type,
        connector_id=connector_id,
        object_name=object_name,
        version=version,
        columns=columns,
        job_id=job_id,
        source_of_truth=source_of_truth,
    )
    prior.append(entry.to_dict())
    data[key] = prior[-100:]  # keep last 100 versions
    _save(_SCHEMAS_PATH, data)
    return entry


def get_schema(
    connector_id: str,
    object_name: str,
    connector_type: str = "",
    version: int | None = None,
) -> SchemaVersion | None:
    """Return a specific or latest schema version."""
    data = _load(_SCHEMAS_PATH)
    key = _schema_key(connector_id, object_name, connector_type)
    versions = data.get(key, [])
    if not versions:
        return None
    if version is None:
        return SchemaVersion.from_dict(versions[-1])
    for v in versions:
        if int(v.get("version", 0)) == version:
            return SchemaVersion.from_dict(v)
    return None


def record_lineage(
    source: dict[str, str],
    target: dict[str, str],
    mappings: list[dict[str, Any]],
    job_id: str = "",
) -> list[LineageEdge]:
    """Store a lineage edge for each source→target column mapping.

    ``source`` and ``target`` are dicts with keys: connector_id, connector_type,
    object_name.
    """
    edges: list[LineageEdge] = []
    data = _load(_LINEAGE_PATH)
    if "edges" not in data:
        data["edges"] = []

    source_connector = source.get("connector_type", "") + ":" + source.get("connector_id", "")
    target_connector = target.get("connector_type", "") + ":" + target.get("connector_id", "")

    for m in mappings or []:
        src_col = str(m.get("source", "")).strip()
        tgt_col = str(m.get("target", "")).strip()
        if not src_col or not tgt_col:
            continue
        transform = str(m.get("transform", "")).strip()
        edge = LineageEdge(
            source_connector=source_connector,
            source_object=source.get("object_name", ""),
            source_column=src_col,
            target_connector=target_connector,
            target_object=target.get("object_name", ""),
            target_column=tgt_col,
            transform=transform,
            job_id=job_id,
        )
        edges.append(edge)
        data["edges"].append(edge.to_dict())

    data["edges"] = data["edges"][-5000:]
    _save(_LINEAGE_PATH, data)
    return edges


def get_lineage(
    *,
    connector_id: str | None = None,
    object_name: str | None = None,
    column: str | None = None,
    job_id: str | None = None,
    upstream: bool = False,
    downstream: bool = False,
) -> list[dict[str, Any]]:
    """Filter lineage edges. If ``upstream`` or ``downstream`` is set, traverse
    one hop in that direction from the given source or target column."""
    data = _load(_LINEAGE_PATH)
    edges = data.get("edges", [])
    matches = edges
    if connector_id:
        needle = connector_id.lower()
        matches = [
            e
            for e in matches
            if needle in e.get("source_connector", "").lower()
            or needle in e.get("target_connector", "").lower()
        ]
    if object_name:
        matches = [
            e
            for e in matches
            if object_name.lower() in (e.get("source_object", "").lower(), e.get("target_object", "").lower())
        ]
    if column:
        matches = [
            e
            for e in matches
            if column.lower() in (e.get("source_column", "").lower(), e.get("target_column", "").lower())
        ]
    if job_id:
        matches = [e for e in matches if e.get("job_id") == job_id]

    if not (upstream or downstream):
        return matches

    result: list[dict[str, Any]] = []
    for e in matches:
        result.append(e)
        if upstream:
            result.extend(
                edge
                for edge in edges
                if edge.get("target_connector") == e.get("source_connector")
                and edge.get("target_object") == e.get("source_object")
                and edge.get("target_column") == e.get("source_column")
            )
        if downstream:
            result.extend(
                edge
                for edge in edges
                if edge.get("source_connector") == e.get("target_connector")
                and edge.get("source_object") == e.get("target_object")
                and edge.get("source_column") == e.get("target_column")
            )
    return result


def detect_schema_drift(
    connector_id: str,
    object_name: str,
    current_columns: list[dict[str, Any]],
    connector_type: str = "",
) -> list[str]:
    """Compare current columns against the latest registered schema and report
    additions, removals, or type changes."""
    prior = get_schema(connector_id, object_name, connector_type)
    if prior is None:
        return []
    prior_cols = {c["name"].lower(): c for c in _normalize_columns(prior.columns)}
    current_cols = {c["name"].lower(): c for c in _normalize_columns(current_columns)}
    issues: list[str] = []
    for name in current_cols:
        if name not in prior_cols:
            issues.append(f"New column '{name}' appeared since version {prior.version}")
        elif current_cols[name].get("type", "").lower() != prior_cols[name].get("type", "").lower():
            issues.append(
                f"Column '{name}' type changed from {prior_cols[name].get('type')} "
                f"to {current_cols[name].get('type')} since version {prior.version}"
            )
    for name in prior_cols:
        if name not in current_cols:
            issues.append(f"Column '{name}' removed since version {prior.version}")
    return issues


def _normalize_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a sorted, comparable representation of a column list."""
    norm: list[dict[str, Any]] = []
    for c in columns:
        name = str(c.get("name") or c.get("column") or "").strip().lower()
        if not name:
            continue
        norm.append({
            "name": name,
            "type": str(c.get("type") or c.get("data_type") or "string").strip().lower(),
            "nullable": bool(c.get("nullable", True)),
            "primary_key": bool(c.get("primary_key", False)),
        })
    return sorted(norm, key=lambda x: x["name"])
