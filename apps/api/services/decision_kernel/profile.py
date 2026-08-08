"""Kernel Schema Profiling SSOT (Phase C6).

Map / invent / Validate consume profiles from this facade. Implementation
remains in ``data_profiler`` + ``decimal_observe`` until the god-module split —
never fork null%/cardinality/precision stats in writers or UI.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class ColumnProfile:
    """Width-aware column profile strip for Decision Artifact / Map invent."""

    name: str
    inferred_type: str = ""
    null_rate: float = 0.0
    distinct_count: int = 0
    distinct_ratio: float = 0.0
    min_value: Any = None
    max_value: Any = None
    pattern: str | None = None
    precision: int | None = None
    scale: int | None = None
    bit_width: int | None = None
    confidence: float = 0.0
    sample_is_population_proof: bool = False
    extras: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["extras"] = dict(self.extras or {})
        return d


@dataclass(frozen=True, slots=True)
class SchemaProfile:
    """Dataset-level profile — preflight screening, never population proof."""

    columns: tuple[ColumnProfile, ...]
    row_sample_size: int
    quality_score: float = 0.0
    primary_key_candidates: tuple[str, ...] = ()
    sample_is_population_proof: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "columns": {c.name: c.to_dict() for c in self.columns},
            "schema": {c.name: c.inferred_type for c in self.columns},
            "row_sample_size": self.row_sample_size,
            "quality_score": self.quality_score,
            "primary_key_candidates": list(self.primary_key_candidates),
            "sample_is_population_proof": False,
            "note": "Preflight screening only — full-population proof is checksum reconcile.",
        }


def _column_from_profiler(raw: Mapping[str, Any]) -> ColumnProfile:
    from services.decision_kernel.types import normalize_logical_type
    from services.type_system import integer_bit_width

    stats = raw.get("statistics") or raw.get("stats") or {}
    inferred = str(raw.get("inferred_type") or "")
    precision = stats.get("observed_precision")
    scale = stats.get("observed_scale")
    if precision is None and isinstance(stats.get("precision"), int):
        precision = stats.get("precision")
    if scale is None and isinstance(stats.get("scale"), int):
        scale = stats.get("scale")
    return ColumnProfile(
        name=str(raw.get("name") or ""),
        inferred_type=inferred,
        null_rate=float(raw.get("null_rate") or 0.0),
        distinct_count=int(raw.get("distinct_count") or raw.get("distinct") or 0),
        distinct_ratio=float(raw.get("distinct_ratio") or 0.0),
        min_value=stats.get("min"),
        max_value=stats.get("max"),
        pattern=raw.get("detected_pattern") or raw.get("pattern"),
        precision=int(precision) if precision is not None else None,
        scale=int(scale) if scale is not None else None,
        bit_width=integer_bit_width(inferred)
        if normalize_logical_type(inferred) == "integer"
        else None,
        confidence=float(raw.get("confidence") or 0.0),
        sample_is_population_proof=False,
        extras={
            k: v
            for k, v in raw.items()
            if k
            not in {
                "name",
                "inferred_type",
                "null_rate",
                "distinct",
                "distinct_count",
                "distinct_ratio",
                "pattern",
                "detected_pattern",
                "confidence",
                "stats",
                "statistics",
            }
        },
    )


def profile_column(name: str, values: list[Any], *, sample_limit: int = 200) -> ColumnProfile:
    """Profile one column (kernel SSOT)."""
    from services.data_profiler import profile_column as _profile_column

    raw = _profile_column(name, values, sample_limit=sample_limit)
    raw = {**raw, "name": name}
    return _column_from_profiler(raw)


def profile_columns(
    columns: list[str],
    rows: list[dict[str, Any]],
    *,
    sample_limit: int = 500,
) -> SchemaProfile:
    """Profile a dataset strip for Map invent + Validate screening."""
    from services.data_profiler import profile_dataset

    raw = profile_dataset(columns, rows, sample_limit=sample_limit)
    cols_raw = raw.get("columns") or {}
    profiles = tuple(
        _column_from_profiler({**(cols_raw.get(c) or {}), "name": c}) for c in columns
    )
    return SchemaProfile(
        columns=profiles,
        row_sample_size=int(raw.get("row_sample_size") or 0),
        quality_score=float(raw.get("quality_score") or 0.0),
        primary_key_candidates=tuple(raw.get("primary_key_candidates") or ()),
        sample_is_population_proof=False,
    )


__all__ = [
    "ColumnProfile",
    "SchemaProfile",
    "profile_column",
    "profile_columns",
]
