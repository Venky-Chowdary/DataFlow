"""Embedding / vector destination honesty — never fabricate zero vectors.

Airbyte-class trap: missing embeddings replaced with ``[0.0] * dim`` pollute
indexes and silently pass dimension checks. DataFlow quarantines instead.
"""

from __future__ import annotations

from typing import Any


def coerce_embedding(
    value: Any,
    *,
    expected_dimension: int | None = None,
) -> tuple[list[float] | None, str | None]:
    """Return ``(values, error)``. error set ⇒ caller must quarantine/skip.

    * Missing / empty → error (never invent zeros)
    * Non-numeric → error
    * Dimension mismatch vs expected → error
    """
    if value is None:
        return None, "missing embedding — refuse zero-vector fabrication"
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"null", "none", "[]"}:
            return None, "missing embedding — refuse zero-vector fabrication"
        if text.startswith("[") and text.endswith("]"):
            try:
                import json

                value = json.loads(text)
            except Exception:
                return None, "embedding string is not a JSON array"
        else:
            return None, "embedding string is not a JSON array"
    if not isinstance(value, (list, tuple)):
        return None, f"embedding must be a list, got {type(value).__name__}"
    if len(value) == 0:
        return None, "missing embedding — refuse zero-vector fabrication"
    out: list[float] = []
    for i, item in enumerate(value):
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            return None, f"embedding[{i}] is not numeric"
    if expected_dimension is not None and len(out) != int(expected_dimension):
        return None, (
            f"embedding dimension mismatch: got {len(out)}, "
            f"expected {expected_dimension}"
        )
    return out, None


def resolve_embedding_dimension(
    rows: list[dict[str, Any]],
    *,
    default: int | None = None,
) -> tuple[int | None, str | None]:
    """Infer dimension from the first valid embedding; refuse silent 384 default when empty."""
    dims: set[int] = set()
    for row in rows:
        values, err = coerce_embedding(row.get("embedding"))
        if err or not values:
            continue
        dims.add(len(values))
    if not dims:
        if default is not None:
            return default, "no embeddings present — using configured dimension only"
        return None, "no embeddings present — cannot invent dimension"
    if len(dims) > 1:
        return None, f"mixed embedding dimensions in batch: {sorted(dims)}"
    return next(iter(dims)), None
