"""Embedding / vector destination honesty — never fabricate zero vectors.

Airbyte-class trap: missing embeddings replaced with ``[0.0] * dim`` pollute
indexes and silently pass dimension checks. Datawrap quarantines instead.
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
    from services.transform_engine import vector_component_carrier

    out: list[float] = []
    for i, item in enumerate(value):
        bound = vector_component_carrier(item)
        if bound is None:
            return None, (
                f"embedding[{i}] cannot bind {item!r} — refuse invent"
            )
        out.append(bound)
    if expected_dimension is not None and len(out) != int(expected_dimension):
        return None, (
            f"embedding dimension mismatch: got {len(out)}, "
            f"expected {expected_dimension}"
        )
    return out, None


def embedding_reject_reason(row: dict[str, Any], coerce_err: str | None) -> str:
    """Prefer vectorize-stamped parse failures over generic coerce messages."""
    stamped = str(row.get("_df_embed_error") or "").strip()
    if stamped:
        return stamped
    return coerce_err or "invalid embedding"


def coerce_chunk_index(value: Any, *, default: int = 0) -> int:
    """Normalize vector ``chunk_index`` for identity hashing / metadata.

    Missing / blank → ``default`` (single-chunk docs). Non-integral floats,
    booleans, and garbage strings refuse — ``int(3.7)`` / ``int(True)`` would
    silently invent wrong chunk identity under at-least-once upsert.
    """
    if value is None:
        return int(default)
    if isinstance(value, bool):
        raise ValueError(
            f"chunk_index refused boolean {value!r} — refuse invent"
        )
    if isinstance(value, int):
        if value < 0:
            raise ValueError(
                f"chunk_index refused negative {value!r} — refuse invent"
            )
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise ValueError(
                f"chunk_index refused non-finite {value!r} — refuse invent"
            )
        if not value.is_integer():
            raise ValueError(
                f"chunk_index refused fractional {value!r} — refuse truncation invent"
            )
        n = int(value)
        if n < 0:
            raise ValueError(
                f"chunk_index refused negative {n!r} — refuse invent"
            )
        return n
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return int(default)
        from connectors.sql_bind import coerce_integer_wire

        n = coerce_integer_wire(token, ddl_type="INTEGER")
        if n is None:
            return int(default)
        if not isinstance(n, int):
            raise ValueError(
                f"chunk_index refused {value!r} — refuse invent"
            )
        if n < 0:
            raise ValueError(
                f"chunk_index refused negative {n!r} — refuse invent"
            )
        return n
    raise ValueError(
        f"chunk_index refused {type(value).__name__} {value!r} — refuse invent"
    )


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
