"""Canonical primary-key / identity-key resolution for preflight + writers.

One helper — G6 DDL, G8 dry-run reconcile, and G9 integrity must agree.
Connector-specific patches that reinvent ``*_id`` heuristics cause false blocks
(Mongo ``user_id`` dupes) or silent misses. Prefer explicit contract keys; fall
back only to exact ``id`` / ``_id`` (and mode-gated ``*_id`` for required-nulls).
"""

from __future__ import annotations

from typing import Any, Iterable, Literal

from services.db_type_utils import SCHEMALESS_DESTS, normalize_dest_kind

Purpose = Literal["uniqueness", "required_nulls"]

_EXACT_SQL_KEYS = ("id", "_id", "uuid", "pk", "key")
_EXACT_SCHEMALESS_KEYS = ("_id",)


def _mapping_pairs(mappings: Iterable[Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for m in mappings or []:
        if isinstance(m, dict):
            src = str(m.get("source") or "")
            tgt = str(m.get("target") or "")
        else:
            src = str(getattr(m, "source", "") or "")
            tgt = str(getattr(m, "target", "") or "")
        if src and tgt:
            pairs.append((src, tgt))
    return pairs


def resolve_identity_key(
    *,
    mappings: Iterable[Any],
    source_columns: list[str] | None = None,
    dest_kind: str = "",
    validation_mode: str = "strict",
    purpose: Purpose = "uniqueness",
    destination_pk_columns: list[str] | None = None,
) -> tuple[str | None, str | None]:
    """Return ``(source_column, target_column)`` for the identity key, or ``(None, None)``.

    Rules (connector-agnostic):
    * Schemaless (Mongo/Dynamo/Redis): only ``_id`` — other ``*_id`` fields are FKs.
    * Prefer introspected destination primary-key columns when mapped.
    * SQL uniqueness (G6/G8): exact ``id`` / ``_id`` on target, else source. Never
      auto-pick ``user_id`` / ``account_id`` when several FK-like columns compete.
    * Sole ``*_id`` only when no dest PK / exact key is available.
    * SQL required-nulls (G9): exact keys always; in ``strict``/``maximum`` also the
      first ``*_id`` source so high-assurance loads still enforce completeness.
    """
    kind = normalize_dest_kind(dest_kind)
    mode = (validation_mode or "strict").strip().lower()
    pairs = _mapping_pairs(mappings)
    srcs = [s for s, _ in pairs]
    if source_columns:
        for c in source_columns:
            if c not in srcs:
                srcs.append(c)
    tgts = [t for _, t in pairs]
    tgt_by_src = {s: t for s, t in pairs}
    src_by_tgt = {t: s for s, t in pairs}
    tgt_lower = {t.lower(): t for t in tgts}

    if kind in SCHEMALESS_DESTS:
        for t in tgts:
            if t.lower() == "_id":
                src = next((s for s, tt in pairs if tt == t), "_id")
                return src, t
        for s in srcs:
            if s.lower() == "_id":
                return s, tgt_by_src.get(s, s)
        return None, None

    # Destination contract wins: first mapped introspected PK column.
    for pk in destination_pk_columns or []:
        name = str(pk or "").strip()
        if not name:
            continue
        matched = tgt_lower.get(name.lower())
        if matched:
            return src_by_tgt.get(matched, matched), matched

    # Prefer exact target names first (destination contract wins).
    exact = _EXACT_SQL_KEYS if purpose == "required_nulls" else ("id", "_id")
    for key in exact:
        for t in tgts:
            if t.lower() == key:
                src = next((s for s, tt in pairs if tt == t), key)
                return src, t
        for s in srcs:
            if s.lower() == key:
                return s, tgt_by_src.get(s, s)

    # Sole ``*_id`` natural key (e.g. order_id-only extracts) — never invent a
    # PK when several FK-like columns compete (user_id + account_id).
    star_id_srcs = [
        s for s in srcs
        if s.lower().endswith("_id") and s.lower() not in {"id", "_id"}
    ]
    if purpose == "uniqueness" and len(star_id_srcs) == 1:
        s = star_id_srcs[0]
        return s, tgt_by_src.get(s, s)

    # Required-nulls: strict/maximum treat first *_id as a completeness candidate.
    if purpose == "required_nulls" and mode in {"strict", "maximum"} and star_id_srcs:
        s = star_id_srcs[0]
        return s, tgt_by_src.get(s, s)

    return None, None


def resolve_primary_key_target(
    mappings: Iterable[Any],
    dest_kind: str,
    *,
    validation_mode: str = "strict",
    destination_pk_columns: list[str] | None = None,
) -> str | None:
    """Target-side identity column for uniqueness probes (DDL / G8)."""
    _src, tgt = resolve_identity_key(
        mappings=mappings,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
        purpose="uniqueness",
        destination_pk_columns=destination_pk_columns,
    )
    return tgt


def resolve_primary_key_source(
    mappings: Iterable[Any],
    source_columns: list[str] | None,
    dest_kind: str,
    *,
    validation_mode: str = "strict",
    purpose: Purpose = "required_nulls",
) -> str | None:
    """Source-side identity column for integrity / null / duplicate audits."""
    src, _tgt = resolve_identity_key(
        mappings=mappings,
        source_columns=source_columns,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
        purpose=purpose,
    )
    return src
