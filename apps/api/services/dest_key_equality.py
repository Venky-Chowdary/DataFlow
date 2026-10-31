"""Distinct source keys the destination's comparison rules make equal (D205).

A text key is unique *as the source compares it*. MySQL, MariaDB and SQL
Server compare ``VARCHAR`` under PAD SPACE (``''`` = ``' '``, ``'a'`` =
``'a '``), and a ``_ci`` / ``CI_AS`` collation also equates ``'a'`` and
``'A'`` (an ``_ai`` one ``'cafe'`` and ``'café'``). Two such source rows are
one key to the destination's unique index: the upsert keeps one of them, or
the batch dies on a duplicate-key error after Validate passed.

The decided principle: never silently merge distinct source keys. This
module turns the destination key column's DDL into the equality the engine
applies (:func:`services.type_system.unique_equality_key`, the same rule G8
uses on the sample) so the population walk can name every colliding group
before anything is written.

``DATAFLOW_KEY_COLLATION_NO_PAD`` (off by default): a table DataFlow creates
on MySQL / MariaDB carries a case-sensitive source's equality as the NO PAD
binary collation (``utf8mb4_0900_bin`` / ``utf8mb4_nopad_bin``) instead of
``utf8mb4_bin`` (PAD SPACE), so ``'a'`` and ``'a '`` stay two keys. SQL
Server has no NO PAD collation: there the refusal is the only honest answer.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

NO_PAD_FLAG = "DATAFLOW_KEY_COLLATION_NO_PAD"

_NO_PAD_COLLATIONS = {"mysql": "utf8mb4_0900_bin", "mariadb": "utf8mb4_nopad_bin"}


def no_pad_key_collation_enabled() -> bool:
    from services.brand_env import getenv_brand_str

    return getenv_brand_str(NO_PAD_FLAG, "").strip().lower() in {"1", "true", "yes", "on"}


def created_case_sensitive_collation(charset: str, dialect: str) -> str:
    """The collation a created MySQL/MariaDB column carries for a CS source.

    ``utf8mb4_bin`` today; the NO PAD binary under the flag. While the flag is
    off, record what it would change.
    """
    legacy = f"{charset}_bin"
    no_pad = _NO_PAD_COLLATIONS.get(dialect, "") if charset == "utf8mb4" else ""
    if not no_pad:
        return legacy
    if no_pad_key_collation_enabled():
        return no_pad
    from services.would_apply_audit import audit_would_apply

    audit_would_apply(
        "create_ddl.would_use_no_pad_collation",
        resource=f"{dialect}:create",
        dedupe_key=dialect,
        details={"dialect": dialect, "today": legacy, "with_flag": no_pad,
                 "effect": "trailing-space keys ('a' / 'a ') stay distinct"},
        flag=NO_PAD_FLAG,
    )
    return legacy


def describe_equality(ddl_type: str, dest_kind: str) -> list[str]:
    """Human-readable rules under which this key column equates distinct text."""
    from services.type_system import (
        is_accent_insensitive_collation,
        is_case_insensitive_collation,
        normalize_logical_type,
        trailing_spaces_insignificant_for_unique,
    )

    if normalize_logical_type(ddl_type) not in {"string", "text"}:
        return []
    rules: list[str] = []
    if trailing_spaces_insignificant_for_unique(ddl_type, dest_kind=dest_kind):
        rules.append("trailing spaces are ignored (PAD SPACE)")
    if is_case_insensitive_collation(ddl_type):
        rules.append("case is ignored")
    if is_accent_insensitive_collation(ddl_type):
        rules.append("accents are ignored")
    return rules


def key_equalizers(
    mappings: Iterable[Mapping[str, Any]] | None,
    key_sources: Iterable[str],
    dest_types: Mapping[str, str] | None,
    dest_kind: str,
    source_db: str = "",
) -> dict[str, tuple[Callable[[str], str], str, list[str]]]:
    """``{source key column: (normalize, dest ddl, rules)}`` where the
    destination equates text the source keeps distinct.

    An existing column is judged by its catalog type. A column DataFlow will
    create is judged by the collation CREATE stamps on it, not the bare Map type.
    """
    from services.collation_carry import apply_dest_native_collation
    from services.mapping_constraints import write_mappings
    from services.source_engine_scope import active_source_engine
    from services.type_system import unique_equality_key

    wanted = {str(c).casefold(): str(c) for c in key_sources}
    folded_types = {str(k).casefold(): str(v or "") for k, v in (dest_types or {}).items()}
    out: dict[str, tuple[Callable[[str], str], str, list[str]]] = {}
    for mapping in write_mappings([dict(m) for m in (mappings or []) if isinstance(m, Mapping)]):
        source = str(mapping.get("source") or "").strip()
        if source.casefold() not in wanted:
            continue
        target = str(mapping.get("target") or source).strip()
        ddl = folded_types.get(target.casefold(), "")
        if not ddl:
            planned = str(mapping.get("dest_native_type") or mapping.get("target_type") or "")
            ddl = apply_dest_native_collation(
                planned,
                dest_dialect=dest_kind,
                source_dialect=source_db or active_source_engine(),
                source_type=str(mapping.get("source_type") or ""),
            )
        rules = describe_equality(ddl, dest_kind)
        if not rules:
            continue

        def _normalize(text: str, _ddl: str = ddl) -> str:
            return unique_equality_key(text, _ddl, dest_kind=dest_kind)

        out[wanted[source.casefold()]] = (_normalize, ddl, rules)
    return out


def collation_name(ddl_type: str) -> str:
    from services.type_system import parse_collation

    name = (parse_collation(ddl_type) or "").strip()
    if name:
        return name
    match = re.match(r"\s*([A-Za-z0-9_]+(?:\s*\([^)]*\))?)", ddl_type or "")
    return match.group(1) if match else (ddl_type or "")


def remediation(dest_kind: str) -> str:
    kind = (dest_kind or "").strip().lower()
    if kind in {"sqlserver", "mssql", "azure_sql"}:
        return (
            "SQL Server ignores trailing spaces in every collation; give the key "
            "column a binary collation (Latin1_General_100_BIN2) for case, and "
            "transform keys that differ only by trailing spaces (or key on "
            "another column)."
        )
    return (
        "Give the key column a binary NO PAD collation (MySQL utf8mb4_0900_bin, "
        "MariaDB utf8mb4_nopad_bin; any *_bin for case alone), or transform the "
        f"keys so they differ under the destination's rules. Tables Datawrap "
        f"creates use that collation when {NO_PAD_FLAG}=1."
    )


def destination_equality_message(
    probe: Mapping[str, Any], *, count: int, label: str, transforms: str, dest_kind: str
) -> str:
    """Name the colliding source keys, the destination rule and the remedy."""
    rules = "; ".join(
        f"{col} as {collation_name(str(eq.get('ddl') or ''))}: {', '.join(eq.get('rules') or [])}"
        for col, eq in (probe.get("destination_equality") or {}).items()
    )
    groups = "; ".join(
        " = ".join(
            repr(parts[0]) if len(parts) == 1 else repr(tuple(parts))
            for parts in (c.get("source_key_parts") or [])
        )
        for c in (probe.get("collisions") or [])[:5]
    )
    via = f" after the key's transform ({transforms})" if transforms else ""
    if any(c.get("destination_collation") for c in (probe.get("collisions") or [])):
        # The destination's own GROUP BY said so (zero-weight characters,
        # expansions): its collation is the rule, not only the modelled ones.
        rules = (f"{rules}; " if rules else "") + (
            "the destination's collation compares them equal (checked on the "
            "destination; e.g. SQL Server version-80 collations give "
            "supplementary characters no weight)"
        )
    return (
        f"{label}: {count:,} source key(s) are distinct in the source but equal "
        f"under the destination's comparison{via} ({rules}) - colliding keys: "
        f"{groups}. The destination's unique key would merge them (an upsert "
        "keeps one row, or the load fails on a duplicate key), so Datawrap "
        f"refuses rather than merge distinct keys. {remediation(dest_kind)}"
    )
