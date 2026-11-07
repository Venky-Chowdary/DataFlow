"""The Map's column spelling versus the one a SQL writer stored (D269).

SQL writers sanitise a target name for the physical column
(``resolve_target_columns`` -> ``sanitize_identifier(..., preserve_case=True)``:
``metadata.r10`` -> ``metadata_r10``, ``first name`` -> ``first_name``). Gate-8
speaks the Map's spelling. Two places crossed the two:

* the writer's ``reconcile_sample`` is keyed by the physical spelling, so the
  sample compare looked every such column up as missing (``source=None``);
* the transform proof selected ``"metadata.r10"`` from the destination — an
  unknown column, which SQLite answers with the literal text — so a correct
  load read as "source changed during run".
"""

from __future__ import annotations

from typing import Any


def rekey_writer_sample(
    rows: list[dict[str, Any]], mappings: list[dict[str, Any]] | None
) -> list[dict[str, Any]]:
    """Give a writer-stamped sample row the Map's source and target spellings.

    A writer keys its sample by the column it wrote (the physical, sanitised
    target); the sample compare reads the source side by the mapping's source
    name, as the stream's own source-keyed sample is. Missing spellings are
    filled from the target or its physical spelling; nothing is overwritten.
    """
    if not rows or not mappings:
        return rows
    plan: list[tuple[str, tuple[str, ...]]] = []
    for m in mappings:
        target = str((m or {}).get("target") or "")
        source = str((m or {}).get("source") or "")
        if not target:
            continue
        found_in = tuple(dict.fromkeys(c for c in (target, physical_column(target)) if c))
        for want in dict.fromkeys(c for c in (source, target) if c):
            others = tuple(c for c in found_in if c != want)
            if others:
                plan.append((want, others))
    if not plan:
        return rows
    out: list[dict[str, Any]] = []
    for row in rows:
        fixed = row
        for want, candidates in plan:
            if want in fixed:
                continue
            hit = next((c for c in candidates if c in fixed), None)
            if hit is not None:
                if fixed is row:
                    fixed = dict(row)
                fixed[want] = fixed[hit]
        out.append(fixed)
    return out


def physical_column(name: str) -> str:
    """The column a SQL writer created for the Map target ``name``."""
    from connectors.sql_identifiers import sanitize_identifier

    return sanitize_identifier(str(name), preserve_case=True) or str(name)


def physical_select_list(columns: list[str], quote: str) -> str:
    """``SELECT`` list reading each physical column under its Map name."""
    from connectors.sql_identifiers import quote_sql_identifier

    parts = []
    for name in columns:
        phys = physical_column(name)
        if phys == name:
            parts.append(quote_sql_identifier(name, quote))
        else:
            parts.append(
                f"{quote_sql_identifier(phys, quote)} AS {quote_sql_identifier(name, quote)}"
            )
    return ", ".join(parts)
