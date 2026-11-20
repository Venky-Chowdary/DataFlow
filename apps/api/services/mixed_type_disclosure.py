"""Say when a few stray values turn a typed file column into text (D319, R3, R8).

Type inference for a file column widens to lossless text when any sampled
value is not of the column's type: one ``NOTANUMBER`` among integers makes
``qty`` a TEXT column. Keeping every value is the right default (pandas,
Fivetran and Airbyte file sources do the same), but it was silent: the plan
listed the bad date in the same row as an issue and said nothing about the
integer column it had just made text.

:func:`mixed_type_text_warnings` names each column the plan writes as text
whose sampled values are otherwise of one numeric or temporal type, how many
values are not, which ones, and the remedy the operator has: remap the column
to that type and accept cast & continue, so the stray rows are quarantined
with their reason instead of the whole column changing type. It only reads
the plan's own mappings and sample, so the plan says exactly what Execute
will create.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

#: Value classes a text column is "really" made of, with their plain name.
_TYPED_CLASSES = {
    "INTEGER": "integer",
    "DECIMAL": "number",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "TIMESTAMPTZ": "timestamp",
}


def _is_text(type_name: Any) -> bool:
    from services.type_system import normalize_logical_type

    return normalize_logical_type(str(type_name or "")) in {"string", "text"}


def _column_finding(column: str, values: list[str]) -> dict[str, Any] | None:
    from connectors.sql_temporal import INVALID_CALENDAR_MAX_SHARE
    from services.schema_inference import classify_value

    classes = [classify_value(v, field_name=column) for v in values]
    typed = Counter(c for c in classes if c in _TYPED_CLASSES)
    if not typed:
        return None
    dominant, hits = typed.most_common(1)[0]
    if dominant in {"INTEGER", "DECIMAL"}:
        hits = typed.get("INTEGER", 0) + typed.get("DECIMAL", 0)
        dominant = "DECIMAL" if typed.get("DECIMAL") else "INTEGER"
    strays = [v for v, c in zip(values, classes) if c not in _TYPED_CLASSES]
    if not strays or hits + len(strays) != len(values):
        return None
    if len(strays) >= INVALID_CALENDAR_MAX_SHARE * len(values):
        return None  # a column this mixed is text, not a typed column with strays
    return {
        "column": column,
        "dominant_type": dominant,
        "typed_values": hits,
        "stray_values": len(strays),
        "sampled_values": len(values),
        "examples": list(dict.fromkeys(strays))[:3],
    }


def mixed_type_text_warnings(
    mappings: list[dict] | None, sample_rows: list[dict] | None
) -> list[dict[str, Any]]:
    from services.value_serializer import evidence_text

    rows = [r for r in (sample_rows or []) if isinstance(r, dict)]
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    for m in mappings or []:
        if not isinstance(m, dict) or not _is_text(m.get("target_type")):
            continue
        if not _is_text(m.get("source_type") or "VARCHAR"):
            continue  # a typed source column is not inferred from its values
        source = str(m.get("source") or "")
        values = [
            evidence_text(r.get(source))
            for r in rows
            if r.get(source) is not None and str(r.get(source)).strip()
        ]
        finding = _column_finding(source, [v for v in values if v.strip()])
        if finding is None:
            continue
        kind = _TYPED_CLASSES[finding["dominant_type"]]
        target = str(m.get("target") or source)
        shown = ", ".join(repr(v) for v in finding["examples"])
        out.append({
            "id": "mixed_type_text",
            "message": (
                f"{target}: written as text because {finding['stray_values']} of "
                f"{finding['sampled_values']} sampled value(s) are not a {kind} "
                f"({shown}); the other {finding['typed_values']} are. Every value "
                f"is kept as text. To keep {target} a {kind} column, remap it to "
                f"{finding['dominant_type']} in Map and accept cast & continue: "
                "the rows holding those values are then quarantined with their reason."
            ),
            "details": {**finding, "target": target, "target_type": m.get("target_type")},
        })
    return out
