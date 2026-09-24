"""Infer workbook column roles from the file + live schemas.

Header aliases are a *prior*, not the product. The brain is: what do the
cells name, against the source and destination the operator already selected?

An LLM is not used here. Invented roles would silently remap the wrong
column. Weak inferences (name hints, left-to-right layout) stay visible
and fail closed at compile time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .classify import classify_rule, parse_lookup
from .normalize import canonical_header, fold, resolve_name, split_qualified

ROLES = (
    "source_table",
    "source_column",
    "dest_table",
    "dest_column",
    "rule",
    "rule_name",
    "join_from",
    "join_on",
    "join_type",
    "lookup_from",
    "lookup_to",
)

# Weak prior only. Content-vs-schema wins when schemas are present.
_HINTS: dict[str, frozenset[str]] = {
    "source_table": frozenset({"source", "src", "from", "table", "object"}),
    "source_column": frozenset({
        "source", "src", "from", "orig", "origin", "legacy", "inbound",
        "field", "column", "col", "attr", "attribute",
    }),
    "dest_table": frozenset({"dest", "destination", "target", "tgt"}),
    "dest_column": frozenset({
        "dest", "destination", "target", "tgt", "landing", "outbound", "out",
    }),
    "rule": frozenset({
        "rule", "logic", "formula", "transform", "how", "convert",
        "instruction", "action", "conversion", "mapping",
    }),
    "join_from": frozenset({"joinfrom", "lefttable"}),
    "join_on": frozenset({"joinon", "leftkey", "rightkey", "joinkey"}),
    "join_type": frozenset({"jointype"}),
    "lookup_from": frozenset({"old", "fromcode", "inbound", "legacycode"}),
    "lookup_to": frozenset({"new", "tocode", "outbound"}),
    "rule_name": frozenset({
        "rulename", "namedrule", "ruleid", "mapplet", "macroname", "reusablerule",
        "validationid", "checkid", "constraintid",
    }),
}

# Single-token priors that collide across roles ("to", "from").
_WEAK_HINTS = frozenset({"to", "from", "src", "tgt", "out", "col", "how"})

_IDENT = re.compile(r"^[A-Za-z_][\w.]*$")
_HEADER_TOKENS = re.compile(r"[^a-z0-9]+")

# Methods that are grounded enough to auto-execute after bind.
GROUNDED_METHODS = frozenset({"alias", "schema", "rule_pattern"})


@dataclass
class InferredRoles:
    """header → role plus the evidence the operator can read."""

    roles: dict[str, str] = field(default_factory=dict)
    evidence: list[dict[str, Any]] = field(default_factory=list)

    @property
    def methods(self) -> dict[str, str]:
        return {item["role"]: str(item.get("method") or "") for item in self.evidence}


def infer_header_roles(
    headers: list[str],
    cell_rows: list[dict[str, str]],
    source_columns: list[str] | None = None,
    dest_columns: list[str] | None = None,
) -> InferredRoles:
    """Assign each header a compiler role, or leave it unassigned."""
    clean = [h for h in headers if str(h or "").strip()]
    if not clean:
        return InferredRoles()

    assigned: dict[str, str] = {}
    used: set[str] = set()
    evidence: list[dict[str, Any]] = []

    def take(role: str, header: str, method: str, confidence: float, reason: str) -> None:
        if role in assigned or header in used:
            return
        assigned[role] = header
        used.add(header)
        evidence.append({
            "header": header,
            "role": role,
            "method": method,
            "confidence": round(float(confidence), 3),
            "reason": reason,
        })

    alias_hits: dict[str, list[str]] = {}
    for header in clean:
        role = canonical_header(header)
        if role:
            alias_hits.setdefault(role, []).append(header)
    for role, hits in alias_hits.items():
        if len(hits) == 1:
            take(role, hits[0], "alias", 0.72, f"Header “{hits[0]}” matches a known mapping-spec name.")

    unused = [h for h in clean if h not in used]
    src = [c for c in (source_columns or []) if c]
    dst = [c for c in (dest_columns or []) if c]

    def values_for(header: str) -> list[str]:
        return [str(row.get(header) or "").strip() for row in cell_rows]

    content: dict[str, dict[str, float]] = {role: {} for role in ROLES if role not in assigned}
    rule_scores: dict[str, float] = {}
    ident_scores: dict[str, float] = {}
    for header in unused:
        values = values_for(header)
        ident_scores[header] = _identifier_rate(values)
        if "source_column" in content:
            content["source_column"][header] = _schema_hit_rate(values, src)
        if "dest_column" in content:
            content["dest_column"][header] = _schema_hit_rate(values, dst)
        if "rule" in content:
            rule_scores[header] = _rule_like_rate(values)
            content["rule"][header] = rule_scores[header]
        if "lookup_from" in content:
            content["lookup_from"][header] = _short_code_rate(values)
        if "lookup_to" in content:
            content["lookup_to"][header] = _short_code_rate(values)

    for role, method, reason_for in (
        ("source_column", "schema", "values match the selected source schema"),
        ("dest_column", "schema", "values match the selected destination schema"),
        ("rule", "rule_pattern", "values look like closed-form mapping instructions"),
    ):
        if role not in content:
            continue
        winner, score = _unique_winner(content[role], used)
        if winner:
            take(role, winner, method, score, f"“{winner}” {reason_for} ({int(score * 100)}% of filled cells).")

    unused = [h for h in clean if h not in used]
    if "rule_name" not in assigned and "rule" in assigned:
        named = [
            header for header in unused
            if ident_scores.get(header, 0.0) >= 0.7
            and content.get("source_column", {}).get(header, 0.0) < 0.4
            and content.get("dest_column", {}).get(header, 0.0) < 0.4
        ]
        if len(named) == 1:
            take(
                "rule_name",
                named[0],
                "schema",
                ident_scores.get(named[0], 0.7),
                f"“{named[0]}” looks like reusable rule names next to an expression column.",
            )

    unused = [h for h in clean if h not in used]
    short_unused = [
        header for header in unused
        if max(
            content.get("lookup_from", {}).get(header, 0.0),
            content.get("lookup_to", {}).get(header, 0.0),
        ) >= 0.55
    ]
    if len(short_unused) >= 2:
        for role, reason_for in (
            ("lookup_from", "values look like short inbound codes"),
            ("lookup_to", "values look like short outbound codes"),
        ):
            if role not in content:
                continue
            winner, score = _unique_winner(content[role], used)
            if winner:
                take(role, winner, "schema", score, f"“{winner}” {reason_for} ({int(score * 100)}% of filled cells).")

    unused = [h for h in clean if h not in used]
    if "source_column" not in assigned or "dest_column" not in assigned:
        dual: list[tuple[str, float, float]] = []
        for header in unused:
            values = values_for(header)
            s = _schema_hit_rate(values, src) if src else 0.0
            d = _schema_hit_rate(values, dst) if dst else 0.0
            if max(s, d) >= 0.55:
                dual.append((header, s, d))
        dual.sort(key=lambda item: (-max(item[1], item[2]), clean.index(item[0]) if item[0] in clean else 0))
        if "source_column" not in assigned and dual:
            header, s, _d = dual[0]
            take(
                "source_column",
                header,
                "schema",
                s,
                f"“{header}” values match the selected source schema ({int(s * 100)}%).",
            )
            dual = dual[1:]
        if "dest_column" not in assigned and dual:
            header, _s, d = dual[0]
            take(
                "dest_column",
                header,
                "schema",
                d,
                f"“{header}” values match the selected destination schema ({int(d * 100)}%).",
            )

    unused = [h for h in clean if h not in used]
    for header in unused:
        role, strength = _unique_hint(header, set(assigned))
        if role:
            take(
                role,
                header,
                "hint",
                min(0.62, 0.45 + strength / 20),
                f"“{header}” is a unique name hint for {role.replace('_', ' ')} — confirm, it is not schema-grounded.",
            )

    unused = [h for h in clean if h not in used]
    if unused and ("source_column" not in assigned or "dest_column" not in assigned or "rule" not in assigned):
        _positional_fallback(unused, assigned, used, evidence, ident_scores, rule_scores, clean)

    return InferredRoles(
        roles={header: role for role, header in assigned.items()},
        evidence=evidence,
    )


def apply_roles(row: dict[str, Any], inferred: InferredRoles | dict[str, str]) -> dict[str, Any]:
    """Fill compiler keys from inferred roles without erasing a prior alias."""
    roles = inferred.roles if isinstance(inferred, InferredRoles) else inferred
    methods = inferred.methods if isinstance(inferred, InferredRoles) else {}
    cells: dict[str, str] = dict(row.get("_cells") or {})
    next_row = dict(row)
    for header, role in roles.items():
        text = str(cells.get(header) or next_row.get(role) or "").strip()
        if not text:
            continue
        if role == "rule":
            next_row[role] = next_row.get(role) or text
        elif not next_row.get(role):
            next_row[role] = text
    next_row["_role_methods"] = {
        **dict(next_row.get("_role_methods") or {}),
        **methods,
    }
    _qualify_bound_source(next_row)
    return next_row


def _qualify_bound_source(mapped: dict[str, Any]) -> None:
    spoken = str(mapped.get("source_column") or "")
    table, column = split_qualified(spoken)
    if table and column:
        mapped["source_column"] = column
        if not mapped.get("source_table"):
            mapped["source_table"] = table


def ground_workbook_rows(
    rows: list[dict[str, Any]],
    source_columns: list[str] | None = None,
    dest_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Infer roles per header-set, apply them, return merged evidence."""
    from collections import defaultdict

    groups: dict[tuple[str, tuple[str, ...]], list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        sheet = str(row.get("_sheet") or "")
        headers = tuple(str(h) for h in (row.get("_headers") or []) if str(h).strip())
        groups[(sheet, headers)].append(index)

    evidence: list[dict[str, Any]] = []
    for (sheet, headers), indices in groups.items():
        cells = [rows[i].get("_cells") or {} for i in indices]
        inferred = infer_header_roles(list(headers), cells, source_columns, dest_columns)
        for index in indices:
            rows[index] = apply_roles(rows[index], inferred)
        for item in inferred.evidence:
            evidence.append({**item, "sheet": sheet})
    return evidence


def _schema_hit_rate(values: list[str], candidates: list[str]) -> float:
    filled = [v for v in values if v]
    if not filled or not candidates:
        return 0.0
    hits = sum(1 for v in filled if resolve_name(v, candidates))
    return hits / len(filled)


def _rule_like_rate(values: list[str]) -> float:
    filled = [v for v in values if v]
    if not filled:
        return 0.0
    hits = 0
    for text in filled:
        kind = str(classify_rule(text).get("kind") or "unknown")
        if kind != "unknown" or parse_lookup(text) or any(ch in text for ch in ("→", "*", "×", "=")):
            hits += 1
    return hits / len(filled)


def _identifier_rate(values: list[str]) -> float:
    filled = [v for v in values if v]
    if not filled:
        return 0.0
    hits = sum(1 for v in filled if _IDENT.match(v) and " " not in v)
    return hits / len(filled)


def _short_code_rate(values: list[str]) -> float:
    filled = [v for v in values if v]
    if not filled:
        return 0.0
    hits = sum(1 for v in filled if 1 <= len(v) <= 16 and " " not in v)
    return hits / len(filled)


def _unique_winner(
    scores: dict[str, float],
    used: set[str],
    threshold: float = 0.55,
    gap: float = 0.15,
) -> tuple[str, float]:
    ranked = sorted(
        ((header, score) for header, score in scores.items() if header not in used),
        key=lambda item: item[1],
        reverse=True,
    )
    if not ranked or ranked[0][1] < threshold:
        return "", 0.0
    if len(ranked) > 1 and ranked[0][1] - ranked[1][1] < gap:
        return "", 0.0
    return ranked[0][0], ranked[0][1]


def _unique_hint(header: str, taken: set[str]) -> tuple[str, int]:
    folded = fold(header)
    tokens = {part for part in _HEADER_TOKENS.split(header.lower()) if part} or {folded}
    scored: list[tuple[int, str]] = []
    for role, hints in _HINTS.items():
        if role in taken:
            continue
        matched = [
            hint for hint in hints
            if hint in tokens or (len(hint) >= 4 and hint in folded)
        ]
        if not matched:
            continue
        strength = max(len(hint) for hint in matched)
        if all(hint in _WEAK_HINTS for hint in matched) or strength < 3:
            continue
        scored.append((strength, role))
    if not scored:
        return "", 0
    scored.sort(reverse=True)
    if len(scored) == 1 or scored[0][0] > scored[1][0]:
        return scored[0][1], scored[0][0]
    return "", 0


def _positional_fallback(
    unused: list[str],
    assigned: dict[str, str],
    used: set[str],
    evidence: list[dict[str, Any]],
    ident_scores: dict[str, float],
    rule_scores: dict[str, float],
    clean: list[str],
) -> None:
    """Left-to-right mapping-spec layout when schemas cannot decide."""
    if "rule" not in assigned:
        ranked_rules = sorted(
            ((header, rule_scores.get(header, 0.0)) for header in unused),
            key=lambda item: item[1],
            reverse=True,
        )
        if ranked_rules and ranked_rules[0][1] >= 0.55:
            header, score = ranked_rules[0]
            assigned["rule"] = header
            used.add(header)
            unused = [h for h in unused if h != header]
            evidence.append({
                "header": header,
                "role": "rule",
                "method": "rule_pattern",
                "confidence": round(score, 3),
                "reason": f"“{header}” values look like mapping instructions ({int(score * 100)}%).",
            })

    ident_cols = [
        header for header in unused
        if ident_scores.get(header, 0.0) >= 0.55
    ]
    ident_cols.sort(key=lambda header: clean.index(header) if header in clean else 0)
    if "source_column" not in assigned and ident_cols:
        header = ident_cols[0]
        assigned["source_column"] = header
        used.add(header)
        evidence.append({
            "header": header,
            "role": "source_column",
            "method": "position",
            "confidence": round(ident_scores.get(header, 0.5), 3),
            "reason": (
                f"“{header}” looks like column identifiers and sits leftmost — "
                "confirm, it is not schema-grounded."
            ),
        })
        ident_cols = ident_cols[1:]
    if "dest_column" not in assigned and ident_cols:
        header = ident_cols[0]
        assigned["dest_column"] = header
        used.add(header)
        evidence.append({
            "header": header,
            "role": "dest_column",
            "method": "position",
            "confidence": round(ident_scores.get(header, 0.5), 3),
            "reason": (
                f"“{header}” looks like column identifiers after the source column — "
                "confirm, it is not schema-grounded."
            ),
        })
