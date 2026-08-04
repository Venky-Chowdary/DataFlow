"""Root Cause Engine — one migration problem, many impacted gates.

Charter: Never confuse operators with duplicate blockers for the same lossy path.
Gates still run and remain auditable on ``gates[]``. Operator-facing
``blockers`` / ``root_causes`` collapse to one explainable root per problem.

Every root carries: impact, columns, sample/estimated rows, risk, fixes,
recovery, quarantine, rollback, and documentation — not just an error string.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass, field
from typing import Any

_FIDELITY_RE = re.compile(
    r"fidelity.?collapse|lossy|precision.?loss|scale.?truncat|"
    r"nested.?shape.?collapse|declared type path collapses|"
    r"width.?truncat|timezone.?shift|type coercion|"
    r"require(?:s)? explicit risk acknowledgment|"
    r"\(\s*[A-Z][A-Z0-9_()\s,]+\s*\)\s*→\s*\(?\s*[A-Z]",
    re.I,
)

_TYPE_ARROW_RE = re.compile(
    r"[\w.]+(?:\s*\([^)]*\))?\s*(?:→|->)\s*[\w.]+(?:\s*\([^)]*\))?",
    re.I,
)

_DUP_RE = re.compile(
    r"duplicate|primary.?key|identity.?key|unique.?constraint|not.?unique",
    re.I,
)

_FIDELITY_GATE_IDS = frozenset(
    {
        "g3_schema_contract",
        "g3_type_compat",
        "g3_type_compatibility",
        "g4_mapping_confidence",
        "g4_transform",
        "g5_dry_run",
        "g5_sample",
        "g5_sample_validation",
        "g6_target_ddl",
        "g8_reconciliation",
        "g9_data_integrity",
        "proof_bundle",
    }
)

_DUP_GATE_IDS = frozenset(
    {
        "g6_target_ddl",
        "g8_reconciliation",
        "g9_data_integrity",
    }
)


@dataclass
class MigrationRootCause:
    root_id: str
    kind: str
    title: str
    summary: str
    business_impact: str
    affected_columns: list[str] = field(default_factory=list)
    affected_rows_sample: int | None = None
    estimated_total_rows: int | None = None
    risk_level: str = "high"
    recommended_fix: str = ""
    alternative_fixes: list[str] = field(default_factory=list)
    recovery_strategy: str = ""
    expected_runtime_impact: str = ""
    quarantine_policy: str = "holdout_rejected_rows"
    rollback_policy: str = "DOCUMENT_ONLY"
    documentation: str = "docs/MIGRATION_RISK_CONTRACT.md"
    impacted_gates: list[str] = field(default_factory=list)
    absorbed_blocker_ids: list[str] = field(default_factory=list)
    severity: str = "block"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def as_operator_blocker(self) -> dict[str, Any]:
        """Single operator-facing blocker representing this root."""
        return {
            "id": self.root_id,
            "message": f"{self.title}: {self.summary}",
            "details": {
                "root_cause": True,
                "kind": self.kind,
                "root_id": self.root_id,
                "title": self.title,
                "summary": self.summary,
                "business_impact": self.business_impact,
                "affected_columns": self.affected_columns,
                "affected_rows_sample": self.affected_rows_sample,
                "estimated_total_rows": self.estimated_total_rows,
                "risk_level": self.risk_level,
                "recommended_fix": self.recommended_fix,
                "alternative_fixes": self.alternative_fixes,
                "recovery_strategy": self.recovery_strategy,
                "expected_runtime_impact": self.expected_runtime_impact,
                "quarantine_policy": self.quarantine_policy,
                "rollback_policy": self.rollback_policy,
                "documentation": self.documentation,
                "impacted_gates": self.impacted_gates,
                "absorbed_blocker_ids": self.absorbed_blocker_ids,
                "gate_chips": self.impacted_gates,
            },
            "guidance": {
                "gate": self.root_id,
                "title": self.title,
                "category": "root_cause",
                "why": self.business_impact,
                "fix": self.recommended_fix,
                "examples": self.alternative_fixes[:4],
                "suggested_actions": [
                    {
                        "kind": "open_map",
                        "label": "Open Map · remap / Risk Contract",
                    }
                ],
            },
        }


def _blob(message: str, details: dict[str, Any] | None) -> str:
    parts = [message or ""]
    if details:
        for key in (
            "issues",
            "issue_texts",
            "reason",
            "framing",
            "errors",
            "columns",
        ):
            val = details.get(key)
            if val is not None:
                parts.append(str(val))
    return " ".join(parts)


def _is_confidence_floor_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    """True for G4 low-confidence / ambiguous review — not lossy risk-ack."""
    if str(gate_id or "") != "g4_mapping_confidence":
        return False
    details = details or {}
    if details.get("risk_unacknowledged") or details.get("structural_unacknowledged"):
        return False
    if details.get("low_confidence") or details.get("ambiguous_mappings"):
        return True
    msg = message or ""
    if re.search(r"risk acknowledgment|STRUCT/specialty|lossy/narrowing", msg, re.I):
        return False
    return bool(re.search(r"below floor|ambiguous mapping", msg, re.I))


def _is_fidelity_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    details = details or {}
    # Pure confidence floor is Module 3 mapping_confidence root — not fidelity.
    if _is_confidence_floor_signal(message, details, gate_id):
        return False
    if details.get("fidelity_collapse") is True:
        return True
    framing = details.get("framing") if isinstance(details.get("framing"), dict) else {}
    kind = str(framing.get("kind") or details.get("kind") or "").lower()
    if kind in {
        "fidelity_collapse",
        "nested_shape_collapse",
        "nested_document_serialization",
    }:
        return True
    blob = _blob(message, details)
    if _FIDELITY_RE.search(blob):
        return True
    if _TYPE_ARROW_RE.search(blob) and gate_id in _FIDELITY_GATE_IDS:
        return True
    if gate_id in _FIDELITY_GATE_IDS and re.search(
        r"loss|truncat|collapse|\bcast\b|coercion|integrity failed", blob, re.I
    ):
        return True
    return False


def _is_duplicate_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    details = details or {}
    if details.get("duplicate_keys") or details.get("identity_duplicates"):
        return True
    blob = _blob(message, details)
    if _DUP_RE.search(blob):
        return True
    return gate_id in _DUP_GATE_IDS and bool(_DUP_RE.search(blob))


def _columns_from_details(details: dict[str, Any] | None) -> list[str]:
    details = details or {}
    cols: list[str] = []
    for key in ("affected_columns", "columns", "column"):
        val = details.get(key)
        if isinstance(val, list):
            cols.extend(str(x) for x in val if x)
        elif isinstance(val, str) and val.strip():
            cols.append(val.strip())
    for issue in details.get("issues_detail") or details.get("issues") or []:
        if isinstance(issue, dict):
            src = issue.get("source") or issue.get("column") or issue.get("target")
            if src:
                cols.append(str(src))
        elif isinstance(issue, str):
            m = re.search(r"Column ['\"]?([\w.]+)['\"]?", issue, re.I)
            if m:
                cols.append(m.group(1))
            else:
                m2 = re.search(r"^([\w.]+)\s*\(", issue)
                if m2:
                    cols.append(m2.group(1))
    # Dedupe preserve order
    seen: set[str] = set()
    out: list[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _sample_rows(preflight: dict[str, Any]) -> int | None:
    cr = preflight.get("coercion_report") or {}
    if isinstance(cr.get("sampled_rows"), int):
        return int(cr["sampled_rows"])
    gates = preflight.get("gates") or []
    for g in gates:
        details = (g or {}).get("details") or {}
        scope = details.get("evidence_scope") or {}
        if isinstance(scope.get("sample_rows"), int):
            return int(scope["sample_rows"])
        if isinstance(details.get("sample_rows"), int):
            return int(details["sample_rows"])
    return None


def _estimated_rows(preflight: dict[str, Any]) -> int | None:
    for key in ("row_count", "row_count_estimate", "estimated_rows"):
        val = preflight.get(key)
        if isinstance(val, int) and val >= 0:
            return val
    plan = preflight.get("validation_plan") or {}
    if isinstance(plan.get("row_count_estimate"), int):
        return int(plan["row_count_estimate"])
    return None


def _root_id(kind: str, columns: list[str], gate_ids: list[str]) -> str:
    raw = f"{kind}|{','.join(sorted(columns))}|{','.join(sorted(gate_ids))}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    return f"rc-{kind.replace('_', '-')}-{digest}"


def build_root_causes(preflight: dict[str, Any] | None) -> list[MigrationRootCause]:
    """Collapse multi-gate failures into operator-facing migration roots."""
    if not preflight:
        return []

    gates = [g for g in (preflight.get("gates") or []) if g]
    blockers = [b for b in (preflight.get("blockers") or []) if b]
    sample_n = _sample_rows(preflight)
    est_n = _estimated_rows(preflight)

    fidelity_gates = [
        g
        for g in gates
        if g.get("status") == "block"
        and _is_fidelity_signal(str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or ""))
    ]
    fidelity_blockers = [
        b
        for b in blockers
        if _is_fidelity_signal(str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or ""))
    ]

    roots: list[MigrationRootCause] = []

    # Fidelity: require ≥2 gate/blocker faces so we do not invent a root for a
    # single unrelated gate message.
    if len(fidelity_gates) + len(fidelity_blockers) >= 2:
        gate_ids = sorted(
            {
                *[str(g.get("id")) for g in fidelity_gates if g.get("id")],
                *[
                    str(b.get("id"))
                    for b in fidelity_blockers
                    if b.get("id") and str(b.get("id")) in _FIDELITY_GATE_IDS
                ],
            }
        )
        if len(gate_ids) >= 2 or len(fidelity_blockers) >= 2:
            cols: list[str] = []
            for g in fidelity_gates:
                cols.extend(_columns_from_details(g.get("details") or {}))
            for b in fidelity_blockers:
                cols.extend(_columns_from_details(b.get("details") or {}))
            # Coercion report columns with fidelity_collapse / block severity
            cr = preflight.get("coercion_report") or {}
            for col in cr.get("columns") or []:
                if not isinstance(col, dict):
                    continue
                if col.get("fidelity_collapse") or col.get("severity") == "block":
                    src = col.get("source") or col.get("column")
                    if src:
                        cols.append(str(src))
            cols = list(dict.fromkeys(cols))
            absorbed = sorted(
                {
                    *[str(g.get("id")) for g in fidelity_gates if g.get("id")],
                    *[str(b.get("id")) for b in fidelity_blockers if b.get("id")],
                }
            )
            col_label = ", ".join(cols[:5]) + (
                f" (+{len(cols) - 5} more)" if len(cols) > 5 else ""
            )
            summary = (
                f"{len(cols)} column(s) collapse fidelity on write"
                + (f" ({col_label})" if cols else "")
                + f" — impacts {len(absorbed)} gate check(s)"
            )
            roots.append(
                MigrationRootCause(
                    root_id=_root_id("fidelity_collapse", cols, gate_ids or absorbed),
                    kind="fidelity_collapse",
                    title="Lossy / fidelity collapse across type path",
                    summary=summary,
                    business_impact=(
                        "Declared source→destination types (or nested shapes) lose "
                        "precision or domain on write. Execute stays locked until Map "
                        "remaps carriers or a Migration Risk Contract with an explicit "
                        "continue policy is signed."
                    ),
                    affected_columns=cols,
                    affected_rows_sample=sample_n,
                    estimated_total_rows=est_n,
                    risk_level="high",
                    recommended_fix=(
                        "Open Map → Remap to a fidelity-preserving type, or "
                        "Accept · cast & continue (Migration Risk Contract with "
                        "CAST_AND_CONTINUE) → re-run Validate."
                    ),
                    alternative_fixes=[
                        "Remap destination column to TEXT/VARCHAR/STRING",
                        "Sign QUARANTINE_ROW contract for cast failures",
                        "Widen DECIMAL/INTEGER capacity when narrowing is the only issue",
                    ],
                    recovery_strategy=(
                        "After remap or Risk Contract, re-Validate. Failed casts under "
                        "CAST_AND_CONTINUE hold out to quarantine/DLQ — primary table "
                        "must not invent NULL unless the contract says so."
                    ),
                    expected_runtime_impact=(
                        "Re-Validate is sample-scoped; changing destination DDL requires "
                        "a new Map revision and full rewrite of already-written tables."
                    ),
                    quarantine_policy=(
                        "holdout_rejected_rows under CAST_AND_CONTINUE "
                        "(see docs/MIGRATION_RISK_CONTRACT.md)"
                    ),
                    rollback_policy="DOCUMENT_ONLY",
                    documentation="docs/MIGRATION_RISK_CONTRACT.md",
                    impacted_gates=gate_ids or absorbed,
                    absorbed_blocker_ids=absorbed,
                    severity="block",
                )
            )

    # Module 3: mapping confidence — G4 is sole hard authority (not proof/G9).
    conf_gates = [
        g
        for g in gates
        if g.get("status") == "block"
        and _is_confidence_floor_signal(
            str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or "")
        )
    ]
    conf_blockers = [
        b
        for b in blockers
        if _is_confidence_floor_signal(
            str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or "")
        )
    ]
    if conf_gates or conf_blockers:
        absorbed = sorted(
            {
                *[str(g.get("id")) for g in conf_gates if g.get("id")],
                *[str(b.get("id")) for b in conf_blockers if b.get("id")],
            }
        )
        cols: list[str] = []
        for src in conf_gates + conf_blockers:
            details = (src.get("details") or {})
            for key in ("low_confidence", "ambiguous_mappings"):
                for entry in details.get(key) or []:
                    name = str(entry).split("(")[0].strip()
                    left = name.split("→")[0].strip() if "→" in name else name
                    if left:
                        cols.append(left)
            cols.extend(_columns_from_details(details))
        cols = list(dict.fromkeys(cols))
        roots.append(
            MigrationRootCause(
                root_id=_root_id("mapping_confidence", cols, absorbed),
                kind="mapping_confidence",
                title="Mapping confidence below floor",
                summary=(
                    f"{len(cols) or 'Some'} mapping(s) below the Map confidence floor "
                    f"— owned by g4_mapping_confidence (not re-blocked by proof/G9)"
                ),
                business_impact=(
                    "Low semantic confidence increases wrong-column risk. Execute stays "
                    "locked until Map review/approve raises confidence or overrides "
                    "with evidence — proof pack reports the score but does not invent "
                    "a second confidence blocker."
                ),
                affected_columns=cols,
                affected_rows_sample=sample_n,
                estimated_total_rows=est_n,
                risk_level="high",
                recommended_fix=(
                    "Open Map → review low-confidence pairs → Approve with evidence "
                    "or remap to the correct target → re-run Validate."
                ),
                alternative_fixes=[
                    "Improve source/target column names for lexical match",
                    "Approve with user_override after human review",
                    "Lower validation mode only when Discovery/Migration mode is intentional",
                ],
                recovery_strategy=(
                    "After Map approve/remap, re-Validate. No write occurs until G4 passes."
                ),
                expected_runtime_impact="Re-Validate only — no dest rewrite until Execute",
                quarantine_policy="n/a — confidence is a Map decision, not a row quarantine",
                rollback_policy="DOCUMENT_ONLY",
                documentation="docs/MAPPING_CONFIDENCE_AUTHORITY.md",
                impacted_gates=absorbed or ["g4_mapping_confidence"],
                absorbed_blocker_ids=absorbed,
                severity="block",
            )
        )

    dup_gates = [
        g
        for g in gates
        if g.get("status") == "block"
        and _is_duplicate_signal(str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or ""))
    ]
    dup_blockers = [
        b
        for b in blockers
        if _is_duplicate_signal(str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or ""))
    ]
    if len(dup_gates) + len(dup_blockers) >= 1:
        absorbed = sorted(
            {
                *[str(g.get("id")) for g in dup_gates if g.get("id")],
                *[str(b.get("id")) for b in dup_blockers if b.get("id")],
            }
        )
        if len(absorbed) >= 1:
            cols: list[str] = []
            for g in dup_gates:
                cols.extend(_columns_from_details(g.get("details") or {}))
            for b in dup_blockers:
                cols.extend(_columns_from_details(b.get("details") or {}))
            cols = list(dict.fromkeys(cols))
            roots.append(
                MigrationRootCause(
                    root_id=_root_id("duplicate_identity", cols, absorbed),
                    kind="duplicate_identity",
                    title="Duplicate identity keys",
                    summary=(
                        "Identity / uniqueness checks failed on the Validate sample "
                        f"— impacts {len(absorbed)} gate check(s)"
                    ),
                    business_impact=(
                        "Append or upsert may duplicate or reject rows. Identity must "
                        "be fixed before Execute claims deterministic loads."
                    ),
                    affected_columns=cols,
                    affected_rows_sample=sample_n,
                    estimated_total_rows=est_n,
                    risk_level="high",
                    recommended_fix=(
                        "Open Sync / identity settings → choose a unique PK or "
                        "composite key → prefer overwrite/upsert over blind append."
                    ),
                    alternative_fixes=[
                        "Dedupe source before transfer",
                        "Switch sync mode to overwrite or upsert",
                        "Pick a composite unique key from Validate suggestions",
                    ],
                    recovery_strategy=(
                        "Fix identity, re-Validate. Already-written duplicates need "
                        "manual dest cleanup — transfer undo is not productized."
                    ),
                    expected_runtime_impact="Re-Validate sample; full table rewrite if sync mode changes",
                    quarantine_policy="n/a — identity must be fixed, not quarantined away",
                    rollback_policy="DOCUMENT_ONLY",
                    documentation="docs/MIGRATION_ROLLBACK.md",
                    impacted_gates=absorbed,
                    absorbed_blocker_ids=absorbed,
                    severity="block",
                )
            )

    return roots


def apply_root_causes_to_preflight(preflight: dict[str, Any]) -> dict[str, Any]:
    """Attach root_causes and replace operator blockers with collapsed roots.

    Gate results stay intact for audit. Absorbed gate blockers are removed from
    ``blockers`` so the UI never lists the same TEXT→INTEGER failure N times.
    Non-absorbed blockers (privilege, connection, etc.) remain.
    """
    roots = build_root_causes(preflight)
    preflight = {**preflight, "root_causes": [r.to_dict() for r in roots]}
    if not roots:
        return preflight

    absorbed: set[str] = set()
    for r in roots:
        absorbed.update(r.absorbed_blocker_ids)

    remaining = [
        b
        for b in (preflight.get("blockers") or [])
        if b and str(b.get("id") or "") not in absorbed
        and not (b.get("details") or {}).get("root_cause")
    ]
    # Roots first — operator sees cause before residual issues.
    preflight["blockers"] = [r.as_operator_blocker() for r in roots] + remaining
    return preflight
