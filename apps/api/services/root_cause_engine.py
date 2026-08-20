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
    r"migration risk contract|"
    r"risk contract required|"
    r"execution policy|"
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

_ENCODING_RE = re.compile(
    r"format-control character|control character|invisible character|"
    r"bidi(?:rectional)? (?:control|override)|zero.?width|"
    r"lone surrogate|unpaired surrogate|invalid (?:utf-?8|byte sequence|encoding)|"
    r"mojibake|replacement character|byte order mark|\bBOM\b|"
    r"mixed encodings|undecodable",
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
        "g6_ddl_compatibility",
        "g8_reconciliation",
        "g9_data_integrity",
        "g9_type_coercion",
        "proof_bundle",
        "risk_contracts",
        "migration_risk_contract",
    }
)

_DUP_GATE_IDS = frozenset(
    {
        "g6_target_ddl",
        "g8_reconciliation",
        "g9_data_integrity",
    }
)


# One root cause, one primary action — the label has to be the action the
# operator actually needs, not the fidelity remap for every kind of block.
_PRIMARY_ACTION_LABELS: dict[str, str] = {
    "encoding_normalization": "Open Map · normalize text",
    "duplicate_identity": "Open Sync · choose identity key",
    "mapping_confidence": "Open Map · confirm mapping",
}


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
                        "label": _PRIMARY_ACTION_LABELS.get(
                            self.kind, "Open Map · remap / Risk Contract"
                        ),
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


def _is_risk_contract_incomplete_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    """G4/proof: signed continue-policy Risk Contract missing — not fidelity collapse.

    Prefer structured ``risk_unacknowledged`` lists. Message-only matching is narrow
    so genuine type-path G4 blocks (with issues_detail / fidelity_collapse) still
    absorb into the fidelity root.
    """
    details = details or {}
    if details.get("fidelity_collapse") is True or details.get("issues_detail"):
        return False
    if details.get("risk_unacknowledged") or details.get("structural_unacknowledged"):
        return True
    gid = str(gate_id or "")
    if gid not in {
        "g4_mapping_confidence",
        "risk_contracts",
        "migration_risk_contract",
        "proof_bundle",
    }:
        return False
    msg = message or ""
    return bool(
        re.search(
            r"migration risk contract|risk contract required|require(?:s)? a signed",
            msg,
            re.I,
        )
    )


def _is_fidelity_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    details = details or {}
    # Pure confidence floor is Module 3 mapping_confidence root — not fidelity.
    if _is_confidence_floor_signal(message, details, gate_id):
        return False
    # Missing Risk Contract is its own root — never "0 columns collapse fidelity".
    if _is_risk_contract_incomplete_signal(message, details, gate_id):
        return False
    # G5/G8/G9 transform / identity / duplicate failures are not type-path
    # fidelity collapse. ``image→image`` empty-url lines must not trip the
    # type-arrow regex into a false "Lossy / fidelity collapse" root after the
    # operator already signed TEXT→INTEGER Risk Contracts.
    gid = str(gate_id or "")
    kind = str(details.get("kind") or "").lower()
    framing = details.get("framing") if isinstance(details.get("framing"), dict) else {}
    framing_kind = str(framing.get("kind") or "").lower()
    if gid in {"g5_dry_run", "g5_sample", "g5_sample_validation", "g8_reconciliation"}:
        if kind in {"transform_errors", "duplicate_keys", "identity_transform"}:
            return False
        msg_l = (message or "").lower()
        if (
            "transform errors" in msg_l
            or "dry-run failed" in msg_l
            or "cannot coerce to" in msg_l
            or "duplicate target key" in msg_l
            or "identity transform" in msg_l
            or "identity mapping" in msg_l
        ):
            # Only absorb when the gate itself stamped collapse.
            return details.get("fidelity_collapse") is True
        # G8: only absorb when the gate itself stamped collapse.
        if gid == "g8_reconciliation":
            return details.get("fidelity_collapse") is True
    if gid == "g9_data_integrity":
        if kind in {"transform_errors", "duplicate_keys", "identity_transform"}:
            return False
        if framing_kind in {"transform_errors"}:
            return False
        msg_l = (message or "").lower()
        # Transform dry-run / empty-url integrity copy — not declared type collapse.
        if (
            "cannot coerce to" in msg_l
            or "transform_dry_run" in str(details.get("failed_checks") or "").lower()
            or any(
                "cannot coerce" in str(i).lower()
                for i in (details.get("issues") or details.get("errors") or [])[:8]
            )
        ) and details.get("fidelity_collapse") is not True:
            # Still allow true fidelity stamps on G9.
            if not details.get("issues_detail") and not details.get("fidelity_collapse"):
                return False
    if details.get("fidelity_collapse") is True:
        return True
    # Invisible / undecodable characters are an encoding root with its own fix
    # (normalize or quarantine the rows). Absorbing them into fidelity collapse
    # told operators to remap a type path that is not the problem — a TEXT→TEXT
    # column was reported as collapsing fidelity because the G9 message says
    # "integrity failed".
    if _is_encoding_signal(message, details, gate_id):
        return False
    kind = framing_kind or kind
    if kind in {
        "fidelity_collapse",
        "nested_shape_collapse",
        "nested_document_serialization",
    }:
        return True
    blob = _blob(message, details)
    # Duplicate / identity integrity failures are their own root — never absorb
    # "Data integrity failed: id duplicate key…" into fidelity collapse.
    if _is_duplicate_signal(message, details, gate_id):
        return False
    if _FIDELITY_RE.search(blob):
        return True
    if _TYPE_ARROW_RE.search(blob) and gate_id in _FIDELITY_GATE_IDS:
        return True
    if gate_id in _FIDELITY_GATE_IDS and re.search(
        r"loss|truncat|collapse|\bcast\b|coercion|integrity failed", blob, re.I
    ):
        return True
    return False


def _is_encoding_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    """True for control-character / undecodable-byte findings on the sample."""
    details = details or {}
    if details.get("fidelity_collapse") is True:
        return False
    if details.get("encoding_issues"):
        return True
    if str(gate_id or "") not in {"g9_data_integrity", "g3_encoding", "g9_encoding"}:
        return False
    return bool(_ENCODING_RE.search(_blob(message, details)))


def _is_transform_error_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    """Sample cast / transform failures — not declared type-path fidelity collapse."""
    details = details or {}
    if details.get("fidelity_collapse") is True:
        return False
    gid = str(gate_id or "")
    if gid not in {
        "g5_dry_run",
        "g5_sample",
        "g5_sample_validation",
        "g8_reconciliation",
        "g9_data_integrity",
    }:
        return False
    kind = str(details.get("kind") or "").lower()
    if kind == "transform_errors":
        return True
    msg_l = (message or "").lower()
    if "dry-run failed" in msg_l or "transform errors" in msg_l:
        return True
    if "cannot coerce to" in msg_l:
        return True
    for issue in (details.get("errors") or details.get("issues") or [])[:12]:
        if "cannot coerce" in str(issue).lower():
            return True
    return False


def _is_destination_collision_signal(details: dict[str, Any] | None) -> bool:
    """True for a key the destination already stores, not a duplicate in the source.

    The append collision probe stamps ``sample_collisions`` with its own rule id.
    Only the probe's own evidence counts: matching on prose would relabel a real
    source duplicate as a destination collision and send the operator to change
    the sync mode instead of deduplicating.
    """
    details = details or {}
    rule_id = str(details.get("rule_id") or "")
    if rule_id.startswith("g6_target_ddl.append_key_collision"):
        return True
    return bool(details.get("sample_collisions")) and (
        details.get("remediation_kind") == "change_sync_mode"
    )


def _is_duplicate_signal(
    message: str,
    details: dict[str, Any] | None,
    gate_id: str,
) -> bool:
    details = details or {}
    # Missing Map PK is a set-identity remediation — not a duplicate-key finding.
    # ``Identity key required`` otherwise matches ``identity.?key`` in ``_DUP_RE``.
    rule_id = str(details.get("rule_id") or "")
    if (
        rule_id.endswith("missing_identity")
        or details.get("remediation_kind") == "set_primary_key"
        or re.search(r"identity key required", str(message or ""), re.I)
    ):
        return False
    if details.get("duplicate_keys") or details.get("identity_duplicates"):
        return True
    blob = _blob(message, details)
    if _DUP_RE.search(blob):
        return True
    return gate_id in _DUP_GATE_IDS and bool(_DUP_RE.search(blob))


def _source_from_pair_label(label: str) -> str:
    """Parse ``source→target`` / ``source -> target`` labels to source column.

    Never invent column names from free prose (e.g. the G9 note
    ``Preflight blocked the transfer (0 rows written)…``).
    """
    name = str(label or "").strip()
    if not name:
        return ""
    # Strip leading ``row N `` from G8-style lines.
    name = re.sub(r"^row\s+\d+\s+", "", name, flags=re.I)
    for sep in ("→", "->"):
        if sep in name:
            left = name.split(sep, 1)[0].strip()
            # Bare identifier only — reject multi-word prose.
            if re.fullmatch(r"[\w.]+", left):
                return left
            return ""
    # No arrow: only accept a bare identifier (optionally with type paren).
    head = name.split("(", 1)[0].strip()
    if re.fullmatch(r"[\w.]+", head):
        return head
    return ""


def _columns_from_details(details: dict[str, Any] | None) -> list[str]:
    details = details or {}
    cols: list[str] = []
    for key in ("affected_columns", "columns", "column"):
        val = details.get(key)
        if isinstance(val, list):
            cols.extend(str(x) for x in val if x)
        elif isinstance(val, str) and val.strip():
            parsed = _source_from_pair_label(val.strip())
            if parsed:
                cols.append(parsed)
            elif re.fullmatch(r"[\w.]+", val.strip()):
                cols.append(val.strip())
    for key in ("risk_unacknowledged", "structural_unacknowledged"):
        for entry in details.get(key) or []:
            left = _source_from_pair_label(str(entry))
            if left:
                cols.append(left)
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
                left = _source_from_pair_label(issue)
                if left:
                    cols.append(left)
    # Also harvest structured dry-run error lines.
    for err in details.get("errors") or []:
        left = _source_from_pair_label(str(err))
        if left:
            cols.append(left)
    # Dedupe preserve order
    seen: set[str] = set()
    out: list[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


#: Separator in a ``SOURCE_TYPE → TARGET_TYPE`` label. One owner, because the
#: label is both printed to the operator and read back to resolve the pair.
_TYPE_PATH_ARROW = " → "


def _collect_type_paths(
    entries: list[dict[str, Any]],
    coercion_report: dict[str, Any],
) -> dict[str, str]:
    """Map column → ``SOURCE_TYPE → TARGET_TYPE`` from every evidence shape.

    Gates carry the pair on ``issues_detail`` rows; the coercion probe carries it
    on its per-column report. Either one is enough to name the path.
    """
    paths: dict[str, str] = {}

    def _record(row: Any) -> None:
        if not isinstance(row, dict):
            return
        col = row.get("source") or row.get("column")
        src_type = str(row.get("source_type") or "").strip()
        tgt_type = str(row.get("target_type") or "").strip()
        if col and src_type and tgt_type:
            paths.setdefault(str(col), f"{src_type}{_TYPE_PATH_ARROW}{tgt_type}")

    for entry in entries:
        details = (entry or {}).get("details") or {}
        if not isinstance(details, dict):
            continue
        for row in details.get("issues_detail") or details.get("issues") or []:
            _record(row)
    for row in coercion_report.get("columns") or []:
        _record(row)
    return paths


def _column_type_path_label(column: str, type_paths: dict[str, str]) -> str:
    """``name SOURCE_TYPE → TARGET_TYPE`` when the pair is known, else the name.

    A bare column name sends the operator hunting through Map for what is wrong
    with it. The type path is the finding itself, and it is what lets them judge
    whether the verdict is even right.
    """
    path = type_paths.get(column)
    return f"{column} {path}" if path else column


def _destination_db(preflight: dict[str, Any]) -> str:
    """The destination engine the gates ran against, or ``""`` when unstamped.

    Carrier semantics are engine-specific — a bare ``date`` token is a
    wall-clock day in SQL and an instant in BSON — so a remediation resolved
    without the engine would be advice about a different database.
    """
    for key in ("destination_db_type", "dest_db_type"):
        val = preflight.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    dest = preflight.get("destination")
    if isinstance(dest, dict):
        for key in ("db_type", "format", "type"):
            val = dest.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return ""


def _zoneless_instant_remedies(
    columns: list[str],
    type_paths: dict[str, str],
    dest_db: str,
) -> list[str]:
    """Per-column remediation for a zoneless source landing on an instant carrier.

    The generic fidelity fix ("remap to a preserving type, or sign
    CAST_AND_CONTINUE") is a dead end for this path: a destination whose only
    temporal carrier is an instant has nothing zoneless to remap to, and casting
    on regardless is precisely the guessed-UTC shift the block exists to prevent.
    The exit is the operator declaring the zone the source recorded, and the
    wording comes from ``services.timezone_policy`` so Map, Validate and this
    summary cannot describe the same policy differently.
    """
    from services.timezone_policy import POLICY_UTC_INVENT, resolve_timezone_policy

    out: list[str] = []
    for col in columns:
        path = type_paths.get(col) or ""
        if _TYPE_PATH_ARROW not in path:
            continue
        src_type, tgt_type = (p.strip() for p in path.split(_TYPE_PATH_ARROW, 1))
        policy = resolve_timezone_policy(src_type, tgt_type, dest_db=dest_db)
        if policy is None or policy.policy != POLICY_UTC_INVENT or not policy.remediation:
            continue
        out.append(f"{col} ({src_type} → {tgt_type}): {policy.remediation}")
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

    # Fidelity: collapse even a single fidelity-facing block into one root so
    # operators never see N duplicate CTAs for one lossy mapping path.
    if len(fidelity_gates) + len(fidelity_blockers) >= 1:
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
        if gate_ids or fidelity_blockers:
            cols: list[str] = []
            for g in fidelity_gates:
                cols.extend(_columns_from_details(g.get("details") or {}))
            for b in fidelity_blockers:
                cols.extend(_columns_from_details(b.get("details") or {}))
            # Coercion report: only blocking severity. Signed continue-policy
            # rows stay fidelity_collapse=true with severity=warn — they must
            # not re-inflate a blocking root after Accept risk.
            cr = preflight.get("coercion_report") or {}
            for col in cr.get("columns") or []:
                if not isinstance(col, dict):
                    continue
                sev = str(col.get("severity") or "").lower()
                if sev != "block":
                    continue
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
            type_paths = _collect_type_paths(fidelity_gates + fidelity_blockers, cr)
            zone_remedies = _zoneless_instant_remedies(
                cols, type_paths, _destination_db(preflight)
            )
            col_label = ", ".join(
                _column_type_path_label(c, type_paths) for c in cols[:5]
            ) + (f" (+{len(cols) - 5} more)" if len(cols) > 5 else "")
            if cols:
                summary = (
                    f"{len(cols)} column(s) collapse fidelity on write"
                    f" ({col_label}) — impacts {len(absorbed)} gate check(s)"
                )
            else:
                # Never claim "0 columns collapse" — that is an extraction bug.
                summary = (
                    f"Fidelity risk across type path — impacts "
                    f"{len(absorbed)} gate check(s); open Map for column detail"
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
                        # A zone the source never recorded cannot be remapped
                        # into existence, so that exit leads the fix when this
                        # is the path — the generic remap advice follows it.
                        "; ".join(r.rstrip(". ") for r in zone_remedies)
                        + ". Otherwise: open Map → Remap to a fidelity-preserving type."
                        if zone_remedies
                        else (
                            "Open Map → Remap to a fidelity-preserving type, or "
                            "Accept · cast & continue (Migration Risk Contract with "
                            "CAST_AND_CONTINUE) → re-run Validate."
                        )
                    ),
                    alternative_fixes=[
                        *(
                            ["Declare the source zone (assume_timezone:<IANA zone>) on Map"]
                            if zone_remedies
                            else []
                        ),
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

    # Incomplete Migration Risk Contract — only a standalone root when there is
    # no fidelity collapse. Same lossy path with G3/G6/G9 + missing contract
    # collapses into the fidelity root (one operator CTA).
    risk_gates = [
        g
        for g in gates
        if g.get("status") == "block"
        and _is_risk_contract_incomplete_signal(
            str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or "")
        )
    ]
    risk_blockers = [
        b
        for b in blockers
        if _is_risk_contract_incomplete_signal(
            str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or "")
        )
    ]
    if risk_gates or risk_blockers:
        risk_absorbed = sorted(
            {
                *[str(g.get("id")) for g in risk_gates if g.get("id")],
                *[str(b.get("id")) for b in risk_blockers if b.get("id")],
            }
        )
        risk_cols: list[str] = []
        for src in risk_gates + risk_blockers:
            risk_cols.extend(_columns_from_details(src.get("details") or {}))
        risk_cols = list(dict.fromkeys(risk_cols))
        fidelity_root = next((r for r in roots if r.kind == "fidelity_collapse"), None)
        if fidelity_root is not None:
            merged_cols = list(
                dict.fromkeys([*fidelity_root.affected_columns, *risk_cols])
            )
            merged_absorbed = sorted(
                set(fidelity_root.absorbed_blocker_ids) | set(risk_absorbed)
            )
            merged_paths = _collect_type_paths(
                [*gates, *blockers], preflight.get("coercion_report") or {}
            )
            col_label = ", ".join(
                _column_type_path_label(c, merged_paths) for c in merged_cols[:5]
            ) + (f" (+{len(merged_cols) - 5} more)" if len(merged_cols) > 5 else "")
            fidelity_root.affected_columns = merged_cols
            fidelity_root.absorbed_blocker_ids = merged_absorbed
            fidelity_root.impacted_gates = sorted(
                set(fidelity_root.impacted_gates) | set(risk_absorbed)
            )
            if merged_cols:
                fidelity_root.summary = (
                    f"{len(merged_cols)} column(s) collapse fidelity on write"
                    f" ({col_label}) — impacts {len(merged_absorbed)} gate check(s)"
                )
            else:
                fidelity_root.summary = (
                    f"Fidelity risk across type path — impacts "
                    f"{len(merged_absorbed)} gate check(s); open Map for column detail"
                )
        else:
            col_label = ", ".join(risk_cols[:5]) + (
                f" (+{len(risk_cols) - 5} more)" if len(risk_cols) > 5 else ""
            )
            roots.append(
                MigrationRootCause(
                    root_id=_root_id(
                        "risk_contract_incomplete", risk_cols, risk_absorbed
                    ),
                    kind="risk_contract_incomplete",
                    title="Migration Risk Contract required",
                    summary=(
                        (
                            f"{len(risk_cols)} mapping(s) need a signed continue-policy "
                            f"Risk Contract ({col_label})"
                            if risk_cols
                            else "Signed continue-policy Risk Contract required"
                        )
                        + f" — impacts {len(risk_absorbed)} gate check(s)"
                    ),
                    business_impact=(
                        "Map already flagged these paths, but Validate did not receive a "
                        "verified Migration Risk Contract with an explicit continue policy "
                        "(CAST_AND_CONTINUE / QUARANTINE_ROW / …). Boolean ack alone never "
                        "unlocks Execute."
                    ),
                    affected_columns=risk_cols,
                    affected_rows_sample=sample_n,
                    estimated_total_rows=est_n,
                    risk_level="high",
                    recommended_fix=(
                        "Open Map → Accept risk → choose an explicit execution policy → "
                        "Sign Risk Contract → re-run Validate (contracts must be on the "
                        "mappings payload)."
                    ),
                    alternative_fixes=[
                        "Remap to a fidelity-preserving destination type",
                        "Use QUARANTINE_ROW when cast failures should hold out rows",
                        "Confirm Validate request includes risk_contract on each column",
                    ],
                    recovery_strategy=(
                        "After a clearing contract is present on each listed column, "
                        "re-Validate. Execute stays locked until G4 / proof contract "
                        "checks pass."
                    ),
                    expected_runtime_impact=(
                        "Re-Validate only — no destination rewrite until Execute"
                    ),
                    quarantine_policy=(
                        "per signed execution_policy "
                        "(see docs/MIGRATION_RISK_CONTRACT.md)"
                    ),
                    rollback_policy="DOCUMENT_ONLY",
                    documentation="docs/MIGRATION_RISK_CONTRACT.md",
                    impacted_gates=risk_absorbed or ["g4_mapping_confidence"],
                    absorbed_blocker_ids=risk_absorbed,
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

    # Sample transform / cast failures (empty url, bad decimal, …) — own root.
    # Must not be framed as Lossy / fidelity collapse after TEXT→INTEGER contracts.
    xf_gates = [
        g
        for g in gates
        if g.get("status") == "block"
        and _is_transform_error_signal(
            str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or "")
        )
        and not _is_fidelity_signal(
            str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or "")
        )
        and not _is_duplicate_signal(
            str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or "")
        )
    ]
    xf_blockers = [
        b
        for b in blockers
        if _is_transform_error_signal(
            str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or "")
        )
        and not _is_fidelity_signal(
            str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or "")
        )
        and not _is_duplicate_signal(
            str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or "")
        )
    ]
    if xf_gates or xf_blockers:
        absorbed = sorted(
            {
                *[str(g.get("id")) for g in xf_gates if g.get("id")],
                *[str(b.get("id")) for b in xf_blockers if b.get("id")],
            }
        )
        cols: list[str] = []
        for src in xf_gates + xf_blockers:
            cols.extend(_columns_from_details(src.get("details") or {}))
        cols = list(dict.fromkeys(cols))
        col_label = ", ".join(cols[:5]) + (
            f" (+{len(cols) - 5} more)" if len(cols) > 5 else ""
        )
        # Classify hazards via Decision Kernel so empty→typed ≠ fractional→int.
        # Prefer coercion_report.failure_class (canonical) before parsing gate prose.
        failure_classes: list[str] = []
        try:
            from services.decision_kernel import (
                FailureClass,
                classify_transform_failure,
                recommended_action_for_failure,
            )

            coercion = preflight.get("coercion_report") or {}
            for col in list(coercion.get("columns") or []):
                if not isinstance(col, dict):
                    continue
                if str(col.get("severity") or "") != "block":
                    continue
                fc_raw = str(col.get("failure_class") or "").strip()
                if fc_raw:
                    failure_classes.append(fc_raw)
            blob_parts: list[str] = []
            for src in xf_gates + xf_blockers:
                blob_parts.append(str(src.get("message") or ""))
                details = src.get("details") or {}
                for err in list(details.get("errors") or [])[:12]:
                    blob_parts.append(str(err))
                for it in list(details.get("issue_texts") or [])[:12]:
                    blob_parts.append(str(it))
            for part in blob_parts:
                fc = classify_transform_failure(part)
                if fc is not FailureClass.UNKNOWN:
                    failure_classes.append(fc.value)
            failure_classes = list(dict.fromkeys(failure_classes))
            # Prefer precision-loss / empty-policy when present (action ranking).
            priority = [
                FailureClass.FRACTIONAL_PRECISION_LOSS.value,
                FailureClass.EMPTY_VALUE_NOT_NULLABLE.value,
                FailureClass.SEMANTIC_TRANSFORM_FAILURE.value,
            ]
            ordered = [c for c in priority if c in failure_classes] + [
                c for c in failure_classes if c not in priority
            ]
            failure_classes = ordered
            primary_fc = (
                FailureClass(failure_classes[0])
                if failure_classes
                else FailureClass.TYPE_CAST_FAILURE
            )
            rec_fix = recommended_action_for_failure(primary_fc)
            if len(failure_classes) > 1:
                rec_fix = (
                    rec_fix
                    + " Distinct failure classes on this sample: "
                    + ", ".join(failure_classes)
                    + "."
                )
        except Exception:  # pragma: no cover — kernel always available in GA
            failure_classes = []
            rec_fix = (
                "Open Map → fix types/transforms or Accept risk with "
                "QUARANTINE_ROW / CAST_AND_CONTINUE → re-run Validate."
            )
        biz = (
            "Sample cells cannot pass the same transforms writers use. "
            "Execute stays locked until Map remediates each failure class "
            "(fractional→numeric widen, empty→nullability/quarantine, semantic "
            "transform off) or a continue-policy Risk Contract holds them out."
        )
        if failure_classes:
            biz = (
                f"Failure class(es): {', '.join(failure_classes)}. " + biz
            )
        roots.append(
            MigrationRootCause(
                root_id=_root_id("sample_transform", cols, absorbed),
                kind="sample_transform",
                title="Sample transform / cast failures",
                summary=(
                    (
                        f"{len(cols)} column(s) fail write-path transforms on the "
                        f"Validate sample ({col_label})"
                        if cols
                        else "Write-path transforms failed on the Validate sample"
                    )
                    + f" — impacts {len(absorbed)} gate check(s)"
                    + (
                        f" · classes: {', '.join(failure_classes)}"
                        if failure_classes
                        else ""
                    )
                ),
                business_impact=biz,
                affected_columns=cols,
                affected_rows_sample=sample_n,
                estimated_total_rows=est_n,
                risk_level="high",
                recommended_fix=rec_fix,
                alternative_fixes=[
                    "Widen FLOAT/DECIMAL→INT to DOUBLE (preserve numeric meaning before text)",
                    "Empty→typed: allow NULL / default or sign QUARANTINE_ROW",
                    "Remap image/url columns off the url semantic transform",
                    "Sign CAST_AND_CONTINUE only for intentional lossy casts",
                ],
                recovery_strategy=(
                    "After remap or Risk Contract, re-Validate. Contracted holdouts "
                    "quarantine at write — primary table must not invent NULL unless "
                    "the contract says so."
                ),
                expected_runtime_impact="Re-Validate is sample-scoped",
                quarantine_policy=(
                    "holdout_rejected_rows under CAST_AND_CONTINUE / QUARANTINE_ROW "
                    "(see docs/MIGRATION_RISK_CONTRACT.md)"
                ),
                rollback_policy="DOCUMENT_ONLY",
                documentation="docs/MIGRATION_RISK_CONTRACT.md",
                impacted_gates=absorbed,
                absorbed_blocker_ids=absorbed,
                severity="block",
            )
        )

    enc_gates = [
        g
        for g in gates
        if g.get("status") == "block"
        and _is_encoding_signal(
            str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or "")
        )
    ]
    enc_blockers = [
        b
        for b in blockers
        if _is_encoding_signal(
            str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or "")
        )
    ]
    if enc_gates or enc_blockers:
        absorbed = sorted(
            {
                *[str(g.get("id")) for g in enc_gates if g.get("id")],
                *[str(b.get("id")) for b in enc_blockers if b.get("id")],
            }
        )
        cols = []
        chars: list[str] = []
        transforms: list[str] = []
        for src in enc_gates + enc_blockers:
            details = src.get("details") or {}
            cols.extend(_columns_from_details(details))
            for issue in details.get("encoding_issues") or details.get("issues") or []:
                if not isinstance(issue, dict):
                    continue
                col = issue.get("column") or issue.get("source")
                if col:
                    cols.append(str(col))
                chars.extend(str(c) for c in (issue.get("chars") or []) if c)
                sug = issue.get("suggested_transform")
                if sug:
                    transforms.append(str(sug))
        cols = list(dict.fromkeys(cols))
        chars = list(dict.fromkeys(chars))
        transform = next(iter(dict.fromkeys(transforms)), "strip_controls")
        char_label = f" ({', '.join(chars[:4])})" if chars else ""
        col_label = ", ".join(cols[:5]) + (
            f" (+{len(cols) - 5} more)" if len(cols) > 5 else ""
        )
        roots.append(
            MigrationRootCause(
                root_id=_root_id("encoding_normalization", cols, absorbed),
                kind="encoding_normalization",
                title="Invisible / undecodable characters in source text",
                summary=(
                    (
                        f"{len(cols)} column(s) carry characters that do not "
                        f"survive as data{char_label}: {col_label}"
                    )
                    if cols
                    else f"Undecodable or invisible characters{char_label} on the Validate sample"
                ),
                business_impact=(
                    "Zero-width, bidi and control characters silently break joins, "
                    "lookups and exact-match search in the destination — the value "
                    "looks identical to a human and compares unequal to a machine. "
                    "The type path is not the problem, so remapping it fixes nothing."
                ),
                affected_columns=cols,
                affected_rows_sample=sample_n,
                estimated_total_rows=est_n,
                risk_level="high",
                recommended_fix=(
                    f"Open Map → apply the '{transform}' transform to "
                    f"{col_label or 'the affected column(s)'} → re-run Validate."
                ),
                alternative_fixes=[
                    "Sign QUARANTINE_ROW to hold out affected rows instead of normalizing",
                    "Normalize at the source (Unicode NFC + control strip) and re-read",
                    "Accept the characters explicitly if the destination is a text archive",
                ],
                recovery_strategy=(
                    "Normalization is applied on read, so already-written rows keep "
                    "the original characters — rewrite the affected table after the "
                    "transform is in place."
                ),
                expected_runtime_impact=(
                    "Re-Validate is sample-scoped; the transform costs one pass per cell"
                ),
                quarantine_policy=(
                    "holdout_rejected_rows under QUARANTINE_ROW when normalization is refused"
                ),
                rollback_policy="DOCUMENT_ONLY",
                documentation="docs/MIGRATION_RISK_CONTRACT.md",
                impacted_gates=absorbed,
                absorbed_blocker_ids=absorbed,
                severity="block",
            )
        )

    # Keys the destination already stores are not duplicates *in the source* —
    # the source can be perfectly unique. Naming it "duplicate identity keys on
    # the Validate sample" and prescribing "dedupe the source" sends the operator
    # to fix data that is already correct, so this collision owns its own root.
    collision_findings = [
        item
        for item in (
            *[g for g in gates if g.get("status") == "block"],
            *blockers,
        )
        if _is_destination_collision_signal(item.get("details") or {})
    ]
    if collision_findings:
        absorbed = sorted({str(i.get("id")) for i in collision_findings if i.get("id")})
        details = collision_findings[0].get("details") or {}
        key = str((details.get("primary_key") or {}).get("target") or "") or "the identity key"
        stored = len(details.get("sample_collisions") or [])
        sync_mode = str(details.get("sync_mode") or "append")
        roots.append(
            MigrationRootCause(
                root_id=_root_id("destination_key_collision", [key], absorbed),
                kind="destination_key_collision",
                title="Destination already stores these keys",
                summary=(
                    f"{stored or 'Some'} key value(s) in this batch are already at rest "
                    f"in the destination on {key}, which enforces uniqueness — "
                    f"a {sync_mode} insert aborts on the first one"
                ),
                business_impact=(
                    "The write fails outright, so no rows land. Nothing is "
                    "duplicated and nothing at the destination is damaged."
                ),
                affected_columns=[key] if key != "the identity key" else [],
                affected_rows_sample=sample_n,
                estimated_total_rows=est_n,
                risk_level="high",
                recommended_fix=(
                    f"Switch this sync to upsert/merge on {key} — that is how a row "
                    "the destination already has is meant to land. Use overwrite "
                    "instead if this load should replace the existing table."
                ),
                alternative_fixes=[
                    "Full refresh · overwrite to replace the destination generation",
                    "Append only the rows the destination does not have yet",
                    "Load into a new destination table if both generations must be kept",
                ],
                recovery_strategy=(
                    "Nothing was written, so no cleanup is needed — change the sync "
                    "mode and re-run Validate."
                ),
                expected_runtime_impact=(
                    "Upsert costs a key-resolved write per row; overwrite rewrites "
                    "the whole destination table"
                ),
                quarantine_policy="n/a — the batch is refused before any write",
                rollback_policy="DOCUMENT_ONLY",
                documentation="docs/MIGRATION_ROLLBACK.md",
                impacted_gates=absorbed,
                absorbed_blocker_ids=absorbed,
                severity="block",
            )
        )

    dup_gates = [
        g
        for g in gates
        if g.get("status") == "block"
        and not _is_destination_collision_signal(g.get("details") or {})
        and _is_duplicate_signal(str(g.get("message") or ""), g.get("details") or {}, str(g.get("id") or ""))
    ]
    dup_blockers = [
        b
        for b in blockers
        if not _is_destination_collision_signal(b.get("details") or {})
        and _is_duplicate_signal(str(b.get("message") or ""), b.get("details") or {}, str(b.get("id") or ""))
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

    Also rewrites ``proof_bundle.transfer_decision.blockers`` so proof copy
    matches the collapsed operator list (GA hardening).
    """
    roots = build_root_causes(preflight)
    preflight = {**preflight, "root_causes": [r.to_dict() for r in roots]}
    if not roots:
        return preflight

    absorbed: set[str] = set()
    for r in roots:
        absorbed.update(r.absorbed_blocker_ids)

    root_msg_blobs = " ".join(
        f"{r.title} {r.summary} {r.business_impact}".lower() for r in roots
    )
    remaining = []
    for b in preflight.get("blockers") or []:
        if not b:
            continue
        bid = str(b.get("id") or "")
        if bid in absorbed:
            continue
        if (b.get("details") or {}).get("root_cause"):
            continue
        # proof_N echoes transfer_decision.blockers that often duplicate the
        # collapsed sample_transform / fidelity root — drop the twin.
        # Never substring-match short column names (e.g. "id") into unrelated
        # proof prose — that falsely drops real proof blockers.
        if bid.startswith("proof_"):
            msg = str(b.get("message") or "").lower()
            if (
                "sample transform" in msg
                or "fail write-path" in msg
                or "dry-run failed" in msg
                or "transform / cast" in msg
                or (msg and msg[:80] in root_msg_blobs)
            ):
                continue
        remaining.append(b)
    # Roots first — operator sees cause before residual issues.
    collapsed = [r.as_operator_blocker() for r in roots] + remaining
    preflight["blockers"] = collapsed

    pb = preflight.get("proof_bundle")
    if isinstance(pb, dict):
        td = pb.get("transfer_decision")
        if isinstance(td, dict):
            td = {
                **td,
                "blockers": [
                    str(b.get("message") or b.get("id") or "")
                    for b in collapsed
                    if b
                ],
                "root_causes": [r.to_dict() for r in roots],
            }
            preflight["proof_bundle"] = {**pb, "transfer_decision": td}

    return preflight
