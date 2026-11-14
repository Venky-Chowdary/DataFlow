"""UX-19 eval rows: Pilot's transfer answer may not contradict itself.

Each row is a plan and preflight result shaped like the live one that produced
the finding (PostgreSQL ``w4ux.customers`` to a MySQL connection whose
database is ``w4ux_dst``: the destination could not be read, every mapping is
``dest_type_unread``, and preflight blocks on a "fidelity collapse" root and
on Target DDL). The answer is rendered by the real renderer and checked for
each contradiction the audit found.
"""

from __future__ import annotations

import copy
import re
from pathlib import Path

import pytest

from src.ai.copilot import transfer_answer
from src.ai.copilot.pilot_agent import _render_transfer

_COLS = ["id", "name", "country", "city", "email", "amount", "created_at"]
_SRC_TYPES = ["INT4", "TEXT", "CHAR(2)", "TEXT", "TEXT", "DECIMAL(12,2)", "TIMESTAMPTZ"]


def _gates(blocked: set[str], warn: set[str], skipped: set[str]) -> list[dict]:
    ids = [
        "g1_source", "g2_destination", "g3_schema_contract", "g4_mapping_confidence", "g5_dry_run",
        "g6_target_ddl", "g7_capacity", "g8_reconciliation", "g9_data_integrity",
        "g14_destination_requirements", "g3f_population_fit", "g13_source_coverage",
        "g15_dest_exists_shape", "g16_field_reduction", "g19_dest_schema_replacement",
        "g20_code_crosswalk", "g21_control_totals", "g22_dest_referential_integrity",
        "g9_sync_contract", "g10_schema_policy", "g11_validation_posture", "g12_staging_policy",
    ]
    status = lambda g: "block" if g in blocked else "warn" if g in warn else "skip" if g in skipped else "pass"  # noqa: E731
    return [{"id": g, "status": status(g), "message": ""} for g in ids]


LIVE_PLAN = {
    "source": {"connector_name": "w4ux PG source", "table": "w4ux.customers", "column_count": 7},
    "destination": {
        "connector_name": "w4ux MySQL dest", "type": "mysql", "table": "w4ux.customers", "database": "w4ux_dst",
        "column_count": 0, "table_exists": None,
        "read_error": "(1044, \"Access denied for user 'dataflow'@'%' to database 'w4ux'\")",
    },
    "sync_mode": "full_refresh_append",
    "mapped_count": 7,
    "type_conversions": [
        {"source_column": c, "target_column": c, "from_type": t, "to_type": "", "fidelity": "dest_type_unread"}
        for c, t in zip(_COLS, _SRC_TYPES)
    ],
    "preflight": {
        "run_id": "pf_eval", "passed": False, "passed_count": 14, "total_gates": 22, "readiness_score": 63.6,
        "gates": _gates({"g4_mapping_confidence", "g6_target_ddl"}, {"g15_dest_exists_shape"},
                        {"g14_destination_requirements", "g19_dest_schema_replacement", "g20_code_crosswalk",
                         "g21_control_totals", "g22_dest_referential_integrity"}),
        "blockers": [
            {"id": "rc-fidelity-collapse-038a8d8fb3de",
             "message": "Lossy / fidelity collapse across type path: 7 column(s) collapse fidelity on write "
                        "(id, name, country, city, email (+2 more)) — impacts 2 gate check(s)",
             "details": {"recommended_fix": "Open Map → Remap to a fidelity-preserving type."}},
            {"id": "g6_target_ddl",
             "message": "Destination table existence is unknown (introspect probe failed) — re-check "
                        "credentials/schema/table before Validate can approve create-new or existing-table DDL. "
                        "Do not assume the table is missing.",
             "details": {"recommended_fix": ""}},
        ],
    },
}

# The same route once the destination reads and every column keeps its type,
# but Target DDL still blocks (for example the role cannot CREATE).
PRESERVE_PLAN = copy.deepcopy(LIVE_PLAN)
PRESERVE_PLAN["destination"].update(table="customers", table_exists=False, read_error="")
PRESERVE_PLAN["type_conversions"] = [
    {"source_column": "created_at", "target_column": "created_at", "from_type": "TIMESTAMPTZ",
     "to_type": "DATETIME(6)", "fidelity": "preserve"},
]
PRESERVE_PLAN["preflight"]["blockers"] = [
    {"id": "g6_target_ddl", "message": "The destination role cannot CREATE TABLE in w4ux_dst.", "details": {}},
]

# The audit's shape: the plan stamped every column preserve (TIMESTAMP_NTZ on
# both sides) while the root-cause engine called the same columns a collapse.
AUDIT_PLAN = copy.deepcopy(LIVE_PLAN)
AUDIT_PLAN["destination"].update(table_exists=True, read_error="")
AUDIT_PLAN["type_conversions"] = [
    {"source_column": "created_at", "target_column": "created_at", "from_type": "TIMESTAMP_NTZ",
     "to_type": "TIMESTAMP_NTZ", "fidelity": "preserve"},
]
AUDIT_PLAN["preflight"]["blockers"] = [LIVE_PLAN["preflight"]["blockers"][0]]

CLEAN_PLAN = copy.deepcopy(PRESERVE_PLAN)
CLEAN_PLAN["preflight"].update(passed=True, blockers=[], gates=_gates(set(), set(), set()), passed_count=22)


def _answer(plan: dict) -> str:
    refusal = transfer_answer.refusal_sentence(plan, plan["preflight"]) if plan["preflight"]["blockers"] else ""
    return (refusal + "\n\n" if refusal else "") + _render_transfer("plan_transfer", plan)


EVAL_ROWS = [
    pytest.param(LIVE_PLAN, [r"collapse"], [r"Destination types not read for \*\*7\*\* column", r"Destination column types not read"],
                 id="unread-destination-types-are-not-a-collapse"),
    pytest.param(LIVE_PLAN, [r"introspect probe failed"],
                 [r"Could not read `w4ux\.customers` on \*\*w4ux MySQL dest\*\*: this connection has no access to database `w4ux`",
                  r"this connection's database is `w4ux_dst`"],
                 id="unreadable-destination-is-named-not-probe-failed"),
    pytest.param(LIVE_PLAN, [r"no cast/mutate/lossy fidelity risks"], [], id="no-risks-never-beside-blockers-live"),
    pytest.param(PRESERVE_PLAN, [r"no cast/mutate/lossy fidelity risks"], [r"1 type conversion\(s\) keep their values"],
                 id="no-risks-never-beside-blockers-preserve"),
    pytest.param(LIVE_PLAN, [r"[✓✗×!–]g\d", r"\bg\d+[a-z]?_[a-z_]+\b", r"\brc-[a-z-]+-[0-9a-f]+"],
                 [r"14 of 22\*\* checks passed · blocked: Column mappings, Target DDL · warning: Dest-exists shape · 5 not applicable"],
                 id="no-raw-gate-marks-or-ids"),
    pytest.param(AUDIT_PLAN, [r"collapse fidelity", r"no cast/mutate/lossy fidelity risks", r"Lossy / fidelity collapse"],
                 [r"Column types not proven", r"not a collapse"], id="same-type-is-not-called-a-collapse"),
    pytest.param(CLEAN_PLAN, [r"Destination types not read"], [r"no cast/mutate/lossy fidelity risks in the plan"],
                 id="a-clean-plan-still-says-no-risks"),
]


@pytest.mark.parametrize("plan,must_not,must", EVAL_ROWS)
def test_transfer_answer_eval_row(plan: dict, must_not: list[str], must: list[str]) -> None:
    answer = _answer(plan)
    for pattern in must_not:
        assert not re.search(pattern, answer), f"{pattern!r} in:\n{answer}"
    for pattern in must:
        assert re.search(pattern, answer), f"{pattern!r} missing from:\n{answer}"


def test_gate_labels_match_the_studio_catalog() -> None:
    catalog = Path(__file__).resolve().parents[2] / "web" / "src" / "lib" / "preflightGates.ts"
    text = catalog.read_text(encoding="utf-8")
    block = text[text.index("GATE_CATALOG"):text.index("];", text.index("GATE_CATALOG"))]
    pairs = dict(re.findall(r'id:\s*"([a-z0-9_]+)",\s*(?:code:\s*"[^"]*",\s*)?label:\s*"([^"]+)"', block))
    assert len(pairs) >= 20
    assert pairs == {k: v for k, v in transfer_answer.GATE_LABELS.items() if k in pairs}
    assert set(pairs) == set(transfer_answer.GATE_LABELS)


def test_start_transfer_and_dataset_refusals_use_the_reconciled_sentence() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "ai" / "copilot"
    for name in ("transfer_tools.py", "dataset_transfer.py"):
        src = (root / name).read_text(encoding="utf-8")
        assert "refusal_sentence(" in src, name
        assert "f\"{b.get('id')}: {b.get('message')}\"" not in src, name
