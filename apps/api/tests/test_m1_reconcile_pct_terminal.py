"""M1 — reconcile_pct reaches 100 on verified jobs.

The heartbeat paints 0–99 while the checksum scans, but nothing stamped 100
when Gate-8 passed: verified jobs showed reconcile_pct=99, and engines whose
verify path skipped the heartbeat (DuckDB) stayed at 0. Every success-status
write that follows a passed reconciliation must stamp reconcile_pct=100.
"""

from __future__ import annotations

import ast
from pathlib import Path

ENGINE = Path(__file__).resolve().parents[1] / "src" / "transfer" / "engine.py"


def _success_calls() -> list[ast.Call]:
    tree = ast.parse(ENGINE.read_text())
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = getattr(func, "id", None) or getattr(func, "attr", None)
        if name == "_write_success_status":
            out.append(node)
    return out


def test_every_success_write_stamps_reconcile_pct_100():
    calls = _success_calls()
    assert calls, "engine must write its success status through _write_success_status"
    missing = []
    for call in calls:
        kwargs = {k.arg for k in call.keywords if k.arg}
        if "progress_pct" not in kwargs:
            # non-terminal calls (e.g. cancel bookkeeping) are out of scope
            continue
        if "reconcile_pct" not in kwargs:
            missing.append(call.lineno)
    assert not missing, (
        f"_write_success_status calls at lines {missing} complete the job "
        "without stamping reconcile_pct=100"
    )
