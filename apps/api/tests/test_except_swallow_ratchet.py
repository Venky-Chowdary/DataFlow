"""Ratchet: no NEW broad except that swallows inside a loop.

A broad ``except`` whose body ends in ``continue``/``pass`` inside a ``for``
or ``while`` loop is the classic silent-skip pattern — in a transfer path it
can drop rows without surfacing anything. The ~60 audited sites (probe and
introspection loops, best-effort cleanup, fail-closed conversions) are frozen
in ``except_swallow_baseline.json``; this test fails when any file gains a
new site so every future swallow is a deliberate, reviewed decision.

Removing sites is always welcome — a file may shrink below its baseline.
"""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path

API_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = Path(__file__).resolve().parent / "except_swallow_baseline.json"
SCAN_DIRS = ("services", "connectors")


class _LoopSwallowFinder(ast.NodeVisitor):
    def __init__(self) -> None:
        self.in_loop = 0
        self.count = 0

    def visit_For(self, node: ast.For) -> None:
        self.in_loop += 1
        self.generic_visit(node)
        self.in_loop -= 1

    def visit_While(self, node: ast.While) -> None:
        self.in_loop += 1
        self.generic_visit(node)
        self.in_loop -= 1

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        broad = node.type is None or (
            isinstance(node.type, ast.Name)
            and node.type.id in {"Exception", "BaseException"}
        )
        if broad and self.in_loop:
            tail = [
                s
                for s in node.body
                if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))
            ]
            if tail and isinstance(tail[-1], (ast.Continue, ast.Pass)):
                self.count += 1
        self.generic_visit(node)


def _current_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for top in SCAN_DIRS:
        for dirpath, _, files in os.walk(API_ROOT / top):
            for name in files:
                if not name.endswith(".py"):
                    continue
                path = Path(dirpath) / name
                try:
                    tree = ast.parse(path.read_text(encoding="utf-8"))
                except (OSError, SyntaxError):
                    continue
                finder = _LoopSwallowFinder()
                finder.visit(tree)
                if finder.count:
                    counts[path.relative_to(API_ROOT).as_posix()] = finder.count
    return counts


def test_no_new_loop_swallow_sites() -> None:
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    current = _current_counts()
    violations = []
    for path, count in sorted(current.items()):
        allowed = int(baseline.get(path, 0))
        if count > allowed:
            violations.append(
                f"{path}: {count} loop-swallow sites (baseline {allowed})"
            )
    assert not violations, (
        "New broad-except swallow sites inside loops — each is a potential "
        "silent data loss. Re-raise, quarantine, or document why the skip is "
        "safe:\n" + "\n".join(violations)
    )
