"""Follow-up review 6: no test leaves the process without a data directory (CI rule 6).

``platform_config.data_dir()`` falls back to the source tree's ``apps/api/data``
when neither ``DATAWRAP_DATA_DIR`` nor ``DATAFLOW_DATA_DIR`` is set: the right
default for a developer's server, and the wrong one inside the suite. Three
audit-script tests deleted ``DATAFLOW_DATA_DIR`` for their whole duration (so
the script's own choice could be restored afterwards). Any background thread an
earlier test left running (a sweeper, a lease heartbeat, the mail lane) that
reached the shared control-plane store in that window wrote
``services.control_plane_store``'s ``.control_plane_seen`` marker into
``apps/api/data``: the reviewer found it there after a run with
``DATAFLOW_DATA_DIR`` pointing at the scratch area. The session guard in
``conftest.py`` (``_guard_no_test_writes_dev_data_dir``) fails a run when that
happens, but only when the race is lost, so it was flaky rather than caught.

This test reads every test module: a function that deletes
``DATAFLOW_DATA_DIR`` from the process environment (``monkeypatch.delenv``,
``os.environ.pop``, ``del os.environ[...]``, including through a loop over
names) must also set ``DATAFLOW_DATA_DIR`` or ``DATAWRAP_DATA_DIR`` to a
directory of its own. A dictionary built for a child process is not the
process environment and is not judged.
"""

from __future__ import annotations

import ast
import itertools
from pathlib import Path

TESTS = Path(__file__).resolve().parent
_NAMES = {"DATAFLOW_DATA_DIR", "DATAWRAP_DATA_DIR"}


def _loop_values(function: ast.AST) -> dict[str, set[str]]:
    """Name -> the string constants a ``for`` loop in ``function`` binds it to."""
    found: dict[str, set[str]] = {}
    for node in ast.walk(function):
        if isinstance(node, ast.For) and isinstance(node.target, ast.Name) and isinstance(
            node.iter, (ast.Tuple, ast.List, ast.Set)
        ):
            values = {e.value for e in node.iter.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)}
            found.setdefault(node.target.id, set()).update(values)
    return found


def _values(expr: ast.AST, loops: dict[str, set[str]]) -> set[str]:
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return {expr.value}
    if isinstance(expr, ast.Name):
        return set(loops.get(expr.id, ()))
    if isinstance(expr, ast.JoinedStr):
        parts: list[set[str]] = []
        for piece in expr.values:
            if isinstance(piece, ast.Constant):
                parts.append({str(piece.value)})
            elif isinstance(piece, ast.FormattedValue):
                parts.append(_values(piece.value, loops) or {"?"})
        return {"".join(combo) for combo in itertools.product(*parts)}
    return set()


def _is_environ(expr: ast.AST) -> bool:
    return isinstance(expr, ast.Attribute) and expr.attr == "environ"


def _effects(function: ast.AST) -> tuple[set[str], set[str]]:
    """(environment names deleted, environment names set) in ``function``."""
    loops = _loop_values(function)
    deleted: set[str] = set()
    set_: set[str] = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.args:
            name = node.func.attr
            target = node.func.value
            if name == "delenv" or (name == "pop" and _is_environ(target)):
                deleted |= _values(node.args[0], loops)
            elif name == "setenv" or (name == "setdefault" and _is_environ(target)):
                value = node.args[1] if len(node.args) > 1 else None
                if isinstance(value, ast.Constant) and value.value == "":
                    continue  # recorded for monkeypatch to restore, not a directory
                set_ |= _values(node.args[0], loops)
        elif isinstance(node, ast.Delete):
            for item in node.targets:
                if isinstance(item, ast.Subscript) and _is_environ(item.value):
                    deleted |= _values(item.slice, loops)
        elif isinstance(node, ast.Assign):
            for item in node.targets:
                if isinstance(item, ast.Subscript) and _is_environ(item.value):
                    set_ |= _values(item.slice, loops)
    return deleted, set_


def test_no_test_leaves_the_process_without_a_data_dir():
    offenders = []
    for path in sorted(TESTS.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            deleted, set_ = _effects(node)
            if "DATAFLOW_DATA_DIR" in deleted and not (set_ & _NAMES):
                offenders.append(f"{path.name}::{node.name}")
    assert offenders == [], (
        "these leave DATAFLOW_DATA_DIR unset, so data_dir() is apps/api/data for the test's "
        f"duration; set it to a tmp directory instead: {offenders}"
    )


def test_the_guard_sees_a_loop_and_a_replacement():
    source = '''
def bad(monkeypatch):
    for var in ("X", "DATAFLOW_DATA_DIR"):
        monkeypatch.setenv(var, "")
        monkeypatch.delenv(var)

def good(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_DATA_DIR", raising=False)
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))

def prefixed(monkeypatch):
    for name in ("DATA_DIR", "EXPORTS_DIR"):
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
'''
    tree = ast.parse(source)
    verdict = {}
    for node in tree.body:
        deleted, set_ = _effects(node)
        verdict[node.name] = "DATAFLOW_DATA_DIR" in deleted and not (set_ & _NAMES)
    # ``bad`` sets "" first (to record the original for monkeypatch), then deletes: still unset.
    assert verdict == {"bad": True, "good": False, "prefixed": True}
