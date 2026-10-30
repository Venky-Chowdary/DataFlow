"""D119 — no undefined-name crash anywhere in the API.

``name 'ci_get' is not defined`` killed every scheduled run 2+ and every
drift-ack / backfill / propagate preflight: a function used a helper its module
never imported, and nothing caught it before production. This sweeps every
API module for the same class, generically:

* an undefined name (pyflakes ``UndefinedName`` / ``UndefinedLocal`` /
  ``UndefinedExport``, the same rules as ruff F821/F822/F823), and
* ``from project.module import name`` (at any scope, so a function-local
  import is covered) where the module defines no such name — the
  ``ImportError`` twin that only fires when that line runs.

The rule is proven against the pre-fix ``preflight_service.py`` (2805c5e1^),
which it flags for ``ci_get``.
"""

from __future__ import annotations

import ast
import os
import subprocess
from pathlib import Path

import pytest

API = Path(__file__).resolve().parents[1]
_ROOTS = [API, API / "src", API.parents[1] / "packages" / "preflight" / "src"]


def _modules() -> list[Path]:
    out = []
    for path in API.rglob("*.py"):
        rel = path.relative_to(API).parts
        if rel[0] in {"tests", "node_modules"} or any(p.startswith(".") for p in rel):
            continue
        out.append(path)
    return out


def _undefined_names(paths: list[Path]) -> list[str]:
    try:
        from pyflakes import api as flakes_api
        from pyflakes import messages as m
        from pyflakes import reporter as flakes_reporter
    except ImportError:
        flakes_api = None
    if flakes_api is None:
        try:
            proc = subprocess.run(
                ["ruff", "check", "--select", "F821,F822,F823", "--no-cache",
                 "--output-format", "concise", *map(str, paths)],
                capture_output=True, text=True, timeout=300, check=False,
            )
        except FileNotFoundError:
            pytest.skip("neither pyflakes nor ruff is installed")
        return [ln for ln in proc.stdout.splitlines() if ": F82" in ln]

    kinds = (m.UndefinedName, m.UndefinedLocal, m.UndefinedExport)
    found: list[str] = []

    class _Collect(flakes_reporter.Reporter):
        def __init__(self):
            super().__init__(open(os.devnull, "w"), open(os.devnull, "w"))

        def flake(self, message):
            if isinstance(message, kinds):
                found.append(str(message))

    rep = _Collect()
    for path in paths:
        flakes_api.checkPath(str(path), rep)
    return found


def _resolve(module: str | None, level: int, here: Path) -> Path | None:
    parts = module.split(".") if module else []
    bases = [here.parent] if level else _ROOTS
    if level:
        for _ in range(level - 1):
            bases = [bases[0].parent]
    for base in bases:
        target = base.joinpath(*parts)
        if target.with_suffix(".py").is_file():
            return target.with_suffix(".py")
        if (target / "__init__.py").is_file():
            return target / "__init__.py"
    return None


def _defined(path: Path, cache: dict) -> tuple[set[str], bool]:
    if path in cache:
        return cache[path]
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    dynamic = False
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                if alias.name == "*":
                    dynamic = True
                names.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.Global):
            names.update(node.names)
    if "__getattr__" in names:
        dynamic = True
    if path.name == "__init__.py":
        names.update(p.stem for p in path.parent.iterdir())
    cache[path] = (names, dynamic)
    return cache[path]


def _guarded_by_import_error(tree: ast.AST) -> set[int]:
    """Line numbers of imports inside ``try: ... except ImportError`` (fallbacks)."""
    lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        names = set()
        for handler in node.handlers:
            for n in ast.walk(handler.type) if handler.type else []:
                if isinstance(n, ast.Name):
                    names.add(n.id)
        if names & {"ImportError", "ModuleNotFoundError", "Exception"}:
            # The try body and its fallback both: one of the two spellings
            # (``services.x`` / ``src.services.x``) is absent by design.
            for stmt in [*node.body, *(s for h in node.handlers for s in h.body)]:
                for n in ast.walk(stmt):
                    if isinstance(n, ast.ImportFrom):
                        lines.add(n.lineno)
    return lines


def test_no_undefined_names_in_api_modules():
    found = _undefined_names(_modules())
    assert found == [], "\n".join(found[:50])


def test_no_import_of_a_name_the_project_module_does_not_define():
    cache: dict = {}
    missing: list[str] = []
    for path in _modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        guarded = _guarded_by_import_error(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.lineno in guarded:
                continue
            target = _resolve(node.module, node.level, path)
            if target is None:
                continue
            names, dynamic = _defined(target, cache)
            if dynamic:
                continue
            for alias in node.names:
                if alias.name != "*" and alias.name not in names:
                    missing.append(
                        f"{path.relative_to(API)}:{node.lineno} imports {alias.name} "
                        f"from {node.module}, which does not define it"
                    )
    assert missing == [], "\n".join(missing[:50])


def test_the_sweep_catches_the_original_ci_get_crash(tmp_path):
    """The pre-fix preflight_service used ci_get without importing it."""
    bad = tmp_path / "preflight_like.py"
    bad.write_text(
        "def widen(schema, col):\n"
        "    return ci_get(schema, col)\n",
        encoding="utf-8",
    )
    assert any("ci_get" in line for line in _undefined_names([bad]))
