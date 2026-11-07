"""QA browser 9: user-facing messages name the product "Datawrap" (services/brand.py).

The CI bundle gate covers the web; API sentences that reach the operator
(rollback refusals, recovery notes, mapping-proof risks) still said "DataWrap".
This scans every string literal the API can return — docstrings and comments
excluded — for the old casing and the old brand.
"""

from __future__ import annotations

import ast
from pathlib import Path

API = Path(__file__).resolve().parents[1]
ROOTS = ("services", "src", "connectors")
#: services/brand.py holds LEGACY_PRODUCT_NAME = "DataFlow" on purpose (it is
#: how old artifacts are recognised); it is never presented.
SKIP_FILES = {"services/brand.py"}
#: Names that are not the product: Google Cloud Dataflow, legacy artifact names
#: recognised on purpose (services.brand LEGACY_*), dialect class names.
ALLOWED = ("Cloud Dataflow", "Dataflow, and Datawrap", "not Dataflow", "DataFlowManifest",
           "DataFlowBigQueryDialect", "google cloud dataflow")


def _docstrings(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                ids.add(id(body[0].value))
    return ids


def test_no_user_facing_string_spells_the_brand_wrong():
    hits: list[str] = []
    for root in ROOTS:
        for path in (API / root).rglob("*.py"):
            if str(path.relative_to(API)) in SKIP_FILES:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            skip = _docstrings(tree)
            for node in ast.walk(tree):
                if not (isinstance(node, ast.Constant) and isinstance(node.value, str)) or id(node) in skip:
                    continue
                text = node.value
                if "DataWrap" in text or ("DataFlow" in text and not any(a in text for a in ALLOWED)):
                    hits.append(f"{path.relative_to(API)}:{node.lineno}: {text[:80]!r}")
    assert not hits, "\n".join(hits[:40])
