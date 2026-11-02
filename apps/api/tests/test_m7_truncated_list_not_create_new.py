"""M7 — a truncated object list must not label an existing table CREATE-NEW.

endpoint_intelligence marked ``table_exists = False`` whenever the named table
was absent from the bounded probe page — including when the page was truncated
(``objects_truncated=True``). G15 then painted "New destination table — types
CREATE on first write" on a table that exists. With a truncated list the
answer is unknown (None → SHAPE_UNKNOWN → reload_dest_schema), not missing.
"""

from __future__ import annotations

import ast
from pathlib import Path

EI = Path(__file__).resolve().parents[1] / "src" / "transfer" / "endpoint_intelligence.py"


def _miss_blocks() -> list[ast.If]:
    tree = ast.parse(EI.read_text())
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        # the "table not in listed page" guard calls _mark_table_listed_if_present
        src_test = ast.dump(node.test)
        if "_mark_table_listed_if_present" in src_test:
            out.append(node)
    return out


def test_truncated_list_miss_is_unknown_not_missing():
    blocks = _miss_blocks()
    assert blocks, "probe miss branches not found"
    unguarded = []
    for block in blocks:
        for stmt in block.body:
            if not isinstance(stmt, ast.Assign):
                continue
            for target in stmt.targets:
                if (
                    isinstance(target, ast.Subscript)
                    and ast.dump(target.slice) == ast.dump(ast.Constant("table_exists"))
                    and isinstance(stmt.value, ast.Constant)
                    and stmt.value.value is False
                ):
                    unguarded.append(stmt.lineno)
    assert not unguarded, (
        f"lines {unguarded} stamp table_exists=False on a bounded-list miss "
        "without checking objects_truncated"
    )
