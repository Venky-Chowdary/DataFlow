#!/usr/bin/env python3
"""Phase F8 — enforce god-module LOC freeze budgets."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    budget_path = _API_ROOT / "module_size_budgets.json"
    data = json.loads(budget_path.read_text(encoding="utf-8"))
    violations: list[str] = []
    rows: list[dict] = []
    for mod in data.get("modules") or []:
        rel = str(mod["path"])
        path = _API_ROOT / rel
        if not path.is_file():
            violations.append(f"MISSING {rel}")
            continue
        n = sum(1 for _ in path.open(encoding="utf-8", errors="replace"))
        max_lines = int(mod["max_lines"])
        rows.append(
            {
                "path": rel,
                "lines": n,
                "max_lines": max_lines,
                "headroom": max_lines - n,
                "facade": mod.get("facade"),
            }
        )
        if n > max_lines:
            violations.append(f"{rel}: {n} > max {max_lines}")

    out = {
        "schema_version": 1,
        "policy": data.get("policy"),
        "modules": rows,
        "violations": violations,
        "ok": not violations,
    }
    dest = _API_ROOT / "data" / "proofs" / "module_size_budgets.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"ok": out["ok"], "violations": violations, "path": str(dest)}, indent=2))
    for r in rows:
        mark = "OK" if r["headroom"] >= 0 else "OVER"
        print(f"  [{mark}] {r['lines']:5d}/{r['max_lines']:<5d} {r['path']}")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
