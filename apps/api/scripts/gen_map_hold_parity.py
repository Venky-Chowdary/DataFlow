"""Regenerate apps/web/src/lib/__fixtures__/map_hold_parity.json from the server's own rules.

Run from apps/api: ``python scripts/gen_map_hold_parity.py``. tests/test_map_hold_parity.py
fails if the stored catalogs or expected holds stop matching the server.
"""
import json
import sys
from pathlib import Path

API = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API))
OUT = API.parent / "web" / "src" / "lib" / "__fixtures__" / "map_hold_parity.json"
from services.mapping_dest_catalog import build_destination_column_catalog  # noqa: E402

def info(cols, measured=True, generated=(), only=None):
    return {
        "connected": True, "table_exists": True,
        "columns": [c[0] for c in cols],
        "schema": {c[0]: c[1] for c in cols},
        "schema_nullability": ({c[0]: c[2] for c in cols if only is None or c[0] in only}
                               if measured else {cols[-1][0]: cols[-1][2]}),
        "schema_defaults": {c[0]: c[3] for c in cols if c[3] is not None},
        "identity_columns": [], "generated_columns": list(generated), "primary_key_columns": [],
    }

BASE = [("a", "BIGINT", False, None), ("b", "VARCHAR(40)", True, None)]
CASES = [
    ("required_unmapped_measured", info(BASE), "manual_review", False, [("x", "b")], ["required_unmapped"]),
    ("required_unmeasured_is_not_held", info(BASE, measured=False), "manual_review", False, [("x", "b")], []),
    ("partial_nullability_holds_nothing", info(BASE, only={"a"}), "manual_review", False, [("x", "b")], []),
    ("recreated_table_holds_no_live_constraint", info(BASE), "pause_on_change", True, [("x", "b"), ("y", "c")], []),
    ("duplicate_target", info(BASE), "manual_review", False, [("x", "a"), ("y", "a")], ["target_conflict"]),
    ("new_name_refused_by_policy", info(BASE), "pause_on_change", False, [("x", "a"), ("y", "c")], ["new_target_refused"]),
    ("new_name_allowed", info(BASE), "manual_review", False, [("x", "a"), ("y", "c")], []),
    ("case_insensitive_is_the_same_column", info(BASE), "manual_review", False, [("x", "A")], []),
    ("generated_target", info(BASE + [("g", "BIGINT", True, None)], generated=["g"]), "manual_review", False,
     [("x", "a"), ("y", "g")], ["generated_target"]),
]
out = []
for name, inf, policy, recreated, pairs, expected in CASES:
    out.append({
        "name": name, "info": inf, "schema_policy": policy, "table_recreated": recreated,
        "catalog": build_destination_column_catalog(inf, dest_type="postgresql", schema_policy=policy),
        "mappings": [{"source": s, "target": t} for s, t in pairs],
        "expected": expected,
    })
json.dump({"_about": "UI Map holds vs server gates on the same inputs (review M6). Regenerate with the API builder; tests/test_map_hold_parity.py proves the catalog and the server outcome.", "cases": out},
          OUT.open("w", encoding="utf-8"), indent=1)
print(len(out))
