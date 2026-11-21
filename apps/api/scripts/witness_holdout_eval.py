#!/usr/bin/env python3
"""Measure the Mapping Witness on a held-out mapping benchmark (false flags and true flags).

The held-out file is never read by the witness itself and never tuned against:
thresholds are the fixed constants in ``services.mapping_witness``.

Input: the benchmark as a Python module exposing ``P`` (the research
``holdout_v1.py``) or as JSON (a list of the same table dicts). Each table has
typed, sampled source columns (``"name:TYPE|s1;s2"``), target columns and a
gold ``{source: target | None}``.

What is measured (each table's samples are cycled into ``--rows`` rows, keyed
by a synthetic row id; the destination is built from the gold mapping, i.e.
"another system already loaded these rows correctly"):

* ``gold_keyed`` / ``gold_source_only`` — the witness on the *correct* mapping:
  every flag is a false flag.
* ``mapper_keyed`` / ``mapper_source_only`` — the witness on what the mapper
  actually proposed: flags on correct proposals are false flags, flags on wrong
  proposals are true flags.
* ``injected_swaps_keyed`` — every pair of same-typed gold columns swapped in
  turn: a swap counts as caught when both halves are flagged.

Run from apps/api:  PYTHONPATH=. python scripts/witness_holdout_eval.py PATH [--rows 20] [--out FILE]
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.mapping_witness import STATUS_FLAGGED, witness_mappings  # noqa: E402

ROW_ID = "__witness_row_id"


def load_tables(path: str) -> list[dict[str, Any]]:
    p = Path(path)
    if p.suffix == ".py":
        spec = importlib.util.spec_from_file_location("holdout_bench", p)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)
        return list(mod.P)
    raw = json.loads(p.read_text(encoding="utf-8"))
    return list(raw["tables"] if isinstance(raw, dict) else raw)


def parse(spec: str) -> tuple[str, str, list[str]]:
    name, _, rest = spec.partition(":")
    typ, _, samp = rest.partition("|")
    return name, typ, [s for s in samp.split(";") if s] if samp else []


def build_rows(table: dict[str, Any], n: int) -> tuple[list[dict], list[dict], dict[str, str]]:
    src_cols = [parse(s) for s in table["source"]]
    types = {name: typ for name, typ, _ in src_cols}
    source: list[dict] = []
    for i in range(n):
        row: dict[str, Any] = {ROW_ID: i}
        for c, (name, _typ, samples) in enumerate(src_cols):
            row[name] = samples[(i + c) % len(samples)] if samples else None
        source.append(row)
    dest: list[dict] = []
    for row in source:
        d: dict[str, Any] = {ROW_ID: row[ROW_ID]}
        for s, t in (table.get("gold") or {}).items():
            if t:
                d[t] = row.get(s)
        dest.append(d)
    return source, dest, types


def _target_types(table: dict[str, Any]) -> dict[str, str]:
    return {n: t for n, t, _ in (parse(x) for x in table["target"])}


def _pairs(mapping: dict[str, str | None], ttypes: dict[str, str]) -> list[dict[str, Any]]:
    return [{"source": s, "target": t, "target_type": ttypes.get(t, "")} for s, t in mapping.items() if t]


def _with_key(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"source": ROW_ID, "target": ROW_ID}] + pairs


def mapper_proposals(table: dict[str, Any]) -> dict[str, str | None]:
    from services.semantic_mapper import map_columns

    src = [parse(s) for s in table["source"]]
    tgt = [parse(t) for t in table["target"]]
    tnames = [t[0] for t in tgt]
    out = map_columns(
        [s[0] for s in src], tnames, destination_db_type="postgresql", destination_table_exists=True,
        source_db_type=table.get("source_db") or "",
        source_schemas=[{"name": n, "inferred_type": t, "samples": s} for n, t, s in src],
        target_schemas=[{"name": n, "inferred_type": t} for n, t, _ in tgt],
    )
    by = {m["source"]: m for m in out}
    proposals: dict[str, str | None] = {}
    for n, _, _ in src:
        m = by.get(n) or {}
        t = m.get("target")
        proposals[n] = t if (t in tnames and not m.get("create_new")) else None
    return proposals


def _score(report: dict[str, Any], gold: dict[str, str | None]) -> Counter:
    c: Counter = Counter()
    for p in report["pairs"]:
        if p["source"] == ROW_ID:
            continue
        correct = gold.get(p["source"]) == p["target"]
        flagged = p["status"] == STATUS_FLAGGED
        c["pairs"] += 1
        c["correct" if correct else "wrong"] += 1
        if flagged:
            c["flag_on_correct" if correct else "flag_on_wrong"] += 1
        c[f"status:{p['status']}"] += 1
    return c


def run(tables: list[dict[str, Any]], rows: int, with_mapper: bool) -> dict[str, Any]:
    totals: dict[str, Counter] = {k: Counter() for k in (
        "gold_keyed", "gold_source_only", "mapper_keyed", "mapper_source_only", "injected_swaps_keyed")}
    false_flags: list[dict[str, Any]] = []
    caught_wrong: list[dict[str, Any]] = []
    for table in tables:
        gold = table.get("gold") or {}
        ttypes = _target_types(table)
        source, dest, stypes = build_rows(table, rows)
        gold_pairs = _pairs(gold, ttypes)
        for name, rep in (
            ("gold_keyed", witness_mappings(_with_key(gold_pairs), source, dest, key=(ROW_ID, ROW_ID))),
            ("gold_source_only", witness_mappings(gold_pairs, source, None)),
        ):
            totals[name] += _score(rep, gold)
            for p in rep["pairs"]:
                if p["status"] == STATUS_FLAGGED:
                    false_flags.append({"table": table["id"], "mode": name, "source": p["source"],
                                        "target": p["target"], "reason": p["flag"]["reason"]})
        if with_mapper:
            proposed = mapper_proposals(table)
            mp = _pairs(proposed, ttypes)
            for name, rep in (
                ("mapper_keyed", witness_mappings(_with_key(mp), source, dest, key=(ROW_ID, ROW_ID))),
                ("mapper_source_only", witness_mappings(mp, source, None)),
            ):
                totals[name] += _score(rep, gold)
                for p in rep["pairs"]:
                    if p["source"] == ROW_ID or p["status"] != STATUS_FLAGGED:
                        continue
                    entry = {"table": table["id"], "mode": name, "source": p["source"], "target": p["target"],
                             "gold": gold.get(p["source"]), "kind": p["flag"]["kind"],
                             "reason": p["flag"]["reason"]}
                    (caught_wrong if gold.get(p["source"]) != p["target"] else false_flags).append(entry)
        # Injected swaps between same-typed gold columns with different values.
        mapped = [(s, t) for s, t in gold.items() if t]
        for i in range(len(mapped)):
            for j in range(i + 1, len(mapped)):
                (s1, t1), (s2, t2) = mapped[i], mapped[j]
                if stypes.get(s1, "").split("(")[0] != stypes.get(s2, "").split("(")[0]:
                    continue
                if [r.get(s1) for r in source] == [r.get(s2) for r in source]:
                    continue  # identical columns: a swap changes nothing
                swapped = {**gold, s1: t2, s2: t1}
                rep = witness_mappings(_with_key(_pairs(swapped, ttypes)), source, dest, key=(ROW_ID, ROW_ID))
                flagged = {p["source"] for p in rep["pairs"] if p["status"] == STATUS_FLAGGED}
                totals["injected_swaps_keyed"]["swaps"] += 1
                totals["injected_swaps_keyed"]["caught_both" if {s1, s2} <= flagged else (
                    "caught_one" if flagged & {s1, s2} else "missed")] += 1
                others = flagged - {s1, s2}
                totals["injected_swaps_keyed"]["false_flags_on_other_columns"] += len(others)
    summary: dict[str, Any] = {}
    for name, c in totals.items():
        if name == "injected_swaps_keyed":
            n = c["swaps"]
            summary[name] = {**dict(c), "recall_both": round(c["caught_both"] / n, 4) if n else None}
            continue
        correct, wrong = c["correct"], c["wrong"]
        summary[name] = {
            **dict(c),
            "false_flag_rate": round(c["flag_on_correct"] / correct, 4) if correct else None,
            "true_flag_rate": round(c["flag_on_wrong"] / wrong, 4) if wrong else None,
        }
    return {"rows_per_table": rows, "tables": len(tables), "summary": summary,
            "false_flags": false_flags, "true_flags": caught_wrong}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--rows", type=int, default=20)
    ap.add_argument("--no-mapper", action="store_true")
    ap.add_argument("--out")
    args = ap.parse_args()
    result = run(load_tables(args.path), args.rows, with_mapper=not args.no_mapper)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    print(json.dumps(result["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
