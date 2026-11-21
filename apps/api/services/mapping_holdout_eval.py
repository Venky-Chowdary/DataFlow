"""Held-out mapping benchmark: score ``semantic_mapper.map_columns`` on a blind set.

The fixture (``tests/fixtures/mapping_holdout_v1.json``) was written before the
mapper's dictionaries were read and is split by table into ``dev`` (tune here)
and ``test`` (report and gate here, never tune). Every number is computed by
calling the real mapper with typed source schemas (with samples) and typed
destination schemas against an existing PostgreSQL table.

Definitions (one row per source column):

* ``pred``: the existing destination column the mapper proposes (create-new and
  pending rows are abstentions, ``pred = None``).
* ``auto``: ``pred`` is set, the row is not flagged for review and its
  confidence is at least the G4 floor (0.85). This is what Map approves without
  an operator.
* ``top1``: ``pred == gold`` over matchable columns (gold exists).
* ``top3``: gold within the proposal plus the first alternatives, in order.
* ``auto_precision``: correct auto rows / auto rows; ``wrong_auto`` counts the rest.
* ``neg_auto``: columns with no true counterpart that were auto-mapped.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

API_ROOT = Path(__file__).resolve().parents[1]
HOLDOUT_V1 = API_ROOT / "tests" / "fixtures" / "mapping_holdout_v1.json"
AUTO_APPROVE_FLOOR = 0.85
SPLITS = ("dev", "test")

# CI floors (minimums) and ceilings (maximums) per split, set to the values
# measured on this fixture. Ratchet only: raise a floor or lower a ceiling
# after a re-measurement; never loosen one. ``test`` is the reported number;
# ``dev`` is where changes are tuned. Auto-approve precision >= 0.98 and
# no auto-mapped negatives hold on every split.
HOLDOUT_FLOORS: dict[str, dict[str, float]] = {
    "test": {"top1": 0.6636, "top3": 0.8909, "auto_precision": 0.98, "auto_precision_wilson_lb": 0.9124,
             "auto_coverage": 0.3636, "proposal_precision": 0.9241},
    "dev": {"top1": 0.75, "top3": 0.9688, "auto_precision": 0.973, "auto_coverage": 0.375,
            "proposal_precision": 0.9351},
    "all": {"top1": 0.7039, "top3": 0.9272, "auto_precision": 0.98},
}
HOLDOUT_CEILINGS: dict[str, dict[str, int]] = {
    "test": {"neg_auto": 0, "wrong_auto": 0, "neg_proposed": 1},
    "dev": {"neg_auto": 0, "wrong_auto": 1, "neg_proposed": 1},
    "all": {"neg_auto": 0, "wrong_auto": 1},
}


def load_holdout(path: Path | None = None) -> dict[str, Any]:
    return json.loads((path or HOLDOUT_V1).read_text(encoding="utf-8"))


def tables_for(doc: dict[str, Any], split: str | None) -> list[dict[str, Any]]:
    if split in (None, "", "all"):
        return list(doc["tables"])
    if split not in SPLITS:
        raise ValueError(f"unknown split {split!r}")
    return [t for t in doc["tables"] if t["split"] == split]


def _ranked(m: dict[str, Any], proposed: str | None, tnames: set[str]) -> list[str]:
    ranked = ([proposed] if proposed else []) + [
        a.get("target") for a in (m.get("alternatives") or []) if a.get("target") in tnames
    ]
    seen: set[str] = set()
    out: list[str] = []
    for r in ranked:
        if r and r not in seen:
            seen.add(r)
            out.append(r)
    return out[:5]


def run_table(table: dict[str, Any], *, names_only: bool = False, dest: dict | None = None) -> list[dict]:
    from services.semantic_mapper import map_columns

    dest = dest or {"destination_db_type": "postgresql", "destination_table_exists": True}
    src = table["source"]
    tgt = table["target"]
    tnames = [t["name"] for t in tgt]
    kw: dict[str, Any] = dict(
        destination_db_type=dest["destination_db_type"],
        destination_table_exists=dest["destination_table_exists"],
        source_db_type=table["source_db"],
    )
    if not names_only:
        kw["source_schemas"] = [
            {"name": s["name"], "inferred_type": s["type"], "samples": list(s.get("samples") or [])}
            for s in src
        ]
        kw["target_schemas"] = [{"name": t["name"], "inferred_type": t["type"]} for t in tgt]
    out = map_columns([s["name"] for s in src], tnames, **kw)
    by = {m["source"]: m for m in out}
    tset = set(tnames)
    rows = []
    for s in src:
        n = s["name"]
        m = by.get(n) or {}
        raw = m.get("target")
        proposed = raw if (raw in tset and not m.get("create_new")) else None
        conf = float(m.get("confidence") or 0)
        auto = bool(proposed) and not m.get("requires_review") and conf >= AUTO_APPROVE_FLOOR
        rows.append(
            dict(
                table=table["id"], bucket=table["bucket"], split=table["split"], source=n,
                gold=table["gold"].get(n), pred=proposed, raw_target=raw,
                create_new=bool(m.get("create_new")), conf=conf,
                review=bool(m.get("requires_review")), auto=auto,
                ranked=_ranked(m, proposed, tset), strategy=m.get("assignment_strategy"),
                review_kind=m.get("review_kind"),
            )
        )
    return rows


def run_holdout(split: str | None = None, *, names_only: bool = False, path: Path | None = None) -> list[dict]:
    doc = load_holdout(path)
    rows: list[dict] = []
    for table in tables_for(doc, split):
        rows.extend(run_table(table, names_only=names_only, dest=doc.get("destination")))
    return rows


def wilson_lower_bound(successes: int, n: int, z: float = 1.959964) -> float | None:
    """Wilson score 95% lower bound for a binomial proportion."""
    if n <= 0:
        return None
    p = successes / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return round((centre - margin) / denom, 4)


def metrics(rows: Iterable[dict]) -> dict[str, Any]:
    rows = list(rows)
    pos = [r for r in rows if r["gold"]]
    neg = [r for r in rows if not r["gold"]]
    prop = [r for r in rows if r["pred"]]
    corr = [r for r in pos if r["pred"] == r["gold"]]
    auto = [r for r in rows if r["auto"]]
    auto_ok = [r for r in auto if r["pred"] == r["gold"]]
    top3 = [r for r in pos if r["gold"] in r["ranked"][:3]]
    mrr = sum(1.0 / (r["ranked"].index(r["gold"]) + 1) for r in pos if r["gold"] in r["ranked"])

    def f(a: int, b: int) -> float | None:
        return round(a / b, 4) if b else None

    prec = f(len(corr), len(prop))
    rec = f(len(corr), len(pos))
    return dict(
        n_sources=len(rows), n_matchable=len(pos), n_negative=len(neg),
        top1=rec, top1_correct=len(corr), top3=f(len(top3), len(pos)),
        mrr=round(mrr / len(pos), 4) if pos else None,
        proposal_precision=prec,
        f1=round(2 * prec * rec / (prec + rec), 4) if prec and rec else 0.0,
        proposals=len(prop), wrong_proposals=len(prop) - len([r for r in prop if r["pred"] == r["gold"]]),
        auto_count=len(auto), auto_correct=len(auto_ok),
        auto_precision=f(len(auto_ok), len(auto)),
        auto_precision_wilson_lb=wilson_lower_bound(len(auto_ok), len(auto)),
        auto_coverage=f(len(auto_ok), len(pos)),
        wrong_auto=len(auto) - len(auto_ok),
        neg_proposed=sum(1 for r in neg if r["pred"]),
        neg_auto=sum(1 for r in neg if r["auto"]),
        abstain_on_matchable=sum(1 for r in pos if not r["pred"]),
    )


def evaluate(split: str | None = None, *, names_only: bool = False, path: Path | None = None) -> dict[str, Any]:
    rows = run_holdout(split, names_only=names_only, path=path)
    by_bucket: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_bucket[r["bucket"]].append(r)
    return {
        "split": split or "all",
        "mode": "names_only" if names_only else "typed_with_samples",
        "overall": metrics(rows),
        "buckets": {b: metrics(v) for b, v in sorted(by_bucket.items())},
        "rows": rows,
    }


def gate_failures(split: str, overall: dict[str, Any]) -> list[str]:
    """Floors/ceilings this split misses (empty when the gate passes)."""
    out = []
    for key, floor in HOLDOUT_FLOORS.get(split, {}).items():
        got = overall.get(key)
        if got is None or got < floor:
            out.append(f"{split}.{key}={got} < floor {floor}")
    for key, ceiling in HOLDOUT_CEILINGS.get(split, {}).items():
        got = overall.get(key)
        if got is None or got > ceiling:
            out.append(f"{split}.{key}={got} > ceiling {ceiling}")
    return out


def auto_threshold_curve(rows: Iterable[dict], thresholds: Iterable[float] | None = None) -> list[dict]:
    """Precision of non-review existing-column proposals at each confidence threshold."""
    cand = [r for r in rows if r["pred"] and not r["review"]]
    out = []
    for t in thresholds or (0.70, 0.75, 0.80, 0.85, 0.90, 0.95):
        sel = [r for r in cand if r["conf"] >= t]
        ok = sum(1 for r in sel if r["pred"] == r["gold"])
        out.append({
            "threshold": t, "auto_count": len(sel), "correct": ok,
            "precision": round(ok / len(sel), 4) if sel else None,
            "wilson_lb": wilson_lower_bound(ok, len(sel)),
        })
    return out


def calibrate_auto_threshold(rows: Iterable[dict], *, target: float = 0.98, min_lower_bound: float = 0.95) -> dict:
    """Lowest threshold whose precision meets ``target`` with a Wilson lower bound
    of at least ``min_lower_bound``; ``None`` when the sample cannot support one.

    Used on the dev split only. The product's auto-approve floor (G4, 0.85) is
    never lowered from this: a recommendation below it is reported, not applied.
    """
    curve = auto_threshold_curve(list(rows), [round(0.70 + 0.01 * i, 2) for i in range(30)])
    safe = [c for c in curve if c["precision"] is not None and c["precision"] >= target]
    supported = [c for c in safe if (c["wilson_lb"] or 0) >= min_lower_bound]
    unsafe = [c["threshold"] for c in curve if c["precision"] is not None and c["precision"] < target]
    return {
        "target_precision": target,
        "recommended": supported[0]["threshold"] if supported else None,
        "lowest_meeting_target": safe[0]["threshold"] if safe else None,
        "highest_unsafe": max(unsafe) if unsafe else None,
        "curve": curve,
    }
