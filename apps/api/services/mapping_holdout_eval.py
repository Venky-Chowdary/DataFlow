"""Mapping benchmark evaluator: score Map's mapping on a labelled fixture.

Two engines are scored:

* ``pipeline`` (default, the gate and every published number):
  :func:`services.mapping_pipeline.run_mapping_pipeline` with ``use_llm=False``,
  the path Map and Validate use, including its later demotions and promotions;
* ``scorer``: :func:`services.semantic_mapper.map_columns` alone, labelled as
  scorer-level wherever it is quoted.

Fixtures. ``tests/fixtures/mapping_holdout_v1.json`` is split by table into
``dev`` (tuned on) and ``dev2`` (formerly called "test"; it was re-measured
after each change and two of its rows were cited while tuning, so it is an
inspected set, not a blind one). A sealed blind fixture from another author is
scored with ``--fixture PATH``: the evaluator then prints aggregates only, and
per-row output needs ``--debug``.

Definitions (one row per source column):

* ``pred``: the existing destination column proposed (create-new and pending
  rows are abstentions, ``pred = None``);
* ``auto``: ``pred`` set, not flagged for review, confidence at least the G4
  floor (0.85): what Map approves without an operator;
* ``top1``: ``pred == gold`` over columns with a right answer; ``top3``: gold in
  the proposal plus the first alternatives;
* ``auto_precision``: correct auto rows / auto rows; ``neg_auto``: columns with
  no right answer that were auto-approved.

Intervals are a cluster bootstrap over tables (columns of one table are not
independent), seed fixed.
"""

from __future__ import annotations

import json
import math
import random
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

API_ROOT = Path(__file__).resolve().parents[1]
HOLDOUT_V1 = API_ROOT / "tests" / "fixtures" / "mapping_holdout_v1.json"
AUTO_APPROVE_FLOOR = 0.85
SPLITS = ("dev", "dev2")
ENGINES = ("pipeline", "scorer")
BOOTSTRAP_SEED = 20261122

# CI floors (minimums) and ceilings (maximums) per engine and split, set to the
# values measured on this fixture. Ratchet only: raise a floor or lower a
# ceiling after a re-measurement; never loosen one. Auto-approve precision
# >= 0.98 and no auto-approved column without a right answer hold everywhere.
# (Scorer coverage was lowered once, by the review-only invariant of
# tests/test_mapping_policy_invariant.py: no policy-made auto-approval.)
HOLDOUT_FLOORS: dict[str, dict[str, dict[str, float]]] = {
    "pipeline": {
        "dev2": {"top1": 0.6727, "top3": 0.8727, "auto_precision": 0.98, "auto_precision_wilson_lb": 0.7009,
                 "auto_coverage": 0.0818, "proposal_precision": 0.9367},
        "dev": {"top1": 0.7708, "top3": 0.9479, "auto_precision": 0.98, "auto_coverage": 0.0938,
                "proposal_precision": 0.9487},
        "all": {"top1": 0.7184, "top3": 0.9078, "auto_precision": 0.98},
    },
    # Scorer-level (map_columns alone): never quoted as what Map approves.
    "scorer": {
        "dev2": {"top1": 0.6636, "top3": 0.8909, "auto_precision": 0.98, "auto_coverage": 0.3455,
                 "proposal_precision": 0.9241},
        "dev": {"top1": 0.7604, "top3": 0.9688, "auto_precision": 0.98, "auto_coverage": 0.3333,
                "proposal_precision": 0.9481},
        "all": {"top1": 0.7087, "top3": 0.9272, "auto_precision": 0.98},
    },
}
_CEIL = {
    "dev2": {"neg_auto": 0, "wrong_auto": 0, "neg_proposed": 1},
    "dev": {"neg_auto": 0, "wrong_auto": 0, "neg_proposed": 1},
    "all": {"neg_auto": 0, "wrong_auto": 0},
}
HOLDOUT_CEILINGS: dict[str, dict[str, dict[str, int]]] = {"pipeline": _CEIL, "scorer": _CEIL}


def load_holdout(path: Path | None = None) -> dict[str, Any]:
    return json.loads((path or HOLDOUT_V1).read_text(encoding="utf-8"))


def tables_for(doc: dict[str, Any], split: str | None) -> list[dict[str, Any]]:
    if split in (None, "", "all"):
        return list(doc["tables"])
    if split not in SPLITS:
        raise ValueError(f"unknown split {split!r}")
    return [t for t in doc["tables"] if t.get("split") == split]


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


def _map(engine: str, sources: list[str], targets: list[str], **kw: Any) -> list[dict]:
    if engine == "scorer":
        from services.semantic_mapper import map_columns

        return map_columns(sources, targets, **kw)
    if engine != "pipeline":
        raise ValueError(f"unknown engine {engine!r}")
    from services.mapping_pipeline import run_mapping_pipeline

    return list(run_mapping_pipeline(sources, targets, use_llm=False, **kw).get("mappings") or [])


def run_table(
    table: dict[str, Any], *, names_only: bool = False, dest: dict | None = None, engine: str = "pipeline"
) -> list[dict]:
    dest = dest or {"destination_db_type": "postgresql", "destination_table_exists": True}
    src = table["source"]
    tgt = table["target"]
    tnames = [t["name"] for t in tgt]
    kw: dict[str, Any] = dict(
        destination_db_type=dest["destination_db_type"],
        destination_table_exists=dest["destination_table_exists"],
        source_db_type=table.get("source_db", ""),
    )
    if not names_only:
        kw["source_schemas"] = [
            {"name": s["name"], "inferred_type": s["type"], "samples": list(s.get("samples") or [])}
            for s in src
        ]
        kw["target_schemas"] = [{"name": t["name"], "inferred_type": t["type"]} for t in tgt]
    out = _map(engine, [s["name"] for s in src], tnames, **kw)
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
                table=table["id"], bucket=table.get("bucket", ""), split=table.get("split", ""), source=n,
                gold=table["gold"].get(n), pred=proposed, raw_target=raw,
                create_new=bool(m.get("create_new")), conf=conf,
                review=bool(m.get("requires_review")), auto=auto,
                ranked=_ranked(m, proposed, tset), strategy=m.get("assignment_strategy"),
                review_kind=m.get("review_kind"),
            )
        )
    return rows


def run_holdout(
    split: str | None = None, *, names_only: bool = False, path: Path | None = None, engine: str = "pipeline"
) -> list[dict]:
    doc = load_holdout(path)
    rows: list[dict] = []
    for table in tables_for(doc, split):
        rows.extend(run_table(table, names_only=names_only, dest=doc.get("destination"), engine=engine))
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


def cluster_bootstrap(rows: Iterable[dict], *, reps: int = 2000, seed: int = BOOTSTRAP_SEED) -> dict[str, Any]:
    """95% percentile intervals, resampling whole tables with replacement."""
    by_table: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_table[r["table"]].append(r)
    tables = sorted(by_table)
    keys = ("top1", "top3", "proposal_precision", "auto_precision", "auto_coverage")
    if not tables:
        return {k: None for k in keys}
    R = random.Random(seed)
    draws: dict[str, list[float]] = {k: [] for k in keys}
    for _ in range(reps):
        sample = [r for _t in range(len(tables)) for r in by_table[R.choice(tables)]]
        m = metrics(sample)
        for k in keys:
            if m[k] is not None:
                draws[k].append(m[k])
    out: dict[str, Any] = {"tables": len(tables), "reps": reps, "seed": seed}
    for k in keys:
        vals = sorted(draws[k])
        out[k] = (
            (round(vals[int(0.025 * (len(vals) - 1))], 4), round(vals[int(0.975 * (len(vals) - 1))], 4))
            if vals else None
        )
    return out


def evaluate(
    split: str | None = None, *, names_only: bool = False, path: Path | None = None,
    engine: str = "pipeline", bootstrap: int = 0,
) -> dict[str, Any]:
    rows = run_holdout(split, names_only=names_only, path=path, engine=engine)
    by_bucket: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_bucket[r["bucket"]].append(r)
    out = {
        "split": split or "all",
        "engine": engine,
        "mode": "names_only" if names_only else "typed_with_samples",
        "overall": metrics(rows),
        "buckets": {b: metrics(v) for b, v in sorted(by_bucket.items())},
        "rows": rows,
    }
    if bootstrap:
        out["intervals"] = cluster_bootstrap(rows, reps=bootstrap)
    return out


def gate_failures(split: str, overall: dict[str, Any], engine: str = "pipeline") -> list[str]:
    """Floors/ceilings this engine+split misses (empty when the gate passes)."""
    out = []
    for key, floor in HOLDOUT_FLOORS.get(engine, {}).get(split, {}).items():
        got = overall.get(key)
        if got is None or got < floor:
            out.append(f"{engine}.{split}.{key}={got} < floor {floor}")
    for key, ceiling in HOLDOUT_CEILINGS.get(engine, {}).get(split, {}).items():
        got = overall.get(key)
        if got is None or got > ceiling:
            out.append(f"{engine}.{split}.{key}={got} > ceiling {ceiling}")
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
