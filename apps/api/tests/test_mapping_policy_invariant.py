"""Hard invariant for the mapping policy (accuracy review findings 1, 9, 10, 11).

No rule this branch added around ``map_columns`` (decisive pinning, rescoring
from inferred abbreviations and value patterns, the suggestion floor) may turn
a row the base mapper sent to review into an auto-approval, or auto-approve a
different column than the base mapper would. Checked base vs new over a
generated cross-table corpus (fixed seed) plus the review's adversarial cases:
every new auto-approved row must be auto-approved by the base code with the
same target.
"""

from __future__ import annotations

import logging
import random

import pytest

import services.semantic_mapper as sm
from services.mapping_holdout_eval import load_holdout

SEED = 20261122
CASES = 400
AUTO = 0.85

ADVERSARIAL = [
    ([("contact", "VARCHAR", ["a@b.io", "c@d.org"])], [("contact_website", "VARCHAR"), ("contact_phone", "VARCHAR")]),
    ([("customer", "VARCHAR", ["cus_9s6", "cus_1Ab"])], [("customer_email", "VARCHAR"), ("customer_name", "VARCHAR")]),
    ([("profile", "VARCHAR", ["usr_81a", "usr_2B"])], [("profile_email", "VARCHAR"), ("profile_name", "VARCHAR")]),
    ([("unsubscribe_url", "VARCHAR", ["https://x.io/u/1"])], [("email_unsubscribe_url", "VARCHAR"), ("email_subject", "VARCHAR")]),
    ([("locale", "VARCHAR", ["en_US", "de_DE"])], [("email_locale", "VARCHAR"), ("email_subject", "VARCHAR")]),
    ([("cat", "VARCHAR", ["Shoes"]), ("price", "DECIMAL", ["9.99"])],
     [("category", "VARCHAR"), ("created_at", "TIMESTAMP"), ("price", "DECIMAL")]),
    ([("manager", "INTEGER", ["1042"]), ("amount", "DECIMAL", ["10.5"]), ("emp_no", "INTEGER", ["7"])],
     [("employee_id", "INTEGER"), ("amount", "DECIMAL"), ("manager_id", "INTEGER")]),
]


def _base(monkeypatch) -> None:
    monkeypatch.setattr(sm, "pin_decisive_pairs", lambda pinned, *a, **k: dict(pinned))
    monkeypatch.setattr(sm, "rescore_pairs", lambda *a, **k: set())
    monkeypatch.setattr(sm, "suggestion_policy", lambda mappings, targets, ctx: mappings)


def _corpus() -> list[tuple[list, list]]:
    R = random.Random(SEED)
    tables = load_holdout()["tables"]
    cases = list(ADVERSARIAL)
    while len(cases) < CASES:
        a, b = R.sample(tables, 2)
        src_pool = a["source"] + R.sample(b["source"], min(3, len(b["source"])))
        tgt_pool = a["target"] + R.sample(b["target"], min(4, len(b["target"])))
        src = R.sample(src_pool, R.randint(2, min(10, len(src_pool))))
        tgt = R.sample(tgt_pool, R.randint(2, min(12, len(tgt_pool))))
        names: set[str] = set()
        src = [s for s in src if not (s["name"] in names or names.add(s["name"]))]
        tnames: set[str] = set()
        tgt = [t for t in tgt if not (t["name"] in tnames or tnames.add(t["name"]))]
        cases.append(([(s["name"], s["type"], s.get("samples") or []) for s in src],
                      [(t["name"], t["type"]) for t in tgt]))
    return cases


def _run(src, tgt) -> dict[str, dict]:
    out = sm.map_columns(
        [s[0] for s in src], [t[0] for t in tgt],
        source_schemas=[{"name": n, "inferred_type": t, "samples": list(s)} for n, t, s in src],
        target_schemas=[{"name": n, "inferred_type": t} for n, t in tgt],
        destination_db_type="postgresql", destination_table_exists=True, source_db_type="postgresql",
    )
    return {m["source"]: m for m in out}


def _auto(row: dict | None, targets: set[str]) -> str | None:
    if not row or row.get("create_new") or row.get("target") not in targets:
        return None
    if row.get("requires_review") or float(row.get("confidence") or 0) < AUTO:
        return None
    return row["target"]


@pytest.fixture(scope="module")
def corpus_results():
    logging.disable(logging.WARNING)
    mp = pytest.MonkeyPatch()
    try:
        cases = _corpus()
        new = [_run(s, t) for s, t in cases]
        _base(mp)
        base = [_run(s, t) for s, t in cases]
    finally:
        mp.undo()
        logging.disable(logging.NOTSET)
    return cases, base, new


def test_no_new_auto_approval_and_no_changed_auto_target(corpus_results) -> None:
    cases, base, new = corpus_results
    bad = []
    for (src, tgt), b, n in zip(cases, base, new, strict=True):
        targets = {t[0] for t in tgt}
        for s in (x[0] for x in src):
            got = _auto(n.get(s), targets)
            if got is not None and got != _auto(b.get(s), targets):
                bad.append((s, got, (b.get(s) or {}).get("target"), [t[0] for t in tgt]))
    assert bad == [], bad[:10]


def test_corpus_is_large_and_exercises_the_rules(corpus_results) -> None:
    cases, base, new = corpus_results
    assert len(cases) >= CASES
    changed = sum(1 for b, n in zip(base, new, strict=True) for s in n
                  if (b.get(s) or {}).get("target") != n[s].get("target"))
    assert changed > 20  # the policy really changes assignments, under review
