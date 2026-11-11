"""Completeness rules of one rehearsal table, the seal and the measured duration.

Deterministic, no I/O: "proven" needs every check measured over the whole
population; a sampled or count-only value check is never proven; Parity's
unexplained differences, a broken seal or a control-total mismatch block it;
a missing clock is "not measured", never estimated.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from services.exit_completeness import (
    Profile,
    judge_table,
    merkle_root,
    profile_rows,
    rehearsal_verdict,
    value_method,
)
from services.exit_rehearsal import duration_block


TYPES = {"id": "BIGINT", "amount": "DECIMAL(12,2)"}


def _profiles(**ext_over):
    rows = [{"id": i, "amount": Decimal(i) / 4} for i in range(1, 11)]
    src = profile_rows(rows, ["id", "amount"], ["id"], None, types=TYPES, engine="postgresql")
    ext = profile_rows([{k: str(v) for k, v in r.items()} for r in rows], ["id", "amount"], ["id"],
                       sorted(src.totals), types=TYPES, engine="postgresql")
    for k, v in ext_over.items():
        setattr(ext, k, v)
    return src, ext


def _judge(src, ext, *, parity=None, seal_ok=True, gaps=()):
    return judge_table(name="t", source=src, extract=ext, key_columns=["id"], seal_ok=seal_ok,
                       parity=parity or {"check": "count_only", "verdict": "unverifiable", "unexplained": 0},
                       not_measured=list(gaps))


def test_native_and_text_extracts_digest_alike_and_totals_are_exact():
    src, ext = _profiles()
    assert src.rows == ext.rows == 10
    assert src.key_set_sha256 == ext.key_set_sha256 and src.row_set_sha256 == ext.row_set_sha256
    assert src.totals["amount"] == {"sum": Decimal("13.75"), "min": Decimal("0.25"), "max": Decimal("2.5")}
    assert src.totals == ext.totals


def test_full_population_match_is_proven_by_the_population_digest():
    src, ext = _profiles()
    out = _judge(src, ext)
    assert out["status"] == "proven" and out["value_method"] == "population_digest"
    assert out["differences"] == []
    assert {c["column"] for c in out["control_totals"]} == {"id", "amount"}


def test_gate8_full_check_is_named_when_it_ran():
    src, ext = _profiles()
    out = _judge(src, ext, parity={"check": "full_checksum", "verdict": "proven", "unexplained": 0})
    assert out["status"] == "proven" and out["value_method"] == "full_checksum"


def test_a_sampled_value_check_alone_is_never_proven():
    out = judge_table(name="t", source=None, extract=None, key_columns=["id"], seal_ok=True,
                      parity={"check": "sampled", "verdict": "no_difference_found", "unexplained": 0},
                      not_measured=["the source could not be re-read for a population digest"])
    assert out["status"] == "not_proven" and out["value_method"] == "sampled"
    assert "not proven" in out["reason"]


def test_missing_extra_and_changed_rows_block_proven():
    src, ext = _profiles(rows=9)
    missing = _judge(src, ext)
    assert missing["status"] == "incomplete"
    assert missing["differences"][0] == {"kind": "missing_rows", "rows": 1, "rows_at_least": True,
                                         "detail": "The extract holds 1 row(s) fewer than the source."}
    src, ext = _profiles(rows=11)
    assert _judge(src, ext)["differences"][0]["kind"] == "extra_rows"
    src, ext = _profiles(row_set_sha256="0" * 64)
    changed = _judge(src, ext)
    assert changed["status"] == "incomplete" and changed["differences"][0]["kind"] == "changed_rows"
    src, ext = _profiles(key_set_sha256="0" * 64)
    assert "key sets differ" in _judge(src, ext)["differences"][0]["detail"]


def test_control_total_mismatch_seal_mismatch_and_parity_unexplained_block_proven():
    src, ext = _profiles()
    ext.totals = {**ext.totals, "amount": {**ext.totals["amount"], "sum": Decimal("13.74")}}
    totals = _judge(src, ext)
    assert totals["status"] == "incomplete"
    assert [d["kind"] for d in totals["differences"]] == ["control_total"]
    src, ext = _profiles()
    seal = _judge(src, ext, seal_ok=False)
    assert seal["status"] == "incomplete" and seal["differences"][0]["kind"] == "seal"
    parity = _judge(src, ext, parity={"check": "sampled", "verdict": "unexplained", "unexplained": 2})
    assert parity["status"] == "incomplete" and "unexplained" in parity["reason"]


def test_a_named_gap_keeps_a_matching_table_not_proven():
    src, ext = _profiles()
    out = _judge(src, ext, gaps=["Parity Studio could not read the extract job's evidence"])
    assert out["status"] == "not_proven"


def test_value_method_prefers_gate8_only_when_it_proved_values():
    src, ext = _profiles()
    assert value_method({"check": "key_diff", "verdict": "unexplained"}, src, ext) == "population_digest"
    assert value_method({"check": "count_only"}, None, None) == "count_only"
    assert value_method({"check": "weird"}, None, None) == "none"


@pytest.mark.parametrize("statuses,verdict", [
    (["proven", "proven"], "proven"),
    (["proven", "not_proven"], "not_proven"),
    (["proven", "incomplete", "not_proven"], "incomplete"),
    (["proven", "failed", "incomplete"], "failed"),
    (["proven", "running"], "running"),
    ([], "not_proven"),
])
def test_rehearsal_verdict(statuses, verdict):
    tables = [{"name": f"t{i}", "status": s} for i, s in enumerate(statuses)]
    assert rehearsal_verdict(tables)[0] == verdict


def test_merkle_root_is_rfc6962_and_order_independent():
    parts = [{"table": t, "size_bytes": i, "sha256": h} for i, (t, h) in
             enumerate([("a", "11"), ("b", "22"), ("c", "33")], start=1)]
    leaf = [hashlib.sha256(b"\x00" + f"{p['table']}:{p['size_bytes']}:{p['sha256']}".encode()).digest()
            for p in parts]
    node = lambda a, b: hashlib.sha256(b"\x01" + a + b).digest()  # noqa: E731
    expected = "sha256:" + node(node(leaf[0], leaf[1]), leaf[2]).hex()
    assert merkle_root(parts) == expected == merkle_root(list(reversed(parts)))
    tampered = [dict(parts[0], sha256="12"), *parts[1:]]
    assert merkle_root(tampered) != expected
    assert merkle_root([]) == "sha256:" + hashlib.sha256(b"").hexdigest()


def test_duration_is_measured_or_says_not_measured():
    measured = duration_block(3600.0, 120.0, 30 * 24 * 3600, 2048)
    assert measured["measured_seconds"] == 3720 and measured["fits_window"] is True
    assert measured["margin_pct"] == pytest.approx(99.86, abs=0.01)
    assert "measured, not projected" in measured["statement"]
    too_slow = duration_block(7200.0, 0.0, 3600, 0)
    assert too_slow["fits_window"] is False and too_slow["margin_pct"] == -100.0
    unknown = duration_block(None, 5.0, 3600, 0)
    assert unknown["measured_seconds"] is None and unknown["fits_window"] is None
    assert "not measured" in unknown["statement"]


def test_profile_counts_duplicates_in_keyless_tables():
    rows = [{"k": "a"}, {"k": "a"}]
    twice = profile_rows(rows, ["k"], [], None)
    once = profile_rows(rows[:1], ["k"], [], None)
    assert twice.key_set_sha256 is None and twice.row_set_sha256 != once.row_set_sha256
    assert isinstance(twice, Profile)
