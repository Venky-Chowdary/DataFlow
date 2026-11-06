"""Second-pass review #2/#3: G3, G5 and G8 read the probe's one per-column verdict.

``validation_mode_contract.mode_holdout_kind`` decides per column whether a
mode holds the refused cells out (lenient: every refusal, while at least half
the sampled cells cast; balanced: invalid-calendar refusals on a file, below one
in five). G3/G5/G8 still passed lenient on the mode alone ("will quarantine")
for a column the probe called a wrong mapping, and G8 asked the policy with
``record_batch`` defaulting to True, so balanced would hold calendar refusals
out on a database source. SFTP CSV -> live PostgreSQL through start_transfer.
"""

from __future__ import annotations

import pytest

from tests.test_r5_bad_date_in_file_e2e import _start, dest_table, sftp  # noqa: F401


def _gates(staged) -> dict[str, dict]:
    pf = (staged.output or {}).get("preflight") or {}
    return {str(g.get("id")): g for g in pf.get("gates") or []}


def test_lenient_does_not_say_will_quarantine_for_a_mostly_unparseable_column(sftp, dest_table):  # noqa: F811
    lines = ["id,name,d"] + [
        f"{i},n{i},{'2024-01-05' if i == 1 else f'word{i}'}" for i in range(1, 41)
    ]
    table = dest_table("postgresql", "date")
    staged = _start(sftp("\n".join(lines) + "\n"), "postgresql", table, mode="lenient")
    assert not staged.success, "1 castable cell of 40 is a wrong mapping, not 39 holdouts"
    gates = _gates(staged)
    for gid in ("g3_schema_contract", "g5_dry_run", "g8_reconciliation"):
        g = gates.get(gid) or {}
        assert not (g.get("status") == "pass" and "will quarantine" in str(g.get("message"))), (gid, g)


def test_the_g8_policy_needs_record_batch_and_reads_the_probe_verdict():
    from preflight import gates

    reason = "Invalid date: '2024-02-30' is not a real calendar date (2024-02 has 29 days)"
    with pytest.raises(TypeError):
        gates._holds_out("balanced", reason)  # record_batch is required
    assert gates._holds_out("balanced", reason, record_batch=False) is False
    assert gates._holds_out("balanced", reason, record_batch=True) is True
    by_source = {"d": {"failed": 3, "severity": "block", "holdout_kind": ""}}
    assert gates._column_holdout(by_source, "d", "lenient", record_batch=True) == ""
    assert gates._probe_refused_columns(by_source) == {"d"}
    held = {"d": {"failed": 1, "severity": "block", "holdout_kind": "cast_failure"}}
    assert gates._column_holdout(held, "d", "lenient", record_batch=True) == "cast_failure"
    assert gates._probe_refused_columns(held) == set()
    # A column the probe did not grade falls back to the mode alone.
    assert gates._column_holdout({}, "x", "lenient", record_batch=False) == "cast_failure"
    assert gates._column_holdout({}, "x", "balanced", record_batch=True) == ""


def _ctx(errors, by_source, mode="lenient"):
    from types import SimpleNamespace

    plan = SimpleNamespace(
        mappings=[SimpleNamespace(source="d", target="d", transform="date_iso", user_override=False)],
        validation_mode=mode,
        source=SimpleNamespace(kind="file", db_type="csv"),
    )
    return SimpleNamespace(
        plan=plan,
        run_dry_run=lambda: (False, list(errors)),
        coercion_report=lambda: {"by_source": by_source},
        _last_dry_run_meta={"sample_rows_scanned": 40},
    )


def test_g5_never_says_will_quarantine_for_an_engine_holdout_line_the_probe_refused():
    """The engine routes "holdout src→tgt" lines to ``contracted`` (risk_contract);
    G5 must not pass them for a column the probe's one verdict refused."""
    from preflight import gates

    line = "holdout d→d: 39 sample row(s) will quarantine and the rest will load (Invalid date: 'word2')"
    refused = {"d": {"failed": 39, "severity": "block", "holdout_kind": ""}}
    result = gates.gate_g5_dry_run(_ctx([line], refused))
    status = getattr(result, "status", None)
    status = getattr(status, "value", status)
    assert status == "block", (status, getattr(result, "message", ""))
    assert "will quarantine" not in str(getattr(result, "message", "")).split("Dry-run failed")[0]

    held = {"d": {"failed": 1, "severity": "block", "holdout_kind": "cast_failure"}}
    ok = gates.gate_g5_dry_run(_ctx([line], held))
    ok_status = getattr(ok.status, "value", ok.status)
    assert ok_status == "pass", (ok_status, ok.message)


def test_the_column_match_is_exact_not_a_suffix():
    from preflight import gates

    assert gates._lines_name_columns(["holdout order_id→order_id: bad"], {"id"}) is False
    assert gates._lines_name_columns(["row 3 id→id: bad"], {"id"}) is True
