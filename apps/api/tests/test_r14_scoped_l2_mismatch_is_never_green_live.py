"""A ladder layer that ran and found a concrete value discrepancy is never green.

Finding (raised in 6cf50ce61): an upsert into a populated PostgreSQL table
where the destination silently rewrote one cell — (ap, 2) amount forced to 0
by a trigger — completed green. The ladder's L2 found the amount mismatch,
but on a ``whole_table_not_comparable`` route its verdict was ``L1 and L3``
(L3 skipped), so the ladder read "passed", ``fidelity_veto`` had nothing to
veto, and the report said "Keyed delta verified … per-cell fidelity is NOT
proven" on a job reported as completed.

Policy (code + docs): the exemption exists because whole-table aggregates
"cover rows this job did not write" (verification_ladder; ZERO_LOSS P5 —
an unscoped dest-before append does not veto). The X-22 contract in
``signed_proof_pack.fidelity_veto`` says a keyed cell mismatch vetoes even on
a row_count-assurance route; the post-write contract says a checksum mismatch
never greens Gate-8. Once reconcile_step has scoped BOTH populations to this
run's keys (X-OW1), the exemption's premise is false, so a scoped L2 miss
fails the ladder and L4/L5 name the key and column.
"""

from __future__ import annotations

import pytest

from tests.test_r14_ladder_composite_key_scope_live import _l2, _run, world  # noqa: F401 — fixture


@pytest.fixture(autouse=True)
def _destination_write_lock_on(monkeypatch):
    """Re-read attribution needs DATAFLOW_DEST_WRITE_LOCK=1 (off by default:
    then this route is disclosed-unattributed, see ZERO_LOSS P5)."""
    monkeypatch.setenv("DATAFLOW_DEST_WRITE_LOCK", "1")


def test_a_silently_rewritten_cell_fails_the_job_naming_key_and_column(world):
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    cur = conn.cursor()
    cur.execute(f"""
        CREATE FUNCTION r14_tamper_{dst}() RETURNS trigger AS $$
        BEGIN
          IF NEW.region = 'ap' AND NEW.id = 2 THEN NEW.amount := 0; END IF;
          RETURN NEW;
        END $$ LANGUAGE plpgsql""")
    cur.execute(f'CREATE TRIGGER r14_t BEFORE INSERT OR UPDATE ON "{dst}" FOR EACH ROW EXECUTE FUNCTION r14_tamper_{dst}()')
    cur.execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    cur.execute(f"UPDATE \"{src}\" SET note = 'changed 東京' WHERE region = 'eu' AND id = 4")
    res = _run(fake, src, dst)
    assert not res.success, "a destination cell that differs from the source must not complete green"
    ladder = (res.reconciliation or {}).get("verification_ladder") or {}
    assert ladder.get("passed") is False and ladder.get("assurance_level") == "failed", ladder
    assert _l2(res).get("passed") is False
    rows = (ladder.get("localization") or {}).get("rows") or []
    assert any(r.get("column") == "amount" and "ap" in str(r.get("pk")) and "2" in str(r.get("pk")) for r in rows), rows
    assert "amount" in (res.error or "") and "ap" in (res.error or ""), res.error


def test_a_correct_run_into_the_same_table_stays_green(world):
    """Control: same route, same scope, no tamper — still completed."""
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    res = _run(fake, src, dst)
    assert res.success, res.error
    assert _l2(res).get("passed") is not False


def _rows(n, *, tamper=None):
    out = [{"region": r, "id": str(i), "amount": f"{i}.50"} for r in ("eu", "ap") for i in range(1, n + 1)]
    if tamper:
        for row in out:
            if (row["region"], row["id"]) == tamper:
                row["amount"] = "0.00"
    return out


@pytest.mark.parametrize("scoped", [True, False])
def test_the_verdict_policy_for_an_l2_miss_on_an_incomparable_route(scoped):
    from services.reconcile_coverage import WHOLE_TABLE_NOT_COMPARABLE
    from services.verification_ladder import run_five_layer_verification

    src, tgt = _rows(5), _rows(5, tamper=("ap", "2"))
    out = run_five_layer_verification(
        source_rows=src, target_rows=tgt, columns=["region", "id", "amount"],
        pk_column="region", pk_columns=["region", "id"],
        source_row_count=len(src), target_row_count=len(tgt),
        checksum_scope=WHOLE_TABLE_NOT_COMPARABLE, allow_extra_rows=True,
        target_rows_before=len(tgt), keyed_cardinality=True, keyed_expected_delta=0,
        scoped_to_written_keys=scoped,
    )
    assert out["layers"]["L2"]["passed"] is False
    if scoped:
        # This run's own rows differ: failed, localized to (ap, 2) amount.
        assert out["passed"] is False and out["assurance_level"] == "failed"
        assert any(r["column"] == "amount" for r in out["localization"]["rows"])
        assert "ap" in out["localization_summary"] and "amount" in out["localization_summary"]
    else:
        # Unscoped dest-before: aggregates cover rows this job did not write —
        # documented as advisory (ZERO_LOSS P5); unchanged.
        assert out["passed"] is True and out["assurance_level"] == "row_count"
