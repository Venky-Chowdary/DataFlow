"""Review of 3f574e314 / d9e8202f3: the witness must never excuse a real loss.

Each test is one reviewer finding, live on PostgreSQL -> PostgreSQL (the
streaming re-read route) unless it says otherwise:

* BLOCKING 1 — a chunk the witness did not fingerprint left it "complete" and
  a key it lacked could be excused; a key missing from both the witness and
  the destination was drift (``None == None``).
* BLOCKING 2 — a resumed attempt declined to start a witness but left the
  failed attempt's in the registry, and the ladder took it.
* SHOULD-FIX 3 — the witness and the ladder rendered with different encoders.
* SHOULD-FIX 4 — the registry evicted an in-flight witness.
* NIT 6 — a written key gone from both populations was never checked.
* NIT 7 — key labels were parsed out of the joined fingerprint.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

import services.keyed_write_witness as kww
import src.transfer.reconcile_step as rs
import src.transfer.stream as stream_mod
import src.transfer.stream_db_chunk as chunk_mod
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.test_r14_concurrent_source_drift_live import _inject_concurrent_update, _trigger
from tests.test_r14_ladder_composite_key_scope_live import PG, _dest, _run, world  # noqa: F401


def _run_as(fake, src, dst, job_id, *, pk=("region", "id")):
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public", table=src, **PG),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public", table=dst, **PG),
        sync_mode="upsert",
        stream_contracts=[{"name": src, "primary_key": list(pk), "sync_mode": "upsert", "selected": True}],
        skip_preflight=True, validation_mode="strict",
    )
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, job_id)


def _foreign_row(conn, dst):
    conn.cursor().execute(f"INSERT INTO \"{dst}\" VALUES ('ap', 99, 9.99, 'loaded elsewhere')")


def _at_ladder(monkeypatch, conn, sql: str) -> None:
    orig = rs._maybe_attach_verification_ladder

    def _then_ladder(report, **kw):
        conn.cursor().execute(sql)
        return orig(report, **kw)

    monkeypatch.setattr(rs, "_maybe_attach_verification_ladder", _then_ladder)


def test_blocking1_a_chunk_the_witness_did_not_see_refuses_all_attribution(world, monkeypatch):
    fake, conn, src, dst = world
    _foreign_row(conn, dst)
    assert _run(fake, src, dst).success
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 10)
    calls: list[int] = []
    orig_map = chunk_mod.map_rows_for_fingerprint

    def _second_chunk_fails(**kw):
        calls.append(1)
        if len(calls) == 2:
            raise ValueError("fingerprint mapper failed on this chunk")
        return orig_map(**kw)

    monkeypatch.setattr(chunk_mod, "map_rows_for_fingerprint", _second_chunk_fails)
    _at_ladder(monkeypatch, conn, f"UPDATE \"{src}\" SET amount = '9.99' WHERE region = 'ap' AND id = 5")
    res = _run(fake, src, dst)
    assert len(calls) >= 3, "the run was not chunked"
    report = res.reconciliation or {}
    # The witness saw two chunks of three: it attributes nothing, so the
    # (ap, 5) difference is not called a concurrent update.
    assert "concurrent_source_drift" not in report, report.get("concurrent_source_drift")
    assert (report.get("value_discrepancy_unattributed") or {}).get("reason") == "chunk_not_witnessed"


def test_blocking2_a_resumed_attempt_never_takes_the_failed_attempts_witness(world, monkeypatch):
    """A stale witness says (ap, 2) was written as 2.50. The resumed attempt
    writes 2.75, the destination silently keeps 2.50: a lost update the stale
    witness would excuse as "the source moved"."""
    fake, conn, src, dst = world
    kept: list = []
    orig_begin = stream_mod.begin_keyed_write_witness

    def _keep(**kw):
        witness = orig_begin(**kw)
        kept.append(witness)
        return witness

    monkeypatch.setattr(stream_mod, "begin_keyed_write_witness", _keep)
    assert _run(fake, src, dst).success and kept and kept[0] is not None
    stale = kept[0]
    _trigger(conn, dst, "IF NEW.region = 'ap' AND NEW.id = 2 THEN NEW.amount := OLD.amount; END IF;")
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    job = "r14resume" + uuid.uuid4().hex[:8]
    kww._REGISTRY[job] = stale  # what a failed first attempt of this job left behind
    monkeypatch.setattr(
        stream_mod, "begin_keyed_write_witness", lambda **kw: orig_begin(**{**kw, "resumed": True})
    )
    res = _run_as(fake, src, dst, job)
    report = res.reconciliation or {}
    # Never excused as "the source moved" on another attempt's evidence. With
    # no witness the run cannot attribute: it says so, naming the column.
    assert "concurrent_source_drift" not in report, report.get("concurrent_source_drift")
    unproven = report.get("value_discrepancy_unattributed") or {}
    assert unproven.get("reason") == "resumed_attempt_sees_part_of_the_population", report
    assert unproven.get("columns") == ["amount"]
    assert job not in kww.held_witness_job_ids()


@pytest.fixture()
def typed_world(monkeypatch):
    """timestamp(3) cells and a decimal landing in a text carrier."""
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    from tests.test_r14_ladder_composite_key_scope_live import _pg

    fake = _FakeMongo()
    import src.transfer.engine as engine_mod

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    conn = _pg()
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"r14t_src_{sfx}", f"r14t_dst_{sfx}"
    cur = conn.cursor()
    cur.execute(
        f'CREATE TABLE "{src}" (grp TEXT, id INT, ts TIMESTAMP(3), amount NUMERIC(12,4), code TEXT, '
        "PRIMARY KEY (grp, id))"
    )
    cur.executemany(
        f'INSERT INTO "{src}" VALUES (%s, %s, %s, %s, %s)',
        [("g", i, f"2026-11-0{1 + i % 5} 10:{i:02d}:07.123", f"{i}.1250", f"c{i}") for i in range(1, 21)],
    )
    cur.execute(
        f'CREATE TABLE "{dst}" (grp TEXT, id INT, ts TIMESTAMP(3), amount TEXT, code TEXT, '
        "PRIMARY KEY (grp, id))"
    )
    cur.execute(f"INSERT INTO \"{dst}\" VALUES ('z', 999, '2026-01-01 00:00:00.000', '1', 'loaded elsewhere')")
    yield fake, conn, src, dst
    cur.execute(f'DROP TABLE IF EXISTS "{src}"')
    cur.execute(f'DROP TABLE IF EXISTS "{dst}" CASCADE')
    conn.close()


def test_should_fix3_attribution_renders_both_sides_with_the_witness_encoder():
    """The write pass rendered ``amount`` with its physical carrier (TEXT:
    '5.5000'); the ladder's logical type (DECIMAL) renders the same cell as
    5.5. Re-hashing the ladder's populations with the ladder's own encoder
    made no key agree with the witness ("witness_scheme_mismatch", advisory)
    or every key "moved". The witness's encoder renders both sides."""
    from services.keyed_write_witness import Encoder, KeyedWriteWitness, split_concurrent_source_drift

    cols = ["grp", "id", "amount"]
    witness_enc = Encoder(cols, "postgresql", {"grp": "TEXT", "id": "INT4", "amount": "TEXT"})
    ladder_enc = Encoder(cols, "postgresql", {"grp": "TEXT", "id": "INT4", "amount": "DECIMAL(12,4)"})
    written = [{"grp": "g", "id": i, "amount": f"{i}.5000"} for i in range(1, 6)]
    w = KeyedWriteWitness(["grp", "id"], max_rows=100, encoder=witness_enc)
    w.add_rows(written)
    source = [{**r, "amount": Decimal(r["amount"])} for r in written]  # the PG re-read
    source[1]["amount"] = Decimal("9.9900")                            # moved after the write
    assert ladder_enc.row_digests(written)[0] != witness_enc.row_digests(written)[0]
    _s, _t, rec = split_concurrent_source_drift(
        witness=w, source_rows=source, target_rows=[dict(r) for r in written],
        key_columns=["grp", "id"], rows_written=5, gate8_source_rows=5,
    )
    assert rec["attributed"] is True and rec["count"] == 1 and rec["sample_keys"][0]["id"] == 2


def test_should_fix3_typed_columns_attribute_exactly_the_moved_key(typed_world, monkeypatch):
    fake, conn, src, dst = typed_world
    first = _run_as(fake, src, dst, "r14t" + uuid.uuid4().hex[:8], pk=("grp", "id"))
    assert first.success, first.error
    conn.cursor().execute(f"UPDATE \"{src}\" SET code = 'c3-new' WHERE id = 3")
    _at_ladder(monkeypatch, conn, f"UPDATE \"{src}\" SET code = 'changed later' WHERE id = 5")
    res = _run_as(fake, src, dst, "r14t" + uuid.uuid4().hex[:8], pk=("grp", "id"))
    assert res.success, res.error
    drift = (res.reconciliation or {}).get("concurrent_source_drift") or {}
    # Exactly the one key that moved; a different encoder made every key
    # "moved" or refused attribution as a scheme mismatch.
    assert drift.get("count") == 1 and [str(k["id"]) for k in drift["sample_keys"]] == ["5"], drift
    assert "value_discrepancy_unattributed" not in (res.reconciliation or {})


def test_should_fix4_an_in_flight_witness_is_never_evicted(world, monkeypatch):
    fake, conn, src, dst = world
    _foreign_row(conn, dst)
    assert _run(fake, src, dst).success
    planted = [f"r14inflight{i}" for i in range(kww._MAX_JOBS)]
    try:
        for jid in planted:
            assert kww.begin_keyed_write_witness(
                job_id=jid, key_columns=["id"], sync_mode="upsert",
                inline_fingerprints_used=False, resumed=False,
            ) is not None
        _inject_concurrent_update(monkeypatch, conn, src, when="before_ladder")
        res = _run(fake, src, dst)
        assert set(planted) <= set(kww.held_witness_job_ids()), "an in-flight witness was evicted"
        report = res.reconciliation or {}
        assert (report.get("value_discrepancy_unattributed") or {}).get("reason") == (
            "witness_capacity_in_use_by_other_jobs"
        )
    finally:
        for jid in planted:
            kww.take_keyed_write_witness(jid)


@pytest.mark.parametrize("dest_kept_old", [True, False], ids=["lost_update", "as_written"])
def test_nit6_a_written_key_the_source_deleted_is_still_checked(world, monkeypatch, dest_kept_old):
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    if dest_kept_old:
        _trigger(conn, dst, "IF NEW.region = 'ap' AND NEW.id = 7 THEN NEW.amount := OLD.amount; END IF;")
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '7.77' WHERE region = 'ap' AND id = 7")
    _at_ladder(monkeypatch, conn, f"DELETE FROM \"{src}\" WHERE region = 'ap' AND id = 7")
    res = _run(fake, src, dst)
    drift = (res.reconciliation or {}).get("concurrent_source_drift") or {}
    if dest_kept_old:
        assert not res.success, "a lost update hidden behind a concurrent delete passed"
        assert drift.get("loss_count") == 1 and "7" in str(drift.get("losses")), drift
    else:
        assert res.success, res.error
        assert drift.get("deleted_count") == 1 and _dest(conn, dst)[("ap", 7)][0] == Decimal("7.77")


def test_nit7_a_cell_cannot_pose_as_a_key(world, monkeypatch):
    """(eu, 5)'s note carries "\\x1fid=4". Parsed from the joined fingerprint it
    keyed as (eu, 4), so its concurrent update excused the destination
    rewriting the real (eu, 4)."""
    fake, conn, src, dst = world
    conn.cursor().execute(
        f"UPDATE \"{src}\" SET note = 'x' || chr(31) || 'id=4' WHERE region = 'eu' AND id = 5"
    )
    assert _run(fake, src, dst).success
    _trigger(conn, dst, "IF NEW.region = 'eu' AND NEW.id = 4 THEN NEW.amount := 0; END IF;")
    _at_ladder(monkeypatch, conn, f"UPDATE \"{src}\" SET amount = '9.99' WHERE region = 'eu' AND id = 5")
    res = _run(fake, src, dst)
    assert not res.success, "the rewritten (eu, 4) was excused"
    rows = ((res.reconciliation or {}).get("verification_ladder") or {}).get("localization", {}).get("rows") or []
    assert any("eu" in str(r.get("pk")) and "4" in str(r.get("pk")) for r in rows), rows
