"""Concurrent source writers vs destination loss on a keyed run into an occupied table.

Review of b76485ce0 (BLOCKING): ``scoped_to_written_keys`` was set whenever
reconcile_step batch-scoped the populations, including on a re-read route
(streaming PG -> PG, ``records == []``) where the source side is a scan taken
AFTER the write. A concurrent ``UPDATE src ... WHERE (eu, 5)`` between the
write and that scan made L2 differ on a key the destination held exactly as
written; the ladder vetoed and the job restored the destination table, undoing
a correct upsert.

Now the write pass keeps a per-key witness of what it wrote
(``services.keyed_write_witness``). A key whose source moved while the
destination equals what was written is concurrent drift: disclosed by name,
never a veto. A destination that differs from what was written still fails,
localized to key and column, on the re-read route and on the buffered route.

SHOULD-FIX: on those scoped populations L3 was still forced green as
``whole_table_not_comparable``. L2 compares count/min/max/sum and a value
multiset, so two written rows that swap a value keep every column aggregate
and passed. L3 now hashes the scoped populations.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

import src.transfer.reconcile_step as rs
import src.transfer.stream as stream_mod
from tests.test_r14_ladder_composite_key_scope_live import PG, _dest, _run, world  # noqa: F401

CONCURRENT = "UPDATE \"{src}\" SET amount = '9.99' WHERE region = 'eu' AND id = 5"


def _ladder(res) -> dict:
    return (res.reconciliation or {}).get("verification_ladder") or {}


def _inject_concurrent_update(monkeypatch, conn, src, *, when: str) -> list[int]:
    """Run CONCURRENT once, after the write pass: before Gate-8's source
    re-read, or after it and before the ladder's own re-read."""
    fired: list[int] = []

    def _fire():
        if not fired:
            conn.cursor().execute(CONCURRENT.format(src=src))
            fired.append(1)

    if when == "before_gate8_reread":
        orig_plan = stream_mod.reread_pagination_plan

        def _plan(*a, **kw):
            _fire()
            return orig_plan(*a, **kw)

        monkeypatch.setattr(stream_mod, "reread_pagination_plan", _plan)
    else:
        orig_ladder = rs._maybe_attach_verification_ladder

        def _ladder_after_update(report, **kw):
            _fire()
            return orig_ladder(report, **kw)

        monkeypatch.setattr(rs, "_maybe_attach_verification_ladder", _ladder_after_update)
    return fired


def _trigger(conn, dst, body: str) -> None:
    cur = conn.cursor()
    cur.execute(f"""
        CREATE FUNCTION r14_tamper_{dst}() RETURNS trigger AS $$
        BEGIN
          {body}
          RETURN NEW;
        END $$ LANGUAGE plpgsql""")
    cur.execute(
        f'CREATE TRIGGER r14_t BEFORE INSERT OR UPDATE ON "{dst}" '
        f"FOR EACH ROW EXECUTE FUNCTION r14_tamper_{dst}()"
    )


@pytest.mark.parametrize("foreign_row", [False, True], ids=["same_keys", "foreign_row"])
@pytest.mark.parametrize("when", ["before_gate8_reread", "before_ladder"])
def test_a_concurrent_source_update_after_the_write_completes_and_is_disclosed(
    world, monkeypatch, when, foreign_row
):
    """``foreign_row``: the destination also holds (ap, 99), a row no run of
    this source wrote, so Gate-8's whole-table digests are not comparable."""
    fake, conn, src, dst = world
    if foreign_row:
        conn.cursor().execute(f"INSERT INTO \"{dst}\" VALUES ('ap', 99, 9.99, 'loaded elsewhere')")
    assert _run(fake, src, dst).success
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    fired = _inject_concurrent_update(monkeypatch, conn, src, when=when)
    res = _run(fake, src, dst)
    assert fired, "the concurrent update never ran"
    assert res.success, res.error
    report = res.reconciliation or {}
    assert "restored" not in str(report.get("message") or "").lower()
    dest = _dest(conn, dst)
    # The upsert landed; (eu, 5) holds what this run wrote, not the later 9.99.
    assert dest[("ap", 2)][0] == Decimal("2.75") and dest[("eu", 5)][0] == Decimal("5.50")
    drift = report.get("concurrent_source_drift") or {}
    assert drift.get("count") == 1, report
    assert [(k["region"], str(k["id"])) for k in drift["sample_keys"]] == [("eu", "5")]
    assert any("concurrent source update" in w and "'eu'" in w for w in report.get("warnings") or [])
    assert _ladder(res).get("passed") is True


def test_a_tampered_destination_still_fails_on_the_reread_route_beside_drift(world, monkeypatch):
    """Loss and drift in one run: (ap, 2) rewritten by the destination fails
    the job naming the cell; (eu, 5) moved at the source is disclosed as drift."""
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    _trigger(conn, dst, "IF NEW.region = 'ap' AND NEW.id = 2 THEN NEW.amount := 0; END IF;")
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    _inject_concurrent_update(monkeypatch, conn, src, when="before_ladder")
    res = _run(fake, src, dst)
    assert not res.success
    ladder = _ladder(res)
    assert ladder.get("passed") is False and ladder.get("assurance_level") == "failed", ladder
    rows = (ladder.get("localization") or {}).get("rows") or []
    # Only the lost cell is localized: the drifted key left the comparison.
    assert rows and all(
        r["column"] == "amount" and "ap" in str(r["pk"]) and "2" in str(r["pk"]) for r in rows
    ), rows
    assert "amount" in (res.error or "") and "ap" in (res.error or ""), res.error
    assert (res.reconciliation or {}).get("concurrent_source_drift", {}).get("count") == 1


def test_two_written_rows_that_swap_a_value_fail_naming_both(world):
    """Column aggregates are identical after a swap; only L3 sees it."""
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    _trigger(
        conn, dst,
        "IF NEW.region = 'ap' AND NEW.id = 3 THEN NEW.amount := 4.50; END IF;\n"
        "          IF NEW.region = 'ap' AND NEW.id = 4 THEN NEW.amount := 3.50; END IF;",
    )
    conn.cursor().execute(f"UPDATE \"{src}\" SET note = 'changed' WHERE region = 'eu' AND id = 1")
    res = _run(fake, src, dst)
    assert not res.success, "a swapped pair of cells must not complete green"
    ladder = _ladder(res)
    assert ladder["layers"]["L2"]["passed"] is True  # the swap keeps every aggregate
    assert ladder["layers"]["L3"]["passed"] is False and ladder["passed"] is False
    rows = (ladder.get("localization") or {}).get("rows") or []
    named = {str(r.get("pk")) for r in rows if r.get("column") == "amount"}
    assert any("3" in k and "ap" in k for k in named) and any("4" in k and "ap" in k for k in named), rows


# --------------------------------------------------------------------------- #
# Buffered route: the ladder is given the written records (no source re-read).
# --------------------------------------------------------------------------- #

def _buffered(conn, dst, records, *, tamper_sql: str = ""):
    from src.transfer.models import EndpointConfig

    cur = conn.cursor()
    cur.executemany(
        f'INSERT INTO "{dst}" (region, id, amount, note) VALUES (%s, %s, %s, %s)',
        [(r["region"], r["id"], r["amount"], r["note"]) for r in records]
        + [("zz", 99, "9.99", "loaded elsewhere")],
    )
    if tamper_sql:
        cur.execute(tamper_sql.format(dst=dst))
    report = {
        "passed": True, "source_rows": len(records), "target_rows": len(records) + 1,
        "rows_written": len(records), "rejected_rows": 0,
        "source_checksum": "engine-digest", "target_checksum": "other-digest",
        "checksum_scope": "whole_table_not_comparable", "message": "Keyed delta verified",
    }
    dest_summary = {
        "sync_mode": "upsert", "primary_key_columns": ["region", "id"],
        "target_rows_before": len(records) + 1,
        "keyed_census": {"unique_batch_keys": len(records), "dest_preexisting": len(records)},
    }
    ep = EndpointConfig(kind="database", format="postgresql", schema="public", table=dst, **PG)
    return rs._maybe_attach_verification_ladder(
        report, endpoint=ep, source_endpoint=ep, records=[dict(r) for r in records],
        columns=["region", "id", "amount", "note"], dest_summary=dest_summary,
        mappings=[
            {"source": c, "target": c, "target_type": t}
            for c, t in (("region", "TEXT"), ("id", "INT4"), ("amount", "DECIMAL(10,2)"), ("note", "TEXT"))
        ],
        validation_mode="strict",
    )


def _records():
    return [
        {"region": r, "id": i, "amount": Decimal(f"{i}.50"), "note": f"{r} {i}"}
        for r in ("eu", "ap") for i in range(1, 6)
    ]


@pytest.mark.parametrize("tamper", [
    "",
    "UPDATE \"{dst}\" SET amount = 0 WHERE region = 'ap' AND id = 2",
    "UPDATE \"{dst}\" SET amount = CASE id WHEN 3 THEN 4.50 ELSE 3.50 END WHERE region = 'ap' AND id IN (3, 4)",
], ids=["correct", "rewritten_cell", "swapped_pair"])
def test_the_buffered_route_grades_the_written_records(world, tamper):
    _fake, conn, _src, dst = world
    out = _buffered(conn, dst, _records(), tamper_sql=tamper)
    ladder = out.get("verification_ladder") or {}
    assert "concurrent_source_drift" not in out and "value_discrepancy_unattributed" not in out
    if not tamper:
        assert ladder.get("passed") is True, ladder
        return
    assert ladder.get("passed") is False and ladder.get("assurance_level") == "failed", ladder
    rows = (ladder.get("localization") or {}).get("rows") or []
    named = {str(r.get("pk")) for r in rows if r.get("column") == "amount"}
    want = ["2"] if "id = 2" in tamper else ["3", "4"]
    for i in want:
        assert any("ap" in k and i in k for k in named), rows
    assert out.get("passed") is False


# --------------------------------------------------------------------------- #
# Attribution rules (no services): what the witness may and may not excuse.
# --------------------------------------------------------------------------- #

def _witnessed(rows, cols):
    from services.keyed_write_witness import Encoder, KeyedWriteWitness

    w = KeyedWriteWitness(["region", "id"], max_rows=100, encoder=Encoder(cols, "", None))
    w.add_rows(rows)
    return w


def _split(w, source, target, *, written=None, gate8=None, full=None):
    """``gate8``: the probe COUNT taken before the read (witness.expected_rows)."""
    from services.keyed_write_witness import split_concurrent_source_drift

    if w is not None and gate8 is not None:
        w.expected_rows = gate8
    return split_concurrent_source_drift(
        witness=w, source_rows=source, target_rows=target, key_columns=["region", "id"],
        rows_written=w.rows_added if written is None and w is not None else written,
        full_target_rows=full,
    )


def test_the_witness_excuses_only_a_moved_source_whose_destination_is_as_written():
    cols = ["region", "id", "amount"]
    written = [{"region": "eu", "id": i, "amount": f"{i}.50"} for i in range(1, 5)]
    w = _witnessed(written, cols)
    source = [dict(r) for r in written]
    source[0]["amount"] = "9.99"                      # moved; dest as written -> drift
    source[1]["amount"] = "8.88"                      # moved; dest ALSO changed -> stays
    source.append({"region": "eu", "id": 9, "amount": "1"})  # inserted after the write
    target = [dict(r) for r in written]
    target[1]["amount"] = "7.77"
    target[2]["amount"] = "0"                         # source as written, dest differs -> stays
    s, t, rec = _split(w, source, target, gate8=4)
    assert rec["attributed"] is True and rec["count"] == 1 and rec["inserted_count"] == 1
    assert [k["id"] for k in rec["sample_keys"]] == [1]
    assert [k["id"] for k in rec["inserted_sample_keys"]] == [9]
    assert [r["id"] for r in s] == [2, 3, 4] and [r["id"] for r in t] == [2, 3, 4]


def test_review_probe_a_key_the_witness_lacks_is_never_drift():
    """Reviewer probe: witness {1, 2}, source {1, 2, 3}, destination {1, 2}
    with row 3 lost. It used to read "concurrent source update", attributed."""
    cols = ["region", "id", "amount"]
    rows = [{"region": "eu", "id": i, "amount": f"{i}.50"} for i in range(1, 4)]
    w = _witnessed(rows[:2], cols)
    # The writer sent three rows; the witness saw two: no attribution at all.
    _s, _t, rec = _split(w, rows, rows[:2], written=3, gate8=3)
    assert rec["attributed"] is False and rec["reason"] == "witnessed_rows_differ_from_rows_written"
    # Even a witness that matches the writer's count never excuses key 3 as
    # drift; without proof that 3 was absent at write time (the probe COUNT
    # before the read saw 3 rows) it is not excused as an insert either.
    s, t, rec = _split(w, rows, rows[:2], written=2, gate8=3)
    assert rec["attributed"] is True and rec["count"] == 0 and rec["inserted_count"] == 0
    assert [r["id"] for r in s] == [1, 2, 3] and [r["id"] for r in t] == [1, 2]


def test_a_written_key_gone_from_both_populations_is_a_loss_or_a_disclosed_delete():
    cols = ["region", "id", "amount"]
    written = [{"region": "eu", "id": i, "amount": f"{i}.50"} for i in range(1, 4)]
    w = _witnessed(written, cols)
    source = written[:1]                                # 2 and 3 deleted at the source
    full = [written[0], written[1], {"region": "eu", "id": 3, "amount": "0"}]
    s, t, rec = _split(w, source, [written[0]], gate8=3, full=full)
    assert rec["deleted_count"] == 1 and [k["id"] for k in rec["deleted_sample_keys"]] == [2]
    assert rec["loss_count"] == 1


def test_review_m3_a_key_moved_out_of_the_window_is_not_called_deleted():
    """A windowed re-read: a concurrent update moved (eu, 2)'s cursor past the
    window after the write. It is outside the compared population but still
    in the source; the old record named it a delete."""
    from services.keyed_write_witness import split_concurrent_source_drift

    cols = ["region", "id", "amount"]
    written = [{"region": "eu", "id": i, "amount": f"{i}.50"} for i in range(1, 4)]
    w = _witnessed(written, cols)
    in_window = [written[0], written[2]]
    after_window = [{"region": "eu", "id": 2, "amount": "2.99"}]
    _s, _t, rec = split_concurrent_source_drift(
        witness=w, source_rows=in_window, target_rows=in_window, key_columns=["region", "id"],
        rows_written=3, full_target_rows=written, out_of_window_rows=after_window,
    )
    assert rec["deleted_count"] == 0 and rec["loss_count"] == 0
    assert rec["moved_out_of_window_count"] == 1
    assert [k["id"] for k in rec["moved_out_of_window_sample_keys"]] == [2]


@pytest.mark.parametrize("case", ["none", "overflow", "scheme", "chunk_failed"])
def test_without_a_usable_witness_nothing_is_excused(case):
    from services.keyed_write_witness import Encoder, KeyedWriteWitness

    cols = ["region", "id", "amount"]
    rows = [{"region": "eu", "id": i, "amount": f"{i}.50"} for i in range(1, 4)]
    w = None
    if case == "overflow":
        w = KeyedWriteWitness(["region", "id"], max_rows=1, encoder=Encoder(cols, "", None))
        w.add_rows(rows)
    elif case == "scheme":
        w = _witnessed([{**r, "amount": r["amount"] + "0"} for r in rows], cols)
    elif case == "chunk_failed":
        w = _witnessed(rows, cols)
        w._give_up("chunk_not_witnessed")
    src = [{**r, "amount": "1"} for r in rows]
    s, t, rec = _split(w, src, rows, written=3, gate8=3)
    assert rec["attributed"] is False and s == src and t == rows


def test_a_reread_without_a_witness_stays_advisory_but_never_silent(world, monkeypatch):
    """A run resumed at reconcile has no witness: the ladder cannot tell drift
    from loss. Where Gate-8's whole-table digests are not comparable (the
    destination holds (ap, 99), which no run of this source wrote) the L2 miss
    stays advisory as before (ZERO_LOSS P5), and the report says so."""
    import services.keyed_write_witness as kww

    fake, conn, src, dst = world
    conn.cursor().execute(f"INSERT INTO \"{dst}\" VALUES ('ap', 99, 9.99, 'loaded elsewhere')")
    assert _run(fake, src, dst).success
    monkeypatch.setattr(kww, "take_keyed_write_witness", lambda _job_id: None)
    _inject_concurrent_update(monkeypatch, conn, src, when="before_gate8_reread")
    res = _run(fake, src, dst)
    assert res.success, res.error
    report = res.reconciliation or {}
    assert "concurrent_source_drift" not in report
    unattributed = report.get("value_discrepancy_unattributed") or {}
    assert unattributed.get("columns") == ["amount"] and unattributed.get("reason") == "no_write_witness"
    assert any("cannot be told from a destination loss" in w for w in report.get("warnings") or [])


@pytest.mark.parametrize("outcome", ["completed", "failed_before_ladder", "raised_before_ladder"])
def test_no_witness_outlives_its_job(world, monkeypatch, outcome):
    """The witness leaves memory when the job ends, whatever the outcome —
    including a run that fails before the ladder ever takes it."""
    import services.keyed_write_witness as kww

    fake, conn, src, dst = world
    assert _run(fake, src, dst).success  # occupy the destination
    begun: list[tuple[str, object]] = []
    orig_begin = stream_mod.begin_keyed_write_witness

    def _begin(**kw):
        witness = orig_begin(**kw)
        begun.append((str(kw["job_id"]), witness))
        return witness

    monkeypatch.setattr(stream_mod, "begin_keyed_write_witness", _begin)
    ladder_calls: list[int] = []
    orig_ladder = rs._maybe_attach_verification_ladder
    monkeypatch.setattr(
        rs, "_maybe_attach_verification_ladder",
        lambda report, **kw: ladder_calls.append(1) or orig_ladder(report, **kw),
    )
    if outcome != "completed":
        exc = RuntimeError if outcome == "raised_before_ladder" else ConnectionError

        def _reread_dies(*_a, **_kw):
            raise exc("the source re-read lost its connection")

        monkeypatch.setattr(stream_mod, "reread_pagination_plan", _reread_dies)
    try:
        res = _run(fake, src, dst)
    except Exception:  # noqa: BLE001 — the raising outcome may propagate
        res = None
    assert begun and begun[0][1] is not None, "the route did not start a witness"
    job_id = begun[0][0]
    if outcome == "completed":
        assert res is not None and res.success and ladder_calls
    else:
        assert not ladder_calls and (res is None or not res.success)
    assert job_id not in kww.held_witness_job_ids()


def test_the_job_scope_releases_the_witness_when_the_body_raises():
    import services.keyed_write_witness as kww

    import services.destination_write_lock as dwl

    dwl._HELD["scope-raise"] = (_NoSession(), dwl.EXCLUSIVE)  # as if held on the table
    with pytest.raises(KeyboardInterrupt):
        with kww.keyed_write_witness_scope("scope-raise"):
            witness = kww.begin_keyed_write_witness(
                job_id="scope-raise", key_columns=["id"], sync_mode="upsert",
                inline_fingerprints_used=False, resumed=False,
            )
            assert witness is not None and "scope-raise" in kww.held_witness_job_ids()
            raise KeyboardInterrupt
    assert "scope-raise" not in kww.held_witness_job_ids()
    dwl.release_destination_write_lock("scope-raise")


class _NoSession:
    def close(self) -> None:
        pass


def test_no_witness_is_registered_outside_a_tracked_job():
    """Nothing would release it: a direct stream call must not fill the registry."""
    import services.keyed_write_witness as kww

    summary: dict = {}
    assert kww.begin_keyed_write_witness(
        job_id="no-scope", key_columns=["id"], sync_mode="upsert",
        inline_fingerprints_used=False, resumed=False, summary=summary,
    ) is None
    assert summary["keyed_write_witness"]["reason"] == "outside_a_tracked_job"
    assert "no-scope" not in kww.held_witness_job_ids()
