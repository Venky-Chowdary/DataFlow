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
], ids=["correct", "rewritten_cell"])
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
    from services.keyed_write_witness import KeyedWriteWitness
    from services.reconciliation import _iter_fingerprints

    w = KeyedWriteWitness(["region", "id"], max_rows=100)
    w.add_many(_iter_fingerprints(rows, cols))
    return w


def test_the_witness_excuses_only_a_moved_source_whose_destination_is_as_written():
    from services.keyed_write_witness import split_concurrent_source_drift

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
    s, t, rec = split_concurrent_source_drift(
        witness=w, source_rows=source, target_rows=target, columns=cols,
        key_columns=["region", "id"], dest_db_type="", dest_types=None,
    )
    assert rec["attributed"] is True and rec["count"] == 2
    assert sorted(k["id"] for k in rec["sample_keys"]) == [1, 9]
    assert [r["id"] for r in s] == [2, 3, 4] and [r["id"] for r in t] == [2, 3, 4]


@pytest.mark.parametrize("case", ["none", "overflow", "scheme"])
def test_without_a_usable_witness_nothing_is_excused(case):
    from services.keyed_write_witness import KeyedWriteWitness, split_concurrent_source_drift

    cols = ["region", "id", "amount"]
    rows = [{"region": "eu", "id": i, "amount": f"{i}.50"} for i in range(1, 4)]
    w = None
    if case == "overflow":
        w = KeyedWriteWitness(["region", "id"], max_rows=1)
        from services.reconciliation import _iter_fingerprints

        w.add_many(_iter_fingerprints(rows, cols))
    elif case == "scheme":
        w = _witnessed([{**r, "amount": r["amount"] + "0"} for r in rows], cols)
    src = [{**r, "amount": "1"} for r in rows]
    s, t, rec = split_concurrent_source_drift(
        witness=w, source_rows=src, target_rows=rows, columns=cols,
        key_columns=["region", "id"], dest_db_type="", dest_types=None,
    )
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
