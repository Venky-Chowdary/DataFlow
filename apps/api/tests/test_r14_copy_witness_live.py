"""COPY upsert routes carry no keyed write witness (review N1 of 9650f97d5).

9650f97d5 filled the witness from the staging table a COPY upsert
(``copy_binary_server_to_server_upsert``: PostgreSQL -> PostgreSQL, one mapped
key) merges from. Staging records what LANDED, not what the run read: a
corruption in the COPY encoding or transport is merged into the destination
too, so the destination equalled the witness while the source differed, and
the corruption was excused as "a concurrent source update". The staging read
is gone; these routes stay unattributed and disclosed.
"""

from __future__ import annotations

import uuid

import pytest

import services.copy_upsert as copy_upsert
from tests.test_r14_concurrent_source_drift_live import _trigger
from tests.test_r14_witness_review_live import _at_ladder, _run_as


@pytest.fixture()
def copy_world(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    from tests.test_r14_ladder_composite_key_scope_live import _pg
    import src.transfer.engine as engine_mod

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    conn = _pg()
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"r14k_src_{sfx}", f"r14k_dst_{sfx}"
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE "{src}" (id INT PRIMARY KEY, region TEXT, amount NUMERIC(10,2))')
    cur.executemany(
        f'INSERT INTO "{src}" VALUES (%s, %s, %s)', [(i, "eu" if i <= 10 else "ap", f"{i}.50") for i in range(1, 21)]
    )
    yield fake, conn, src, dst
    cur.execute(f'DROP TABLE IF EXISTS "{src}"')
    cur.execute(f'DROP TABLE IF EXISTS "{dst}" CASCADE')
    cur.execute(f"DROP FUNCTION IF EXISTS r14_tamper_{dst}() CASCADE")
    conn.close()


def _copy_run(fake, src, dst):
    res = _run_as(fake, src, dst, "r14k" + uuid.uuid4().hex[:8], pk=("id",))
    assert (res.destination_summary or {}).get("load_method") == "copy_binary_server_to_server_upsert", (
        res.destination_summary or {}
    ).get("copy_decline_reason")
    return res


def _rows(res):
    return ((res.reconciliation or {}).get("verification_ladder") or {}).get("localization", {}).get("rows") or []


def test_review_n1_a_corruption_in_staging_fails_the_copy_upsert(copy_world, monkeypatch):
    """Reviewer repro: staging carries id=12 amount=0 (an encoding fault); the
    MERGE lands it. It must fail, never read as a concurrent source update."""
    fake, conn, src, dst = copy_world
    assert _copy_run(fake, src, dst).success
    orig_merge = copy_upsert.merge_staging_into_dest

    def _corrupt_staging(dst_cur, **kw):
        dst_cur.execute(f"UPDATE {kw['staging_q']} SET amount = 0 WHERE id = 12")  # nosec B608
        return orig_merge(dst_cur, **kw)

    monkeypatch.setattr(copy_upsert, "merge_staging_into_dest", _corrupt_staging)
    res = _copy_run(fake, src, dst)
    assert not res.success, "a COPY transport corruption completed green"
    report = res.reconciliation or {}
    assert "concurrent_source_drift" not in report, report.get("concurrent_source_drift")
    assert any(str(r.get("pk")).strip("'") == "12" and r.get("column") == "amount" for r in _rows(res)), _rows(res)


def test_a_concurrent_source_update_on_the_copy_route_is_never_excused(copy_world, monkeypatch):
    fake, conn, src, dst = copy_world
    assert _copy_run(fake, src, dst).success
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = 12.75 WHERE id = 12")
    _at_ladder(monkeypatch, conn, f"UPDATE \"{src}\" SET amount = 9.99 WHERE id = 5")
    res = _copy_run(fake, src, dst)
    report = res.reconciliation or {}
    assert "concurrent_source_drift" not in report
    assert (report.get("value_discrepancy_unattributed") or {}).get("columns") == ["amount"], report


def test_a_destination_rewrite_on_the_copy_route_fails_naming_the_cell(copy_world, monkeypatch):
    fake, conn, src, dst = copy_world
    assert _copy_run(fake, src, dst).success
    _trigger(conn, dst, "IF NEW.id = 12 THEN NEW.amount := 0; END IF;")
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = 12.75 WHERE id = 12")
    res = _copy_run(fake, src, dst)
    assert not res.success
    assert any(str(r.get("pk")).strip("'") == "12" and r.get("column") == "amount" for r in _rows(res)), _rows(res)
