"""COPY upsert routes carry the keyed write witness too.

A COPY upsert (``copy_binary_server_to_server_upsert``: PostgreSQL ->
PostgreSQL, one mapped key) moves rows inside the engines; none passes
through this process, so the streaming witness never saw them and the ladder
could not attribute. A concurrent ``UPDATE`` on the source after the merge
then failed the run and restored the destination (L3 recomputed on the
batch-scoped re-read). ``merge_staging_into_dest`` now reads the staged batch,
which is exactly what the MERGE wrote, back onto the witness before dropping it.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

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


def _amounts(conn, dst):
    cur = conn.cursor()
    cur.execute(f'SELECT id, amount FROM "{dst}"')
    return dict(cur.fetchall())


def test_a_concurrent_source_update_after_a_copy_upsert_is_disclosed_drift(copy_world, monkeypatch):
    fake, conn, src, dst = copy_world
    assert _copy_run(fake, src, dst).success
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = 12.75 WHERE id = 12")
    _at_ladder(monkeypatch, conn, f"UPDATE \"{src}\" SET amount = 9.99 WHERE id = 5")
    res = _copy_run(fake, src, dst)
    assert res.success, res.error
    report = res.reconciliation or {}
    drift = report.get("concurrent_source_drift") or {}
    assert drift.get("count") == 1 and [str(k["id"]) for k in drift["sample_keys"]] == ["5"], report
    amounts = _amounts(conn, dst)
    assert amounts[12] == Decimal("12.75") and amounts[5] == Decimal("5.50")


def test_a_destination_rewrite_on_the_copy_route_still_fails_naming_the_cell(copy_world, monkeypatch):
    fake, conn, src, dst = copy_world
    assert _copy_run(fake, src, dst).success
    _trigger(conn, dst, "IF NEW.id = 12 THEN NEW.amount := 0; END IF;")
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = 12.75 WHERE id = 12")
    _at_ladder(monkeypatch, conn, f"UPDATE \"{src}\" SET amount = 9.99 WHERE id = 5")
    res = _copy_run(fake, src, dst)
    assert not res.success
    rows = ((res.reconciliation or {}).get("verification_ladder") or {}).get("localization", {}).get("rows") or []
    assert rows and all(str(r.get("pk")).strip("'") == "12" and r.get("column") == "amount" for r in rows), rows
    assert ((res.reconciliation or {}).get("concurrent_source_drift") or {}).get("count") == 1
