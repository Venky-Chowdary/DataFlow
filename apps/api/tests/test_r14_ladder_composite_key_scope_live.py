"""ACC-03 review — the verification ladder addresses rows by the WHOLE composite key.

``_maybe_attach_verification_ladder`` resolved ``pk_column = pk_cols[0]`` and
three steps used it on a ``(region, id)`` key:

* X-OW1 batch scoping kept every destination row whose *region* was in the
  batch, so a row this run never wrote, ``(ap, 99)``, was graded as an extra;
* the latest-per-key survivor pick (fixed in 0eddb33cb, now on the same key);
* the D67/D95 quarantine exclusion: a composite ``source_pk`` (the value list
  ``['ap', '1']``) was stringified and matched no row, so a correct run that
  quarantined ``(ap, 1)`` failed L5 on its own quarantine; keyed by region it
  would instead have excluded every ``ap`` row and hidden a missing ``(ap, 2)``.

One tuple key (``_ladder_key``) now serves all of them. Live PostgreSQL -> PG.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"), "port": int(os.environ.get("PGPORT", "5432")),
    "database": os.environ.get("PGDATABASE", "dataflow"), "username": os.environ.get("PGUSER", "dataflow"),
    "password": os.environ.get("PGPASSWORD", "dataflow"),
}
REGIONS = ("eu", "us", "ap")


def _pg():
    import psycopg2

    try:
        conn = psycopg2.connect(host=PG["host"], port=PG["port"], dbname=PG["database"],
                                user=PG["username"], password=PG["password"], connect_timeout=3)
    except Exception as exc:  # noqa: BLE001 — a foreign PG on :5432 refuses these credentials
        pytest.skip(f"project PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture()
def world(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    conn = _pg()
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"r14_src_{sfx}", f"r14_dst_{sfx}"
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE "{src}" (region TEXT NOT NULL, id INT NOT NULL, amount TEXT, note TEXT, PRIMARY KEY (region, id))')
    rows = [(r, i, f"{i}.50", None if i == 3 else ("" if i == 4 else f"{r} {i} ✓")) for r in REGIONS for i in range(1, 11)]
    cur.executemany(f'INSERT INTO "{src}" VALUES (%s, %s, %s, %s)', rows)
    cur.execute(f'CREATE TABLE "{dst}" (region TEXT NOT NULL, id INT NOT NULL, amount NUMERIC(10,2), note TEXT, PRIMARY KEY (region, id))')
    yield fake, conn, src, dst
    cur.execute(f'DROP TABLE IF EXISTS "{src}"')
    cur.execute(f'DROP TABLE IF EXISTS "{dst}" CASCADE')
    cur.execute(f"DROP FUNCTION IF EXISTS r14_tamper_{dst}() CASCADE")
    conn.close()


def _run(fake, src, dst, *, mode="strict"):
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public", table=src, **PG),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public", table=dst, **PG),
        sync_mode="upsert",
        stream_contracts=[{"name": src, "primary_key": ["region", "id"], "sync_mode": "upsert", "selected": True}],
        skip_preflight=True, validation_mode=mode,
    )
    jid = "r14" + uuid.uuid4().hex[:12]
    fake.update_job_status(jid, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, jid)


def _l2(res) -> dict:
    ladder = (res.reconciliation or {}).get("verification_ladder") or {}
    return (ladder.get("layers") or {}).get("L2") or {}


def _dest(conn, dst):
    cur = conn.cursor()
    cur.execute(f'SELECT region, id, amount, note FROM "{dst}"')
    return {(r[0], r[1]): r[2:] for r in cur.fetchall()}


def test_batch_scope_grades_the_written_keys_not_their_region(world):
    """A destination row this run never wrote, (ap, 99), is outside the batch."""
    fake, conn, src, dst = world
    conn.cursor().execute(f"INSERT INTO \"{dst}\" VALUES ('ap', 99, 9.99, 'loaded elsewhere')")
    first = _run(fake, src, dst)
    assert first.success, first.error
    conn.cursor().execute(f"UPDATE \"{src}\" SET note = 'changed 東京' WHERE region = 'ap' AND id = 5")
    second = _run(fake, src, dst)
    assert second.success, second.error
    # Region-keyed scope kept (ap, 99) on the target side: L2 then graded 31
    # target rows against 30 written ones and failed on a correct run.
    assert _l2(second).get("passed") is not False, _l2(second)
    dest = _dest(conn, dst)
    assert len(dest) == 31 and dest[("ap", 99)][1] == "loaded elsewhere"
    assert dest[("ap", 5)][1] == "changed 東京" and dest[("eu", 3)][1] is None and dest[("us", 4)][1] == ""


def test_a_quarantined_composite_key_is_excluded_exactly(world):
    """A correct lenient run that holds out (ap, 1) passes; every other row is exact."""
    fake, conn, src, dst = world
    assert _run(fake, src, dst).success
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = 'not-a-number' WHERE region = 'ap' AND id = 1")
    conn.cursor().execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    res = _run(fake, src, dst, mode="lenient")
    assert int((res.destination_summary or {}).get("rejected_rows") or 0) == 1
    assert res.success, res.error
    # The quarantined key is excluded from both compared populations, so L2
    # does not grade a correct run on its own quarantine (29 vs 30 before).
    assert _l2(res).get("passed") is not False, _l2(res)
    dest = _dest(conn, dst)
    assert str(dest[("ap", 2)][0]) == "2.75" and str(dest[("ap", 1)][0]) == "1.50"


def test_one_quarantined_key_does_not_hide_a_changed_sibling(world):
    """(ap, 1) is quarantined; the destination silently rewrites (ap, 2). The
    ladder must exclude exactly (ap, 1) and still compare (ap, 2).

    Source amounts are i + 0.50 for i in 1..10 in three regions (180.00).
    Excluding only (ap, 1) and taking (ap, 2) at 2.75 gives 178.75 on the
    source side; region-keyed exclusion would drop every ``ap`` row (and with
    it the rewritten one), nothing-excluded would keep (ap, 1).
    """
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
    cur.execute(f"UPDATE \"{src}\" SET amount = 'not-a-number' WHERE region = 'ap' AND id = 1")
    cur.execute(f"UPDATE \"{src}\" SET amount = '2.75' WHERE region = 'ap' AND id = 2")
    res = _run(fake, src, dst, mode="lenient")
    l2 = _l2(res)
    assert l2.get("passed") is False, l2
    amount = next(m for m in l2["details"]["mismatches"] if m["column"] == "amount")
    assert amount["diffs"]["sum_value"]["source"] == "178.75", amount
    assert amount["diffs"]["sum_value"]["target"] == "176.00", amount


def test_ladder_key_and_rejected_key_units():
    from src.transfer.reconcile_step import _ladder_key, _rejected_key

    cols = ["region", "id"]
    assert _ladder_key({"region": "ap", "id": 1}, cols) == ("ap", "1")
    assert _ladder_key({"region": "ap", "id": None}, cols) is None
    assert _rejected_key({"source_pk": ["ap", "1"]}, cols) == ("ap", "1")
    assert _rejected_key({"pk_value": {"region": "ap", "id": 1}}, cols) == ("ap", "1")
    assert _rejected_key({"row": {"region": "eu", "id": 7, "amount": "x"}}, cols) == ("eu", "7")
    # A scalar source_pk or a bare row number is never read as a composite key.
    assert _rejected_key({"source_pk": "ap"}, cols) is None
    assert _rejected_key({"row": 3}, cols) is None
    assert _rejected_key({"source_pk": 7}, ["id"]) == ("7",)
    assert _rejected_key({"row": 3}, ["id"]) == ("3",)
