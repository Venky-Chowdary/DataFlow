"""E3-006 review — the CDC snapshot sweep removes destination keys only when provably safe.

The first sweep (commit 20146de9a) hard-deleted every destination key a
snapshot did not present, gated only on "a snapshot ran into a populated SQL
table". Four ways that deletes rows the source still has:

(a) a query / procedure source yields the query's rows, not the table's;
(b) a destination table written by two streams of one request;
(c) a Map transform on a primary-key column: the writer binds the transformed
    cell, the census spooled the raw one, the anti-join misses every key;
(d) ``snapshot_mode=always`` re-snapshots every run.

Now ``plan_snapshot_sweep`` refuses each of those and the run *discloses* the
count instead ("N keys at the destination are not in the source; not removed
because ..."). The stream contract's mirror vocabulary (``delete_mode``
hard|soft|off) is the switch; the plan-time gate names the count before the job.
"""

from __future__ import annotations

import uuid

import pytest

from services.cdc_snapshot_sweep import (
    SnapshotKeyCensus,
    SweepPlan,
    build_snapshot_leftover_preflight_gate,
    plan_snapshot_sweep,
)
from tests.test_e3006_cdc_snapshot_removes_deleted_keys import (  # noqa: F401 — fixture + helpers
    _MY,
    _change,
    _create,
    _dest,
    _dest_conn,
    _drop,
    _exec,
    _fetch,
    _src_conn,
    _up,
    isolated,
)
from tests.test_d196_sqlserver_change_tracking_cdc_live import _DESTS

_RECOVERY = {"run_snapshot": True, "reason": "watermark_missing", "snapshot_mode": "initial"}
_ALWAYS = {"run_snapshot": True, "reason": "always", "snapshot_mode": "always"}
_MAPS = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def _plan(plan=_RECOVERY, **over):
    kw = dict(
        dest_type="postgresql", dest_absent_before=False, source_cfg={"type": "mysql"},
        mappings=_MAPS, pk_sources=["id"], delete_mode="",
    )
    kw.update(over)
    return plan_snapshot_sweep(plan, **kw)


def test_recovery_snapshot_into_a_table_source_is_allowed_and_hard_by_default():
    sweep = _plan()
    assert sweep == SweepPlan("hard", False, True)


def test_a_query_source_is_refused_with_the_reason():
    sweep = _plan(source_cfg={"type": "mysql", "extra": {"source_read_mode": "query"}})
    assert sweep is not None and not sweep.allowed
    assert "query/procedure extract" in sweep.reason and "delete_mode=off" in sweep.next_action


def test_the_run_finding_states_the_sole_writer_assumption():
    """Cross-request sharing is undetectable (F7): the finding says what it assumes."""
    from services.cdc_snapshot_sweep import SOLE_WRITER_ASSUMPTION

    assert "no other route writes this table" in SOLE_WRITER_ASSUMPTION


def test_a_transform_on_a_key_column_is_refused_naming_the_column():
    maps = [{"source": "id", "target": "id", "transform": "trim_id"}, {"source": "name", "target": "name"}]
    sweep = _plan(mappings=maps)
    assert not sweep.allowed and "primary key column(s) id" in sweep.reason
    # Identity spellings, typed carriers (the automap stamps ``integer`` on an
    # INT key) and transforms on non-key columns do not refuse.
    for carrier in ("identity", "integer", "decimal", "uuid", "string"):
        assert _plan(mappings=[{"source": "id", "target": "id", "transform": carrier},
                               {"source": "name", "target": "name", "transform": "upper"}]).allowed, carrier
    for rewrite in ("trim", "hash_pii", "replace:a:b", "default:0", "upper"):
        assert not _plan(mappings=[{"source": "id", "target": "id", "transform": rewrite}]).allowed, rewrite


def test_snapshot_mode_always_needs_a_declared_delete_mode():
    sweep = _plan(_ALWAYS)
    assert not sweep.allowed and "snapshot_mode=always" in sweep.reason
    assert _plan(_ALWAYS, delete_mode="hard") == SweepPlan("hard", True, True)
    assert _plan(_ALWAYS, delete_mode="soft") == SweepPlan("soft", True, True)


def test_delete_mode_off_keeps_and_reports():
    sweep = _plan(delete_mode="off")
    assert sweep.mode == "off" and not sweep.allowed and "delete_mode=off" in sweep.reason


def test_an_unknown_delete_mode_is_refused_with_the_vocabulary():
    from services.transfer_policy_values import PolicyValueError

    with pytest.raises(PolicyValueError, match="hard, off, soft"):
        _plan(delete_mode="purge")
    # delete_mode is validated by the one registry, canonical spelling included.
    assert _plan(delete_mode="OFF").mode == "off"


def test_no_snapshot_or_created_table_or_non_sql_destination_means_nothing_to_measure():
    assert _plan({"run_snapshot": False}) is None
    assert _plan({**_RECOVERY, "snapshot_dump_open": True}) is None
    assert _plan(dest_absent_before=True) is None and _plan(dest_absent_before=None) is None
    assert _plan(dest_type="mongodb") is None


def test_a_partial_key_in_the_snapshot_disables_the_census():
    from services.cdc_engine import ChangeBatch

    census = SnapshotKeyCensus(["id"], SweepPlan("hard", False, True))
    census.add(ChangeBatch(inserts=[{"id": 1}, {"id": None}]))
    assert census.complete is False
    out = census.settle(object(), mappings=[], pk_target_cols=["id"])
    assert out["status"] == "unmeasured" and out["removed"] == 0 and "nothing was removed" in out["finding"]
    census.close()


# --- live: MySQL binlog -> PostgreSQL ----------------------------------------

def _cdc_request(table, *, delete_mode="", mappings=None, sync="cdc"):
    from src.transfer.models import EndpointConfig, TransferRequest

    contract = {"name": table, "selected": True, "primary_key": ["id"], "sync_mode": sync,
                "cursor_semantics": "cdc_position"}
    if delete_mode:
        contract["delete_mode"] = delete_mode
    return TransferRequest(
        source=EndpointConfig(kind="database", format="mysql", table=table, **_MY),
        destination=EndpointConfig(kind="database", format="postgresql", table=table, **_DESTS["postgresql"]),
        sync_mode=sync, stream_contracts=[contract], mappings=list(mappings or []),
        skip_preflight=True, validation_mode="strict")


def _run(fake, req):
    from src.transfer.engine import UniversalTransferEngine

    jid = "swp" + uuid.uuid4().hex[:12]
    fake.update_job_status(jid, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, jid)


def _live_or_skip():
    if not (_up(_MY) and _up(_DESTS["postgresql"])):
        pytest.skip("live MySQL / PostgreSQL not reachable")


def test_live_delete_mode_off_keeps_the_row_and_discloses_the_count(isolated):
    _live_or_skip()
    table = "swp_off_" + uuid.uuid4().hex[:8]
    _create("mysql", table)
    try:
        assert _run(isolated, _cdc_request(table, sync="full_refresh_overwrite")).success
        _change("mysql", table)
        res = _run(isolated, _cdc_request(table, delete_mode="off"))
        assert res.success, res.error
        dest = _dest(table)
        assert 2 in dest, "delete_mode=off keeps the leftover key"
        left = (res.destination_summary or {}).get("snapshot_leftover") or {}
        assert left.get("status") == "kept" and left.get("leftover") == 1 and left.get("removed") == 0
        assert "1 key(s) at the destination are not in the source" in left["finding"]
        assert "delete_mode=off" in left["finding"]
        assert "snapshot_leftover_deleted" not in (res.destination_summary or {})
    finally:
        _drop("mysql", table)


def test_live_a_transform_on_the_key_refuses_and_discloses(isolated):
    """A text key with ``upper``: the writer binds the rewritten cell, so the raw
    snapshot key cannot vouch for the destination row — kept, and disclosed.
    (On an INT key the resolver replaces a text transform with the ``integer``
    carrier, which keeps the value; that route stays allowed.)"""
    _live_or_skip()
    from src.transfer.models import EndpointConfig, TransferRequest

    table = "swp_tf_" + uuid.uuid4().hex[:8]
    conn = _src_conn("mysql")
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE `{table}` (code VARCHAR(10) PRIMARY KEY, name VARCHAR(40) NULL) CHARSET utf8mb4")
    cur.executemany(f"INSERT INTO `{table}` VALUES (%s, %s)", [("a1", "x"), ("a2", "y"), ("a3", None)])
    conn.close()

    def _req(sync, maps=None):
        contract = {"name": table, "selected": True, "primary_key": ["code"], "sync_mode": sync,
                    "cursor_semantics": "cdc_position"}
        return TransferRequest(
            source=EndpointConfig(kind="database", format="mysql", table=table, **_MY),
            destination=EndpointConfig(kind="database", format="postgresql", table=table, **_DESTS["postgresql"]),
            sync_mode=sync, stream_contracts=[contract], mappings=list(maps or []),
            skip_preflight=True, validation_mode="strict")

    maps = [{"source": "code", "target": "code", "transform": "upper"}, {"source": "name", "target": "name"}]
    try:
        assert _run(isolated, _req("full_refresh_overwrite")).success
        conn = _src_conn("mysql")
        conn.cursor().execute(f"DELETE FROM `{table}` WHERE code = 'a2'")
        conn.close()
        # The rewritten key duplicates every row (a1 and A1), so Gate-8 fails the
        # job honestly; what this test guards is that the sweep deleted nothing
        # and said why.
        res = _run(isolated, _req("cdc", maps))
        d = _dest_conn("postgresql")
        try:
            codes = {r[0] for r in _fetch(d, f'SELECT code FROM "{table}"')}
        finally:
            d.close()
        assert {"a1", "a2", "a3"} <= codes, "a transformed key column must never drive a delete"
        finding = " ".join(
            [str((res.destination_summary or {}).get("snapshot_leftover", {}).get("finding") or "")]
            + [line for line in (res.ddl_executed or []) if "not removed" in line]
        )
        assert "not removed because a Map transform rewrites primary key column(s) code" in finding
        assert "delete_mode=off" in finding
    finally:
        _drop("mysql", table)


def test_live_declared_hard_removes_and_says_so(isolated):
    _live_or_skip()
    table = "swp_hd_" + uuid.uuid4().hex[:8]
    _create("mysql", table)
    try:
        assert _run(isolated, _cdc_request(table, sync="full_refresh_overwrite")).success
        _change("mysql", table)
        res = _run(isolated, _cdc_request(table, delete_mode="hard"))
        assert res.success, res.error
        assert 2 not in _dest(table)
        left = (res.destination_summary or {}).get("snapshot_leftover") or {}
        assert left.get("status") == "removed" and left.get("removed") == 1
        assert "delete_mode=hard" in left["finding"] and "(default)" not in left["finding"]
        assert (res.destination_summary or {}).get("snapshot_leftover_deleted") == 1
    finally:
        _drop("mysql", table)


def test_live_multi_table_heterogeneous_tables_each_sweep_their_own_keys(isolated):
    """Item 2: two streams, different columns; each table's leftover goes, nothing else."""
    _live_or_skip()
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    sfx = uuid.uuid4().hex[:8]
    orders, users = f"swp_o_{sfx}", f"swp_u_{sfx}"
    conn = _src_conn("mysql")
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE `{orders}` (id INT PRIMARY KEY, amount DECIMAL(10,2), note VARCHAR(40)) CHARSET utf8mb4")
    cur.execute(f"CREATE TABLE `{users}` (id INT PRIMARY KEY, name VARCHAR(40), email VARCHAR(80), active TINYINT(1)) CHARSET utf8mb4")
    cur.executemany(f"INSERT INTO `{orders}` VALUES (%s,%s,%s)", [(i, i * 1.5, None if i == 2 else f"n{i} ✓") for i in range(1, 5)])
    cur.executemany(f"INSERT INTO `{users}` VALUES (%s,%s,%s,%s)", [(i, f"u{i}", "" if i == 3 else f"u{i}@x.io", i % 2) for i in range(1, 5)])
    conn.close()
    src = EndpointConfig(kind="database", format="mysql", table="", **_MY)
    dst = EndpointConfig(kind="database", format="postgresql", table="", **_DESTS["postgresql"])
    # snapshot_mode=always re-snapshots each run; a declared delete_mode=hard
    # is what lets that snapshot remove keys (the default refuses, item d).
    contracts = [
        {"name": orders, "selected": True, "sync_mode": "cdc", "primary_key": "id",
         "snapshot_mode": "always", "delete_mode": "hard"},
        {"name": users, "selected": True, "sync_mode": "cdc", "primary_key": "id",
         "snapshot_mode": "always", "delete_mode": "hard"},
    ]
    try:
        rows0, _d0, s0, _w0 = run_cdc_database_transfer(
            src, dst, [], {}, sync_mode="cdc", stream_contracts=contracts,
            job_id="swpmt0" + sfx, limit=0,
        )
        assert rows0 >= 8, rows0
        assert "snapshot_leftover" not in s0, "tables this run created are not swept"
        conn = _src_conn("mysql")
        cur = conn.cursor()
        cur.execute(f"DELETE FROM `{orders}` WHERE id = 2")
        cur.execute(f"DELETE FROM `{users}` WHERE id = 4")
        cur.execute(f"INSERT INTO `{orders}` VALUES (9, 9.5, 'nine')")
        conn.close()
        rows, _ddl, summary, _w = run_cdc_database_transfer(
            src, dst, [], {}, sync_mode="cdc", stream_contracts=contracts,
            job_id="swpmt" + sfx, limit=0,
        )
        d = _dest_conn("postgresql")
        try:
            # The snapshot created the tables without a declared schema, so the
            # key lands as text; compare by value spelling.
            o = {str(r[0]): r[1:] for r in _fetch(d, f'SELECT id, amount, note FROM "{orders}"')}
            u = {str(r[0]): r[1:] for r in _fetch(d, f'SELECT id, name, email FROM "{users}"')}
        finally:
            d.close()
        assert set(o) == {"1", "3", "4", "9"} and set(u) == {"1", "2", "3"}
        assert o["3"][1] == "n3 ✓" and u["3"][1] == ""
        left = summary.get("snapshot_leftover") or {}
        assert left[orders]["removed"] == 1 and left[users]["removed"] == 1
        assert summary.get("snapshot_leftover_deleted") == {orders: 1, users: 1}
    finally:
        for t in (orders, users):
            _drop("mysql", t)


def test_plan_time_gate_names_the_count_and_the_action(isolated):
    _live_or_skip()
    from src.transfer.models import EndpointConfig

    table = "swp_pf_" + uuid.uuid4().hex[:8]
    _create("mysql", table)
    try:
        assert _run(isolated, _cdc_request(table, sync="full_refresh_overwrite")).success
        _change("mysql", table)
        src = EndpointConfig(kind="database", format="mysql", table=table, **_MY)
        dst = EndpointConfig(kind="database", format="postgresql", table=table, **_DESTS["postgresql"])
        snap_gate = {"id": "g18_cdc_snapshot_mode", "status": "pass",
                     "details": {"run_snapshot": True, "snapshot_mode": "initial",
                                 "snapshot_plan_reason": "watermark_missing"}}
        gate = build_snapshot_leftover_preflight_gate(
            snapshot_gate=snap_gate,
            stream_contracts=[{"name": table, "selected": True, "sync_mode": "cdc", "primary_key": ["id"]}],
            source_endpoint=src, destination_endpoint=dst, source_config=dict(_MY, type="mysql"),
            mappings=[], dest_type="postgresql",
        )
        assert gate and gate["status"] == "warn"
        assert gate["details"]["leftover_keys"] == 1 and gate["details"]["will_remove"] is True
        assert "will remove 1 destination key" in gate["message"] and "delete_mode=off" in gate["message"]
        gate_off = build_snapshot_leftover_preflight_gate(
            snapshot_gate=snap_gate,
            stream_contracts=[{"name": table, "selected": True, "sync_mode": "cdc", "primary_key": ["id"], "delete_mode": "off"}],
            source_endpoint=src, destination_endpoint=dst, source_config=dict(_MY, type="mysql"),
            mappings=[], dest_type="postgresql",
        )
        assert gate_off["details"]["will_remove"] is False and "will NOT be removed" in gate_off["message"]
        assert build_snapshot_leftover_preflight_gate(
            snapshot_gate={"details": {"run_snapshot": False}}, stream_contracts=[], source_endpoint=src,
            destination_endpoint=dst, source_config={}, mappings=[], dest_type="postgresql") is None
        assert "no other route writes this table" in gate["message"]

        # F1: the REST / MCP / plan-service routes hand the policy gates plain
        # dicts; the gate must fire there too, through run_transfer_policy_gates.
        from services.preflight_transfer_policy import run_transfer_policy_gates

        contracts = [{"name": table, "selected": True, "sync_mode": "cdc", "primary_key": ["id"]}]
        src_dict = {"type": "mysql", "kind": "database", "table": table, **_MY}
        dst_dict = {"type": "postgresql", "kind": "database", "table": table, **_DESTS["postgresql"]}
        gates = run_transfer_policy_gates(
            sync_mode="cdc", source_type="mysql", dest_type="postgresql", source_kind="database",
            source_table=table, stream_contracts=contracts, source_endpoint=src_dict,
            destination_endpoint=dst_dict, source_config=dict(src_dict),
        )
        by_id = {g.get("id"): g for g in gates}
        assert by_id["g18_cdc_snapshot_mode"]["details"].get("snapshot_plan_reason") == "watermark_missing", by_id
        leftover = by_id.get("g18b_cdc_snapshot_leftover")
        assert leftover and leftover["status"] == "warn" and leftover["details"]["leftover_keys"] == 1, by_id

        # F2: Validate reasons exactly as Execute will. snapshot_mode=always with
        # a present position: the run refuses an undeclared sweep, so must the plan.
        from services.cdc_snapshot_mode import build_snapshot_mode_preflight_gate

        always = [{"name": table, "selected": True, "sync_mode": "cdc", "primary_key": ["id"], "snapshot_mode": "always"}]
        g18_always = build_snapshot_mode_preflight_gate(sync_mode="cdc", stream_contracts=always, watermark="0/16B3748")
        assert g18_always["details"]["run_snapshot"] and g18_always["details"]["snapshot_plan_reason"] == "always"
        plan_always = build_snapshot_leftover_preflight_gate(
            snapshot_gate=g18_always, stream_contracts=always, source_endpoint=src_dict,
            destination_endpoint=dst_dict, source_config=dict(src_dict), mappings=[], dest_type="postgresql",
        )
        assert plan_always["details"]["will_remove"] is False and "will NOT be removed" in plan_always["message"]
        assert "snapshot_mode=always" in plan_always["message"]
        # An open dump: Execute never sweeps after a resumed dump; the plan says nothing.
        g18_open = build_snapshot_mode_preflight_gate(
            sync_mode="cdc", stream_contracts=contracts, watermark={"phase": "snapshot", "table": table, "last_pk": "3"},
        )
        assert g18_open["details"]["snapshot_dump_open"] is True
        assert build_snapshot_leftover_preflight_gate(
            snapshot_gate=g18_open, stream_contracts=contracts, source_endpoint=src_dict,
            destination_endpoint=dst_dict, source_config=dict(src_dict), mappings=[], dest_type="postgresql",
        ) is None
    finally:
        _drop("mysql", table)


def test_plan_time_census_states_its_real_bound(monkeypatch):
    """F8: an unmeasured count names the census cap, which is the key-list cap."""
    import services.cdc_snapshot_sweep as sweep
    import services.preflight_cursor_gate as pcg
    from services.dest_precount import _KEYSET_CENSUS_MAX
    from src.transfer.models import EndpointConfig

    assert sweep.plan_probe_key_cap() == _KEYSET_CENSUS_MAX
    monkeypatch.setattr(sweep, "_plan_time_leftover", lambda *a, **k: None)
    monkeypatch.setattr(pcg, "dest_object_missing", lambda *a, **k: False)
    dst = EndpointConfig(kind="database", format="postgresql", host="h", port=5432, database="d", table="t")
    gate = build_snapshot_leftover_preflight_gate(
        snapshot_gate={"details": {"run_snapshot": True, "snapshot_plan_reason": "watermark_missing", "snapshot_mode": "initial"}},
        stream_contracts=[{"name": "t", "selected": True, "sync_mode": "cdc", "primary_key": ["id"]}],
        source_endpoint=dst, destination_endpoint=dst, source_config={"type": "postgresql"}, mappings=[], dest_type="postgresql",
    )
    assert gate["details"]["leftover_keys"] is None
    assert f"census bounded to {_KEYSET_CENSUS_MAX:,} keys" in gate["message"]


def test_an_unanswerable_existence_probe_is_unknown_not_populated(monkeypatch):
    """F9: a probe that errors is None; plan_snapshot_sweep then measures nothing."""
    import services.dest_precount as dp
    from src.transfer.cdc_transfer import _dest_table_absent

    def _boom(*a, **k):
        raise RuntimeError("catalog down")

    monkeypatch.setattr(dp, "destination_table_exists", _boom)
    assert _dest_table_absent("postgresql", {"type": "postgresql"}, "t") is None
    monkeypatch.setattr(dp, "destination_table_exists", lambda *a, **k: None)
    assert _dest_table_absent("postgresql", {"type": "postgresql"}, "t") is None
    monkeypatch.setattr(dp, "destination_table_exists", lambda *a, **k: False)
    assert _dest_table_absent("postgresql", {"type": "postgresql"}, "t") is True
    assert _plan(dest_absent_before=None) is None
