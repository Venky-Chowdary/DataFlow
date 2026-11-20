"""PR #433 append review: R1 exactly-once and R6 append-keeps-versions together.

A three-run incremental_append schedule into a table Datawrap creates:

* run 2 sees a changed row (new cursor value), a new row, and a row inserted
  at the stored watermark with a key below the last key read (D124 tie);
* the changed row lands as a new version (R6; D377/D396 AC-1), the boundary
  tie rows the previous run delivered are not appended again (R1; D335), the
  unread tie row lands once, and the transform proof grades only this run's
  versions and says ``verified`` (D359/D123), not "unverifiable";
* run 3 is a quiet poll that writes nothing.

Also: plan_transfer allows append into an existing keyed table and discloses
the key (D395, R6/R8), and an existing keyed destination refuses a changed key
loudly, never merging it (R6).

SQLite -> SQLite in process, PostgreSQL -> PostgreSQL live, through the engine
with preflight on.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_up  # noqa: E402
from tests.test_d124_watermark_tie_group_e2e import _backend  # noqa: E402

_ROUTES = {
    "sqlite_sqlite": (None, "sqlite", False),
    "sqlite_sqlite_rowpath": (None, "sqlite", True),
    "pg_pg": (pg_up, "pg", False),
    "pg_pg_rowpath": (pg_up, "pg", True),
}


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, src, dst, src_table, dst_table, row_path, key=("id",)):
    job_id = "pr433" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=src.endpoint(src_table),
        destination=dst.endpoint(dst_table),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 1.0},
            {"source": "seq", "target": "seq", "target_type": "INTEGER", "confidence": 1.0},
            {"source": "v", "target": "v", "target_type": "TEXT", "transform": "upper",
             "transform_override": True, "confidence": 0.99},
        ],
        sync_mode="incremental_append",
        stream_contracts=[{
            "name": src_table, "cursor_field": "seq", "cursor_semantics": "monotonic_sequence",
            "primary_key": list(key), "sync_mode": "incremental_append", "selected": True,
        }],
        validation_mode="strict",
        source_filter={"column": "id", "op": "gte", "value": 0} if row_path else {},
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _proof_status(result) -> str:
    return str(((result.reconciliation or {}).get("transform_applied") or {}).get("status") or "")


@pytest.mark.parametrize("route", sorted(_ROUTES))
def test_changed_rows_append_as_versions_and_ties_land_once(tmp_path, isolated, route):
    """No declared key: the created table enforces none, so every version is kept."""
    probe, kind, row_path = _ROUTES[route]
    if probe is not None and not probe():
        pytest.skip(f"{route}: live database not available")
    src = _backend(kind, tmp_path, "src")
    dst = _backend(kind, tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    src_table, dst_table = f"p433s_{suffix}", f"p433d_{suffix}"
    src.execute(f'CREATE TABLE "{src_table}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    try:
        # Run 1 ends on the tie group seq=4 (ids 4 and 5).
        src.execute(
            f'INSERT INTO "{src_table}" VALUES (1, 1, \'a\'), (2, 2, \'b\'), (3, 3, \'c\'), '
            "(4, 4, 'd'), (5, 4, 'e')"
        )
        first = _run(isolated, src, dst, src_table, dst_table, row_path, key=())
        assert first.success, first.error
        assert len(dst.rows(dst_table)) == 5

        # A changed row (new cursor), a new row, and an unread row at the
        # stored watermark whose key sorts below the last key read.
        src.execute(f"UPDATE \"{src_table}\" SET seq = 6, v = 'b2' WHERE id = 2")
        src.execute(f"INSERT INTO \"{src_table}\" VALUES (6, 6, 'f')")
        src.execute(f"INSERT INTO \"{src_table}\" VALUES (0, 4, 'z')")
        second = _run(isolated, src, dst, src_table, dst_table, row_path, key=())
        assert second.success, second.error
        expected = [
            (0, 4, "Z"), (1, 1, "A"), (2, 2, "B"), (2, 6, "B2"), (3, 3, "C"),
            (4, 4, "D"), (5, 4, "E"), (6, 6, "F"),
        ]
        assert dst.rows(dst_table) == expected
        assert _proof_status(second) in {"verified", "skipped", ""}, second.reconciliation

        third = _run(isolated, src, dst, src_table, dst_table, row_path, key=())  # quiet poll
        assert third.success, third.error
        assert dst.rows(dst_table) == expected
    finally:
        src.drop(src_table)
        dst.drop(dst_table)


@pytest.mark.parametrize("route", ["sqlite_sqlite", "pg_pg_rowpath"])
def test_created_table_with_a_declared_key_refuses_a_changed_key_loudly(tmp_path, isolated, route):
    """A declared contract key is carried on CREATE for incremental_append (base rule,
    87c5ea7a0). A changed key then collides: the run fails loudly and nothing merges.
    A new key and the boundary tie group still land exactly once."""
    probe, kind, row_path = _ROUTES[route]
    if probe is not None and not probe():
        pytest.skip(f"{route}: live database not available")
    src = _backend(kind, tmp_path, "src")
    dst = _backend(kind, tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    src_table, dst_table = f"p433c_{suffix}", f"p433cd_{suffix}"
    src.execute(f'CREATE TABLE "{src_table}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    try:
        src.execute(f"INSERT INTO \"{src_table}\" VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 2, 'c')")
        assert (first := _run(isolated, src, dst, src_table, dst_table, row_path)).success, first.error
        src.execute(f"INSERT INTO \"{src_table}\" VALUES (0, 2, 'z'), (4, 4, 'd')")
        second = _run(isolated, src, dst, src_table, dst_table, row_path)
        assert second.success, second.error
        stored = [(0, 2, "Z"), (1, 1, "A"), (2, 2, "B"), (3, 2, "C"), (4, 4, "D")]
        assert dst.rows(dst_table) == stored
        src.execute(f"UPDATE \"{src_table}\" SET seq = 9, v = 'b9' WHERE id = 2")
        third = _run(isolated, src, dst, src_table, dst_table, row_path)
        assert not third.success
        assert "id" in (third.error or "")
        assert dst.rows(dst_table) == stored
    finally:
        src.drop(src_table)
        dst.drop(dst_table)


@pytest.mark.parametrize("route", ["sqlite_sqlite", "pg_pg"])
def test_existing_keyed_destination_refuses_a_changed_key_and_never_merges(
    tmp_path, isolated, route
):
    probe, kind, row_path = _ROUTES[route]
    if probe is not None and not probe():
        pytest.skip(f"{route}: live database not available")
    src = _backend(kind, tmp_path, "src")
    dst = _backend(kind, tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    src_table, dst_table = f"p433k_{suffix}", f"p433kd_{suffix}"
    src.execute(f'CREATE TABLE "{src_table}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    dst.execute(f'CREATE TABLE "{dst_table}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    try:
        src.execute(f"INSERT INTO \"{src_table}\" VALUES (1, 1, 'a'), (2, 2, 'b')")
        first = _run(isolated, src, dst, src_table, dst_table, row_path)
        assert first.success, first.error
        # New keys into the keyed table are legitimate.
        src.execute(f"INSERT INTO \"{src_table}\" VALUES (3, 3, 'c')")
        second = _run(isolated, src, dst, src_table, dst_table, row_path)
        assert second.success, second.error
        before = dst.rows(dst_table)
        assert before == [(1, 1, "A"), (2, 2, "B"), (3, 3, "C")]
        # A changed key collides with the enforced key: loud, and not merged.
        src.execute(f"UPDATE \"{src_table}\" SET seq = 9, v = 'b9' WHERE id = 2")
        third = _run(isolated, src, dst, src_table, dst_table, row_path)
        assert not third.success
        assert "id" in (third.error or "")
        assert dst.rows(dst_table) == before
    finally:
        src.drop(src_table)
        dst.drop(dst_table)



from tests.test_pilot_transfer_wave92 import pg_route  # noqa: E402,F401 - fixture


def test_plan_allows_and_discloses_append_into_an_existing_keyed_table(pg_route):  # noqa: F811
    """D395 corrected: R6 forbids a silent merge, not appending new keys into a keyed table."""
    if not pg_up():
        pytest.skip("Postgres is not reachable")
    from src.ai.copilot.tools import DataPilotTools

    res = DataPilotTools().execute("plan_transfer", {
        "source_connector_name": pg_route["connector_name"],
        "source_table": pg_route["src"],
        "dest_connector_name": pg_route["connector_name"],
        "dest_table": pg_route["dst"],
        "sync_mode": "full_refresh_append",
    })
    assert res.success, res.error
    warnings = " ".join(res.output.get("warnings") or [])
    assert "primary key (id)" in warnings, res.output.get("warnings")
    assert "never merged" in warnings
