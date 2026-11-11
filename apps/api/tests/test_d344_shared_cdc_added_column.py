"""D344 — the shared multi-table CDC consumer and a column added mid-capture.

Two PostgreSQL tables share one logical slot (``_run_cdc_shared_multi_table``)
into MySQL. While the capture runs, table A gains ``tier INT`` and an UPDATE
sets it; table B only changes a status before and after.

Before the fix a table with its own declared mapping dropped ``tier`` from
every event and the run reported success; with inherited (job-level)
mappings it either wrote ``tier`` as text or failed inside the writer and
fell back to the sequential reader.

* ``backfill_new_fields`` (what ``schema_policy=propagate_columns`` arms):
  A gains a typed, nullable ``tier`` carrying 9; B applies both changes and
  gets no column.
* otherwise: the run stops before A's first event that carries ``tier``,
  naming A and ``tier``; B's earlier change is applied, nothing after the
  stop is, and a later run that propagates applies every held change.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_d344_cdc_added_column_propagate_or_stop import (
    _live,
    _my_cfg,
    _my_sql,
    _pg_cfg,
    _pg_sql,
)

pytestmark = pytest.mark.timeout(300)

needs_live = pytest.mark.skipif(
    not _live(), reason="needs PostgreSQL with wal_level=logical and MySQL"
)


@pytest.fixture()
def cursors(tmp_path, monkeypatch):
    import services.sync_cursor as sc

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_CDC_MAPPING_REVIEW_STORE", str(tmp_path / "reviews.jsonl"))
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)


_MAPS = [{"source": c, "target": c} for c in ("id", "name", "status")]


def _run(a: str, b: str, *, declared: bool, propagate: bool, maps_a: list[dict] | None = None):
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    contracts = [
        {
            "name": t,
            "selected": True,
            "sync_mode": "cdc",
            "primary_key": "id",
            **({"mappings": list(maps_a if (maps_a and t == a) else _MAPS)} if declared else {}),
        }
        for t in (a, b)
    ]
    src = EndpointConfig(kind="database", format="postgresql", schema="public", table="", **_pg_cfg())
    dst = EndpointConfig(kind="database", format="mysql", table="", **_my_cfg())
    return run_cdc_database_transfer(
        src,
        dst,
        list(_MAPS),
        {"id": "integer", "name": "text", "status": "text"},
        sync_mode="cdc",
        stream_contracts=contracts,
        job_id="d344s" + uuid.uuid4().hex[:8],
        backfill_new_fields=propagate,
    )


def _rows(table: str) -> dict[int, tuple]:
    cols = _columns(table)
    picked = ["status", "tier"] if "tier" in cols else ["status"]
    return {
        r[0]: tuple(r[1:])
        for r in _my_sql(f"SELECT id, {', '.join(picked)} FROM `{table}`", fetch=True)
    }


def _columns(table: str) -> dict[str, str]:
    return {
        r[0]: str(r[1]).lower()
        for r in _my_sql(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = '{table}'",
            fetch=True,
        )
    }


def _setup(a: str, b: str) -> None:
    for t in (a, b):
        _pg_sql(f'CREATE TABLE "{t}" (id INT PRIMARY KEY, name TEXT, status TEXT)')
        _pg_sql(f'ALTER TABLE "{t}" REPLICA IDENTITY FULL')
        for i in (1, 2, 3):
            _pg_sql(f'INSERT INTO "{t}" VALUES (%s, %s, %s)', (i, f"n{i}", "new"))


def _alter_mid_capture(monkeypatch, a: str, b: str) -> None:
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    original = PostgreSqlChangeStreamCdc.poll
    fired = {"done": False}

    def poll(self):
        if not fired["done"]:
            fired["done"] = True
            _pg_sql(f"UPDATE \"{b}\" SET status = 'paid' WHERE id = 3")
            _pg_sql(f'ALTER TABLE "{a}" ADD COLUMN tier INT')
            _pg_sql(f"UPDATE \"{a}\" SET status = 'paid', tier = 9 WHERE id = 2")
            _pg_sql(f"UPDATE \"{b}\" SET status = 'late' WHERE id = 1")
        yield from original(self)

    monkeypatch.setattr(PostgreSqlChangeStreamCdc, "poll", poll)


def _cleanup(base: str, a: str, b: str) -> None:
    from services.cdc_multi_table import tables_digest

    # The shared slot / publication are named by a digest of the table set.
    marks = {base, f"mt_{tables_digest([a, b])}".lower(), f"mt_{tables_digest([b, a])}".lower()}
    for name, active in _pg_sql("SELECT slot_name, active FROM pg_replication_slots", fetch=True):
        if not active and any(m in name for m in marks):
            _pg_sql("SELECT pg_drop_replication_slot(%s)", (name,))
    for (pub,) in _pg_sql("SELECT pubname FROM pg_publication", fetch=True):
        if any(m in pub for m in marks):
            _pg_sql(f'DROP PUBLICATION IF EXISTS "{pub}"')
    for t in (a, b):
        _pg_sql(f'DROP TABLE IF EXISTS "{t}"')
        _my_sql(f"DROP TABLE IF EXISTS `{t}`")


@needs_live
@pytest.mark.parametrize("declared", [True, False], ids=["declared-maps", "inherited-maps"])
def test_shared_consumer_propagates_the_added_column(cursors, monkeypatch, declared):
    base = "d344s_" + uuid.uuid4().hex[:6]
    a, b = f"{base}_a", f"{base}_b"
    try:
        _setup(a, b)
        _run(a, b, declared=declared, propagate=True)
        _alter_mid_capture(monkeypatch, a, b)

        _rows_written, _ddl, summary, _ = _run(a, b, declared=declared, propagate=True)

        assert summary.get("cdc_shared_reader") is True
        assert _columns(a).get("tier") in {"int", "bigint"}, _columns(a)
        assert _rows(a) == {1: ("new", None), 2: ("paid", 9), 3: ("new", None)}
        assert "tier" not in _columns(b)
        assert _rows(b) == {1: ("late",), 2: ("new",), 3: ("paid",)}
    finally:
        _cleanup(base, a, b)


@needs_live
@pytest.mark.parametrize("declared", [True, False], ids=["declared-maps", "inherited-maps"])
def test_shared_consumer_stops_naming_table_and_column(cursors, monkeypatch, declared):
    from src.transfer.cdc_added_columns import CdcAddedColumnRefused

    base = "d344t_" + uuid.uuid4().hex[:6]
    a, b = f"{base}_a", f"{base}_b"
    try:
        _setup(a, b)
        _run(a, b, declared=declared, propagate=False)
        _alter_mid_capture(monkeypatch, a, b)

        with pytest.raises(CdcAddedColumnRefused) as refused:
            _run(a, b, declared=declared, propagate=False)

        assert refused.value.columns == ["tier"]
        assert a in str(refused.value) and "tier" in str(refused.value)
        assert "tier" not in _columns(a)
        # A's event was not applied without the column, and nothing after it.
        assert _rows(a) == {1: ("new",), 2: ("new",), 3: ("new",)}
        assert _rows(b)[1] == ("new",)

        # The remedy the error names: give tier a destination column and a
        # mapping entry. The position was not advanced, so the next run
        # applies every held change, tier included.
        _my_sql(f"ALTER TABLE `{a}` ADD COLUMN tier INT NULL")
        from services.reflection_cache import invalidate_named_tables

        invalidate_named_tables(a)  # as the engine does when a run starts
        _MAPS_WITH_TIER = [*_MAPS, {"source": "tier", "target": "tier"}]
        _run(a, b, declared=declared, propagate=False, maps_a=_MAPS_WITH_TIER)

        assert _rows(a) == {1: ("new", None), 2: ("paid", 9), 3: ("new", None)}
        assert _rows(b) == {1: ("late",), 2: ("new",), 3: ("paid",)}
    finally:
        _cleanup(base, a, b)
