"""D403 — SQL Server CDC → MySQL must not report drift over exact DATETIMEOFFSET text.

QA (r47-r75): schedule SQL Server CDC → MySQL, runs 1-3 FAILED "CDC verification
found 29/28/27 rows ... do not match the source image" while the destination was
exact: MySQL holds the DATETIMEOFFSET as ISO text (VARCHAR(64)).

The production fix (``stamp_cdc_instant_carriers``) only reached the CDC value
proof. The message QA saw comes from the exactly-once drift scan
(``identity_rows_missing_on_dest``), which still fingerprinted the source
instant against the destination text. And the stamp only recognised a text
type by name: the CDC scan's types are the source schema, where
``datetimeoffset`` materialises on MySQL as ``VARCHAR(64)`` but is not a text
name, so a UTC instant (``…Z`` text) still differed.

SQL Server is CI-only here. This drives ``run_cdc_database_transfer`` with the
SQL Server native reader replaced by a fake that hands over what pyodbc hands
over (aware ``datetime`` cells), the source image scan answered with the same
rows, and a live MySQL destination written by the real CDC apply and read back
by the real scan.
"""

from __future__ import annotations

import datetime as dt
import uuid

import pytest

from tests import value_fidelity_route as r

UTC = dt.timezone.utc
_OFFSETS = [UTC, dt.timezone(dt.timedelta(hours=-5)), dt.timezone(dt.timedelta(hours=5, minutes=30))]


def _image(run: int) -> list[dict]:
    """The source table after ``run`` runs; each run updates three keys."""
    rows = []
    for i in range(1, 31):
        stamp = None
        if i != 30:
            stamp = dt.datetime(2024, 11, 3, 5, 37, i, 123456, tzinfo=_OFFSETS[i % 3])
            if i <= 3 * run and run > 1:
                stamp += dt.timedelta(minutes=run)
        rows.append({"id": i, "tstz": stamp, "note": f"n{i}-{run if i <= 3 * run else 1}"})
    return rows


@pytest.fixture()
def route(tmp_path, monkeypatch):
    from services.cdc_engine import ChangeBatch
    from src.transfer.models import EndpointConfig

    import services.cdc_value_digest as value_digest
    import services.sync_cursor as sync_cursor
    import src.transfer.cdc_transfer as transfer

    cfg = r.dest_config("mysql")
    table = "d403_" + uuid.uuid4().hex[:8]
    state = {"run": 0, "events": {}}

    class FakeSqlServerCdc:
        def __init__(self, *args, **kwargs):
            self.sent = False
            self.start_lsn = f"0x{state['run']:02d}"
            self.start_seqval = ""
            self._resume_inclusive = False

        def capture_head_position(self):
            return f"0x{state['run'] + 1:02d}"

        def capture_reader_position(self):
            return f"0x{state['run'] + 1:02d}"

        def is_available(self):
            return True

        def snapshot(self):
            return iter(())

        def poll(self):
            if self.sent:
                return
            self.sent = True
            run = state["run"]
            image = state["events"].get(run) or _image(run)
            if run == 1:
                yield ChangeBatch(inserts=[dict(x) for x in image], resume_token=f"lsn=0x{run + 1:02d}", table="orders")
            else:
                changed = [dict(x) for x in image if x["id"] <= 3 * run]
                yield ChangeBatch(updates=changed, resume_token=f"lsn=0x{run + 1:02d}", table="orders")

        def ack(self, token=None):
            pass

        def close(self):
            pass

    for key, value in {
        "DATAFLOW_JOB_STORE": "memory",
        "DATAFLOW_DISABLE_OBJECT_STORE": "1",
        "DATAFLOW_CDC_MAX_IDLE_POLLS": "1",
        "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2",
        "DATAFLOW_CDC_TXN_HOLD_SLEEP_SEC": "0",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "sync.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(sync_cursor, "_stamp_job_watermark", lambda *a, **k: None)
    monkeypatch.setattr(transfer, "SqlServerNativeCdc", FakeSqlServerCdc)
    real_scan = value_digest._scan_table

    def _scan(source_type, scan_cfg, scan_table, columns):
        if source_type in {"sqlserver", "mssql"}:  # the source image, as pyodbc reads it
            return [{c: row[c] for c in columns} for row in _image(state["run"])]
        return real_scan(source_type, scan_cfg, scan_table, columns)

    monkeypatch.setattr(value_digest, "_scan_table", _scan)
    source = EndpointConfig(
        kind="database", format="sqlserver", host="127.0.0.1", port=1, database="d403src",
        table="orders", schema="dbo", username="ci", password="ci",
    )
    destination = EndpointConfig(
        kind="database", format="mysql", table=table, host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
    )
    try:
        yield source, destination, table, state
    finally:
        r.drop("mysql", table)


def _run(route_, tstz_schema: str):
    from src.transfer.cdc_transfer import run_cdc_database_transfer

    source, destination, _table, state = route_
    state["run"] += 1
    return run_cdc_database_transfer(
        source,
        destination,
        mappings=[
            {"source": "id", "target": "id", "source_type": "int", "target_type": "BIGINT", "confidence": 1.0},
            {"source": "tstz", "target": "tstz", "source_type": "datetimeoffset(7)",
             "target_type": "VARCHAR(64)", "confidence": 1.0},
            {"source": "note", "target": "note", "source_type": "nvarchar(50)", "target_type": "TEXT", "confidence": 1.0},
        ],
        schema={"id": "integer", "tstz": tstz_schema, "note": "string"},
        sync_mode="cdc",
        stream_contracts=[
            {"name": "orders", "selected": True, "primary_key": "id", "cursor_field": "id", "sync_mode": "cdc"}
        ],
        delivery_guarantee="exactly_once",
        delivery_pinned=True,
    )


@pytest.mark.parametrize("tstz_schema", ["datetimeoffset", "string"])
def test_three_exact_cdc_runs_are_green(route, tstz_schema):
    _source, _destination, table, _state = route
    for run in (1, 2, 3):
        result = _run(route, tstz_schema)
        assert result[2].get("cdc_delivery") == "exactly_once"
        assert "image_repair" not in result[2].get("cdc", {}), run
        stored = dict(r.query("mysql", f"SELECT id, tstz FROM `{table}`"))  # nosec B608
        assert len(stored) == 30 and stored[30] is None
        for row in _image(run)[:29]:
            got = dt.datetime.fromisoformat(str(stored[row["id"]]).replace("Z", "+00:00"))
            assert got == row["tstz"], (run, row["id"], stored[row["id"]])


def test_a_wrong_instant_on_the_destination_is_still_drift(route):
    """AC-2: a real difference fails and is evidenced by its key only."""
    from services.cdc_drift_repair import CdcDriftRequiresFencedRepair

    _source, _destination, _table, state = route
    wrong = _image(1)
    wrong[6] = {**wrong[6], "tstz": wrong[6]["tstz"] + dt.timedelta(seconds=1)}  # id 7
    state["events"][1] = wrong
    with pytest.raises(CdcDriftRequiresFencedRepair) as caught:
        _run(route, "datetimeoffset")
    evidence = caught.value.evidence
    assert int(evidence.get("drift_count") or len(evidence.get("keys") or [])) == 1, evidence
