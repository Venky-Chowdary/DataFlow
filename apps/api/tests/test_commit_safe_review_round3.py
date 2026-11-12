"""PR 407 round-3 review — regressions of the commit-safe incremental window.

Each test reproduces a round-3 finding. Live cases need a reachable
PostgreSQL; they skip, never pass, when it is not there.
"""

from __future__ import annotations

import os
import random
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_commit_safe_review_round2 import _connect, isolated  # noqa: E402,F401

SCHEMA = "df_csw_round3"


def _sql(statement: str, fetch: bool = False):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _endpoint(table: str) -> EndpointConfig:
    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    return EndpointConfig(
        kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
        database=c["database"], schema=SCHEMA, table=table,
        username=c["username"], password=c["password"],
    )


@pytest.fixture()
def pg():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    _sql(f'CREATE SCHEMA "{SCHEMA}"')
    yield SCHEMA
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _run(fake, src: str, dst: str, *, cursor: str, pk: str = ""):
    contract = {"name": src, "cursor_field": cursor, "sync_mode": "incremental_append", "selected": True}
    if pk:
        contract["primary_key"] = pk
    request = TransferRequest(
        source=_endpoint(src), destination=_endpoint(dst), sync_mode="incremental_append",
        stream_contracts=[contract], skip_preflight=True, validation_mode="strict",
    )
    job_id = "r3p" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _window(result) -> dict:
    return (result.destination_summary or {}).get("commit_safe_window") or {}


def _missing_and_dups(src: str, dst: str) -> tuple[int, int]:
    missing = _sql(
        f'SELECT count(*) FROM "{SCHEMA}".{src} s LEFT JOIN "{SCHEMA}".{dst} d ON d.id = s.id '
        "WHERE d.id IS NULL",
        fetch=True,
    )[0][0]
    dups = _sql(f'SELECT count(*) - count(DISTINCT id) FROM "{SCHEMA}".{dst}', fetch=True)[0][0]
    return int(missing), int(dups)


# --------------------------------------------------------------------------
# Finding 1 — the late-commit count and its page walk share one snapshot
# --------------------------------------------------------------------------


def test_busy_table_rereads_nothing_and_a_late_commit_is_still_recovered(isolated, pg):
    """The reviewer's busy-table probe: old rows behind the watermark are
    edited in place every millisecond or so by another connection. The count
    and the page walk used to run as separate READ COMMITTED statements, so the
    candidate set moved between them, "checked != count", and every run re-read
    the table from its oldest edited row. On one snapshot nothing is re-read,
    and a real late commit is still found and recovered."""
    _sql(f'CREATE TABLE "{SCHEMA}".bz (id bigint PRIMARY KEY, v text)')
    _sql(f"INSERT INTO \"{SCHEMA}\".bz SELECT g, 'r' FROM generate_series(1, 5000) g")
    _sql(f'ANALYZE "{SCHEMA}".bz')
    first = _run(isolated, "bz", "bz_out", cursor="id")
    assert first.success, first.error
    _sql(f"UPDATE \"{SCHEMA}\".bz SET v = v || '!' WHERE id <= 1500")

    stop = threading.Event()
    edits = [0]

    def churn():
        conn = _connect()
        try:
            while not stop.is_set():
                with conn.cursor() as cur:
                    cur.execute(
                        f'UPDATE "{SCHEMA}".bz SET v = v || \'.\' WHERE id = %s',
                        (random.randint(1, 4900),),
                    )
                edits[0] += 1
                time.sleep(0.001)
        finally:
            conn.close()

    worker = threading.Thread(target=churn, daemon=True)
    worker.start()
    try:
        for i in range(3):
            lo = 5001 + i * 10
            _sql(f"INSERT INTO \"{SCHEMA}\".bz SELECT g, 'new' FROM generate_series({lo}, {lo + 9}) g")
            busy = _run(isolated, "bz", "bz_out", cursor="id")
            assert busy.success, busy.error
            window = _window(busy)
            assert window.get("late_rows_detected"), window
            assert window.get("late_rows_checked") == window.get("late_rows_detected"), window
            assert not window.get("replay_seed"), window
            assert not window.get("rows_replayed"), window
            assert busy.records_transferred == 10, (busy.records_transferred, window)
        late = _connect(autocommit=False)
        try:
            late.cursor().execute(f"INSERT INTO \"{SCHEMA}\".bz VALUES (5035, 'LATE')")
            _sql(f"INSERT INTO \"{SCHEMA}\".bz SELECT g, 'fast' FROM generate_series(5040, 5049) g")
            assert _run(isolated, "bz", "bz_out", cursor="id").success
            late.commit()
        finally:
            late.close()
        recovering = _run(isolated, "bz", "bz_out", cursor="id")
        assert recovering.success, recovering.error
        window = _window(recovering)
        assert window.get("replay_seed") == "5035", window
        assert window.get("rows_recovered") == 1, window
    finally:
        stop.set()
        worker.join()
    assert edits[0] > 0
    assert _missing_and_dups("bz", "bz_out") == (0, 0)


def test_rows_only_partly_checked_are_never_said_to_be_more_than_were_found():
    """The evidence never says more rows "could be checked" than were found."""
    from services import commit_safe_window as csw

    def _plan(late):
        return csw.plan_commit_safe_window(
            source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
            cursor_type="bigint", stored_watermark="100", can_replay=True, contract={},
            metadata={csw.HORIZON_KEY: {"xmin": "10", "strategy": csw.STRATEGY_PG}},
            capture_bound=lambda: {"xmin": "20", "strategy": csw.STRATEGY_PG},
            probe_late=lambda **_kw: late, relation_kind="r",
        )

    over = _plan(csw.LateRows(count=5, min_cursor="3", undelivered=0, checked=7, rule="horizon"))
    assert over.replays and str(over.seed) == "3", over
    assert "7 of them" not in over.reason, over.reason
    assert "only" not in over.reason, over.reason
    partly = _plan(csw.LateRows(count=5, min_cursor="3", undelivered=0, checked=3, rule="horizon"))
    assert "only 3 of them" in partly.reason, partly.reason


# --------------------------------------------------------------------------
# Finding 2 — a date cell the file's date order cannot read is refused, never
# compared as text (text ranks above every date and became the watermark)
# --------------------------------------------------------------------------


def _file_run(fake, tmp_path, fmt: str, cells: list[str]):
    import json
    import sqlite3

    from services import sync_cursor as sc

    src = tmp_path / f"orders.{fmt}"
    if fmt == "jsonl":
        src.write_text("".join(json.dumps({"id": i, "d": d, "v": "r"}) + "\n" for i, d in enumerate(cells, 1)))
    else:
        src.write_text("id,d,v\n" + "".join(f"{i},{d},r\n" for i, d in enumerate(cells, 1)))
    dst = tmp_path / "dest.sqlite"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format=fmt), source_filename=src.name,
        source_path=str(src), source_content=src.read_bytes(),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="orders"),
        mappings=[{"source": k, "target": k, "confidence": 1.0} for k in ("id", "d", "v")],
        sync_mode="incremental_append", validation_mode="strict", skip_preflight=True,
        stream_contracts=[{"name": "orders", "sync_mode": "incremental_append", "primary_key": "id",
                           "cursor_field": "d", "selected": True, "cursor_semantics": "insert_only"}],
    )
    job_id = "r3f" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    landed: list = []
    if dst.exists():
        with sqlite3.connect(dst) as conn:
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'orders'").fetchone():
                landed = [r[0] for r in conn.execute("SELECT id FROM orders ORDER BY id")]
    stored = sc.STORE_PATH.read_text() if sc.STORE_PATH.exists() else ""
    return result, landed, stored


@pytest.mark.parametrize(
    "fmt,cells,placeholder",
    [
        ("csv-copy", ["2025-12-30", "2025-12-31", "2025-12-29", "2025-12-28", "N/A"], "N/A"),
        ("csv-row", ["2025-12-30", "2025-12-31", "2025-12-29", "2025-12-28", "N/A"], "N/A"),
        ("jsonl", ["2025-12-30T10:00:00Z", "2025-12-31T10:00:00Z", "2025-12-29T10:00:00Z",
                   "2025-12-28T10:00:00Z", "N/A"], "N/A"),
    ],
)
def test_an_unparsed_date_cell_is_refused_before_any_write(
    isolated, tmp_path, monkeypatch, fmt, cells, placeholder
):
    """An ISO-dated file with one 'N/A' cell used to store 'N/A' as the
    watermark (text ranks above every date), and every later run wrote 0 rows
    with success. The run is refused before any write, naming the column and
    how many cells did not parse — never their values."""
    if fmt == "csv-row":
        monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    result, landed, stored = _file_run(isolated, tmp_path, fmt.split("-")[0], cells)
    assert not result.success
    error = str(result.error or "")
    assert "'d'" in error and "1 cell" in error, error
    assert "date order" in error, error
    assert placeholder not in error, error
    assert landed == []
    assert placeholder not in stored and "watermark" not in stored, stored


def test_an_ambiguous_declared_date_under_auto_order_is_refused_not_kept_as_text():
    """A date column under the Auto date order: '04/03/2026' is April 3rd or
    March 4th, so it does not parse. It used to be compared as text and kept as
    the high mark, above every later ISO date."""
    from services.batch_incremental import BatchIncrementalBound
    from services.sync_cursor import (
        IncrementalReadScope,
        cursor_value_coercer,
        records_after_watermark,
    )

    records = [{"id": "3", "d": "03/03/2026"}, {"id": "4", "d": "04/03/2026"}]
    coerce = cursor_value_coercer("date")
    with pytest.raises(ValueError) as raised:
        records_after_watermark(records, "d", "2026-03-01\x1f2", primary_key="id", coerce=coerce)
    message = str(raised.value)
    assert "'d'" in message and "1 cell" in message and "date order" in message, message
    assert "04/03/2026" not in message, message
    bound = BatchIncrementalBound(
        scope=IncrementalReadScope(
            cursor_column="d", cursor_key="k", watermark=None, primary_key="id",
        ),
        reads_cells_by_type=True,
    )
    with pytest.raises(ValueError, match="date order"):
        bound.bound(records, {"d": "date"})
    assert bound.high_mark is None


# --------------------------------------------------------------------------
# Finding 3 — a never-analysed table is sized by its pages before it is
# called large
# --------------------------------------------------------------------------


def test_small_unanalysed_table_with_a_text_cursor_still_gets_the_late_commit_check(isolated, pg):
    """A 50-row table with autovacuum off is never analysed (reltuples = -1).
    Its size used to be "unknown, so large", and a text cursor (no unit to
    floor the probe by) skipped the late-commit check: a late row was lost.
    Sized by its pages it is small, so the check runs and recovers the row."""
    from services import commit_safe_window as csw

    _sql(f'CREATE TABLE "{SCHEMA}".ua (id int PRIMARY KEY, c text NOT NULL, v text) '
         "WITH (autovacuum_enabled = false)")
    _sql(f"INSERT INTO \"{SCHEMA}\".ua SELECT g, lpad(g::text, 6, '0'), 'r' FROM generate_series(1, 50) g")
    assert _sql(f"SELECT reltuples FROM pg_class WHERE oid = '\"{SCHEMA}\".ua'::regclass", fetch=True)[0][0] < 0
    from tests.test_commit_safe_review_round2 import _pg_cfg

    kind, rows = csw.pg_relation_facts({**_pg_cfg(), "schema": SCHEMA}, SCHEMA, "ua")
    assert kind == "r" and rows is not None and 0 <= rows <= csw.PG_UNBOUNDED_PROBE_ROWS, rows
    assert _run(isolated, "ua", "ua_out", cursor="c", pk="id").success
    late = _connect(autocommit=False)
    try:
        late.cursor().execute(f"INSERT INTO \"{SCHEMA}\".ua VALUES (55, '000055', 'LATE')")
        _sql(f"INSERT INTO \"{SCHEMA}\".ua SELECT g, lpad(g::text, 6, '0'), 'fast' FROM generate_series(60, 62) g")
        second = _run(isolated, "ua", "ua_out", cursor="c", pk="id")
        assert second.success, second.error
        late.commit()
    finally:
        late.close()
    third = _run(isolated, "ua", "ua_out", cursor="c", pk="id")
    assert third.success, third.error
    window = _window(third)
    assert window.get("guarantee") != csw.GUARANTEE_NONE, window
    assert window.get("replay_seed") == "000055", window
    assert _sql(
        f'SELECT s.id FROM "{SCHEMA}".ua s LEFT JOIN "{SCHEMA}".ua_out d ON d.id = s.id WHERE d.id IS NULL',
        fetch=True,
    ) == []


# --------------------------------------------------------------------------
# Finding 4 — the calendar-text refusal names the remedy that works first
# --------------------------------------------------------------------------


def test_calendar_text_refusal_names_the_date_order_remedy_first():
    """For day/month or month/day dates the working remedy is the file's date
    order (date_locale=DMY loads them); the refusal says that first."""
    from services.sync_cursor import records_after_watermark

    with pytest.raises(ValueError) as raised:
        records_after_watermark([{"d": "01/15/2026"}], "d", "12/31/2025", text_calendar_refusal=True)
    message = str(raised.value)
    assert "date_locale=DMY" in message, message
    remedy = message.index("date order")
    assert all(remedy < message.index(other) for other in ("ISO-8601", "full refresh")), message
    assert "Declare the column" not in message[:remedy], message
