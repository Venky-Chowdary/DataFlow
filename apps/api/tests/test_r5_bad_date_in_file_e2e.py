"""One bad date in a file must not block the whole transfer (round 5).

SFTP CSV, 5,000 rows, through MCP start_transfer -> /copilot/confirm -> job,
into real PostgreSQL / MySQL / SQL Server tables (existing, and the table a
first run created). The ``d`` column is ISO dates (or timestamps); a handful of
rows hold values that look like dates and are not:

* ``2024-02-30`` / ``0000-00-00`` / ``2024-13-01`` / ``... 25:61:00``: ISO-shaped,
  naming no real day or clock. The bulk-load census graded shape only, so the
  PostgreSQL COPY (and MySQL LOAD DATA) took them and aborted the whole load -
  in balanced mode and under a signed CAST_AND_CONTINUE contract alike;
* ``01/02/2024`` / ``03/04/2024`` in an ISO column: a second, day/month-ordered
  format valid under both DMY and MDY (a different day under each). It was
  read one way silently; it must be held out as ambiguous. ``31/12/2024`` /
  ``15/01/2024`` are valid under one order only: they load as that day;

The contract, as round 4 set it for numbers:

* strict, no contract: refused at start_transfer, naming the row and value,
  nothing written;
* balanced, or a signed contract: every good row lands exactly, exactly the bad
  rows are quarantined with a reason, never NULL or a shifted date in their place.

Blank cells (unquoted empty, quoted ``""``, whitespace) on the nullable DATE
column are SQL NULL by the shared blank-cell contract, on every engine.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests._r4_temporal_harness import (  # noqa: E402
    connector,
    mssql_exec,
    mysql_exec,
    pg_exec,
)

ROWS = 5000
CONTRACT = {
    "approved_by": "qa", "reason": "hold the bad dates out",
    "execution_policy": "CAST_AND_CONTINUE", "on_cast_failure": "quarantine",
}
# Beyond every plan / Validate sample.
BAD_DATES = {4800: "2024-02-30", 4850: "0000-00-00", 4900: "2024-13-01"}
BAD_STAMPS = {
    4800: "2024-02-30 10:00:00", 4850: "2024-02-10 25:61:00", 4900: "0000-00-00 00:00:00",
}
# Valid under DMY and under MDY, a different day under each: ambiguous.
MIXED = {4800: "01/02/2024", 4801: "03/04/2024"}
# Valid under one order only (31 / 15 cannot be a month): one reading.
RESOLVABLE = {4800: ("31/12/2024", dt.date(2024, 12, 31)), 4801: ("15/01/2024", dt.date(2024, 1, 15))}
# Blank cells: unquoted empty and quoted "" -> NULL (blank-cell rule). A
# whitespace cell is its own test: it is not COPY-safe, so it would send every
# run down the row path and hide the bulk-load abort.
BLANKS = {997: "", 1994: '""'}
PRIOR = [(900001, "prior a", dt.date(2001, 1, 1)), (900002, None, None)]


def _good(i: int, kind: str) -> dt.date | dt.datetime:
    day = dt.date(2010, 1, 1) + dt.timedelta(days=i)
    if kind == "date":
        return day
    # A wall clock: TIMESTAMP / DATETIME(6) / DATETIME2 carry no zone.
    return dt.datetime(day.year, day.month, day.day, 10, i % 60, i % 59)  # noqa: DTZ001


def _wire(value: dt.date | dt.datetime) -> str:
    return value.isoformat(sep=" ") if isinstance(value, dt.datetime) else value.isoformat()


def _name(i: int) -> str:
    return f"Zoë ✓ {i}"


def _csv(kind: str, bad: dict[int, str], blanks: dict[int, str] = BLANKS) -> str:
    lines = ["id,name,d"]
    for i in range(1, ROWS + 1):
        if i in bad:
            cell = bad[i]
        elif i in blanks:
            cell = blanks[i]
        else:
            cell = _wire(_good(i, kind))
        lines.append(f"{i},{_name(i)},{cell}")
    return "\n".join(lines) + "\n"


_DDL = {
    ("postgresql", "date"): 'CREATE TABLE public."{t}" (id INTEGER PRIMARY KEY, name TEXT, d DATE)',
    ("postgresql", "ts"): 'CREATE TABLE public."{t}" (id INTEGER PRIMARY KEY, name TEXT, d TIMESTAMP(6))',
    ("mysql", "date"): "CREATE TABLE `{t}` (id INT PRIMARY KEY, name TEXT, d DATE)",
    ("mysql", "ts"): "CREATE TABLE `{t}` (id INT PRIMARY KEY, name TEXT, d DATETIME(6))",
    ("sqlserver", "date"): "CREATE TABLE dbo.[{t}] (id INT PRIMARY KEY, name NVARCHAR(100), d DATE)",
    ("sqlserver", "ts"): "CREATE TABLE dbo.[{t}] (id INT PRIMARY KEY, name NVARCHAR(100), d DATETIME2(6))",
}
_EXEC = {"postgresql": pg_exec, "mysql": mysql_exec, "sqlserver": mssql_exec}
_QUOTE = {
    "postgresql": lambda t: f'public."{t}"',
    "mysql": lambda t: f"`{t}`",
    "sqlserver": lambda t: f"dbo.[{t}]",
}


def _sql(engine: str, sql: str):
    return _EXEC[engine](sql)


@pytest.fixture
def sftp(tmp_path):
    pytest.importorskip("paramiko")
    from services.connector_store import create_connector, delete_connector
    from tests.sftp_test_server import start_sftp_server

    root = tmp_path / "sftp"
    root.mkdir()
    server, runner = start_sftp_server(str(root))
    created: list[str] = []

    def source(csv_text: str) -> str:
        (root / "dates.csv").write_text(csv_text, encoding="utf-8")
        cfg = server.endpoint_config("/dates.csv")
        conn = create_connector({
            "name": f"sftp-{uuid.uuid4().hex[:8]}", "type": "sftp", "role": "source",
            "host": cfg["host"], "port": cfg["port"], "username": cfg["username"],
            "password": cfg["password"], "database": cfg["database"],
            "host_key": cfg["host_key"],
        })
        created.append(conn.id)
        return conn.id

    yield source
    runner.stop()
    for cid in created:
        delete_connector(cid)


@pytest.fixture(scope="module")
def s3_store():
    """S3 wire protocol from moto's HTTP server, through the product's boto3."""
    boto3 = pytest.importorskip("boto3")
    moto_server = pytest.importorskip("moto.server")
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    client = boto3.client(
        "s3", endpoint_url=f"http://{host}:{port}", aws_access_key_id="test",
        aws_secret_access_key="test", region_name="us-east-1",
    )
    bucket = "r5d-" + uuid.uuid4().hex[:8]
    client.create_bucket(Bucket=bucket)
    try:
        yield host, int(port), bucket, client
    finally:
        server.stop()


@pytest.fixture
def s3(s3_store):
    from services.connector_store import create_connector, delete_connector

    host, port, bucket, client = s3_store
    created: list[str] = []

    def source(csv_text: str) -> str:
        client.put_object(Bucket=bucket, Key="dates.csv", Body=csv_text.encode("utf-8"))
        conn = create_connector({
            "name": "s3-" + uuid.uuid4().hex[:8], "type": "s3", "role": "source",
            "host": host, "port": port, "database": bucket, "username": "test",
            "password": "test", "ssl": False, "extra": {"path_style": True},
            "workspace_id": "",
        })
        created.append(str(conn.id))
        return str(conn.id)

    yield source
    for cid in created:
        delete_connector(cid)


@pytest.fixture
def dest_table():
    made: list[tuple[str, str]] = []

    def make(engine: str, kind: str | None) -> str:
        table = f"r5d_{uuid.uuid4().hex[:8]}"
        # Reach the engine first, also when the job will create the table: the
        # harness skips an engine this host does not run (SQL Server in the
        # api-and-web CI job) instead of the job failing to reach it.
        _sql(engine, "SELECT 1")
        made.append((engine, table))
        if kind is not None:
            _sql(engine, _DDL[(engine, kind)].format(t=table))
            q = _QUOTE[engine](table)
            for pid, name, d in PRIOR:
                n = "NULL" if name is None else f"'{name}'"
                v = "NULL" if d is None else f"'{d.isoformat()}'"
                _sql(engine, f"INSERT INTO {q} (id, name, d) VALUES ({pid}, {n}, {v})")
        return table

    yield make
    for engine, table in made:
        for t in (table, f"{table}_df_quarantine"):
            with contextlib.suppress(Exception):  # best-effort cleanup
                _sql(engine, f"DROP TABLE IF EXISTS {_QUOTE[engine](t)}")


def _start(src: str, engine: str, table: str, *, mode: str):
    from src.ai.copilot.tools import get_pilot_tools

    args = {
        "source_connector_id": src, "source_table": "dates.csv",
        "dest_connector_id": connector(engine, "destination"), "dest_table": table,
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
        "validation_mode": "strict" if mode == "contract" else mode,
    }
    if mode == "contract":
        args["risk_acceptance"] = dict(CONTRACT)
    return get_pilot_tools().execute("start_transfer", args)


def _job(staged) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "r5-bad-date")["job_id"]
    deadline = time.time() + 420
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def _norm(value):
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):  # pyodbc DATE as text on some drivers
        return dt.date.fromisoformat(value)
    return value


def _rows(engine: str, table: str) -> dict[int, tuple]:
    got = _sql(engine, f"SELECT id, name, d FROM {_QUOTE[engine](table)} ORDER BY id")
    return {int(r[0]): (r[1], _norm(r[2])) for r in got}


def _quarantined_values(engine: str, table: str) -> dict[str, str]:
    got = _sql(engine, f"SELECT _df_value, _df_reason FROM {_QUOTE[engine](table + '_df_quarantine')}")
    return {str(v): str(r) for v, r in got}


def _assert_held_out_exactly(
    engine: str, table: str, kind: str, bad: dict[int, str], job: dict, blanks=BLANKS
):
    assert job.get("status") == "completed_with_quarantine", {
        k: job.get(k) for k in ("status", "error", "rejected_rows", "records_processed")
    }
    assert int(job.get("rejected_rows") or 0) == len(bad), job.get("rejected_rows")
    assert int(job.get("quarantine_row_count") or 0) == len(bad), job.get("quarantine_row_count")
    assert int(job.get("coerced_null_rows") or 0) == 0, job.get("coerced_null_rows")
    assert int(job.get("records_processed") or 0) == ROWS - len(bad), job.get("records_processed")

    rows = _rows(engine, table)
    # Overwrite: the prior rows are replaced; no bad row is written at all.
    assert sorted(rows) == [i for i in range(1, ROWS + 1) if i not in bad]
    for i, (name, d) in rows.items():
        assert name == _name(i), (i, name)
        expected = None if i in blanks else _good(i, kind)
        assert d == expected, (i, d, expected)

    held = _quarantined_values(engine, table)
    assert set(held) >= set(bad.values()), held
    for value in bad.values():
        assert held[value], (value, held[value])


# --------------------------------------------------- invalid calendar values


@pytest.mark.parametrize("mode", ["balanced", "contract"])
@pytest.mark.parametrize("kind", ["date", "ts"])
@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_invalid_calendar_values_beyond_every_sample_are_held_out(sftp, dest_table, engine, kind, mode):
    bad = BAD_DATES if kind == "date" else BAD_STAMPS
    table = dest_table(engine, kind)
    job = _job(_start(sftp(_csv(kind, bad)), engine, table, mode=mode))
    _assert_held_out_exactly(engine, table, kind, bad, job)
    for value, reason in _quarantined_values(engine, table).items():
        if value in bad.values():
            assert "not a real" in reason, (value, reason)


@pytest.mark.parametrize("mode", ["balanced", "contract"])
def test_s3_csv_into_postgresql_holds_out_invalid_dates(s3, dest_table, mode):
    """S3 CSV -> PostgreSQL has its own GET + COPY route; it must hold out the
    same cells the row path does instead of aborting the COPY."""
    table = dest_table("postgresql", "date")
    job = _job(_start(s3(_csv("date", BAD_DATES)), "postgresql", table, mode=mode))
    _assert_held_out_exactly("postgresql", table, "date", BAD_DATES, job)


@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_invalid_date_inside_the_sample_under_a_contract_is_held_out(sftp, dest_table, engine):
    bad = {2: "2024-02-30", **BAD_DATES}
    table = dest_table(engine, "date")
    job = _job(_start(sftp(_csv("date", bad)), engine, table, mode="contract"))
    _assert_held_out_exactly(engine, table, "date", bad, job)


@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_strict_without_a_contract_refuses_naming_the_row_and_value(sftp, dest_table, engine):
    table = dest_table(engine, "date")
    staged = _start(sftp(_csv("date", {4800: "2024-02-30"})), engine, table, mode="strict")
    assert not staged.success
    text = str(staged.error)
    assert "2024-02-30" in text and "4800" in text, text
    assert _rows(engine, table) == {pid: (name, d) for pid, name, d in PRIOR}


@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_second_run_into_the_table_the_first_run_created(sftp, dest_table, engine):
    table = dest_table(engine, None)
    src = sftp(_csv("date", BAD_DATES))
    first = _job(_start(src, engine, table, mode="contract"))
    _assert_held_out_exactly(engine, table, "date", BAD_DATES, first)
    second = _job(_start(src, engine, table, mode="contract"))
    _assert_held_out_exactly(engine, table, "date", BAD_DATES, second)


@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_whitespace_date_cell_is_the_same_blank_on_every_engine(sftp, dest_table, engine):
    """The blank-cell rule (``services.blank_cell_contract``) makes a whitespace
    cell on a nullable typed file column SQL NULL. SQL Server's writer never
    received the file route's flag, so the same cell failed the whole run under
    a signed contract there (and was quarantined in balanced) while PostgreSQL
    and MySQL stored NULL."""
    blanks = {**BLANKS, 2991: '"   "', 2992: "  "}
    table = dest_table(engine, "date")
    job = _job(_start(sftp(_csv("date", {4800: "2024-02-30"}, blanks)), engine, table, mode="contract"))
    _assert_held_out_exactly(engine, table, "date", {4800: "2024-02-30"}, job, blanks)


# ---------------------------------------------------- mixed date formats


@pytest.mark.parametrize("mode", ["balanced", "contract"])
@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_slash_dates_valid_under_both_orders_in_an_iso_column_are_ambiguous(
    sftp, dest_table, engine, mode
):
    table = dest_table(engine, "date")
    job = _job(_start(sftp(_csv("date", MIXED)), engine, table, mode=mode))
    _assert_held_out_exactly(engine, table, "date", MIXED, job)
    held = _quarantined_values(engine, table)
    for value in MIXED.values():
        assert "ambiguous" in held[value], (value, held[value])


@pytest.mark.parametrize("mode", ["balanced", "contract"])
@pytest.mark.parametrize("engine", ["postgresql", "mysql", "sqlserver"])
def test_slash_dates_valid_under_one_order_in_an_iso_column_load_as_that_day(
    sftp, dest_table, engine, mode
):
    """``31/12/2024`` cannot be MDY and ``15/01/2024`` cannot be MDY: nothing is
    ambiguous, so each loads as its single valid day - never held out, never
    NULL, never a shifted day."""
    table = dest_table(engine, "date")
    job = _job(_start(
        sftp(_csv("date", {i: text for i, (text, _day) in RESOLVABLE.items()})),
        engine, table, mode=mode,
    ))
    assert job.get("status") == "completed", {
        k: job.get(k) for k in ("status", "error", "rejected_rows", "records_processed")
    }
    assert int(job.get("rejected_rows") or 0) == 0, job.get("rejected_rows")
    assert int(job.get("coerced_null_rows") or 0) == 0, job.get("coerced_null_rows")
    assert int(job.get("records_processed") or 0) == ROWS, job.get("records_processed")
    rows = _rows(engine, table)
    assert sorted(rows) == list(range(1, ROWS + 1))
    for i, (name, d) in rows.items():
        assert name == _name(i), (i, name)
        expected = None if i in BLANKS else (RESOLVABLE[i][1] if i in RESOLVABLE else _good(i, "date"))
        assert d == expected, (i, d, expected)


def test_mixed_formats_inside_the_sample_under_a_contract_are_held_out(sftp, dest_table):
    bad = {2: "05/06/2024", **MIXED}
    table = dest_table("postgresql", "date")
    job = _job(_start(sftp(_csv("date", bad)), "postgresql", table, mode="contract"))
    _assert_held_out_exactly("postgresql", table, "date", bad, job)


def test_strict_refuses_a_mixed_format_column_naming_the_value(sftp, dest_table):
    table = dest_table("postgresql", "date")
    staged = _start(sftp(_csv("date", MIXED)), "postgresql", table, mode="strict")
    assert not staged.success
    text = str(staged.error)
    assert "01/02/2024" in text and "4800" in text and "ambiguous" in text, text
    assert _rows("postgresql", table) == {pid: (name, d) for pid, name, d in PRIOR}
