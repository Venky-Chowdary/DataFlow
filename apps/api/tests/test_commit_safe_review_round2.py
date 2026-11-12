"""PR 407 round-2 review — regressions of the commit-safe incremental window.

Each test reproduces a round-2 finding. Live cases need a reachable
PostgreSQL; they skip, never pass, when it is not there.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Fake(_FakeMongo):
        def update_job_fields(self, job_id, fields=None, **kw):
            self.jobs.setdefault(job_id, {}).update({**(fields or {}), **kw})
            return True

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(tmp_path))
    return fake


# --------------------------------------------------------------------------
# H-1 — a file source's date cursor is bounded on the value the watermark keeps
# --------------------------------------------------------------------------

_DATES = {
    "us": (["12/30/2025", "12/31/2025"], ["01/15/2026", "02/03/2026"]),
    "eu": (["30/12/2025", "31/12/2025"], ["15/01/2026", "03/02/2026"]),
}


def _write_source(path, fmt: str, rows: list[tuple[int, str, str]]) -> None:
    if fmt == "jsonl":
        import json

        path.write_text("".join(json.dumps({"id": i, "d": d, "v": v}) + "\n" for i, d, v in rows))
    else:
        path.write_text("id,d,v\n" + "".join(f"{i},{d},{v}\n" for i, d, v in rows))


def _two_file_runs(fake, tmp_path, *, dates: str, fmt: str, preflight: bool):
    first, second = _DATES[dates]
    dst = tmp_path / "dest.sqlite"
    src = tmp_path / f"orders.{fmt}"
    rows: list[tuple[int, str, str]] = []
    outcomes = []
    for run, new in enumerate((first, second), start=1):
        rows += [(len(rows) + 1 + i, d, f"r{run}") for i, d in enumerate(new)]
        _write_source(src, fmt, rows)
        request = TransferRequest(
            source=EndpointConfig(kind="file", format=fmt), source_filename=src.name,
            source_path=str(src), source_content=src.read_bytes(),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="orders"),
            mappings=[{"source": k, "target": k, "confidence": 1.0} for k in ("id", "d", "v")],
            sync_mode="incremental_append", validation_mode="strict", skip_preflight=not preflight,
            stream_contracts=[{"name": "orders", "sync_mode": "incremental_append", "primary_key": "id",
                               "cursor_field": "d", "selected": True,
                               "cursor_semantics": "insert_only"}],
        )
        job_id = "r2f" + uuid.uuid4().hex[:16]
        fake.update_job_status(job_id, "pending", transfer_request={})
        outcomes.append(UniversalTransferEngine().execute_tracked(request, job_id))
    with sqlite3.connect(dst) as conn:
        landed = conn.execute("SELECT id FROM orders ORDER BY id").fetchall()
    return outcomes, [r[0] for r in landed]


@pytest.mark.parametrize("path", ["copy", "row"])
@pytest.mark.parametrize("preflight", [False, True], ids=["no-preflight", "preflight"])
@pytest.mark.parametrize("dates", ["us", "eu"])
def test_csv_non_iso_date_cursor_second_run_writes_only_new_rows(
    isolated, tmp_path, monkeypatch, dates, preflight, path
):
    """Run 2 of a CSV schedule whose cursor is a US or EU date writes the two
    new rows only. The bound used to compare the raw '12/30/2025' with the
    stored ISO watermark, rank it as text above every date, and re-append
    every earlier row while the run reported success."""
    if path == "row":
        monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    (run1, run2), landed = _two_file_runs(isolated, tmp_path, dates=dates, fmt="csv", preflight=preflight)
    assert run1.success and run1.records_transferred == 2, run1.error
    assert run2.success, run2.error
    assert run2.records_transferred == 2, (run2.records_transferred, landed)
    assert landed == [1, 2, 3, 4]


@pytest.mark.parametrize("preflight", [False, True], ids=["no-preflight", "preflight"])
@pytest.mark.parametrize("dates", ["us", "eu"])
def test_jsonl_non_iso_date_cursor_second_run_writes_only_new_rows(isolated, tmp_path, dates, preflight):
    (run1, run2), landed = _two_file_runs(isolated, tmp_path, dates=dates, fmt="jsonl", preflight=preflight)
    assert run1.success and run1.records_transferred == 2, run1.error
    assert run2.success, run2.error
    assert run2.records_transferred == 2, (run2.records_transferred, landed)
    assert landed == [1, 2, 3, 4]


def test_file_cursor_bound_reads_cells_as_the_cursor_type():
    """The bound reads a file cell the way the write path does: by the column's
    declared or inferred type, under the file's date order."""
    from services.sync_cursor import cursor_value_coercer, records_after_watermark
    from services.transform_engine import reset_active_date_locale, set_active_date_locale

    records = [{"d": d} for d in ("12/30/2025", "12/31/2025", "01/15/2026")]
    token = set_active_date_locale("MDY")
    try:
        coerce = cursor_value_coercer("date")
        assert coerce is not None and coerce("01/15/2026") == "2026-01-15"
        delta, unbounded = records_after_watermark(records, "d", "2025-12-31", coerce=coerce)
    finally:
        reset_active_date_locale(token)
    assert unbounded == 0
    assert [r["d"] for r in delta] == ["01/15/2026"]
    # A text column keeps its own text; an unreadable date keeps its text (it
    # stays in the delta and the write path judges it).
    assert cursor_value_coercer("string") is None
    assert cursor_value_coercer("date")("not a date") == "not a date"


@pytest.mark.parametrize("dates", ["us", "eu"])
def test_file_to_file_export_non_iso_date_cursor_second_run_writes_only_new_rows(
    isolated, tmp_path, monkeypatch, dates
):
    """The buffered (batch) path bounds a file the same way: run 2 exports
    the two new rows, neither every row again nor none."""
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    first, second = _DATES[dates]
    src = tmp_path / "orders.csv"
    rows: list[tuple[int, str, str]] = []
    written = []
    for run, new in enumerate((first, second), start=1):
        rows += [(len(rows) + 1 + i, d, f"r{run}") for i, d in enumerate(new)]
        _write_source(src, "csv", rows)
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"), source_filename=src.name,
            source_path=str(src), source_content=src.read_bytes(),
            destination=EndpointConfig(kind="file_export", format="csv"),
            mappings=[{"source": k, "target": k, "confidence": 1.0} for k in ("id", "d", "v")],
            sync_mode="incremental_append", validation_mode="strict", skip_preflight=True,
            stream_contracts=[{"name": "orders", "sync_mode": "incremental_append", "primary_key": "id",
                               "cursor_field": "d", "selected": True,
                               "cursor_semantics": "insert_only"}],
        )
        job_id = "r2e" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        written.append(result.records_transferred)
    assert written == [2, 2]


def test_text_typed_calendar_cursor_on_a_file_route_is_refused_not_misread():
    """A cursor the route keeps as text cannot be read as a date: as text
    '01/15/2026' sorts before '12/31/2025', so a later row would be skipped.
    The bound refuses it, as a SQL text cursor of that shape is refused."""
    from services.sync_cursor import records_after_watermark

    with pytest.raises(ValueError, match="day/month or month/day"):
        records_after_watermark([{"d": "01/15/2026"}], "d", "12/31/2025", text_calendar_refusal=True)


@pytest.mark.parametrize("cells", ["date-cells", "us-text-cells"])
def test_excel_date_cursor_second_run_writes_only_new_rows(isolated, tmp_path, cells):
    """An Excel source: real date cells and US date text both bound run 2 to
    the two new rows."""
    from datetime import date

    import openpyxl

    first = [date(2025, 12, 30), date(2025, 12, 31)]
    second = [date(2026, 1, 15), date(2026, 2, 3)]
    dst = tmp_path / "dest.sqlite"
    src = tmp_path / "orders.xlsx"
    rows: list[tuple[int, object, str]] = []
    written = []
    for run, new in enumerate((first, second), start=1):
        rows += [(len(rows) + 1 + i, d, f"r{run}") for i, d in enumerate(new)]
        book = openpyxl.Workbook()
        sheet = book.active
        sheet.append(["id", "d", "v"])
        for i, d, v in rows:
            sheet.append([i, d if cells == "date-cells" else d.strftime("%m/%d/%Y"), v])
        book.save(src)
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="excel"), source_filename=src.name,
            source_path=str(src), source_content=src.read_bytes(),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="orders"),
            mappings=[{"source": k, "target": k, "confidence": 1.0} for k in ("id", "d", "v")],
            sync_mode="incremental_append", validation_mode="strict", skip_preflight=True,
            stream_contracts=[{"name": "orders", "sync_mode": "incremental_append", "primary_key": "id",
                               "cursor_field": "d", "selected": True,
                               "cursor_semantics": "insert_only"}],
        )
        job_id = "r2x" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        written.append(result.records_transferred)
    with sqlite3.connect(dst) as conn:
        landed = [r[0] for r in conn.execute("SELECT id FROM orders ORDER BY id")]
    assert written == [2, 2] and landed == [1, 2, 3, 4], (written, landed)


# --------------------------------------------------------------------------
# Live PostgreSQL helpers
# --------------------------------------------------------------------------

SCHEMA = "df_csw_round2"


def _connect(autocommit: bool = True):
    import psycopg2

    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    conn = psycopg2.connect(
        host=c["host"], port=c["port"], user=c["username"],
        password=c["password"], dbname=c["database"],
    )
    conn.autocommit = autocommit
    return conn


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


def _pg_cfg() -> dict:
    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    return {"host": c["host"], "port": int(c["port"]), "database": c["database"],
            "username": c["username"], "password": c["password"], "schema": SCHEMA}


@pytest.fixture()
def pg():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    _sql(f'CREATE SCHEMA "{SCHEMA}"')
    yield SCHEMA
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _run(fake, src: str, dst: str, *, cursor: str, mode: str = "incremental_append",
         pk: str = "", skip_preflight: bool = True, **extra):
    contract = {"name": src, "cursor_field": cursor, "sync_mode": mode, "selected": True, **extra}
    if pk:
        contract["primary_key"] = pk
    request = TransferRequest(
        source=_endpoint(src), destination=_endpoint(dst), sync_mode=mode,
        stream_contracts=[contract], skip_preflight=skip_preflight, validation_mode="strict",
    )
    job_id = "r2p" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _window(result) -> dict:
    return (result.destination_summary or {}).get("commit_safe_window") or {}


def _missing(src: str, dst: str) -> list:
    return _sql(
        f'SELECT s.id FROM "{SCHEMA}".{src} s LEFT JOIN "{SCHEMA}".{dst} d ON d.id = s.id '
        "WHERE d.id IS NULL ORDER BY 1",
        fetch=True,
    )


# --------------------------------------------------------------------------
# M-1 — the wall-clock text rule refuses only a mixed separator
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "values",
    [
        ["2026-01-01 10:00:00.13", "2026-01-01 10:00:01.3", "2026-01-01 10:00:00.013"],
        ["2026-01-01", "2026-01-01 10:00:00", "2026-01-02"],
        ["2026-01-01 10:00", "2026-01-01 10:00:30", "2026-01-01 10:01"],
        ["2026-01-01T10:00", "2026-01-01T10:00:30.5", "2026-01-02"],
    ],
    ids=["trimmed-fractions", "date-beside-date-time", "minutes-beside-seconds", "T-prefix-layouts"],
)
def test_wallclock_text_in_prefix_layouts_is_orderable(values):
    """Text order equals time order for these: a shorter layout is a prefix
    of the longer one and sorts first, and fraction digits compare left to
    right whatever their width."""
    from services.cursor_order import order_key, text_cursor_shape

    shape = text_cursor_shape(values)
    assert shape.orderable, shape
    assert sorted(values) == sorted(values, key=order_key)


@pytest.mark.parametrize(
    "values",
    [
        ["2026-01-01T10:00:00", "2026-01-01 11:00:00"],
        ["2026-01-01T10:00", "2026-01-01 10:00:30.5"],
    ],
)
def test_wallclock_text_with_mixed_separators_is_refused(values):
    from services.cursor_order import SHAPE_MIXED_LAYOUTS, text_cursor_refusal, text_cursor_shape

    shape = text_cursor_shape(values)
    assert shape.kind == SHAPE_MIXED_LAYOUTS, shape
    assert "'T' or a space" in text_cursor_refusal("c", shape)
    # SQLite reads both separators as one: the same values are orderable there.
    assert text_cursor_shape(values, separator_normalized=True).orderable


def test_trimmed_fraction_text_cursor_loads_on_live_postgresql(isolated, pg):
    """The reviewer's Q4: a text column filled from ``timestamp::text``
    (PostgreSQL trims trailing fraction zeros). No row is misordered as text,
    and all 400 rows load; the route used to be refused as "more than one
    layout"."""
    _sql(f'CREATE TABLE "{SCHEMA}".tf (id int PRIMARY KEY, ts timestamp NOT NULL, c text NOT NULL)')
    _sql(
        f"""INSERT INTO "{SCHEMA}".tf SELECT g, timestamp '2026-01-01 10:00:00' + g * interval '0.013 s', ''
        FROM generate_series(1, 400) g"""
    )
    _sql(f'UPDATE "{SCHEMA}".tf SET c = ts::text')
    misordered = _sql(
        f'SELECT count(*) FROM (SELECT row_number() OVER (ORDER BY c COLLATE "C") a, '
        f'row_number() OVER (ORDER BY ts) b FROM "{SCHEMA}".tf) q WHERE a <> b',
        fetch=True,
    )[0][0]
    assert misordered == 0
    result = _run(isolated, "tf", "tf_out", cursor="c")
    assert result.success, result.error
    assert result.records_transferred == 400
    assert _missing("tf", "tf_out") == []


def test_mixed_separator_text_cursor_is_refused_on_live_postgresql(isolated, pg):
    _sql(f'CREATE TABLE "{SCHEMA}".ms (id int PRIMARY KEY, c text NOT NULL)')
    _sql(
        f"""INSERT INTO "{SCHEMA}".ms VALUES (1, '2026-01-01T10:00:00'), (2, '2026-01-01 11:00:00'),
        (3, '2026-01-01T12:00:00')"""
    )
    result = _run(isolated, "ms", "ms_out", cursor="c")
    assert not result.success
    assert "'T' or a space" in str(result.error)


# --------------------------------------------------------------------------
# M-2 — a relation probe that cannot connect is "unreadable", not a crash
# --------------------------------------------------------------------------

_UNREACHABLE = {"host": "127.0.0.1", "port": 1, "database": "x", "username": "u", "password": "p"}


def _refuse_connect(*_a, **_kw):
    raise ConnectionError("probe connection refused")


def test_relation_facts_probe_that_cannot_connect_is_unreadable(monkeypatch):
    from services import commit_safe_window as csw
    from services.preflight_cursor_gate import source_commit_facts

    monkeypatch.setattr(csw, "_pg_connect", _refuse_connect)
    assert csw.pg_relation_facts(_UNREACHABLE, "public", "t") == ("", None)
    # Run-time path: the kind is "" (planned as "not confirmed a table"), never an IndexError.
    assert csw.pg_relation_kind(_UNREACHABLE, "public", "t") == ""
    # Preflight path: the facts are read, not raised.
    facts = source_commit_facts("postgresql", _UNREACHABLE, "t", "incremental_append")
    assert facts("id") == {"relation_kind": "", "relation_rows": None}
    claim = csw.static_commit_safety(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
        cursor_type="bigint", has_identity=True, **facts("id"),
    )
    assert claim["guarantee"] != csw.GUARANTEE_COMMIT_SAFE
    assert "could not be confirmed to be a table" in claim["statement"]


def test_run_whose_relation_probe_cannot_connect_takes_the_overlap_window(isolated, pg, monkeypatch):
    from services import commit_safe_window as csw
    from src.transfer import stream_commit_window as scw

    _sql(f'CREATE TABLE "{SCHEMA}".rk (id bigint PRIMARY KEY, v text)')
    _sql(f'INSERT INTO "{SCHEMA}".rk SELECT g, \'v\' FROM generate_series(1, 5) g')
    real_kind = scw.pg_relation_kind

    def kind_with_refused_connect(*args):
        # The real probe, with only its own connection refused.
        with monkeypatch.context() as inner:
            inner.setattr(csw, "_pg_connect", _refuse_connect)
            return real_kind(*args)

    monkeypatch.setattr(scw, "pg_relation_kind", kind_with_refused_connect)
    result = _run(isolated, "rk", "rk_out", cursor="id")
    assert result.success, result.error
    window = _window(result)
    assert window.get("strategy") == csw.STRATEGY_OVERLAP, window
    assert "kind could not be read" in str(window.get("probe_error")), window
    assert "IndexError" not in str(window.get("probe_error"))


def test_a_policy_gate_that_raises_is_an_honest_gate_not_a_preflight_error(monkeypatch):
    import services.preflight_transfer_policy as policy

    def _boom(*_a, **_kw):
        raise RuntimeError("relation probe exploded")

    monkeypatch.setattr(policy, "source_commit_facts", _boom)
    gates = policy.run_transfer_policy_gates(
        sync_mode="incremental_append", source_type="postgresql", source_kind="database",
        dest_type="postgresql", source_table="t",
        stream_contracts=[{"name": "t", "cursor_field": "id", "sync_mode": "incremental_append",
                           "primary_key": "id", "selected": True}],
        source_config={"type": "postgresql", "host": "db.example", "database": "x"},
    )
    errored = [g for g in gates if "could not be checked" in str(g.get("message"))]
    assert errored, [g.get("id") for g in gates]
    assert errored[0]["status"] == "warn"
    assert "RuntimeError" in str(errored[0].get("details"))
    # Every other gate still ran.
    assert any(g.get("id") == "g10_schema_policy" for g in gates)


# --------------------------------------------------------------------------
# M-3 — a large table's late-commit check is floored on the cursor's own unit,
# or stated "not guaranteed" where it cannot be
# --------------------------------------------------------------------------


@pytest.mark.parametrize("rows", [50_000_000, -1])
def test_large_table_date_cursor_is_claimed_within_a_day_look_back(rows):
    from services import commit_safe_window as csw

    claim = csw.static_commit_safety(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="d",
        cursor_type="date", has_identity=True, relation_kind="r", relation_rows=rows,
    )
    assert claim["guarantee"] == csw.GUARANTEE_WITHIN_WINDOW, claim
    assert "day" in claim["statement"], claim["statement"]


@pytest.mark.parametrize("cursor_type", ["text", "uuid", "varchar(40)"])
@pytest.mark.parametrize("rows", [50_000_000, -1])
def test_large_table_cursor_without_a_unit_is_not_claimed_covered(cursor_type, rows):
    from services import commit_safe_window as csw

    claim = csw.static_commit_safety(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="c",
        cursor_type=cursor_type, has_identity=True, relation_kind="r", relation_rows=rows,
    )
    assert claim["guarantee"] == csw.GUARANTEE_NONE, claim
    assert "the late-commit check would scan the whole table" in claim["statement"]


def test_date_cursor_probe_is_floored_by_days_on_a_large_table(pg):
    from services import commit_safe_window as csw

    _sql(f'CREATE TABLE "{SCHEMA}".bd (id bigint PRIMARY KEY, d date NOT NULL, v text)')
    _sql(f"INSERT INTO \"{SCHEMA}\".bd SELECT g, date '2025-01-01' + g, 'x' FROM generate_series(1, 50) g")
    _sql(f'ANALYZE "{SCHEMA}".bd')
    _sql(f"UPDATE pg_class SET reltuples = 50000000 WHERE oid = '\"{SCHEMA}\".bd'::regclass")
    xmin = _sql("SELECT txid_snapshot_xmin(txid_current_snapshot())", fetch=True)[0][0]
    late = csw.probe_pg_late_rows(
        _pg_cfg(), schema=SCHEMA, table="bd", cursor_column="d", since_xmin=str(xmin),
        below="2025-02-10", cursor_type="date",
    )
    assert late.floor == "2025-02-09", late


def test_text_cursor_on_a_large_table_run_says_not_guaranteed_like_preflight(isolated, pg):
    """50M estimated rows and a text cursor: the check cannot be limited, so
    preflight states "not guaranteed" and the run says the same (it does not
    run a whole-table scan and call the route covered)."""
    from services import commit_safe_window as csw
    from services.preflight_cursor_gate import source_commit_facts

    _sql(f'CREATE TABLE "{SCHEMA}".bt (id bigint PRIMARY KEY, c text NOT NULL)')
    _sql(f"INSERT INTO \"{SCHEMA}\".bt SELECT g, lpad(g::text, 6, '0') FROM generate_series(1, 20) g")
    first = _run(isolated, "bt", "bt_out", cursor="c")
    assert first.success, first.error
    _sql(f'ANALYZE "{SCHEMA}".bt')
    _sql(f"UPDATE pg_class SET reltuples = 50000000 WHERE oid = '\"{SCHEMA}\".bt'::regclass")
    facts = source_commit_facts("postgresql", _pg_cfg(), "bt", "incremental_append")("c")
    assert facts["relation_rows"] == 50_000_000
    claim = csw.static_commit_safety(
        source_type="postgresql", sync_mode="incremental_append", cursor_column="c",
        cursor_type="text", has_identity=True, **facts,
    )
    assert claim["guarantee"] == csw.GUARANTEE_NONE
    _sql(f"INSERT INTO \"{SCHEMA}\".bt VALUES (21, '000021')")
    second = _run(isolated, "bt", "bt_out", cursor="c")
    assert second.success, second.error
    window = _window(second)
    assert window.get("guarantee") == csw.GUARANTEE_NONE, window
    assert "the late-commit check would scan the whole table" in window.get("reason", ""), window
    assert "late_rows_detected" not in window, window
    assert _missing("bt", "bt_out") == []


# --------------------------------------------------------------------------
# M-4 — the snapshot narrowing's safety checks, each with a test that fails
# without it
# --------------------------------------------------------------------------


def test_presnapshot_subtransaction_row_is_recovered(isolated, pg):
    """The reviewer's Q6: a SAVEPOINT row whose subtransaction id was assigned
    before the capture, while its parent writes nothing itself. Only the
    parent's id is in the in-progress list, so the run must judge rows on the
    horizon (one subtransaction open), and recovers the row."""
    _sql(f'CREATE TABLE "{SCHEMA}".ps (id bigint PRIMARY KEY, v text)')
    _sql(f"INSERT INTO \"{SCHEMA}\".ps SELECT g, 'r' FROM generate_series(1, 10) g")
    _sql(f'ANALYZE "{SCHEMA}".ps')
    assert _run(isolated, "ps", "ps_out", cursor="id").success
    writer = _connect(autocommit=False)
    try:
        cur = writer.cursor()
        cur.execute("SAVEPOINT s")
        cur.execute(f"INSERT INTO \"{SCHEMA}\".ps VALUES (11, 'SUBXACT-row')")
        _sql(f"INSERT INTO \"{SCHEMA}\".ps SELECT g, 'fast' FROM generate_series(12, 30) g")
        second = _run(isolated, "ps", "ps_out", cursor="id")
        assert second.success, second.error
        bound = _window(second).get("bound") or {}
        assert bound.get("open_subxacts") == 1, bound
        writer.commit()
    finally:
        writer.close()
    _sql(f"INSERT INTO \"{SCHEMA}\".ps VALUES (40, 'n')")
    third = _run(isolated, "ps", "ps_out", cursor="id")
    assert third.success, third.error
    assert _window(third).get("late_row_rule") == "horizon", _window(third)
    assert _missing("ps", "ps_out") == []


def test_late_row_on_the_second_page_of_candidates_is_recovered(isolated, pg):
    """The reviewer's Q7: the late row sits behind 1,500 in-place edits with
    lower cursors, so it is on the second page the check walks."""
    _sql(f'CREATE TABLE "{SCHEMA}".sp (id bigint PRIMARY KEY, v text)')
    _sql(f"INSERT INTO \"{SCHEMA}\".sp SELECT g, 'r' FROM generate_series(1, 1500) g")
    _sql(f"INSERT INTO \"{SCHEMA}\".sp SELECT g, 'r' FROM generate_series(2000, 3000) g")
    _sql(f'ANALYZE "{SCHEMA}".sp')
    assert _run(isolated, "sp", "sp_out", cursor="id").success
    late = _connect(autocommit=False)
    try:
        late.cursor().execute(f"INSERT INTO \"{SCHEMA}\".sp VALUES (1700, 'LATE')")
        _sql(f"INSERT INTO \"{SCHEMA}\".sp SELECT g, 'fast' FROM generate_series(3001, 3100) g")
        assert _run(isolated, "sp", "sp_out", cursor="id").success
        late.commit()
    finally:
        late.close()
    _sql(f"UPDATE \"{SCHEMA}\".sp SET v = v || '!' WHERE id <= 1500")
    third = _run(isolated, "sp", "sp_out", cursor="id")
    assert third.success, third.error
    window = _window(third)
    assert window.get("late_rows_detected") == 1501, window
    assert window.get("late_rows_checked") == 1501, window
    assert window.get("replay_seed") == "1700", window
    assert _missing("sp", "sp_out") == []


class _FakeCursor:
    def __init__(self, answers):
        self.answers = list(answers)
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        self.statements.append(statement)
        answer = self.answers.pop(0) if self.answers else None
        if isinstance(answer, BaseException):
            raise answer
        self._row = answer

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.autocommit = True

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def test_a_truncated_in_progress_list_is_never_stored_as_complete(monkeypatch):
    from services import commit_safe_window as csw

    xip = [str(1000 + i) for i in range(csw.PG_XIP_MAX + 1)]
    cursor = _FakeCursor([("1000", "5000", xip)])
    monkeypatch.setattr(csw, "_pg_connect", lambda _cfg: _FakeConn(cursor))
    bound = csw.capture_pg_bound({})
    assert bound.get("xip_complete") is False and bound.get("xip_truncated") is True, bound
    assert "xip" not in bound
    assert csw.late_row_rule(bound) == ("horizon", 1000, ())


@pytest.mark.parametrize(
    "matched,why",
    [(1, "a listed transaction has no backend (prepared, or finished)"), (0, "none matched")],
)
def test_an_unmatched_or_prepared_backend_falls_back_to_the_horizon(monkeypatch, matched, why):
    """Two transactions in progress; only some are found running in a
    backend (a prepared transaction has none). Their open subtransactions
    cannot be counted, so the count is unknown and the horizon rule applies."""
    from services import commit_safe_window as csw

    cursor = _FakeCursor([("1000", "1003", ["1000", "1002"]), (matched, 0, False)])
    monkeypatch.setattr(csw, "_pg_connect", lambda _cfg: _FakeConn(cursor))
    bound = csw.capture_pg_bound({})
    assert bound.get("open_subxacts") is None, (why, bound)
    assert csw.late_row_rule(bound)[0] == "horizon"
    # Every listed transaction found, none with a subtransaction: narrowed.
    cursor = _FakeCursor([("1000", "1003", ["1000", "1002"]), (2, 0, False)])
    monkeypatch.setattr(csw, "_pg_connect", lambda _cfg: _FakeConn(cursor))
    complete = csw.capture_pg_bound({})
    assert complete.get("open_subxacts") == 0
    assert csw.late_row_rule(complete) == ("in_progress", 1003, (1000, 1002))


def test_a_missing_relation_is_unreadable_not_a_table(pg):
    from services import commit_safe_window as csw

    assert csw.pg_relation_facts(_pg_cfg(), SCHEMA, "no_such_table") == ("", None)


def test_nothing_to_re_read_only_when_every_reported_row_was_checked():
    """``undelivered == 0`` proves nothing about rows that were never checked:
    the run re-reads from the lowest reported cursor instead."""
    from services import commit_safe_window as csw

    def _plan(late):
        return csw.plan_commit_safe_window(
            source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
            cursor_type="bigint", stored_watermark="100", can_replay=True, contract={},
            metadata={csw.HORIZON_KEY: {"xmin": "10", "strategy": csw.STRATEGY_PG}},
            capture_bound=lambda: {"xmin": "20", "strategy": csw.STRATEGY_PG},
            probe_late=lambda **_kw: late, relation_kind="r",
        )

    partly = _plan(csw.LateRows(count=5, min_cursor="3", undelivered=0, checked=3, rule="horizon"))
    assert partly.replays and str(partly.seed) == "3", partly
    assert "3 of them" in partly.reason or "3 of 5" in partly.reason, partly.reason
    whole = _plan(csw.LateRows(count=5, min_cursor=None, undelivered=0, checked=5, rule="horizon"))
    assert not whole.replays and "nothing to re-read" in whole.reason, whole


# --------------------------------------------------------------------------
# PostgreSQL < 16 — no pg_stat_get_backend_subxact: the horizon rule, said
# --------------------------------------------------------------------------


def test_server_without_the_subtransaction_function_uses_the_horizon_and_says_so(
    isolated, pg, monkeypatch
):
    """Simulated PostgreSQL 15: the subtransaction function does not exist.
    The bound falls back to the horizon rule and the next run's evidence
    names why."""
    from services import commit_safe_window as csw

    monkeypatch.setattr(
        csw, "_PG_SUBXACT_SQL",
        csw._PG_SUBXACT_SQL.replace("pg_stat_get_backend_subxact(", "pg_stat_get_backend_subxact_absent("),
    )
    _sql(f'CREATE TABLE "{SCHEMA}".o16 (id bigint PRIMARY KEY, v text)')
    _sql(f'CREATE TABLE "{SCHEMA}".other (id int)')
    _sql(f"INSERT INTO \"{SCHEMA}\".o16 SELECT g, 'r' FROM generate_series(1, 5) g")
    writer = _connect(autocommit=False)
    try:
        writer.cursor().execute(f'INSERT INTO "{SCHEMA}".other VALUES (1)')  # holds an xid
        _sql(f'INSERT INTO "{SCHEMA}".other VALUES (2)')  # commits after it: it is listed
        bound = csw.capture_pg_bound(_pg_cfg())
        assert bound.get("xip") and bound.get("open_subxacts") is None, bound
        assert bound.get("horizon_reason") == csw.SUBXACT_FUNCTION_MISSING
        assert csw.late_row_rule(bound)[0] == "horizon"
        first = _run(isolated, "o16", "o16_out", cursor="id")
        assert first.success, first.error
    finally:
        writer.rollback()
        writer.close()
    _sql(f"INSERT INTO \"{SCHEMA}\".o16 VALUES (6, 'n')")
    second = _run(isolated, "o16", "o16_out", cursor="id")
    assert second.success, second.error
    window = _window(second)
    assert window.get("late_row_rule") == "horizon", window
    assert "PostgreSQL 16" in window.get("reason", ""), window
    assert window.get("horizon_reason") == csw.SUBXACT_FUNCTION_MISSING, window
    assert _missing("o16", "o16_out") == []


# --------------------------------------------------------------------------
# Lows
# --------------------------------------------------------------------------

# Low: engine type words


@pytest.mark.parametrize(
    "declared,unit",
    [
        ("TIMESTAMP_NTZ", "seconds"), ("TIMESTAMP_TZ(9)", "seconds"), ("TIMESTAMP_LTZ", "seconds"),
        ("HUGEINT", "values"), ("UBIGINT", "values"), ("UINTEGER", "values"),
        ("DateTime64(3)", "seconds"), ("DateTime64(6, 'UTC')", "seconds"), ("UInt64", "values"),
        ("Int64", "values"), ("Nullable(DateTime64(3))", "seconds"),
        ("pg_catalog.int8", "values"), ('"pg_catalog"."timestamptz"', "seconds"),
        ("pg_catalog.text", ""), ("LONGTEXT", ""),
    ],
)
def test_engine_type_words_get_their_distance(declared, unit):
    from services import commit_safe_window as csw

    assert csw.cursor_distance_unit(declared) == unit


def test_snowflake_timestamp_ntz_cursor_takes_the_overlap_window_it_is_promised():
    from services import commit_safe_window as csw

    claim = csw.static_commit_safety(
        source_type="snowflake", sync_mode="incremental_append", cursor_column="ts",
        cursor_type="TIMESTAMP_NTZ", has_identity=True,
    )
    assert claim["guarantee"] == csw.GUARANTEE_WITHIN_WINDOW, claim
    assert claim["overlap"] == {"unit": "seconds", "size": csw.DEFAULT_OVERLAP_SECONDS}
    assert csw.probe_floor_unit("Date32") == "days"
