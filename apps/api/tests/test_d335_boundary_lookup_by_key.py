"""D335 - incremental_append must not re-append the watermark row, on any engine.

QA r62-r75 (build 70cb4669c): PG -> Oracle appended the watermark row again on
every run (key 30 on run 2, key 31 on run 3), with or without ADD COLUMN; PG ->
SQL Server doubled key 31 on run 3; both under a GREEN "Append delta verified".
The train's test_qa_nov21_defects::test_d335_* only calls the unchanged
``_boundary_append_dedupe`` helper.

Root cause: the boundary dedupe asked the destination for its tie group with
``cursor IN (:watermark_text)``. Whether that text equals a stored value is the
destination's conversion: Oracle parses it with NLS_TIMESTAMP_FORMAT
``YYYY-MM-DD HH24:MI:SS.FF9`` and the ISO ``T`` is ORA-01861; SQL Server
converts it to the carrier (a DATETIME rounds to 1/300 s); a TEXT cursor
matches one spelling. A miss or an error fails open - the row is appended
again - and the append verdict only counts rows.

Local reproductions (real engine, ``execute_tracked``):
* SQLite TEXT cursor stored with a space: the same text-equality miss, no
  fakes at all;
* PostgreSQL destination whose cursor lookup raises ORA-01861 on the ISO
  ``T`` text - a fake of the Oracle conversion only, the rest is real;
* value shapes of Oracle/SQL Server read-backs (DATETIME 1/300 s rounding,
  DATETIME2(7) text, naive DATE) against ``same_cursor``.
SQL Server and Oracle themselves: ``test_d335_boundary_live_oracle.py``
(oracle-live) and ``test_d335_boundary_sqlserver_live.py`` (cdc-sqlserver).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d335k" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _contract(name: str) -> list[dict]:
    return [
        {
            "name": name,
            "cursor_field": "updated_at",
            "cursor_semantics": "modification_timestamp",
            "primary_key": ["id"],
            "sync_mode": "incremental_append",
            "selected": True,
        }
    ]


# --- SQLite: the text-equality miss, no fakes --------------------------------


def _ts_space(i: int) -> str:
    return f"2026-11-24 10:{i:02d}:00.{i * 1111:06d}"


def _sqlite_request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=_contract("t"),
        schema_policy="propagate_columns",
        validation_mode="strict",
    )


def _sqlite_ids(dst: Path) -> list[int]:
    with sqlite3.connect(dst) as conn:
        return [r[0] for r in conn.execute("SELECT id FROM t ORDER BY id")]


@pytest.mark.parametrize("dest_has_key", [True, False], ids=["dest-pk", "dest-no-pk"])
def test_sqlite_text_cursor_boundary_row_is_not_appended_again(tmp_path, isolated, dest_has_key):
    """Run 2 with NO schema change, run 3 after ADD COLUMN: each appends only
    the new ids. On 70cb run 2 re-appended id 30 (dest-pk: refused by the
    key, the run failed; dest-no-pk: the duplicate landed)."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    if not dest_has_key:
        with sqlite3.connect(dst) as pre:
            pre.execute("CREATE TABLE t (id INTEGER, note TEXT, updated_at TEXT)")
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, updated_at TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?)",
        [(i, None if i == 30 else f"n{i}", _ts_space(i)) for i in range(1, 31)],
    )
    conn.commit()
    first = _run(isolated, _sqlite_request(src, dst))
    assert first.success, first.error

    conn.execute("INSERT INTO t VALUES (31, 'n31', ?)", (_ts_space(31),))
    conn.commit()
    second = _run(isolated, _sqlite_request(src, dst))
    assert _sqlite_ids(dst) == list(range(1, 32)), "run 2 re-appended the watermark row"
    assert second.success, second.error

    conn.execute("ALTER TABLE t ADD COLUMN tier INTEGER DEFAULT 1")
    conn.executemany(
        "INSERT INTO t (id, note, updated_at, tier) VALUES (?,?,?,1)",
        [(i, f"n{i}", _ts_space(i)) for i in (32, 33)],
    )
    conn.commit()
    conn.close()
    third = _run(isolated, _sqlite_request(src, dst))
    assert _sqlite_ids(dst) == list(range(1, 34)), "run 3 re-appended the watermark row"
    assert third.success, third.error


# --- PostgreSQL destination with Oracle's cursor-text conversion (fake) ------


def _pg_exec(sql: str, params=None) -> None:
    from tests import r4_route_harness as h

    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        if not conn.autocommit:
            conn.commit()
    finally:
        conn.close()


def _pg_ids(table: str) -> list[int]:
    from tests import r4_route_harness as h

    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT id FROM public."{table}" ORDER BY id')  # nosec B608
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


@pytest.fixture()
def oracle_cursor_text(monkeypatch):
    """The destination converts a text cursor bind the way Oracle does.

    ``NLS_TIMESTAMP_FORMAT`` is ``YYYY-MM-DD HH24:MI:SS.FF9``: an ISO ``T`` in
    the bound text is ORA-01861. Only the *conversion* is faked; the table,
    the write and every other read are the real PostgreSQL destination.
    """
    import services.target_sample as ts

    original = ts._read_destination_sample
    calls = {"cursor_text": 0}

    def _oracle_like(db_type, dest, *, schema, table_name, columns=None, limit=50,
                     sort_key=None, key_values=None):
        if sort_key == "updated_at" and any("T" in str(k) for k in key_values or []):
            calls["cursor_text"] += 1
            raise RuntimeError(
                "(oracledb.exceptions.DatabaseError) ORA-01861: literal does not "
                "match format string"
            )
        return original(db_type, dest, schema=schema, table_name=table_name,
                        columns=columns, limit=limit, sort_key=sort_key,
                        key_values=key_values)

    monkeypatch.setattr(ts, "_read_destination_sample", _oracle_like)
    return calls


def _pg_request(src: str, dst: str) -> TransferRequest:
    from tests import typed_fidelity_helpers as tf

    return TransferRequest(
        source=tf.pg_endpoint(src),
        destination=tf.pg_endpoint(dst),
        sync_mode="incremental_append",
        stream_contracts=_contract(src),
        schema_policy="propagate_columns",
        validation_mode="strict",
    )


@pytest.mark.parametrize("add_column", [False, True], ids=["no-schema-change", "add-column"])
def test_oracle_cursor_conversion_does_not_reappend_or_go_green(isolated, oracle_cursor_text, add_column):
    """QA r66-r75 PG -> Oracle: the watermark row appended again on every run,
    and the job green. The destination here has no unique key (an append
    table), so a re-appended row lands, as on Oracle."""
    src, dst = f"d335s_{uuid.uuid4().hex[:8]}", f"d335d_{uuid.uuid4().hex[:8]}"
    try:
        _pg_exec(f'CREATE TABLE public."{src}" (id INT PRIMARY KEY, note TEXT, updated_at TIMESTAMP(6) NOT NULL)')
        _pg_exec(f'CREATE TABLE public."{dst}" (id BIGINT, note TEXT, updated_at TIMESTAMP(6))')
        for i in range(1, 31):
            _pg_exec(f'INSERT INTO public."{src}" VALUES (%s, %s, %s)',
                     (i, None if i == 30 else f"n{i}", _ts_space(i)))
        assert _run(isolated, _pg_request(src, dst)).success
        _pg_exec(f'INSERT INTO public."{src}" VALUES (31, %s, %s)', ("n31", _ts_space(31)))
        second = _run(isolated, _pg_request(src, dst))
        assert _pg_ids(dst) == list(range(1, 32)), "run 2 re-appended the watermark row"
        assert second.success, second.error
        if add_column:
            _pg_exec(f'ALTER TABLE public."{src}" ADD COLUMN tier INT DEFAULT 1')
        _pg_exec(f'INSERT INTO public."{src}" (id, note, updated_at) VALUES (32, %s, %s)',
                 ("n32", _ts_space(32)))
        third = _run(isolated, _pg_request(src, dst))
        assert _pg_ids(dst) == list(range(1, 33)), "run 3 re-appended the watermark row"
        assert third.success, third.error
    finally:
        from tests import r4_route_harness as h

        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_a_reappended_watermark_row_fails_loudly(isolated, oracle_cursor_text, monkeypatch):
    """Whatever the dedupe decides, a second copy of the boundary key is a
    failed run that names the key - never "Append delta verified". The
    destination converts cursor text as Oracle does (fake), so the window
    read-back keyed on the cursor cannot see the copy either."""
    import src.transfer.stream as stream_mod
    import src.transfer.stream_db_page as page

    src, dst = f"d335s_{uuid.uuid4().hex[:8]}", f"d335d_{uuid.uuid4().hex[:8]}"
    try:
        _pg_exec(f'CREATE TABLE public."{src}" (id INT PRIMARY KEY, note TEXT, updated_at TIMESTAMP(6) NOT NULL)')
        _pg_exec(f'CREATE TABLE public."{dst}" (id BIGINT, note TEXT, updated_at TIMESTAMP(6))')
        for i in range(1, 31):
            _pg_exec(f'INSERT INTO public."{src}" VALUES (%s, %s, %s)', (i, f"n{i}", _ts_space(i)))
        assert _run(isolated, _pg_request(src, dst)).success
        # Break the dedupe (what 70cb did on Oracle): every peer is kept.
        monkeypatch.setattr(page, "_boundary_rows_already_landed", lambda *a, **k: set())
        monkeypatch.setattr(stream_mod, "_boundary_rows_already_landed", lambda *a, **k: set())
        _pg_exec(f'INSERT INTO public."{src}" VALUES (31, %s, %s)', ("n31", _ts_space(31)))
        second = _run(isolated, _pg_request(src, dst))
        assert _pg_ids(dst).count(30) == 2  # the duplicate the run produced
        assert not second.success, "a doubled watermark row went green"
        assert "Append delta verified" not in str(second.error or "")
        assert "id=30" in str(second.error or ""), second.error
    finally:
        from tests import r4_route_harness as h

        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


# --- value shapes the Oracle / SQL Server read-backs hand back ---------------


@pytest.mark.parametrize(
    ("src", "dest", "ddl", "dialect", "same"),
    [
        # SQL Server DATETIME rounds to 1/300 s: .827167 is stored as .827.
        ("2026-11-24T10:31:00.827167", datetime(2026, 11, 24, 10, 31, 0, 827000), "DATETIME", "sqlserver", True),
        # ...but a different row 10 ms away is a different value.
        ("2026-11-24T10:31:00.827167", datetime(2026, 11, 24, 10, 31, 0, 837000), "DATETIME", "sqlserver", False),
        # DATETIME2(7) read as SQL Server's own 27-character text.
        ("2026-11-24T10:31:00.827167", "2026-11-24 10:31:00.8271670", "DATETIME2(7)", "sqlserver", True),
        # DATETIME2(6) through pymssql: an exact datetime.
        ("2026-11-24T10:31:00.827167", datetime(2026, 11, 24, 10, 31, 0, 827167), "DATETIME2(6)", "sqlserver", True),
        # A microsecond apart on an exact carrier is not the same row.
        ("2026-11-24T10:31:00.827167", datetime(2026, 11, 24, 10, 31, 0, 827168), "DATETIME2(6)", "sqlserver", False),
        # Oracle TIMESTAMP(6) and DATE (whole seconds).
        ("2026-11-24T10:31:00.827167", datetime(2026, 11, 24, 10, 31, 0, 827167), "TIMESTAMP(6)", "oracle", True),
        ("2026-11-24T10:31:00.400000", datetime(2026, 11, 24, 10, 31, 0), "DATE", "oracle", True),
        # A 1/300 s tick: .005 is stored (and read back) as .007.
        ("2026-11-24T10:31:00.005", datetime(2026, 11, 24, 10, 31, 0, 7000), "DATETIME", "sqlserver", True),
        # Bare DATETIME2 is DATETIME2(7): an eighth digit rounds into the seventh.
        ("2026-11-24T10:31:00.12345678", "2026-11-24 10:31:00.1234568", "DATETIME2", "sqlserver", True),
        # PostgreSQL bare timestamp keeps microseconds and rounds a seventh digit.
        ("2026-11-24 10:10:00.9999996", datetime(2026, 11, 24, 10, 10, 1), "timestamp", "postgresql", True),
        ("2026-11-24 10:10:00.9999996", datetime(2026, 11, 24, 10, 10, 0, 999999), "timestamp", "postgresql", False),
        ("2026-11-24 10:10:00.9999994", datetime(2026, 11, 24, 10, 10, 0, 999999), "timestamp", "postgresql", True),
        # MySQL DATETIME / DATETIME(0) keeps whole seconds and rounds.
        ("2026-11-24 10:10:00.654321", datetime(2026, 11, 24, 10, 10, 1), "DATETIME", "mysql", True),
        ("2026-11-24 10:10:00.654321", datetime(2026, 11, 24, 10, 10, 0), "DATETIME", "mysql", False),
        ("2026-11-24 10:10:00.4", datetime(2026, 11, 24, 10, 10, 0), "DATETIME(0)", "mysql", True),
        # Oracle TIMESTAMP WITH TIME ZONE read back as an aware instant.
        ("2026-11-24T10:31:00.827167+00:00", "2026-11-24T16:01:00.827167+05:30", "TIMESTAMP(6) WITH TIME ZONE", "oracle", True),
    ],
)
def test_same_cursor_on_destination_value_shapes(src, dest, ddl, dialect, same):
    from connectors.oracle_tstz import parse_oracle_tstz_text
    from src.transfer.stream_boundary_keyed import same_cursor

    if isinstance(dest, str) and "+" in dest:
        dest = parse_oracle_tstz_text(dest)
    assert same_cursor(src, dest, ddl=ddl, dialect=dialect) is same


# --- Oracle's cursor-keyed sample read converts the text explicitly (fake) ---


class _CatalogConn:
    def __init__(self, data_type):
        self.data_type = data_type

    def execute(self, stmt, params=None):
        data_type = self.data_type

        class _R:
            def first(self_inner):  # noqa: N805
                return (data_type,)

        return _R()


@pytest.mark.parametrize(
    ("data_type", "sql", "text_in", "text_out"),
    [
        ("TIMESTAMP(6)", "TO_TIMESTAMP(:k0, 'YYYY-MM-DD HH24:MI:SS.FF9')",
         "2026-11-24T10:30:00.703710", "2026-11-24 10:30:00.703710"),
        ("DATE", "TO_TIMESTAMP(:k0, 'YYYY-MM-DD HH24:MI:SS.FF9')",
         "2026-11-24T10:30:00", "2026-11-24 10:30:00.0"),
        ("TIMESTAMP(6) WITH TIME ZONE", "TO_TIMESTAMP_TZ(:k0, 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')",
         "2026-11-24T10:30:00.7+05:30", "2026-11-24 10:30:00.7 +05:30"),
    ],
)
def test_oracle_temporal_key_bind_parses_the_iso_watermark(data_type, sql, text_in, text_out):
    """ORA-01861 was the ISO ``T`` against NLS_TIMESTAMP_FORMAT: the cursor
    key is now converted with an explicit format, in a spelling it parses."""
    from services.reconciliation_oracle import oracle_temporal_key_sql, oracle_temporal_key_text

    expr = oracle_temporal_key_sql(_CatalogConn(data_type), "T", "APP", "UPDATED_AT")
    assert expr.format(bind=":k0") == sql
    assert oracle_temporal_key_text(text_in, zoned="_TZ" in expr) == text_out
    assert oracle_temporal_key_sql(_CatalogConn("NUMBER"), "T", "APP", "ID") == ""

