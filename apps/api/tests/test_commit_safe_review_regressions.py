"""PR 407 review — regressions of the commit-safe incremental window, on live engines.

Each test reproduces a review finding that lost rows silently or claimed more
than the run did. The live cases need a reachable PostgreSQL (and MySQL where
named); they skip, never pass, when it is not there.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services import commit_safe_window as csw  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

SCHEMA = "df_csw_review"


def _connect(autocommit: bool = True):
    import psycopg2

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
    c = pg_creds()
    return EndpointConfig(
        kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
        database=c["database"], schema=SCHEMA, table=table,
        username=c["username"], password=c["password"],
    )


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
    return fake


@pytest.fixture()
def pg():
    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    _sql(f'CREATE SCHEMA "{SCHEMA}"')
    yield SCHEMA
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _run(fake, src: str, dst: str, *, cursor: str, mode: str, pk: str = "",
         skip_preflight: bool = True, source: EndpointConfig | None = None, **extra):
    contract = {"name": src, "cursor_field": cursor, "sync_mode": mode, "selected": True, **extra}
    if pk:
        contract["primary_key"] = pk
    request = TransferRequest(
        source=source or _endpoint(src), destination=_endpoint(dst), sync_mode=mode,
        stream_contracts=[contract], skip_preflight=skip_preflight, validation_mode="strict",
    )
    job_id = "csr" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _window(result) -> dict:
    return (result.destination_summary or {}).get("commit_safe_window") or {}


# --------------------------------------------------------------------------
# C1 — a composite key with a coarse cursor
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode,pk",
    [
        ("incremental_deduped", "order_id,line_id"),
        ("incremental_append", "order_id,line_id"),
        ("incremental_append", ""),  # the catalog primary key is the identity
    ],
    ids=["deduped", "append", "append-catalog-key"],
)
def test_late_row_sharing_cursor_and_first_key_column_is_not_skipped(isolated, pg, mode, pk):
    """PK (order_id, line_id), cursor a DATE. The late row (7, 2) shares the
    cursor value and order_id with the delivered (7, 1): judging delivery on
    (cursor, first key column) called it an in-place edit and skipped it."""
    _sql(
        f"CREATE TABLE {SCHEMA}.lines (order_id int, line_id int, d date NOT NULL, v text, "
        "PRIMARY KEY (order_id, line_id))"
    )
    _sql(f"INSERT INTO {SCHEMA}.lines VALUES (7, 1, '2026-11-10', 'a'), (6, 1, '2026-11-09', 'b')")
    dst = "lines_out_" + uuid.uuid4().hex[:6]
    kw = dict(cursor="d", mode=mode, pk=pk)
    assert _run(isolated, "lines", dst, **kw).success
    slow = _connect(autocommit=False)
    try:
        slow.cursor().execute(f"INSERT INTO {SCHEMA}.lines VALUES (7, 2, '2026-11-10', 'LATE')")
        _sql(f"INSERT INTO {SCHEMA}.lines VALUES (8, 1, '2026-11-11', 'fast')")
        assert _run(isolated, "lines", dst, **kw).success
        slow.commit()
    finally:
        slow.close()
    _sql(f"INSERT INTO {SCHEMA}.lines VALUES (9, 1, '2026-11-12', 'next')")
    third = _run(isolated, "lines", dst, **kw)
    assert third.success, third.error
    evidence = _window(third)
    assert evidence.get("strategy") == csw.STRATEGY_PG, evidence
    assert int(evidence.get("late_rows_undelivered") or 0) == 1, evidence
    assert "edited in place" not in str(evidence.get("reason")), evidence
    assert _run(isolated, "lines", dst, **kw).success
    missing = _sql(
        f"SELECT s.order_id, s.line_id, s.v FROM {SCHEMA}.lines s LEFT JOIN {SCHEMA}.{dst} d "
        "ON d.order_id = s.order_id AND d.line_id = s.line_id WHERE d.order_id IS NULL",
        fetch=True,
    )
    assert missing == [], "the late commit stayed behind the watermark"
    dupes = _sql(
        f"SELECT order_id, line_id FROM {SCHEMA}.{dst} GROUP BY 1, 2 HAVING count(*) > 1",
        fetch=True,
    )
    assert dupes == []


def test_replay_key_is_the_whole_unique_key_or_says_it_is_not():
    from types import SimpleNamespace

    from src.transfer.stream_commit_window import route_replay_key

    def cfg(**kw):
        base = dict(
            cursor_source_col="d", effective_sync="incremental_append", pk_source_cols=[],
            pk_target_cols=[], cursor_pk_source="",
        )
        base.update(kw)
        return SimpleNamespace(**base)

    composite = cfg(pk_source_cols=["order_id", "line_id"], pk_target_cols=["order_id", "line_id"],
                    cursor_pk_source="order_id")
    assert route_replay_key(composite, None) == (("order_id", "line_id"), True)
    upsert = cfg(effective_sync="incremental_deduped", pk_source_cols=["a", "b"],
                 pk_target_cols=["a", "b"])
    assert route_replay_key(upsert, None) == (("a", "b"), True)
    # Catalog primary key: the whole of it, not the tie-break column alone.
    catalog = ({}, {"a": False, "b": False}, {"primary_key_columns": ["a", "b"]})
    assert route_replay_key(cfg(cursor_pk_source="a"), catalog) == (("a", "b"), True)
    # A non-null unique key the tie-break comes from.
    uk = ({}, {"d": False, "x": False}, {"unique_keys": [{"columns": ["d", "x"]}]})
    assert route_replay_key(cfg(cursor_pk_source="x"), uk) == (("d", "x"), True)
    # A cursor the catalog proves unique.
    alone = ({}, {"d": False}, {"primary_key_columns": ["d"]})
    assert route_replay_key(cfg(), alone) == (("d",), True)
    # A tie-break column with no key evidence: usable to look rows up, but not
    # a unique key — every reported row then counts as undelivered.
    assert route_replay_key(cfg(cursor_pk_source="x"), None) == (("x",), False)
    assert route_replay_key(cfg(), None) == ((), False)


# --------------------------------------------------------------------------
# H1 — a text cursor whose type name contains INT or LONG
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "declared,unit",
    [
        ("LONGTEXT", ""),
        ("LONG VARCHAR", ""),
        ("LONG", ""),  # Oracle LONG is character data
        ("POINT", ""),
        ("MULTIPOINT", ""),
        ("INTERVAL", ""),
        ("INTERVAL DAY TO SECOND", ""),
        ("VARCHAR(20)", ""),
        ("TEXT", ""),
        ("NUMBER", ""),  # no scale: not an integer
        ("NUMBER(10,2)", ""),
        ("DECIMAL(18,4)", ""),
        ("FLOAT", ""),
        ("DATE", ""),
        ("INT", "values"),
        ("int unsigned", "values"),
        ("INTEGER", "values"),
        ("BIGINT", "values"),
        ("BIGINT UNSIGNED", "values"),
        ("SMALLINT", "values"),
        ("TINYINT", "values"),
        ("MEDIUMINT", "values"),
        ("SERIAL", "values"),
        ("BIGSERIAL", "values"),
        ("INT2", "values"),
        ("INT4", "values"),
        ("INT8", "values"),
        ("INT64", "values"),
        ("NUMBER(10)", "values"),
        ("NUMBER(10,0)", "values"),
        ("NUMERIC(12, 0)", "values"),
        ("DECIMAL(18)", "values"),
        ("TIMESTAMP", "seconds"),
        ("TIMESTAMP(6) WITH TIME ZONE", "seconds"),
        ("TIMESTAMPTZ", "seconds"),
        ("DATETIME", "seconds"),
        ("DATETIME2", "seconds"),
        ("SMALLDATETIME", "seconds"),
    ],
)
def test_cursor_distance_is_decided_by_whole_type_words(declared, unit):
    assert csw.cursor_distance_unit(declared) == unit


def test_zero_padded_text_never_gets_an_integer_seed():
    # The seed would lose its padding: '4000' sorts above '005001' as text.
    assert csw.overlap_seed("005000", "LONGTEXT", seconds=300, values=1000) is None
    assert csw.overlap_seed("005000", "", seconds=300, values=1000) is None
    assert csw.overlap_seed("005000", "BIGINT", seconds=300, values=1000) is None
    assert csw.overlap_seed("+5000", "BIGINT", seconds=300, values=1000) is None
    assert csw.overlap_seed("5000", "BIGINT", seconds=300, values=1000) == "4000"
    assert csw.overlap_seed("-5", "INT", seconds=300, values=10) == "-15"


def test_text_cursor_route_is_not_guaranteed_and_reads_from_the_watermark():
    window = csw.plan_commit_safe_window(
        source_type="mysql", sync_mode="incremental_append", cursor_column="code",
        cursor_type="LONGTEXT", stored_watermark="005000", metadata={}, can_replay=True,
        contract={},
    )
    assert window.strategy == csw.STRATEGY_NONE and window.guarantee == csw.GUARANTEE_NONE
    assert window.seed == "005000" and not window.replays
    assert window.census_recommended
    static = csw.static_commit_safety(
        source_type="mysql", sync_mode="incremental_append", cursor_column="code",
        cursor_type="LONGTEXT", has_identity=True,
    )
    assert static["guarantee"] == csw.GUARANTEE_NONE, static
    assert static["statement"].startswith("Cannot guarantee no skipped rows"), static
    assert static["census_recommended"]


def test_mysql_longtext_cursor_delivers_every_new_row(isolated, pg):
    """Live MySQL → PostgreSQL. A LONGTEXT cursor of zero-padded numbers: the
    integer re-read seed '4000' sat above every new value as text, so runs 2
    and 3 read nothing and reported commit_safe_within_window."""
    from tests.helpers.live_env import mysql_up
    from tests.test_incremental_commit_safe_window import _mysql, _mysql_endpoint

    if not mysql_up():
        pytest.skip("live MySQL not reachable")
    src = "csr_lt_" + uuid.uuid4().hex[:6]
    dst = "csr_ltd_" + uuid.uuid4().hex[:6]
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, code LONGTEXT NOT NULL, v VARCHAR(20))")
            cur.executemany(
                f"INSERT INTO {src} VALUES (%s, %s, %s)",
                [(i, f"{i:06d}", f"v{i}") for i in range(4995, 5001)],
            )
        kw = dict(cursor="code", mode="incremental_append", source=_mysql_endpoint(src))
        assert _run(isolated, src, dst, **kw).success
        with conn.cursor() as cur:
            cur.executemany(
                f"INSERT INTO {src} VALUES (%s, %s, %s)",
                [(i, f"{i:06d}", f"new{i}") for i in range(5001, 5004)],
            )
        second = _run(isolated, src, dst, **kw)
        assert second.success, second.error
        assert int(second.records_transferred) == 3
        evidence = _window(second)
        assert evidence.get("guarantee") == csw.GUARANTEE_NONE, evidence
        assert evidence.get("replay_seed") is None, evidence
        assert _run(isolated, src, dst, **kw).success
        got = sorted(r[0] for r in _sql(f"SELECT id FROM {SCHEMA}.{dst}", fetch=True))
        assert got == list(range(4995, 5004))
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
        conn.close()
