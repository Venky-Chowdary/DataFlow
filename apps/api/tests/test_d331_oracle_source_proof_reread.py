"""D331 - the transform proof over an Oracle source, through ``execute_tracked``.

QA r61-r75 (build 70cb4669c): every shaped run from an Oracle source said
"transform proof unverifiable - N of N keys do not hold the recipe output and
their source rows no longer read back as this run read them" over exact data
(ORA -> PG upsert 29 of 30: only the all-NULL row 30 passed), and a strict
Oracle -> Oracle upsert then deleted its 21 correct rows.

Root cause: the proof re-read an Oracle source with its own raw ``SELECT``
through the driver, not through the reader the write used. python-oracledb
hands TIMESTAMP WITH TIME ZONE back as a naive ``datetime`` (the offset is
dropped, the wall clock kept); the write's reader renders it with ``TO_CHAR``
and keeps the instant (``generic_sql._tz_safe_projection``). Every row holding
such a value lost its pin and, re-shaped from the shifted instant, no longer
equalled the destination.

No Oracle runs locally. These tests are *fakes* of the Oracle side:

* the write's reader (``batch_readers._read_batch_unscoped`` for ``oracle``)
  serves the wire text the real reader emits: catalog (upper-case) headers,
  NUMBER as its digits, TIMESTAMP WITH TIME ZONE as ``TO_CHAR`` ISO text with
  the offset;
* the raw driver path (``warehouse_sql_engine("oracle")``) answers the proof's
  hand-built ``SELECT`` with the value types python-oracledb returns: ``int``
  for NUMBER(10), ``str`` for VARCHAR2, a *naive* ``datetime`` for TIMESTAMP
  WITH TIME ZONE;
* the catalog types are the ones ``_oracle_to_logical`` gives.

Everything else - the stream, the recipe, the pin ledger, the PostgreSQL
destination, Gate-8 and the proof - is the product's real path. The live
proof is ``test_d331_oracle_source_proof_live.py`` in CI's oracle-live job.
"""

from __future__ import annotations

import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_COLS = ["ID", "NOTE", "UPDATED_AT", "SEEN_AT"]
_TYPES = {
    "ID": "DECIMAL(10)",
    "NOTE": "VARCHAR(40)",
    "UPDATED_AT": "TIMESTAMP_TZ(6)",
    "SEEN_AT": "TIMESTAMP_TZ(6)",
}
_IST = timezone(timedelta(hours=5, minutes=30))


def _source_rows() -> list[dict]:
    """The canonical seed's shape: row 30 is NULL except its key and cursor."""
    rows = []
    for i in range(1, 31):
        rows.append(
            {
                "ID": i,
                "NOTE": None if i in (3, 30) else f"n{i}",
                "UPDATED_AT": datetime(2026, 11, 24, 10, i % 60, 0, tzinfo=timezone.utc),
                "SEEN_AT": None if i == 30 else datetime(2026, 11, 24, 8, i % 60, 7, 250000, tzinfo=_IST),
            }
        )
    return rows


def _wire(value) -> str:
    """The real reader's wire text for one Oracle cell (TO_CHAR for TSTZ)."""
    from services.value_serializer import driver_cell_to_string

    if isinstance(value, datetime):
        # ORACLE_TSTZ_READ_FORMAT: YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f") + value.strftime("%z")[:3] + ":" + value.strftime("%z")[3:]
    return driver_cell_to_string(value)


def _oracledb_value(value):
    """What python-oracledb returns for the same cell on a raw SELECT."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)  # TSTZ: wall clock kept, offset dropped
    return value


class _FakeOracleResult:
    def __init__(self, rows, cols):
        self._rows = rows
        self._cols = cols
        self.cursor = None

    def keys(self):
        return list(self._cols)

    def __iter__(self):
        return iter(self._rows)

    def fetchmany(self, n):
        out, self._rows = self._rows[:n], self._rows[n:]
        return out

    def fetchall(self):
        out, self._rows = self._rows, []
        return out


class _FakeOracleConn:
    """Answers the proof's raw ``SELECT "COL", ... FROM "OWNER"."T"``."""

    def __init__(self, source):
        self.source = source
        self.sql: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, *a, **k):
        text = str(stmt)
        self.sql.append(text)
        m = re.match(r'\s*SELECT\s+(.*?)\s+FROM\s+', text, re.S | re.I)
        cols = re.findall(r'"([^"]+)"', m.group(1)) if m else []
        rows = [
            tuple(_oracledb_value(r.get(c)) for c in cols)
            for r in self.source
        ]
        return _FakeOracleResult(rows, cols)

    def execution_options(self, **_k):
        return self


class _FakeOracleEngine:
    def __init__(self, source, log):
        self.source = source
        self.log = log

    def connect(self):
        conn = _FakeOracleConn(self.source)
        self.log.append(conn)
        return conn


@pytest.fixture()
def oracle_source(tmp_path: Path, monkeypatch):
    """Fake the Oracle side only; everything else is the real engine."""
    import contextlib

    import services.dest_precount as dp
    import src.transfer.adapters_introspect as ai
    import src.transfer.batch_readers as br
    import src.transfer.stream as stream_mod
    from connectors.base import ReadBatch
    from services import reconciliation as recon_mod

    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_k):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)

    state = {"rows": _source_rows(), "raw_reads": [], "reader_calls": 0}
    original_read = br._read_batch_unscoped

    def _reader(src_type, cfg, table, columns, offset, limit, *a, **k):
        if src_type != "oracle":
            return original_read(src_type, cfg, table, columns, offset, limit, *a, **k)
        state["reader_calls"] += 1
        wanted = [
            next(h for h in _COLS if h.casefold() == str(c).casefold())
            for c in (columns or _COLS)
        ]
        scan = k.get("scan_state")
        if scan is not None:
            start = int(scan.get("pos") or 0)
            scan["started"] = True
        else:
            start = int(offset or 0)
        page = state["rows"][start : start + int(limit or 1)]
        if scan is not None:
            scan["pos"] = start + len(page)
        return ReadBatch(
            headers=wanted,
            rows=[[_wire(r.get(c)) for c in wanted] for r in page],
            offset=start,
            total_rows=len(state["rows"]),
        )

    monkeypatch.setattr(br, "_read_batch_unscoped", _reader)

    original_rich = ai._introspect_table_schema_rich

    def _rich(db_type, cfg, table, headers, *a, **k):
        if str(db_type).lower() == "oracle":
            return dict(_TYPES), {c: c != "ID" for c in _COLS}, {"primary_key_columns": ["ID"]}
        return original_rich(db_type, cfg, table, headers, *a, **k)

    monkeypatch.setattr(ai, "_introspect_table_schema_rich", _rich)
    monkeypatch.setattr(stream_mod, "_introspect_table_schema_rich", _rich)
    monkeypatch.setattr(
        stream_mod,
        "_introspect_table_schema",
        lambda db_type, cfg, table, headers, *a, **k: dict(_TYPES)
        if str(db_type).lower() == "oracle"
        else ai._introspect_table_schema(db_type, cfg, table, headers, *a, **k),
    )
    original_engine = dp.warehouse_sql_engine

    @contextlib.contextmanager
    def _engine(db_type, cfg):
        if str(db_type).lower() == "oracle":
            yield _FakeOracleEngine(state["rows"], state["raw_reads"])
            return
        with original_engine(db_type, cfg) as eng:
            yield eng

    monkeypatch.setattr(dp, "warehouse_sql_engine", _engine)
    # The raw SELECT result is drained through this helper; a fake result has
    # no DBAPI cursor, so hand back its rows directly.
    original_stream = recon_mod.sa_streaming_result

    def _stream(conn, statement, *, itersize=1000):
        if isinstance(conn, _FakeOracleConn):
            res = conn.execute(statement)
            return res.keys(), list(res)
        return original_stream(conn, statement, itersize=itersize)

    monkeypatch.setattr(recon_mod, "sa_streaming_result", _stream)
    state["store"] = store
    return state


def _oracle_endpoint() -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="oracle", host="oracle.invalid", port=1521,
        database="FREEPDB1", schema="APP", table="ORDERS", username="app", password="x",
    )


def _request(dest: EndpointConfig, mode: str = "incremental_upsert") -> TransferRequest:
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe

    _s, chains = split_column_transforms({"NOTE": ["upper"]})
    recipe, refusal = compile_column_chains(chains, _COLS)
    assert not refusal, refusal
    return TransferRequest(
        source=_oracle_endpoint(),
        destination=dest,
        mappings=[
            {"source": c, "target": c.lower(), "approved": True, "confidence": 0.99}
            for c in _COLS
        ],
        sync_mode=mode,
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
        stream_contracts=[
            {"name": dest.table, "primary_key": ["ID"], "sync_mode": mode, "selected": True}
        ],
    )


def _run(state, request):
    job_id = "d331o" + uuid.uuid4().hex[:12]
    state["store"].update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _pg_dest(table: str) -> EndpointConfig:
    from tests import typed_fidelity_helpers as tf

    return tf.pg_endpoint(table)


def _pg_rows(table: str) -> list[tuple]:
    from tests import r4_route_harness as h

    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT id, note, seen_at FROM public."{table}" ORDER BY id')  # nosec B608
        return cur.fetchall()
    finally:
        conn.close()


def _pg_count(table: str) -> int:
    """Rows in the destination; 0 when the undo dropped the table it created."""
    from tests import r4_route_harness as h

    conn = h.pg_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s)", (f'public."{table}"',))
        if cur.fetchone()[0] is None:
            return 0
        cur.execute(f'SELECT count(*) FROM public."{table}"')  # nosec B608
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def test_oracle_source_upsert_proof_verifies_exact_data(oracle_source):
    """ORA -> PG upsert with a recipe over the canonical seed shape: the proof
    verifies. On 70cb it said "29 of 30 keys do not hold the recipe output"."""
    from tests import r4_route_harness as h

    table = f"d331o_{uuid.uuid4().hex[:8]}"
    try:
        result = _run(oracle_source, _request(_pg_dest(table)))
        rows = _pg_rows(table)
        assert len(rows) == 30
        assert rows[0][1] == "N1" and rows[2][1] is None
        # The destination holds the instant (08:01:07.25+05:30 = 02:31:07.25Z).
        assert rows[0][2] == datetime(2026, 11, 24, 2, 31, 7, 250000, tzinfo=timezone.utc)
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert "do not hold the recipe output" not in str(proof.get("reason") or ""), proof
        assert proof.get("status") == "verified", proof
        assert result.success, result.error
        # The re-read went through the write's reader, not a raw SELECT.
        assert not oracle_source["raw_reads"], [c.sql for c in oracle_source["raw_reads"]]
    finally:
        h.drop_table("postgresql", table)


def test_oracle_source_wrong_destination_value_still_fails(oracle_source, monkeypatch):
    """R9: the check is not weakened - a value changed in the destination
    after the write fails the proof, naming the key; the strict run fails and
    its batch is undone."""
    import src.transfer.reconcile_step as rs
    from tests import r4_route_harness as h

    table = f"d331o_{uuid.uuid4().hex[:8]}"
    original = rs._transform_applied_proof

    def _tamper_then_prove(**kwargs):
        conn = h.pg_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"UPDATE public.\"{table}\" SET note = 'n7' WHERE id = 7")  # nosec B608
            if not conn.autocommit:
                conn.commit()
        finally:
            conn.close()
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _tamper_then_prove)
    try:
        result = _run(oracle_source, _request(_pg_dest(table)))
        proof = (result.reconciliation or {}).get("transform_applied") or {}
        assert proof.get("status") == "failed", proof
        assert proof.get("culprit_columns") == ["note"], proof
        assert "7" in (proof.get("culprit_keys") or {}).get("changed", []), proof
        assert not result.success
        # The job names the culprit key, and the failed run's own batch is
        # undone as before (the destination was empty: nothing older is lost).
        assert "changed: 7" in str(result.error or ""), result.error
        assert _pg_count(table) == 0
    finally:
        h.drop_table("postgresql", table)


def test_wire_decode_is_the_readers_exact_inverse():
    """Numbers and zoneless timestamps return to the driver's value; zone-
    bearing timestamps and text keep the reader's text."""
    from decimal import Decimal

    from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string
    from src.transfer.proof_source_reader import decode_wire_rows

    ddl = {
        "ID": "DECIMAL(10)", "AMT": "DECIMAL(18,6)", "F": "BINARY_DOUBLE",
        "D": "TIMESTAMP", "T": "TIMESTAMP_NTZ(6)", "Z": "TIMESTAMP_TZ(6)",
        "S": "VARCHAR(10)", "C": "CHAR(4)",
    }
    typed = {
        "ID": 31, "AMT": Decimal("99999999999.999999"), "F": 1.25,
        "D": datetime(2026, 11, 24), "T": datetime(2026, 11, 24, 10, 1, 2, 123456),
        "Z": "2026-11-24T08:31:07.250000+05:30", "S": "01", "C": "ab  ",
    }
    wire = {k: cell_to_string(v, preserve_sql_null=True) for k, v in typed.items()}
    wire["N"] = SQL_NULL_SENTINEL
    out = decode_wire_rows([wire], lambda c: ddl.get(c, ""))[0]
    assert {k: out[k] for k in typed} == typed
    assert out["N"] is None
    assert all(cell_to_string(out[k], preserve_sql_null=True) == wire[k] for k in typed)


class _FakeOracleDestConn(_FakeOracleConn):
    """An Oracle *destination* holding ``rows`` (dicts keyed by lower-case name).

    Answers the zero-row description probe with python-oracledb's type names
    and renders ``TO_CHAR`` columns as Oracle's ISO text with the stored
    offset; a raw zone-bearing column comes back naive, as python-oracledb
    returns it.
    """

    def __init__(self, rows, tz_cols):
        super().__init__(rows)
        self.tz_cols = set(tz_cols)

    def rollback(self):
        return None

    def execute(self, stmt, *a, **k):
        text = str(stmt)
        self.sql.append(text)
        head = re.match(r"\s*SELECT\s+(.*?)\s+FROM\s+(\(|\")", text, re.S | re.I)
        parts = [p.strip() for p in re.split(r",(?![^(]*\))", head.group(1))] if head else []
        if text.rstrip().endswith("WHERE 1=0"):
            inner = re.search(r"FROM \(SELECT (.*?) FROM ", text, re.S)
            names = re.findall(r'"([^"]+)"(?:\s*,|\s*$)', inner.group(1) + ",") if inner else []
            res = _FakeOracleResult([], names)
            res.cursor = type("C", (), {"description": [
                (n, "DB_TYPE_TIMESTAMP_TZ" if n in self.tz_cols else "DB_TYPE_VARCHAR", None, None, None, 6, True)
                for n in names
            ]})()
            return res
        cols, render = [], []
        for p in parts:
            alias = re.findall(r'"([^"]+)"', p)[-1]
            cols.append(alias)
            render.append(p.upper().startswith("TO_CHAR("))

        def _cell(row, col, as_text):
            v = row.get(col)
            if isinstance(v, datetime) and col in self.tz_cols:
                local = v.astimezone(_IST)
                if as_text:
                    return local.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+05:30"
                return local.replace(tzinfo=None)  # python-oracledb: naive wall clock
            return v

        return _FakeOracleResult(
            [tuple(_cell(r, c, t) for c, t in zip(cols, render)) for r in self.source], cols
        )


def test_oracle_destination_proof_reads_zone_bearing_columns_as_instants(oracle_source, monkeypatch):
    """The proof's Oracle destination read-back renders TIMESTAMP WITH TIME
    ZONE with TO_CHAR (an instant). Read raw, python-oracledb's naive wall
    clock (+05:30 dropped) no longer equals the recipe output on any row."""
    import contextlib

    import services.dest_precount as dp
    import src.transfer.reconcile_step as rs
    from tests import r4_route_harness as h

    captured = {}
    original = rs._transform_applied_proof

    def _spy(**kwargs):
        captured.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _spy)
    table = f"d331o_{uuid.uuid4().hex[:8]}"
    try:
        result = _run(oracle_source, _request(_pg_dest(table)))
        assert result.success, result.error
        conn = h.pg_conn()
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT id, note, updated_at, seen_at FROM public."{table}"')  # nosec B608
            stored = [dict(zip(["id", "note", "updated_at", "seen_at"], r)) for r in cur.fetchall()]
        finally:
            conn.close()
    finally:
        h.drop_table("postgresql", table)

    dest_conn = _FakeOracleDestConn(stored, {"updated_at", "seen_at"})

    @contextlib.contextmanager
    def _dest_engine(db_type, cfg):
        yield type("E", (), {"connect": lambda self: dest_conn})()

    monkeypatch.setattr(dp, "warehouse_sql_engine", _dest_engine)
    monkeypatch.setattr(
        rs, "_oracle_projection",
        lambda conn, ep, cfg, cols: (", ".join(f'"{c}"' for c in cols), lambda n: f'"{n}"'),
    )
    oracle_dest = EndpointConfig(
        kind="database", format="oracle", host="oracle.invalid", port=1521,
        database="FREEPDB1", schema="APP", table=table.upper(), username="app", password="x",
    )
    proof = rs._transform_applied_proof_impl(**{**captured, "endpoint": oracle_dest})
    assert proof.get("status") == "verified", proof
    assert any("TO_CHAR" in s for s in dest_conn.sql), dest_conn.sql
