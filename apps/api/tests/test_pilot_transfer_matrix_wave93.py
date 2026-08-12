"""Cross-engine transfer proof for Datawrap Pilot — Postgres, MySQL, MongoDB.

Wave 92 proved the pilot can plan and run a transfer, but only Postgres onto
Postgres, where every type maps to itself. That route cannot catch the failures
that matter: a ``NUMERIC(12,2)`` silently widened to ``DECIMAL(38,15)``, a
``DOUBLE PRECISION`` rewritten as a decimal, or ``JSONB`` flattened to text.
Those only appear when the two ends speak different type systems.

Running the matrix surfaced three real defects, all fixed and pinned here:

1. ``merge_profiler_schema`` let sample inference overwrite the declared DDL of
   an introspected database, so precision, the numeric class and JSON semantics
   were lost before the destination DDL was ever generated.
2. A TLS handshake refused by a plaintext server was reported to the operator as
   "connection timed out", pointing at firewalls instead of the SSL checkbox.
3. Cross-engine identity is not row equality: values have to be compared after
   normalising carrier differences (Decimal vs float, date vs datetime), which is
   what an operator means by "the data arrived intact".

Engines that are not running are skipped, never faked.
"""

from __future__ import annotations

import datetime as dt
import socket
import time
import uuid
from decimal import Decimal

import pytest

PG = ("localhost", 5432)
MYSQL = ("127.0.0.1", 3306)
MONGO = ("localhost", 27017)


def _reachable(hostport: tuple[str, int]) -> bool:
    try:
        with socket.create_connection(hostport, timeout=2):
            return True
    except OSError:
        return False


_PG_UP = _reachable(PG)
_MYSQL_UP = _reachable(MYSQL)
_MONGO_UP = _reachable(MONGO)


# --------------------------------------------------------------------------
# Type fidelity — no engine required
# --------------------------------------------------------------------------


def test_declared_database_types_survive_sample_inference():
    """An introspected DDL type outranks a guess made from sample values."""
    from services.data_profiler import merge_profiler_schema

    declared = {
        "amount": "DECIMAL(12,2)",
        "ratio": "FLOAT",
        "payload": "JSONB",
        "name": "TEXT",
        "untyped": "",
    }
    # What profiling concludes from the values alone — plausible, but weaker.
    profiled = {
        "amount": "DECIMAL",
        "ratio": "DECIMAL",
        "payload": "VARCHAR",
        "name": "VARCHAR",
        "untyped": "INTEGER",
    }

    merged = merge_profiler_schema(declared, profiled, authoritative_existing=True)
    assert merged["amount"] == "DECIMAL(12,2)", "scale must not be widened away"
    assert merged["ratio"] == "FLOAT", "a float is not a decimal"
    assert merged["payload"] == "JSONB", "JSON semantics must not degrade to text"
    assert merged["name"] == "TEXT"
    # Only a column the source declared nothing for may be inferred.
    assert merged["untyped"] == "INTEGER"


def test_file_sources_still_get_statistical_inference():
    """Files declare nothing useful, so inference must still win there."""
    from services.data_profiler import merge_profiler_schema

    merged = merge_profiler_schema(
        {"qty": "VARCHAR", "ts": "VARCHAR"},
        {"qty": "INTEGER", "ts": "TIMESTAMP"},
    )
    assert merged == {"qty": "INTEGER", "ts": "TIMESTAMP"}


def test_declared_precision_is_kept_even_for_file_sources():
    """A parameterised type is real evidence whoever declared it."""
    from services.data_profiler import merge_profiler_schema

    merged = merge_profiler_schema({"amount": "DECIMAL(12,2)"}, {"amount": "DECIMAL"})
    assert merged["amount"] == "DECIMAL(12,2)"


def test_pipeline_preserves_declared_types_end_to_end():
    """The regression that produced DECIMAL(38,15) destination columns."""
    from services.mapping_pipeline import run_mapping_pipeline

    rows = [
        {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
        {"name": "amount", "inferred_type": "DECIMAL(12,2)", "samples": ["10.50", "20.25"]},
        {"name": "ratio", "inferred_type": "FLOAT", "samples": ["1.5", "2.25"]},
        {"name": "narrow", "inferred_type": "REAL", "samples": ["1.5", "2.25"]},
    ]
    result = run_mapping_pipeline(
        [r["name"] for r in rows],
        [],
        source_schemas=rows,
        target_schemas=[],
        source_samples={r["name"]: r["samples"] for r in rows},
        validation_mode="balanced",
        destination_db_type="mysql",
        schema_policy="manual_review",
        sync_mode="full_refresh_append",
        destination_table_exists=False,
        source_types_authoritative=True,
        use_llm=False,
    )
    by_source = {m["source"]: m for m in result["mappings"]}
    assert by_source["amount"]["target_type"] == "DECIMAL(12,2)"
    # Bare FLOAT does not declare a width, and Property 1 forbids reading one out
    # of the spelling, so it invents IEEE-64 rather than silently narrowing to a
    # 24-bit mantissa. A source that means single precision says so.
    assert by_source["ratio"]["target_type"] == "DOUBLE"
    assert by_source["narrow"]["target_type"] == "FLOAT"


# --------------------------------------------------------------------------
# Connector diagnosis
# --------------------------------------------------------------------------


def test_tls_refusal_is_not_reported_as_a_network_timeout():
    """The driver says "timed out"; the operator needs to hear "TLS"."""
    from src.transfer.connector_registry import humanize_connection_error

    raw = (
        "SSL handshake failed: localhost:27017: EOF occurred in violation of "
        "protocol (_ssl.c:1129) (configured timeouts: socketTimeoutMS: 10000.0ms), "
        "Timeout: 10.0s"
    )
    message = humanize_connection_error("mongodb", raw)
    assert "TLS" in message
    assert "SSL" in message
    assert "timed out" not in message.lower()


def test_unverifiable_certificate_does_not_advise_disabling_ssl():
    """Never tell an operator to turn off verification to fix a bad cert."""
    from src.transfer.connector_registry import humanize_connection_error

    message = humanize_connection_error(
        "postgresql",
        "SSL: CERTIFICATE_VERIFY_FAILED certificate verify failed: self signed certificate",
    )
    assert "certificate" in message.lower()
    assert "do not disable ssl" in message.lower()


# --------------------------------------------------------------------------
# Live cross-engine matrix
# --------------------------------------------------------------------------

# One source shape exercised against every destination: an integer key, an
# unbounded string, a fixed-scale decimal, a float, a boolean, a date and a
# timestamp. Each one maps differently on MySQL and MongoDB.
_SOURCE_DDL = (
    "id INTEGER PRIMARY KEY, "
    "name TEXT NOT NULL, "
    "amount NUMERIC(12,2), "
    "ratio DOUBLE PRECISION, "
    "active BOOLEAN, "
    "ordered_at DATE, "
    "created_at TIMESTAMP"
)
_SOURCE_ROWS = [
    (1, "alpha", "150.25", 1.5, True, "2024-01-05", "2024-01-05 10:30:00"),
    (2, "beta", "20.00", 2.25, False, "2024-02-11", "2024-02-11 23:59:59"),
    (3, "gamma", "300.50", -0.75, True, "2024-02-14", "2024-02-14 00:00:01"),
    (4, "delta", "1.00", 0.0, False, "2025-03-03", "2025-03-03 12:00:00"),
    (5, "epsilon", "400.75", 99.125, True, "2025-03-21", "2025-03-21 08:15:30"),
]
_COLUMNS = ["id", "name", "amount", "ratio", "active", "ordered_at", "created_at"]


def _pg_conn():
    import psycopg2

    return psycopg2.connect(
        host=PG[0], port=PG[1], dbname="dataflow", user="dataflow", password="dataflow"
    )


def _mysql_conn():
    import pymysql

    return pymysql.connect(
        host=MYSQL[0],
        port=MYSQL[1],
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )


def _mongo_db():
    from pymongo import MongoClient

    return MongoClient(f"mongodb://{MONGO[0]}:{MONGO[1]}")["dataflow_w93"]


def _normalize(value):
    """Compare what the value *means*, not the carrier the engine handed back.

    A decimal that arrives as ``Decimal("150.25")`` on Postgres and ``150.25``
    on MongoDB is the same number; a date that widens to midnight-datetime is
    the same day. Normalising is not the same as being lenient — a changed
    number or a truncated string still fails.
    """
    if isinstance(value, bool):
        return value
    # MongoDB returns Decimal128, which is the *correct* carrier for a NUMERIC
    # source — comparing its repr would fail a transfer that in fact kept the
    # number exactly.
    if type(value).__name__ == "Decimal128":
        return float(value.to_decimal())
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return float(value) if abs(value) > 1 or value in (0, 1) else float(value)
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None, microsecond=0)
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)
    if value is None:
        return None
    return str(value)


def _normalize_row(row: dict) -> tuple:
    out = []
    for col in _COLUMNS:
        v = row.get(col)
        if col in ("active",):
            out.append(bool(v) if v is not None else None)
        elif col == "id":
            out.append(int(v))
        else:
            out.append(_normalize(v))
    return tuple(out)


@pytest.fixture
def matrix_env():
    """Real source data plus one saved connector per reachable engine."""
    from services.connector_store import create_connector, delete_connector

    suffix = uuid.uuid4().hex[:6]
    src_table = f"w93_src_{suffix}"
    dst_table = f"w93_dst_{suffix}"

    pg = _pg_conn()
    pg.autocommit = True
    with pg.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_table} ({_SOURCE_DDL})")
        cur.executemany(
            f"INSERT INTO {src_table} ({', '.join(_COLUMNS)}) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            _SOURCE_ROWS,
        )

    saved = {}
    saved["postgresql"] = create_connector({
        "name": f"W93PG{suffix}",
        "type": "postgresql",
        "host": PG[0],
        "port": PG[1],
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
        "ssl": False,
    })
    if _MYSQL_UP:
        saved["mysql"] = create_connector({
            "name": f"W93MY{suffix}",
            "type": "mysql",
            "host": MYSQL[0],
            "port": MYSQL[1],
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "dataflow",
            "ssl": False,
        })
    if _MONGO_UP:
        saved["mongodb"] = create_connector({
            "name": f"W93MG{suffix}",
            "type": "mongodb",
            "host": MONGO[0],
            "port": MONGO[1],
            "database": "dataflow_w93",
            # Local Mongo speaks plaintext; the store defaults ssl on.
            "ssl": False,
        })

    try:
        yield {
            "pg": pg,
            "saved": saved,
            "src": src_table,
            "dst": dst_table,
            "suffix": suffix,
        }
    finally:
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src_table}")
            cur.execute(f"DROP TABLE IF EXISTS {dst_table}")
        pg.close()
        if _MYSQL_UP:
            try:
                my = _mysql_conn()
                with my.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
                my.close()
            except Exception:
                pass
        if _MONGO_UP:
            try:
                _mongo_db().drop_collection(dst_table)
            except Exception:
                pass
        for conn in saved.values():
            delete_connector(conn.id)


def _read_back(engine: str, table: str) -> list[dict]:
    """Rows currently at the destination; empty when it does not exist yet.

    A missing table before Confirm is the correct state, not an error — the
    engine must not create anything until the operator approves.
    """
    if engine == "postgresql":
        conn = _pg_conn()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (table,))
                if cur.fetchone()[0] is None:
                    return []
                cur.execute(f"SELECT {', '.join(_COLUMNS)} FROM {table} ORDER BY id")
                return [dict(zip(_COLUMNS, r)) for r in cur.fetchall()]
        finally:
            conn.close()
    if engine == "mysql":
        conn = _mysql_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema=%s AND table_name=%s",
                    ("dataflow", table),
                )
                if not cur.fetchone()[0]:
                    return []
                cur.execute(f"SELECT {', '.join(_COLUMNS)} FROM `{table}` ORDER BY id")
                return [dict(zip(_COLUMNS, r)) for r in cur.fetchall()]
        finally:
            conn.close()
    return list(_mongo_db()[table].find({}, {"_id": 0}).sort("id", 1))


def _expected_rows() -> list[tuple]:
    return [
        _normalize_row({
            "id": r[0],
            "name": r[1],
            "amount": Decimal(r[2]),
            "ratio": r[3],
            "active": r[4],
            "ordered_at": dt.date.fromisoformat(r[5]),
            "created_at": dt.datetime.fromisoformat(r[6]),
        })
        for r in _SOURCE_ROWS
    ]


def _routes() -> list:
    out = []
    if _MYSQL_UP:
        out.append(pytest.param("mysql", id="postgres_to_mysql"))
    if _MONGO_UP:
        out.append(pytest.param("mongodb", id="postgres_to_mongodb"))
    return out


@pytest.mark.skipif(not _PG_UP, reason="Postgres not reachable")
@pytest.mark.skipif(not (_MYSQL_UP or _MONGO_UP), reason="No cross-engine destination reachable")
@pytest.mark.parametrize("dest_engine", _routes())
def test_live_cross_engine_plan_preserves_declared_types(matrix_env, dest_engine):
    """Planning onto a foreign engine must not invent or widen source types."""
    from src.ai.copilot.tools import DataPilotTools

    planned = DataPilotTools().execute("plan_transfer", {
        "source_connector_id": matrix_env["saved"]["postgresql"].id,
        "source_table": matrix_env["src"],
        "dest_connector_id": matrix_env["saved"][dest_engine].id,
        "dest_table": matrix_env["dst"],
    })
    assert planned.success, planned.error
    plan = planned.output

    assert plan["mapped_count"] == len(_COLUMNS)
    assert plan["unmapped_source_columns"] == []
    assert plan["destination"]["table_exists"] is False

    from_types = {
        c["source_column"]: c["from_type"] for c in plan["type_conversions"]
    }
    # The source side of every conversion is the declared Postgres type, never
    # a re-guess from the sampled values. The defaults are deliberately absent:
    # with one, a plan that reported no conversion at all satisfied this by
    # falling back to the expected string.
    assert from_types["amount"] == "DECIMAL(12,2)"
    # _SOURCE_DDL declares `ratio DOUBLE PRECISION`, and that is what must be
    # carried — 1.5 and 2.25 would sample as a narrow DECIMAL.
    assert from_types["ratio"] == "DOUBLE PRECISION"

    gate_ids = {g["id"] for g in plan["preflight"]["gates"]}
    assert len(gate_ids) >= 8, f"expected the full gate suite, got {sorted(gate_ids)}"
    assert plan["preflight"]["run_id"], "a plan must be citable"


@pytest.mark.skipif(not _PG_UP, reason="Postgres not reachable")
@pytest.mark.skipif(not (_MYSQL_UP or _MONGO_UP), reason="No cross-engine destination reachable")
@pytest.mark.parametrize("dest_engine", _routes())
def test_live_cross_engine_confirm_moves_every_row_intact(matrix_env, dest_engine):
    """End to end: chat stages it, Confirm runs it, the values survive."""
    import asyncio

    from src.ai.copilot.tools import DataPilotTools
    from src.routers.copilot_router import ConfirmActionRequest, copilot_confirm

    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_id": matrix_env["saved"]["postgresql"].id,
        "source_table": matrix_env["src"],
        "dest_connector_id": matrix_env["saved"][dest_engine].id,
        "dest_table": matrix_env["dst"],
    })
    assert staged.success, staged.error
    assert staged.output["requires_confirm"] is True

    # Nothing may exist at the destination before the operator confirms.
    assert _read_back(dest_engine, matrix_env["dst"]) == []

    confirmed = asyncio.get_event_loop().run_until_complete(
        copilot_confirm(ConfirmActionRequest(ack_id=staged.output["ack_id"], actor="wave93"))
    )
    assert confirmed["ok"] is True
    job_id = confirmed["job_id"]

    deadline = time.time() + 180
    landed: list[dict] = []
    while time.time() < deadline:
        landed = _read_back(dest_engine, matrix_env["dst"])
        if len(landed) >= len(_SOURCE_ROWS):
            break
        time.sleep(1.0)

    assert len(landed) == len(_SOURCE_ROWS), (
        f"job {job_id} landed {len(landed)}/{len(_SOURCE_ROWS)} rows on {dest_engine}"
    )
    assert [_normalize_row(r) for r in landed] == _expected_rows()
