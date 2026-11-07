"""Wave 88 — Postgres JSON/JSONB binds as text, because psycopg2 cannot adapt a dict.

``normalize_sql_bind_value`` returned a native ``dict`` for Postgres-family
engines on the theory that the driver adapts it ("psycopg / Airbyte Destinations
V2"). That holds for psycopg3 only. The shipped writer uses **psycopg2**, which
raises ``can't adapt type 'dict'``, so every JSON-bearing transfer into Postgres
aborted with ``records_transferred=0`` — JSON/JSONL files, DuckDB STRUCT columns,
messy-CSV documents, and Mongo docs alike.

Postgres casts an unknown-typed text parameter straight into json/jsonb, and
psycopg2 still parses jsonb back into native dict/list on read, so JSON text is
the portable wire with no fidelity cost. Proven below against live Postgres.
"""

from __future__ import annotations

import json
import socket
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from connectors.sql_bind import normalize_sql_bind_value  # noqa: E402

PG_HOST, PG_PORT = "localhost", 5432
PG_DSN = {
    "host": PG_HOST,
    "port": PG_PORT,
    "dbname": "dataflow",
    "user": "dataflow",
    "password": "dataflow",
}

PG_FAMILY = [
    "postgresql",
    "postgres",
    "redshift",
    "cockroachdb",
    "timescaledb",
    "alloydb",
    "yugabytedb",
    "citus",
    "supabase",
    "greenplum",
]


@pytest.mark.parametrize("engine", PG_FAMILY)
@pytest.mark.parametrize("ddl", ["JSON", "JSONB"])
def test_no_postgres_engine_hands_the_driver_a_dict(engine, ddl):
    """A dict or list reaching psycopg2 is an immediate ProgrammingError."""
    bound = normalize_sql_bind_value({"k": 1, "n": {"a": [1, 2]}}, ddl, engine=engine)
    assert isinstance(bound, str), f"{engine}/{ddl} would raise can't adapt type 'dict'"
    assert json.loads(bound) == {"k": 1, "n": {"a": [1, 2]}}

    bound_list = normalize_sql_bind_value([1, {"a": 2}], ddl, engine=engine)
    assert isinstance(bound_list, str)
    assert json.loads(bound_list) == [1, {"a": 2}]


def test_document_content_survives_the_text_wire_exactly():
    doc = {
        "unicode": "Café Münich ✓",
        "nested": {"deep": [{"x": 1}, None, True]},
        "empty_obj": {},
        "empty_arr": [],
        "zero": 0,
        "false": False,
    }
    assert json.loads(normalize_sql_bind_value(doc, "JSONB", engine="postgresql")) == doc


def test_json_text_input_is_not_double_encoded():
    """Text that is already JSON must stay one document, not a JSON string of it."""
    bound = normalize_sql_bind_value('{"a":1}', "JSONB", engine="postgresql")
    assert json.loads(bound) == {"a": 1}, f"double-encoded: {bound!r}"


def test_empty_json_refuses_null_invent_and_scalars_stay_loadable():
    import pytest

    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        normalize_sql_bind_value("", "JSONB", engine="postgresql")
    assert normalize_sql_bind_value(None, "JSONB", engine="postgresql") is None
    # A bare scalar is losslessly wrapped so it can still land in a JSON column.
    assert json.loads(normalize_sql_bind_value("hi", "JSONB", engine="postgresql")) == "hi"


def test_sql_variant_envelope_reaches_jsonb_as_text_with_content_intact():
    bound = normalize_sql_bind_value(42, "SQL_VARIANT", engine="postgresql")
    assert isinstance(bound, str)
    assert json.loads(bound) == {"sql_variant_base": "bigint", "value": 42}


def test_sqlserver_native_variant_is_untouched():
    """Only the JSONB-bound engines get the envelope; native stays scalar."""
    assert normalize_sql_bind_value(42, "SQL_VARIANT", engine="sqlserver") == 42


# --------------------------------------------------------------------------
# Live Postgres proof
# --------------------------------------------------------------------------


def _pg_conn():
    try:
        with socket.create_connection((PG_HOST, PG_PORT), timeout=3):
            pass
    except OSError:
        pytest.skip(f"Postgres {PG_HOST}:{PG_PORT} not reachable")
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        return psycopg2.connect(**PG_DSN)
    except Exception as exc:  # credentials differ on this node
        pytest.skip(f"cannot connect to Postgres: {exc}")


def test_bound_document_inserts_into_live_jsonb_and_reads_back_identical():
    conn = _pg_conn()
    conn.autocommit = True
    table = "wave88_jsonb_bind"
    doc = {"k": 1, "nested": {"a": [1, 2]}, "unicode": "Café ✓"}
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} (id text, doc jsonb, arr jsonb)")
            cur.execute(
                f"INSERT INTO {table} (id, doc, arr) VALUES (%s, %s, %s)",
                (
                    "r1",
                    normalize_sql_bind_value(doc, "JSONB", engine="postgresql"),
                    normalize_sql_bind_value([1, 2], "JSONB", engine="postgresql"),
                ),
            )
            cur.execute(f"SELECT doc, arr, pg_typeof(doc)::text FROM {table}")
            got_doc, got_arr, typ = cur.fetchone()

        assert typ == "jsonb", "text parameter must land as jsonb, not text"
        # psycopg2 parses jsonb back into native structures on read.
        assert got_doc == doc
        assert got_arr == [1, 2]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def test_raw_dict_bind_is_what_psycopg2_refuses():
    """Pins the root cause so the native-dict shortcut is not reintroduced."""
    conn = _pg_conn()
    conn.autocommit = True
    table = "wave88_jsonb_refuse"
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} (doc jsonb)")
            with pytest.raises(Exception) as excinfo:
                cur.execute(f"INSERT INTO {table} (doc) VALUES (%s)", ({"k": 1},))
            assert "can't adapt type 'dict'" in str(excinfo.value)
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()
