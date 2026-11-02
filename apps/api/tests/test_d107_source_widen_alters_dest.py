"""D107 — a declared source type widening must reach an existing destination.

Fivetran/Airbyte treat ``VARCHAR(10) -> VARCHAR(200)`` and
``NUMERIC(8,2) -> NUMERIC(14,4)`` source drift as safe changes: the destination
column is ALTERed wider after operator acknowledgement and the rows load
exactly. Before this fix the drift was graded hard-breaking ``narrow_type``
against the *current* destination DDL, every later run stayed refused with
"collapse fidelity on write", and the only offered routes were data loss
(CAST_AND_CONTINUE) or a manual rebuild — even with
``schema_drift_acknowledged=True``.
"""

from __future__ import annotations

import socket
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG = dict(
    host="localhost",
    port=5432,
    database="dataflow",
    username="dataflow",
    password="dataflow",
)
# psycopg2 takes user=, the engine takes username=
PG_CONN = dict(
    host="localhost",
    port=5432,
    database="dataflow",
    user="dataflow",
    password="dataflow",
)


def _pg():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL not reachable on localhost:5432")
    import psycopg2

    return psycopg2.connect(**PG_CONN)


def _request(src_table: str, dst_table: str, **extra) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="postgresql",
            schema="public",
            table=src_table,
            **PG,
        ),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            schema="public",
            table=dst_table,
            **PG,
        ),
        sync_mode="upsert",
        stream_contracts=[
            {
                "name": "payments",
                "sync_mode": "upsert",
                "primary_key": "id",
                "selected": True,
            }
        ],
        skip_preflight=True,
        **extra,
    )


def _col_type(cur, table: str, col: str) -> str:
    cur.execute(
        "SELECT data_type || COALESCE('(' || character_maximum_length || ')', "
        "CASE WHEN data_type IN ('numeric','decimal') "
        "THEN '(' || numeric_precision || ',' || numeric_scale || ')' ELSE '' END) "
        "FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s AND column_name=%s",
        (table, col),
    )
    row = cur.fetchone()
    return (row[0] if row else "") or ""


def test_source_widen_is_additive_not_hard_breaking_drift():
    """Drift kernel: a widen-into-existing-dest pair is additive widen_type,
    never hard narrow_type, and a same-family INT4->INT8 source widen is
    detected at all."""
    from services.schema_drift import detect_schema_drift

    drift = detect_schema_drift(
        source_columns=["id", "name", "amt", "qty"],
        source_schema={
            "id": "INT4",
            "name": "VARCHAR(200)",
            "amt": "NUMERIC(14,4)",
            "qty": "INT8",
        },
        target_columns=["id", "name", "amt", "qty"],
        target_schema={
            "id": "INT4",
            "name": "VARCHAR(10)",
            "amt": "NUMERIC(8,2)",
            "qty": "INT4",
        },
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
            {"source": "amt", "target": "amt"},
            {"source": "qty", "target": "qty"},
        ],
        previous_source_columns=["id", "name", "amt", "qty"],
        previous_source_schema={
            "id": "INT4",
            "name": "VARCHAR(10)",
            "amt": "NUMERIC(8,2)",
            "qty": "INT4",
        },
        previous_primary_key=["id"],
        live_primary_key=["id"],
        table_exists=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    evolution = drift.get("schema_evolution") or {}
    hard = [
        str(b.get("column"))
        for b in (evolution.get("hard_breaking") or [])
    ]
    # Every widened column is additive — nothing hard-breaking remains.
    assert "name" not in hard and "amt" not in hard and "qty" not in hard, hard
    additive_widens = {
        str(a.get("column"))
        for a in (evolution.get("additive") or [])
        if a.get("kind") == "widen_type"
    }
    assert {"name", "amt", "qty"} <= additive_widens, additive_widens
    from services.schema_drift import execute_must_pause

    # A review action still surfaces to the operator but must not hard-refuse
    # an acknowledged run the way a real narrow does.
    assert not evolution.get("should_pause") or not execute_must_pause(
        evolution, schema_drift_acknowledged=True
    )


def test_pg_source_widen_acknowledged_alters_destination_and_loads_exact():
    _pg()
    import psycopg2

    src_table = f"pg_src_d107_{uuid.uuid4().hex[:8]}"
    dst_table = f"pg_dst_d107_{uuid.uuid4().hex[:8]}"

    conn = psycopg2.connect(**PG_CONN)
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
        cur.execute(
            f'CREATE TABLE public."{src_table}" '
            f"(id INT PRIMARY KEY, name VARCHAR(10), amt NUMERIC(8,2), qty INT)"
        )
        cur.execute(
            f'CREATE TABLE public."{dst_table}" '
            f"(id INT PRIMARY KEY, name VARCHAR(10), amt NUMERIC(8,2), qty INT)"
        )
        cur.execute(
            f'INSERT INTO public."{src_table}" (id, name, amt, qty) '
            f"VALUES (1, %s, %s, %s), (2, %s, %s, %s)",
            ("short", Decimal("12.34"), 10, "small", Decimal("56.78"), 20),
        )
        conn.commit()
    conn.close()

    request = _request(
        src_table,
        dst_table,
        schema_drift_acknowledged=True,
    )
    engine = UniversalTransferEngine()
    result1 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result1.success is True, result1.error
    assert result1.records_transferred == 2

    # Widen the declared source types, then write rows in the wider ranges.
    conn = psycopg2.connect(**PG_CONN)
    with conn.cursor() as cur:
        cur.execute(
            f'ALTER TABLE public."{src_table}" '
            f"ALTER COLUMN name TYPE VARCHAR(200)"
        )
        cur.execute(
            f'ALTER TABLE public."{src_table}" '
            f"ALTER COLUMN amt TYPE NUMERIC(14,4)"
        )
        cur.execute(
            f'ALTER TABLE public."{src_table}" '
            f"ALTER COLUMN qty TYPE BIGINT"
        )
        cur.execute(
            f'UPDATE public."{src_table}" SET name=%s, amt=%s, qty=%s '
            f"WHERE id = 1",
            ("a-much-longer-name", Decimal("1234567890.1234"), 5000000000),
        )
        cur.execute(
            f'INSERT INTO public."{src_table}" (id, name, amt, qty) '
            f"VALUES (3, %s, %s, %s)",
            ("wide", Decimal("9999999999.9999"), 9000000000),
        )
        conn.commit()
    conn.close()

    result2 = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result2.success is True, result2.error
    assert result2.records_transferred == 3

    conn = psycopg2.connect(**PG_CONN)
    try:
        with conn.cursor() as cur:
            # The destination was ALTERed wider — the wider ranges landed.
            assert _col_type(cur, dst_table, "name") == "character varying(200)"
            assert _col_type(cur, dst_table, "amt") == "numeric(14,4)"
            assert _col_type(cur, dst_table, "qty") == "bigint"
            cur.execute(
                f'SELECT id, name, amt, qty FROM public."{dst_table}" ORDER BY id'
            )
            rows = cur.fetchall()
            assert rows[0] == (
                1, "a-much-longer-name", Decimal("1234567890.1234"), 5000000000
            )
            assert rows[1][0] == 2
            assert rows[2][0] == 3
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
            conn.commit()
        conn.close()


def test_pg_true_narrow_still_refuses_closed():
    """A real narrowing (wider source declared but dest cannot hold the
    values) must still fail closed — the widen path must not absorb it."""
    _pg()
    import psycopg2

    src_table = f"pg_src_d107n_{uuid.uuid4().hex[:8]}"
    dst_table = f"pg_dst_d107n_{uuid.uuid4().hex[:8]}"

    conn = psycopg2.connect(**PG_CONN)
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
        # Cross-family change: a TEXT source column feeding a numeric dest is
        # not an ALTERable widen — the dest cannot be widened to hold text.
        cur.execute(
            f'CREATE TABLE public."{src_table}" '
            f"(id INT PRIMARY KEY, amt TEXT)"
        )
        cur.execute(
            f'CREATE TABLE public."{dst_table}" '
            f"(id INT PRIMARY KEY, amt NUMERIC(8,2))"
        )
        cur.execute(
            f'INSERT INTO public."{src_table}" (id, amt) VALUES (1, %s)',
            ("not-a-number",),
        )
        conn.commit()
    conn.close()

    engine = UniversalTransferEngine()
    try:
        result = engine.execute_tracked(
            _request(src_table, dst_table, schema_drift_acknowledged=True),
            uuid.uuid4().hex[:24],
        )
        assert result.success is False, (
            "TEXT->NUMERIC must not be treated as an ALTERable widen"
        )
        conn = psycopg2.connect(**PG_CONN)
        with conn.cursor() as cur:
            assert _col_type(cur, dst_table, "amt").startswith("numeric(8")
    finally:
        conn = psycopg2.connect(**PG_CONN)
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
            conn.commit()
        conn.close()
