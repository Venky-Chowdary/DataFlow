"""D320 (QA batch 1) — PostgreSQL ``varbit`` into MySQL quarantined valid rows.

The create-new planner mapped every PostgreSQL bit string onto MySQL
``BIT(n)``, clamping to ``BIT(64)``: ``varbit(16)`` -> ``BIT(16)``,
unbounded ``varbit`` / ``varbit(100)`` -> ``BIT(64)``. MySQL ``BIT(M)`` is a
fixed-width number (M <= 64): it cannot keep a varying bit string's length
('1010' and '001010' are different values) nor more than 64 bits, so with
the risk signed every value shorter than n was quarantined (QA: 3 of 5).

A fixed ``bit(n)`` with n <= 64 still lands as ``BIT(n)``; every other bit
string lands as the 0/1 digit text the other engines already use
(``VARCHAR(n)``, ``LONGTEXT`` when unbounded or very wide), value for value.
Live: local PostgreSQL -> MariaDB, strict validation, no risk contract.
"""

from __future__ import annotations

import os
import uuid

import pytest

from services.decision_kernel import InventContext, invent_dest_type
from services.type_system import is_lossy_coercion
from tests.test_d161_double_create_new_confidence import _isolated  # noqa: F401


@pytest.mark.parametrize(
    "source_type,planned",
    [
        ("BIT VARYING(16)", "VARCHAR(16)"),
        ("varbit(100)", "VARCHAR(100)"),
        ("BIT VARYING", "LONGTEXT"),
        ("bit varying(5000)", "LONGTEXT"),
        ("BIT(16)", "BIT(16)"),
        ("BIT(64)", "BIT(64)"),
        ("BIT(100)", "VARCHAR(100)"),
    ],
)
@pytest.mark.parametrize("db", ["mysql", "mariadb"])
def test_planned_mysql_type_holds_every_source_bit_string(db, source_type, planned):
    got = invent_dest_type(source_type, dest_db=db, context=InventContext.CREATE_NEW)
    assert got == planned
    assert not is_lossy_coercion(source_type, got, dest_db=db)


def _mysql_cfg() -> dict:
    return {
        "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
        "user": os.environ.get("DATAFLOW_TEST_MYSQL_USER", "dataflow"),
        "password": os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD", "dataflow"),
        "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    }


_VALUES = {
    # QA's shape: five rows, short and full-width values, one NULL.
    "varbit(16)": ["1", "1010", "1010101010101010", "0000111100001111", None],
    "varbit": ["1", "0" * 65, "10" * 50, "001", None],
    "varbit(100)": ["1", "1" * 70, "0010", None, "0" * 100],
    "bit(16)": ["1010101010101010", "0000000000000001", "1111111111111111", None, "0" * 16],
}


@pytest.mark.parametrize("decl", sorted(_VALUES))
def test_pg_bit_strings_land_in_mysql_value_for_value(_isolated, decl):  # noqa: F811
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    from conftest import pg_test_cfg

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    pg_cfg, my_cfg = pg_test_cfg(), _mysql_cfg()
    try:
        pg = psycopg2.connect(
            host=pg_cfg["host"], port=pg_cfg["port"], dbname=pg_cfg["database"],
            user=pg_cfg["username"], password=pg_cfg["password"], connect_timeout=3,
        )
        my = pymysql.connect(autocommit=True, connect_timeout=3, **my_cfg)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL / MySQL not reachable: {exc}")
    pg.autocommit = True
    src = f"d320_s_{uuid.uuid4().hex[:8]}"
    dst = f"d320_d_{uuid.uuid4().hex[:8]}"
    values = _VALUES[decl]
    try:
        with pg.cursor() as cur:
            cur.execute(f"CREATE TABLE public.{src} (id int PRIMARY KEY, bv {decl})")
            for i, v in enumerate(values):
                cur.execute(f"INSERT INTO public.{src} VALUES (%s, %s::{decl})", (i, v))
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="postgresql", schema="public", table=src,
                host=pg_cfg["host"], port=pg_cfg["port"], database=pg_cfg["database"],
                username=pg_cfg["username"], password=pg_cfg["password"],
            ),
            destination=EndpointConfig(
                kind="database", format="mysql", table=dst, host=my_cfg["host"],
                port=my_cfg["port"], database=my_cfg["database"],
                username=my_cfg["user"], password=my_cfg["password"],
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
        )
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        assert result.records_transferred == len(values)
        with my.cursor() as cur:
            if decl == "bit(16)":
                cur.execute(f"SELECT id, LPAD(BIN(bv), 16, '0') FROM `{dst}` ORDER BY id")
            else:
                cur.execute(f"SELECT id, bv FROM `{dst}` ORDER BY id")
            landed = [r[1] for r in cur.fetchall()]
        assert landed == values
    finally:
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{src}")
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
            cur.execute(f"SHOW TABLES LIKE '{dst}%'")
            for (name,) in cur.fetchall():
                cur.execute(f"DROP TABLE IF EXISTS `{name}`")
        pg.close()
        my.close()


def test_a_declared_bit_string_is_not_judged_a_card_number_by_its_digits():
    """16-bit masks (all-zero passes Luhn) held the run for a PCI review."""
    from services.compliance_guard import detect_pii_fields

    rows = [{"bv": v} for v in _VALUES["bit(16)"] if v]
    for declared in ("bit(16)", "BIT VARYING(16)", "varbit"):
        report = detect_pii_fields(["bv"], rows, column_types={"bv": declared})
        assert report["sensitive_fields"] == [], declared
    # The same digits in a text column are still judged by their content.
    assert detect_pii_fields(["bv"], rows, column_types={"bv": "text"})["sensitive_fields"] == ["bv"]
