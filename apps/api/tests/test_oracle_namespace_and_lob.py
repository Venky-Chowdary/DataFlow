"""Oracle identifier case, owner isolation, and LOB/FLOAT carriers.

A destination probe that adopts another owner's table reports a create-new
target as "already exists", which skips the fidelity DDL entirely — so
``strict_namespace`` must reach the Oracle introspector, not just PG/MySQL.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.schema_introspect import introspect_schema

_ORA = {
    "host": os.environ.get("ORA_HOST", "127.0.0.1"),
    "port": int(os.environ.get("ORA_PORT", "1521")),
    "database": os.environ.get("ORA_DB", "FREEPDB1"),
    "username": os.environ.get("ORA_USER", "system"),
    "password": os.environ.get("ORA_PASSWORD", "dataflow"),
}


def _oracle_up() -> bool:
    try:
        with socket.create_connection((_ORA["host"], _ORA["port"]), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _oracle_up(), reason="live Oracle not reachable")


def _introspect(table: str, schema: str, *, strict: bool = False) -> dict:
    return introspect_schema(
        "oracle",
        host=_ORA["host"], port=_ORA["port"], database=_ORA["database"],
        username=_ORA["username"], password=_ORA["password"],
        schema=schema, table=table, strict_namespace=strict,
    )


@pytest.fixture()
def oracle_tables():
    oracledb = pytest.importorskip("oracledb")

    conn = oracledb.connect(
        user=_ORA["username"], password=_ORA["password"],
        dsn=f"{_ORA['host']}:{_ORA['port']}/{_ORA['database']}",
    )
    sfx = uuid.uuid4().hex[:6]
    names = {
        "upper": f"ORA_UP_{sfx.upper()}",
        "lower": f"ora_low_{sfx}",
        "mixed": f"Ora_Mix_{sfx}",
        "lob": f"ORA_LOB_{sfx.upper()}",
    }
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {names['upper']} (ID NUMBER PRIMARY KEY, NOTE VARCHAR2(40))")
    cur.execute(f'CREATE TABLE "{names["lower"]}" ("id" NUMBER PRIMARY KEY, "note" VARCHAR2(40))')
    cur.execute(f'CREATE TABLE "{names["mixed"]}" ("Id" NUMBER PRIMARY KEY, "Note" VARCHAR2(40))')
    cur.execute(
        f"CREATE TABLE {names['lob']} (ID NUMBER PRIMARY KEY, BODY CLOB, BLOB_COL BLOB, "
        "F FLOAT, BF BINARY_FLOAT, BD BINARY_DOUBLE, N124 NUMBER(12,4))"
    )
    cur.execute(f"INSERT INTO {names['lob']} (ID, BODY) VALUES (1, :1)", ["x" * 5000])
    conn.commit()
    try:
        yield names
    finally:
        for name in names.values():
            for stmt in (f'DROP TABLE "{name}" PURGE', f"DROP TABLE {name} PURGE"):
                try:
                    conn.cursor().execute(stmt)
                    break
                except oracledb.DatabaseError:
                    continue
        conn.close()


def test_quoted_lowercase_and_mixed_case_tables_resolve(oracle_tables):
    lower = _introspect(oracle_tables["lower"], "SYSTEM")
    mixed = _introspect(oracle_tables["mixed"], "SYSTEM")
    assert [c["name"] for c in lower["columns"]] == ["id", "note"]
    assert [c["name"] for c in mixed["columns"]] == ["Id", "Note"]


def test_strict_destination_probe_does_not_adopt_another_owners_table(oracle_tables):
    healed = _introspect(oracle_tables["upper"], "SOME_OTHER_OWNER")
    strict = _introspect(oracle_tables["upper"], "SOME_OTHER_OWNER", strict=True)

    # A source probe may still heal, but only while reporting the owner it read.
    if healed.get("columns"):
        assert healed["schema"] == "SYSTEM"
    assert not strict.get("columns"), (
        "destination probe adopted a table from an owner the operator did not choose"
    )


def test_lob_and_float_carriers_are_width_honest(oracle_tables):
    info = _introspect(oracle_tables["lob"], "SYSTEM")
    types = {c["name"]: c["inferred_type"] for c in info["columns"]}

    assert types["BF"] == "BINARY_FLOAT"
    assert types["BD"] == "BINARY_DOUBLE"
    assert types["N124"] == "DECIMAL(12,4)"
    # Oracle FLOAT is binary precision (default 126 bits) — never a 4-byte carrier.
    assert types["F"] != "FLOAT32"
    assert "FLOAT32" not in types.values()
    # A LOB is not an identity candidate.
    assert info["primary_key_columns"] == ["ID"]
    for uk in info.get("unique_keys") or []:
        cols = uk.get("columns") if isinstance(uk, dict) else uk
        assert "BODY" not in (cols or [])


def test_clob_column_does_not_break_sample_refinement(oracle_tables):
    info = _introspect(oracle_tables["lob"], "SYSTEM")
    assert info["ok"], info.get("error")
    assert {c["name"] for c in info["columns"]} >= {"BODY", "BLOB_COL"}
