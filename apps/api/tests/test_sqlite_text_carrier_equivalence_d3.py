"""SQLite's one untyped TEXT carrier is not a fidelity collapse (D3).

SQLite gives ``CHAR(36)``, ``VARCHAR(36)`` and ``TEXT`` the same TEXT affinity:
the declared length is parsed and discarded, nothing is blank-padded, and no
carrier the dialect can spell enforces a UUID domain. Grading ``UUID → TEXT``
there as a narrowing described a carrier the engine does not have — the value is
carried byte-exact and no alternative DDL enforces more. The missing domain is
still stated to the operator as a warn-level carrier note; what does not apply
is the fail-closed collapse and the Migration Risk Contract it demanded.

Every non-SQLite dialect keeps its previous grading.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import tempfile
import uuid

import pytest

from services.type_coercion_validator import (
    coercion_blocks_transfer,
    validate_mapping_coercions,
)
from services.create_new_risk_stamp import create_new_risk_locks_review
from services.dest_dialect_facts import dest_string_length_is_unenforced
from services.type_system import (
    assess_create_new_type_risk,
    ddl_type,
    is_lossy_coercion,
    is_precision_collapse_coercion,
    uuid_carrier_is_dialect_equivalent,
    uuid_would_collapse,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

#: Routes the fidelity gate used to refuse on SQLite.
_SQLITE_TEXT_ROUTES = (
    ("UUID", "TEXT"),
    ("UUID", "VARCHAR"),
    ("UUID", "CLOB"),
    ("CHAR(36)", "TEXT"),
    ("UNIQUEIDENTIFIER", "TEXT"),
)

_OTHER_DIALECTS = ("postgresql", "mysql", "snowflake", "bigquery", "sqlserver")


@pytest.mark.parametrize("source_type,target_type", _SQLITE_TEXT_ROUTES)
def test_sqlite_text_carrier_is_not_lossy_or_collapse(source_type, target_type):
    assert uuid_would_collapse(source_type, target_type, dest_db="sqlite") is False
    assert is_lossy_coercion(source_type, target_type, dest_db="sqlite") is False
    assert (
        is_precision_collapse_coercion(source_type, target_type, dest_db="sqlite")
        is False
    )


@pytest.mark.parametrize("source_type,target_type", _SQLITE_TEXT_ROUTES)
@pytest.mark.parametrize("dest_db", _OTHER_DIALECTS)
def test_other_dialects_keep_the_collapse(source_type, target_type, dest_db):
    """Only the dynamically typed engine is excused — nothing else moves."""
    assert (
        uuid_would_collapse(source_type, target_type, dest_db=dest_db)
        is uuid_would_collapse(source_type, target_type)
    )
    if source_type != "CHAR(36)":
        assert uuid_would_collapse(source_type, target_type, dest_db=dest_db) is True


def test_bounded_uuid_wire_still_passes_everywhere():
    for dest_db in ("sqlite", *_OTHER_DIALECTS):
        assert uuid_would_collapse("UUID", "VARCHAR(36)", dest_db=dest_db) is False
        assert is_lossy_coercion("UUID", "CHAR(36)", dest_db=dest_db) is False


def test_genuinely_narrower_sqlite_route_still_blocks():
    """Equivalence is about text carriers, not about narrowing in general."""
    assert is_lossy_coercion("UUID", "INTEGER", dest_db="sqlite") is True
    assert is_lossy_coercion("DECIMAL(20,9)", "INTEGER", dest_db="sqlite") is True
    assert uuid_carrier_is_dialect_equivalent("UUID", "INTEGER", dest_db="sqlite") is False
    assert uuid_carrier_is_dialect_equivalent("VARCHAR(64)", "TEXT", dest_db="sqlite") is False


def test_sqlite_uuid_create_new_risk_is_a_warn_note_that_does_not_lock():
    risks = assess_create_new_type_risk("UUID", "TEXT", destination_db_type="sqlite")
    kinds = {r["kind"] for r in risks}
    assert kinds == {"uuid_carrier_equivalent"}
    for risk in risks:
        assert risk["severity"] == "warn"
        assert create_new_risk_locks_review(risk) is False
        assert "not enforced" in risk["message"]


def test_sqlite_fixed_width_create_new_risk_states_the_missing_padding():
    risks = assess_create_new_type_risk("CHAR(36)", "TEXT", destination_db_type="sqlite")
    kinds = {r["kind"] for r in risks}
    assert kinds == {"fixed_width_not_enforced"}
    for risk in risks:
        assert risk["severity"] == "warn"
        assert create_new_risk_locks_review(risk) is False


def test_non_sqlite_uuid_create_new_still_blocks_with_domain_note():
    risks = assess_create_new_type_risk("UUID", "TEXT", destination_db_type="snowflake")
    kinds = {r["kind"] for r in risks}
    assert "uuid_domain" in kinds
    assert any(r["severity"] == "block" for r in risks)


def _create_new_mapping(source: str, target: str, target_type: str) -> dict:
    return {
        "source": source,
        "target": target,
        "create_new": True,
        "target_type": target_type,
        "confidence": 1.0,
    }


def test_preflight_does_not_demand_a_risk_contract_on_sqlite():
    mappings = [
        _create_new_mapping("uid", "uid", "TEXT"),
        _create_new_mapping("code", "code", "TEXT"),
    ]
    issues = validate_mapping_coercions(
        mappings,
        source_types={"uid": "UUID", "code": "CHAR(36)"},
        target_types={},
        dest_db_type="sqlite",
        dest_table_exists=False,
    )
    assert issues == []
    assert coercion_blocks_transfer(issues) is False


def test_preflight_still_demands_a_contract_on_snowflake():
    issues = validate_mapping_coercions(
        [_create_new_mapping("uid", "uid", "TEXT")],
        source_types={"uid": "UUID"},
        target_types={},
        dest_db_type="snowflake",
        dest_table_exists=False,
    )
    assert coercion_blocks_transfer(issues) is True


def test_sqlite_invent_for_uuid_is_text():
    assert ddl_type("sqlite", "UUID").upper() == "TEXT"


def test_sqlite_declared_string_length_is_measured_as_unenforced():
    """The claim the rule rests on, measured on a real SQLite file."""
    assert dest_string_length_is_unenforced("sqlite") is True
    assert dest_string_length_is_unenforced("postgresql") is False
    long_value = "x" * 64
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "affinity.db")
        con = sqlite3.connect(db)
        try:
            con.execute("CREATE TABLE t (a VARCHAR(8), b CHAR(36), c TEXT)")
            con.execute("INSERT INTO t VALUES (?, ?, ?)", (long_value, "ab", "ab"))
            a, b, c = con.execute("SELECT a, b, c FROM t").fetchone()
        finally:
            con.close()
    # No truncation into VARCHAR(8), and no blank padding into CHAR(36).
    assert a == long_value
    assert b == "ab"
    assert c == "ab"


def _pg_source(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        connection_string="postgresql://dataflow:dataflow@localhost:5432/dataflow",
        database="dataflow",
        table=table,
    )


def _sqlite_dest(path: str, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=path,
        database=path,
        table=table,
    )


_UIDS = (
    "550e8400-e29b-41d4-a716-446655440000",
    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
)


def test_live_postgresql_uuid_lands_in_a_new_sqlite_table_byte_exact():
    """Real transfer: UUID/CHAR(36) → invented SQLite TEXT, re-read independently."""
    try:
        with socket.create_connection(("localhost", 5432), timeout=2):
            pass
    except OSError as exc:  # pragma: no cover — environment dependent
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    import psycopg2

    src_table = "d3_uuid_src_" + uuid.uuid4().hex[:8]
    dst_table = "d3_uuid_dst_" + uuid.uuid4().hex[:8]
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as handle:
        path = handle.name
    os.remove(path)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {src_table} "
                "(id INT PRIMARY KEY, uid UUID, code CHAR(36))"
            )
            for i, value in enumerate(_UIDS, start=1):
                cur.execute(
                    f"INSERT INTO {src_table} VALUES (%s, %s, %s)",
                    (i, value, value),
                )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(
            TransferRequest(
                source=_pg_source(src_table),
                destination=_sqlite_dest(path, dst_table),
                sync_mode="full_refresh_overwrite",
                stream_contracts=[{
                    "name": "data",
                    "sync_mode": "full_refresh_overwrite",
                    "primary_key": "id",
                    "selected": True,
                }],
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        assert result.records_transferred == len(_UIDS)

        # Independent re-read of the destination the product just created.
        dest = sqlite3.connect(path)
        try:
            declared = {
                row[1]: row[2]
                for row in dest.execute(f"PRAGMA table_info({dst_table})")
            }
            assert declared["uid"].upper() == "TEXT"
            rows = dest.execute(
                f"SELECT id, typeof(uid), uid, typeof(code), code "
                f"FROM {dst_table} ORDER BY id"
            ).fetchall()
        finally:
            dest.close()
        assert [r[0] for r in rows] == [1, 2]
        for (_id, uid_kind, uid_value, code_kind, code_value), expected in zip(
            rows, _UIDS
        ):
            assert uid_kind == "text"
            assert uid_value == expected
            assert code_kind == "text"
            assert code_value.strip() == expected

        # The source is untouched by the read.
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, uid::text FROM {src_table} ORDER BY id")
            assert cur.fetchall() == [(1, _UIDS[0]), (2, _UIDS[1])]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src_table}")
        conn.close()
        if os.path.exists(path):
            os.remove(path)
