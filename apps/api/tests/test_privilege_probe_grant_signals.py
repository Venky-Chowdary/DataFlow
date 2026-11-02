"""Grants honesty: measure each capability, never infer absence from blindness.

The Postgres probe passed ``schema.table`` to ``has_table_privilege`` as a bare
name. That argument is parsed as an SQL identifier, so any mixed-case or quoted
destination folded to lower case and raised ``undefined_table`` — a real,
INSERT-granted table was reported as ``status="unavailable"``, discarding the
schema privileges measured a line earlier. A role without schema USAGE hit the
same cliff, which meant "I am not allowed to look" and "it is not there" were
indistinguishable to every caller.
"""

from __future__ import annotations

import os
import uuid

import pytest

ADMIN = {
    "host": "localhost",
    "port": 5433,
    "database": "dataflow",
    "user": "postgres",
    "password": "postgres",  # nosec B106 - local test container
}
ROLE_PW = "probe_pw"  # nosec B105 - local test container

pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_LIVE_PG") == "1", reason="live postgres disabled"
)


def _admin():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=ADMIN["host"],
            port=ADMIN["port"],
            dbname=ADMIN["database"],
            user=ADMIN["user"],
            password=ADMIN["password"],
            connect_timeout=3,
        )
    except psycopg2.Error:  # pragma: no cover - env without local postgres
        pytest.skip("local postgres unavailable")
    conn.autocommit = True
    return conn


@pytest.fixture()
def restricted_schema():
    """A schema with a MixedCase table, an INSERT-granted role and a blind role."""
    conn = _admin()
    suffix = uuid.uuid4().hex[:6]
    schema = f"gsig_{suffix}"
    reader = f"gsig_reader_{suffix}"
    blind = f"gsig_blind_{suffix}"
    with conn.cursor() as cur:
        for role in (reader, blind):
            cur.execute(
                "DO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s) "
                "THEN EXECUTE format('CREATE ROLE %%I LOGIN PASSWORD %%L', %s, %s); "
                "END IF; END$$;",
                (role, role, ROLE_PW),
            )
        cur.execute(f'CREATE SCHEMA "{schema}"')
        cur.execute(f'CREATE TABLE "{schema}"."MixedCase" (id INTEGER)')
        cur.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"')
        cur.execute(f'GRANT INSERT ON "{schema}"."MixedCase" TO "{reader}"')
    try:
        yield conn, schema, reader, blind
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            for role in (reader, blind):
                cur.execute(f'DROP OWNED BY "{role}" CASCADE')
                cur.execute(f'DROP ROLE IF EXISTS "{role}"')
        conn.close()


def _probe(role: str, schema: str, table: str, table_exists: bool):
    from services.destination_privilege_probe import probe_destination_privileges

    return probe_destination_privileges(
        "postgresql",
        host=ADMIN["host"],
        port=ADMIN["port"],
        database=ADMIN["database"],
        schema=schema,
        table=table,
        username=role,
        password=ROLE_PW,
        table_exists=table_exists,
    )


def test_mixed_case_table_privileges_are_measured(restricted_schema):
    _conn, schema, reader, _blind = restricted_schema
    result = _probe(reader, schema, "MixedCase", True)
    assert result.status == "ok", result.detail
    assert result.can_write is True
    assert result.signals["table_visible_in_catalog"] is True
    assert result.signals["table_insert_grant"] is True
    assert result.signals["table_owner"] is False


def test_blind_role_reports_unmeasured_not_absent(restricted_schema):
    """No USAGE: existence and write access are both unknown, not denied."""
    _conn, schema, _reader, blind = restricted_schema
    result = _probe(blind, schema, "MixedCase", True)
    assert result.status == "unavailable"
    assert result.can_write is None
    assert result.signals["schema_usage"] is False
    assert result.signals["table_visible_in_catalog"] is None
    assert "not proof the object is absent" in result.detail


def test_missing_table_is_reported_as_unresolvable(restricted_schema):
    _conn, schema, reader, _blind = restricted_schema
    result = _probe(reader, schema, "no_such_table", True)
    assert result.status == "unavailable"
    assert result.can_write is None
    assert result.signals["table_visible_in_catalog"] is False
    # Schema-level measurement survives an unresolvable table.
    assert result.signals["schema_usage"] is True
    assert result.can_create_table is False


def test_create_new_path_reports_schema_signals(restricted_schema):
    _conn, schema, reader, _blind = restricted_schema
    result = _probe(reader, schema, "brand_new", False)
    assert result.status == "denied"
    assert result.can_create_table is False
    assert result.signals["schema_usage"] is True
    assert result.signals["schema_create"] is False
    # Never claimed as measured when the table was never inspected.
    assert result.signals["table_insert_grant"] is None
