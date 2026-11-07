"""A constraint violation must quarantine the row, not abandon the chunk.

``is_sql_data_error`` decided whether a failed chunk is retried row by row
under a quarantine policy. It read the *concrete* driver class name, so
psycopg2's ``NotNullViolation`` (a subclass of ``IntegrityError``) and every
Oracle ``ORA-`` constraint code fell through to the re-raise path: the whole
chunk was abandoned with ``rejected_rows == 0``, leaving rows neither written
nor quarantined and the conservation ledger silently short.
"""

from __future__ import annotations

import os
import uuid

import pytest

from connectors.sql_temporal import is_identity_collision_error, is_sql_data_error


class _Error(Exception):
    pass


class _DatabaseError(_Error):
    pass


class _IntegrityError(_DatabaseError):
    pass


class _NotNullViolation(_IntegrityError):
    """Shape of psycopg2.errors.NotNullViolation."""


class _DataError(_DatabaseError):
    pass


def test_driver_subclass_of_integrity_error_is_a_row_contract_error():
    exc = _NotNullViolation(
        'null value in column "label" violates not-null constraint'
    )
    assert is_sql_data_error(exc)


def test_bare_data_error_subclass_is_a_row_contract_error():
    assert is_sql_data_error(_DataError("value out of bounds"))


@pytest.mark.parametrize(
    "message",
    [
        'null value in column "label" of relation "t" violates not-null constraint',
        "Column 'label' cannot be null",
        "Cannot insert the value NULL into column 'label'",
        'new row for relation "t" violates check constraint "t_chk"',
        'insert or update on table "t" violates foreign key constraint "t_fk"',
        "ORA-01400: cannot insert NULL into (\"S\".\"T\".\"LABEL\")",
        "ORA-12899: value too large for column",
        "String or binary data would be truncated",
    ],
)
def test_constraint_text_is_a_row_contract_error(message):
    assert is_sql_data_error(message)


@pytest.mark.parametrize(
    "message",
    [
        'duplicate key value violates unique constraint "t_pkey"',
        "Duplicate entry '1' for key 'PRIMARY'",
        "ORA-00001: unique constraint violated",
        "Cannot insert duplicate key row in object 'dbo.t'",
    ],
)
def test_identity_collisions_still_abort_the_chunk(message):
    """Replay identity is not a row-value defect — quarantining it would let a
    non-idempotent replay report success."""
    assert is_identity_collision_error(message)
    assert not is_sql_data_error(message)
    assert not is_sql_data_error(_IntegrityError(message))


PG = {
    "host": "localhost",
    "port": 5433,
    "database": "dataflow",
    "username": "postgres",
    "password": "postgres",  # nosec B106 - local test container
}


def _connect():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host=PG["host"],
            port=PG["port"],
            dbname=PG["database"],
            user=PG["username"],
            password=PG["password"],
            connect_timeout=3,
        )
    except psycopg2.Error:  # pragma: no cover - env without local postgres
        pytest.skip("local postgres unavailable")
    conn.autocommit = True
    return conn


@pytest.mark.skipif(
    os.environ.get("SKIP_LIVE_PG") == "1", reason="live postgres disabled"
)
def test_not_null_violation_balances_the_conservation_ledger():
    from connectors.postgresql_writer import write_mapped_rows
    from services.value_serializer import SQL_NULL_SENTINEL

    conn = _connect()
    table = f"nn_ledger_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE public."{table}" '
            "(id INTEGER, label VARCHAR(10) NOT NULL)"
        )
    try:
        result = write_mapped_rows(
            host=PG["host"],
            port=PG["port"],
            database=PG["database"],
            username=PG["username"],
            password=PG["password"],
            schema="public",
            connection_string="",
            ssl=False,
            table_name=table,
            headers=["id", "label"],
            data_rows=[["1", "ok"], ["2", SQL_NULL_SENTINEL], ["3", "fine"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "label", "target": "label"},
            ],
            column_types={"id": "INTEGER", "label": "VARCHAR(10)"},
            create_table=False,
            error_policy="quarantine",
            job_id=f"job-{uuid.uuid4().hex[:8]}",
        )
        rows_read = 3
        accounted = (
            result.rows_written + result.rejected_rows + result.rows_skipped
        )
        assert accounted == rows_read, (
            f"ledger unbalanced: written={result.rows_written} "
            f"quarantined={result.rejected_rows} skipped={result.rows_skipped}"
        )
        assert result.rejected_rows == 1
        assert result.rejected_details, "quarantined row carries no evidence"
        assert "not-null" in str(result.rejected_details[0]).lower()

        # Independent read-back: the fit rows landed, the unfit one did not.
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, label FROM public."{table}" ORDER BY id')
            assert cur.fetchall() == [(1, "ok"), (3, "fine")]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()
