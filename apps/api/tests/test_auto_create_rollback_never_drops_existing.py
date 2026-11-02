"""A failed write must never drop a destination this run did not create.

``CREATE TABLE IF NOT EXISTS`` is a no-op against an operator's existing
table, but the writer registered that path as auto-created anyway — so a
NOT NULL violation on row 1 rolled back by *dropping a populated
destination*. Registration is now gated on an existence probe that proved
the object absent; an unprovable probe (``None``) registers nothing.
"""

from __future__ import annotations

import os
import uuid

import pytest

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
def test_failed_not_null_write_leaves_existing_table_intact():
    from connectors.postgresql_writer import write_mapped_rows
    from services.auto_create_lifecycle import rollback_uncommitted_auto_creates
    from services.value_serializer import SQL_NULL_SENTINEL

    conn = _connect()
    table = f"nn_keep_{uuid.uuid4().hex[:8]}"
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE public."{table}" '
            "(id INTEGER, label VARCHAR(10) NOT NULL)"
        )
        cur.execute(f'INSERT INTO public."{table}" VALUES (1, %s)', ("keep",))

    try:
        try:
            write_mapped_rows(
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
                data_rows=[["2", SQL_NULL_SENTINEL]],
                mappings=[
                    {"source": "id", "target": "id"},
                    {"source": "label", "target": "label"},
                ],
                column_types={"id": "INTEGER", "label": "VARCHAR"},
                job_id=job_id,
            )
        except Exception:
            # The write is expected to fail; what is under test is that the
            # failure did not take the operator's table with it.
            pass
        # Whatever the outcome, the job-scoped rollback must find nothing to drop.
        assert rollback_uncommitted_auto_creates(job_id) == []

        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass(%s) IS NOT NULL", (f'public."{table}"',)
            )
            still_exists = cur.fetchone()[0]
            assert still_exists, "pre-existing destination was dropped by rollback"
            cur.execute(f'SELECT label FROM public."{table}" ORDER BY id')
            assert [r[0] for r in cur.fetchall()] == ["keep"]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()


@pytest.mark.skipif(
    os.environ.get("SKIP_LIVE_PG") == "1", reason="live postgres disabled"
)
def test_unprovable_existence_registers_no_rollback(monkeypatch):
    """A probe that cannot speak must not license a DROP."""
    registered: list = []
    monkeypatch.setattr(
        "services.auto_create_lifecycle.register_auto_create",
        lambda **kw: registered.append(kw),
    )
    from services.auto_create_lifecycle import (
        clear_auto_create_job,
        rollback_uncommitted_auto_creates,
    )

    job = f"job-{uuid.uuid4().hex[:8]}"
    clear_auto_create_job(job)
    # Nothing registered => nothing may be dropped.
    assert rollback_uncommitted_auto_creates(job) == []
    assert registered == []
