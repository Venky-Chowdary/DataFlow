"""D50 — PostgreSQL ``timetz`` was created as ``time`` on PG -> PG.

``ddl_carrier_type`` collapsed every LOGICAL_TIME to bare ``TIME``, so the
create-new stamp for a ``TIMETZ`` source dropped the offset and the fidelity
gate then (correctly) blocked DataFlow's own ``TIMETZ -> TIME``.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from services.type_system import ddl_carrier_type, ddl_type


@pytest.mark.parametrize(
    ("src", "carrier", "pg_ddl"),
    [
        ("TIMETZ", "TIMETZ", "TIME WITH TIME ZONE"),
        ("TIMETZ(3)", "TIMETZ(3)", "TIME(3) WITH TIME ZONE"),
        ("time(3) with time zone", "TIMETZ(3)", "TIME(3) WITH TIME ZONE"),
        ("TIME", "TIME", "TIME"),
    ],
)
def test_time_carrier_keeps_offset_polarity(src, carrier, pg_ddl):
    assert ddl_carrier_type(src) == carrier
    assert ddl_type("postgresql", ddl_carrier_type(src)) == pg_ddl


_PG = {
    "host": os.getenv("PGHOST", ""),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", ""),
    "username": os.getenv("PGUSER", ""),
    "password": os.getenv("PGPASSWORD", ""),
}


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(
    not (_PG["host"] and _reachable(_PG["host"], _PG["port"])),
    reason="live PostgreSQL: set PG* (reachable)",
)
def test_pg_to_pg_timetz_lands_as_timetz_with_offsets(monkeypatch):
    import psycopg2

    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    src, dst = "d50s_" + uuid.uuid4().hex[:10], "d50d_" + uuid.uuid4().hex[:10]
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    try:
        cur.execute(f"CREATE TABLE {src} (id BIGINT PRIMARY KEY, tz TIMETZ, tz3 TIME(3) WITH TIME ZONE)")  # nosec B608
        cur.execute(
            f"INSERT INTO {src} VALUES (1, '10:00:00+05:30', '10:00:00.123-04:00'), (2, NULL, NULL)"  # nosec B608
        )
        pg.commit()
        job_id = "d50" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="postgresql", table=src, **_PG),
                destination=EndpointConfig(kind="database", format="postgresql", table=dst, **_PG),
                sync_mode="overwrite",
                validation_mode="strict",
            ),
            job_id,
        )
        assert result.success, result.error
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = %s AND column_name IN ('tz', 'tz3') ORDER BY column_name",
            (dst,),
        )
        assert cur.fetchall() == [("tz", "time with time zone"), ("tz3", "time with time zone")]
        cur.execute(f"SELECT tz::text, tz3::text FROM {dst} ORDER BY id")  # nosec B608
        assert cur.fetchall() == [("10:00:00+05:30", "10:00:00.123-04"), (None, None)]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")  # nosec B608
        cur.execute(f"DROP TABLE IF EXISTS {dst}")  # nosec B608
        pg.commit()
        pg.close()
