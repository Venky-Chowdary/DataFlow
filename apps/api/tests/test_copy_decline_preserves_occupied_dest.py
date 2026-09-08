"""A declined identity COPY must leave an occupied destination untouched.

The cross-engine COPY fast paths clean up after a failed load so a re-run
starts from the state they found: a table they created is dropped, a table
that already existed is truncated. That truncate was gated on ``pk_map is
None`` — but the *decline* for "append into a non-empty dest without a key"
raises inside the same ``try`` with exactly that shape, so every declined
append wiped the operator's existing rows before the row path appended the new
ones. ``full_refresh_append`` into an occupied MySQL table landed N rows
instead of 2N, with ``target_rows_before`` reading 0.

The cleanup may only reset a destination to the *empty* state this run found
it in. An occupied destination is never this run's to truncate.
"""

from __future__ import annotations

import re
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.sync_mode_probe import (  # noqa: E402
    COLUMNS,
    MAPPINGS,
    RECORDS,
    SCHEMA,
    stream_contract,
)

MODE = "full_refresh_append"


def _pg_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )


def _mysql_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="mysql",
        host="localhost",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="dataflow",
        table=table,
    )


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="dataflow",
            user="dataflow",
            password="dataflow",
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


def _mysql_conn():
    pymysql = pytest.importorskip("pymysql")
    try:
        return pymysql.connect(
            host="localhost",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            autocommit=True,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL unavailable: {exc}")


def _count(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])


def _run_append(source: EndpointConfig, dest: EndpointConfig, stream: str):
    engine = UniversalTransferEngine()
    return engine.execute_tracked(
        TransferRequest(
            source=source,
            destination=dest,
            sync_mode=MODE,
            skip_preflight=False,
            validation_mode="balanced",
            stream_contracts=[stream_contract(stream, MODE)],
            mappings=list(MAPPINGS),
        ),
        uuid.uuid4().hex[:24],
    )


@pytest.mark.parametrize(
    ("source_engine", "dest_engine"),
    [("postgresql", "mysql"), ("mysql", "mysql"), ("mysql", "postgresql")],
)
def test_second_append_keeps_the_first_run_rows(source_engine, dest_engine):
    """Two appends land 2N; the decline in between must not truncate run one."""
    pg = _pg_conn()
    my = _mysql_conn()
    conns = {"postgresql": pg, "mysql": my}
    endpoints = {"postgresql": _pg_endpoint, "mysql": _mysql_endpoint}
    quote = {"postgresql": '"{}"', "mysql": "`{}`"}
    src_table = f"decl_src_{uuid.uuid4().hex[:8]}"
    dst_table = f"decl_dst_{uuid.uuid4().hex[:8]}"
    source = endpoints[source_engine](src_table)
    dest = endpoints[dest_engine](dst_table)
    dest_ref = quote[dest_engine].format(dst_table)
    src_ref = quote[source_engine].format(src_table)
    try:
        write_destination_database(source, RECORDS, COLUMNS, SCHEMA, MAPPINGS)
        first = _run_append(source, dest, src_table)
        assert first.success, first.error
        assert _count(conns[dest_engine], f"SELECT COUNT(*) FROM {dest_ref}") == len(RECORDS)

        second = _run_append(source, dest, src_table)
        assert second.success, second.error
        summary = second.destination_summary or {}
        assert summary.get("copy_fast_path") == "declined", summary
        assert summary.get("target_rows_before") == len(RECORDS), summary
        assert _count(conns[dest_engine], f"SELECT COUNT(*) FROM {dest_ref}") == 2 * len(
            RECORDS
        )
    finally:
        for conn, ref in ((conns[dest_engine], dest_ref), (conns[source_engine], src_ref)):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {ref}")
        pg.close()
        my.close()


def test_every_copy_cleanup_truncate_is_gated_on_an_empty_dest():
    """Fleet guard for engines this box cannot run (Oracle, SQL Server).

    Every ``TRUNCATE TABLE`` in a fast-path failure handler must sit under the
    ``reset_empty_dest_on_failure`` gate, which is only ever true when the
    destination existed *and was empty* when the run began.
    """
    services = _API_ROOT / "services"
    offenders: list[str] = []
    for path in sorted(services.glob("copy_*.py")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"TRUNCATE TABLE", text):
            head = text[: match.start()]
            branch = head.rfind("elif existed_before")
            if branch == -1:
                continue
            gate = head[branch : branch + 80].splitlines()[0]
            if "reset_empty_dest_on_failure" not in gate:
                offenders.append(f"{path.name}: {gate.strip()}")
            if "reset_empty_dest_on_failure = not dest_occupied" not in text:
                offenders.append(f"{path.name}: gate never derived from dest_occupied")
    assert not offenders, offenders
