"""Resume must never be an action the operator cannot take.

Reported from Transfer Studio: a 1M-row keyless CSV load failed, and the only
offered action — Resume — answered "Cannot resume a streaming insert without a
primary key; set primary_key on the stream contract or use an upsert sync mode".
The load was a full refresh overwrite, which needs no key to be safe, and a
keyless CSV cannot supply one. The operator had no way forward.

A full refresh has nothing to resume *into*: it truncates and reloads, so
replaying from the top is idempotent whether or not an identity key exists.
Resuming one restarts it.

The refusal is still correct for append-class modes, where replaying an insert
without a key really would duplicate rows — but it now names a remedy the
operator can actually act on.
"""

from __future__ import annotations

import csv
import io
import socket
import uuid

import pytest

from services.checkpoint_service import Checkpoint
from src.transfer.file_stream import stream_file_to_database
from src.transfer.models import EndpointConfig


def _pg_reachable() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="PostgreSQL not reachable on 127.0.0.1:5432"
)

ROWS = 400
COLUMNS = ["col1", "col2", "col3"]
MAPPINGS = [{"source": c, "target": c, "confidence": 0.99} for c in COLUMNS]
SCHEMA = {"col1": "VARCHAR", "col2": "INTEGER", "col3": "VARCHAR"}


def _csv_bytes() -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(COLUMNS)
    for i in range(1, ROWS + 1):
        # col1 repeats on every row: the shape of any real export, and the
        # reason no column here can serve as an identity key.
        writer.writerow(["Asia", i, f"item_{i}"])
    return buf.getvalue().encode()


@pytest.fixture()
def destination():
    psycopg2 = pytest.importorskip("psycopg2")
    table = "resume_dead_end_" + uuid.uuid4().hex[:8]
    endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    try:
        yield endpoint, conn
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


def _resume_checkpoint(job_id: str) -> Checkpoint:
    checkpoint = Checkpoint(job_id=job_id)
    checkpoint.chunk_index = 2
    checkpoint.write_mode = "insert"
    return checkpoint


def _request(sync_mode: str, contracts=None):
    from src.transfer.models import TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql"),
        sync_mode=sync_mode,
        stream_contracts=contracts or [],
    )


@pytest.mark.parametrize(
    "sync_mode", ["full_refresh_overwrite", "FULL_REFRESH_OVERWRITE", "overwrite"]
)
def test_overwrite_resume_restarts_instead_of_continuing(sync_mode: str):
    """The reported dead end: Resume on a keyless full-refresh load."""
    from src.routers.connectors_router import _resume_restarts_from_scratch

    assert _resume_restarts_from_scratch(_request(sync_mode)) is True


@pytest.mark.parametrize(
    "sync_mode", ["full_refresh_append", "incremental_append", "upsert", "cdc"]
)
def test_non_overwrite_resume_still_continues(sync_mode: str):
    """Append-class modes have real partial state; they must resume, not reload."""
    from src.routers.connectors_router import _resume_restarts_from_scratch

    assert _resume_restarts_from_scratch(_request(sync_mode)) is False


def test_stream_contract_mode_decides_over_the_request():
    """A per-stream contract is the effective mode, so it decides."""
    from src.routers.connectors_router import _resume_restarts_from_scratch

    assert (
        _resume_restarts_from_scratch(
            _request(
                "full_refresh_append",
                [{"name": "t", "sync_mode": "full_refresh_overwrite"}],
            )
        )
        is True
    )
    assert (
        _resume_restarts_from_scratch(
            _request(
                "full_refresh_overwrite",
                [{"name": "t", "sync_mode": "incremental_append"}],
            )
        )
        is False
    )


def test_mixed_stream_modes_do_not_restart():
    """One appending stream means a restart would duplicate its rows."""
    from src.routers.connectors_router import _resume_restarts_from_scratch

    assert (
        _resume_restarts_from_scratch(
            _request(
                "full_refresh_overwrite",
                [
                    {"name": "a", "sync_mode": "full_refresh_overwrite"},
                    {"name": "b", "sync_mode": "full_refresh_append"},
                ],
            )
        )
        is False
    )


def test_engine_overwrite_resume_lands_the_population_exactly_once(destination):
    """The operator's path: Resume a failed overwrite job, twice, via the engine.

    The engine drops the destination for an overwrite before the load, so a
    restart replaces rather than appends. Asserted here because the restart in
    this module is what makes a resumed overwrite replay the whole file.
    """
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    endpoint, conn = destination
    for _ in range(2):
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="file", format="csv"),
                destination=endpoint,
                mappings=MAPPINGS,
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                skip_preflight=True,
                source_content=_csv_bytes(),
                source_filename="keyless.csv",
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success, result.error
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM "{endpoint.table}"')
        assert cur.fetchone()[0] == ROWS


def test_append_resume_without_a_key_still_refuses(destination):
    """The guard is right for append: replaying an insert would duplicate rows."""
    endpoint, _conn = destination
    with pytest.raises(ValueError) as excinfo:
        stream_file_to_database(
            _csv_bytes(),
            "keyless.csv",
            endpoint,
            MAPPINGS,
            SCHEMA,
            sync_mode="full_refresh_append",
            job_id=uuid.uuid4().hex[:24],
            checkpoint=_resume_checkpoint(uuid.uuid4().hex[:24]),
            skip_preflight=True,
        )
    message = str(excinfo.value)
    assert "without a primary key" in message
    # The remedy has to be one the operator can actually take. A keyless CSV
    # cannot produce a key, so naming only that left them stuck.
    assert "Full refresh" in message and "Overwrite" in message
