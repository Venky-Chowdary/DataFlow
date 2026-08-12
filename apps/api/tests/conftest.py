"""Pytest configuration — isolate tests from live infrastructure."""

import logging
import os
import socket
import sys
from pathlib import Path

import pytest

_api_root = Path(__file__).resolve().parents[1]
_src_root = _api_root / "src"

# Both `apps/api/services` and `apps/api/src/services` exist as top-level
# `services` packages. Runtime (uvicorn from apps/api) resolves the bare
# `services` import to `apps/api/services`; src-only modules are always reached
# via the `src.services` prefix. Force the same ordering here — api root ahead of
# src — so the test suite imports the same modules the app does. Remove any
# stale entries first so pytest's own path munging can't flip the order.
for path in (_src_root, _api_root):
    p = str(path)
    while p in sys.path:
        sys.path.remove(p)
sys.path.insert(0, str(_src_root))
sys.path.insert(0, str(_api_root))

# Pin the bare package name before tests import `src.*`.  Without this, Python
# can cache src/services as `services` when a runner exposes src on sys.path,
# causing compatibility shims to import themselves recursively.
import services as _canonical_services  # noqa: E402,F401

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

# fakesnow keeps the emulated warehouse in a DuckDB file, and DuckDB allows a
# single writer per file. Under `pytest -n` every worker would open the same
# default path and all but one would die on "Conflicting lock is held", so the
# Snowflake matrices only pass when the suite runs serially. Give each xdist
# worker its own catalog; the product already reads this override.
_xdist_worker = os.environ.get("PYTEST_XDIST_WORKER")
if _xdist_worker:
    os.environ.setdefault(
        "FAKESNOW_DB_PATH",
        str(_api_root / "data" / f"fakesnow_data_{_xdist_worker}"),
    )

# Slim CI images omit sentence-transformers; use a deterministic hash embedder
# so vector destination matrices still exercise the write path.
try:
    import sentence_transformers  # noqa: F401
except ImportError:
    os.environ.setdefault("DATAFLOW_EMBEDDING_MODEL", "hash/32")


#: Bucket the object-store matrix routes read and write.
LOCAL_OBJECT_STORE_BUCKET = "dataflow-matrix"


@pytest.fixture(scope="session")
def local_object_store() -> str:
    """Endpoint URL of a local S3, or ``""`` when one cannot be started.

    Object-store routes were the largest block of never-executed transfers —
    they need an endpoint to be reachable, and no cloud account exists here, so
    the matrix skipped them and nothing reported what they did. That is how an
    S3 source came to land three ``text`` columns where the identical file
    upload landed ``bigint``/``numeric``/``date``.

    ``moto`` answers the S3 API in-process, and the connectors already accept a
    custom endpoint, so the routes become executable without credentials. An
    externally provided ``DATAFLOW_TEST_S3_ENDPOINT`` (MinIO, a real account)
    wins; without moto the value is empty and callers skip honestly rather than
    reporting a pass nothing proved.
    """
    external = os.environ.get("DATAFLOW_TEST_S3_ENDPOINT", "").strip()
    if external:
        yield external
        return

    try:
        import boto3
        from moto.server import ThreadedMotoServer
    except ImportError:
        yield ""
        return

    # Port 0 lets the OS assign, so parallel workers never collide.
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    try:
        server.start()
    except Exception as exc:  # noqa: BLE001 — an unstartable emulator is a skip
        logging.getLogger(__name__).info("local object store unavailable: %s", exc)
        yield ""
        return

    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    try:
        boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        ).create_bucket(Bucket=LOCAL_OBJECT_STORE_BUCKET)
        yield endpoint
    finally:
        try:
            server.stop()
        except Exception as exc:  # noqa: BLE001 — teardown must not fail a run
            logging.getLogger(__name__).info("object store stop failed: %s", exc)


@pytest.fixture(autouse=True)
def _isolate_cdc_leases(monkeypatch):
    """Isolate CDC leases per test (memory backend — no shared file/Redis bleed).

    Connectors acquire leases on poll/snapshot; unit tests rarely call close().
    Force an in-process store so leftovers never poison the suite, developer
    ``cdc_leases.json``, or a shared Redis DB.
    """
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "memory")
    from services.cdc_lease import configure_store

    configure_store(backend="memory")


def _is_mongo_reachable() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1.0).close()
        return True
    except Exception:
        return False


def _is_postgresql_auth_ok(
    *,
    host: str = "localhost",
    port: int = 5432,
    database: str = "dataflow",
    user: str = "dataflow",
    password: str = "dataflow",
) -> bool:
    """Port open ≠ role/password valid (common on shared localhost:5432)."""
    try:
        socket.create_connection((host, port), timeout=1.0).close()
    except Exception:
        return False
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    """Skip live-service tests when emulators are absent or unauthenticated."""
    if not _is_mongo_reachable():
        skip_mongo = pytest.mark.skip(reason="MongoDB not reachable on this runner")
        for item in items:
            if "mongodb" in item.nodeid.lower():
                item.add_marker(skip_mongo)

    # Live execute_tracked / CDC PG paths hard-code dataflow/dataflow.
    # Port-open-only skips previously failed with auth errors mid-test.
    if not _is_postgresql_auth_ok():
        skip_pg = pytest.mark.skip(
            reason=(
                "PostgreSQL auth failed for dataflow/dataflow on localhost:5432 "
                "(skip_honest — port open ≠ usable)"
            )
        )
        for item in items:
            nid = item.nodeid.lower()
            path = str(getattr(item, "path", "") or getattr(item, "fspath", "")).lower()
            live_pg = (
                "execute_tracked" in nid
                and (
                    "postgres" in nid
                    or "postgresql" in nid
                    or "pgvector" in nid
                )
            ) or (
                "execute_tracked" in path
                and (
                    "postgres" in path
                    or "postgresql" in path
                    or "pgvector" in path
                )
            ) or any(
                token in nid
                for token in (
                    "cdc_postgres",
                    "postgresql_cdc",
                    "cross_schema_edge_types",
                    "csv_to_postgres_upsert",
                    "[pgvector]",
                    "mongodb_to_postgresql",
                    "pilot_aggregation_wave89",
                    "pilot_transfer_wave92",
                    "pilot_transfer_matrix_wave93",
                    "postgresql_to_postgresql_incremental",
                    "postgresql_writer_dedupe",
                    "postgresql_writer_upsert_dedupes",
                    "security_hardening_e2e",
                    "live_duplicate_key_probe",
                    "source_duplicate_probe_live",
                    "csv_to_postgresql_hostile",
                )
            ) or (
                "postgresql_to_postgresql_incremental" in path
                or "postgresql_writer_dedupe" in path
                or "security_hardening_e2e" in path
                or "source_duplicate_probe_live" in path
            ) or (
                "live_emulator" in nid
                and ("[postgresql]" in nid or "[pgvector]" in nid)
            ) or (
                "pilot_aggregation" in path
                or "mongodb_to_postgresql" in path
                or "pilot_transfer_wave92" in path
                or (
                    "pilot_transfer_matrix_wave93" in path
                    and "live_cross_engine" in nid
                )
            )
            if live_pg:
                item.add_marker(skip_pg)
