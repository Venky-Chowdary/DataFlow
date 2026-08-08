"""Pytest configuration — isolate tests from live infrastructure."""

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

# Slim CI images omit sentence-transformers; use a deterministic hash embedder
# so vector destination matrices still exercise the write path.
try:
    import sentence_transformers  # noqa: F401
except ImportError:
    os.environ.setdefault("DATAFLOW_EMBEDDING_MODEL", "hash/32")


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
                )
            ) or (
                "postgresql_to_postgresql_incremental" in path
                or "postgresql_writer_dedupe" in path
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
