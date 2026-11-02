"""Pytest configuration — isolate tests from live infrastructure."""

import logging
import os
import socket
import tempfile
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

# Session-level data-dir redirect: modules that bind `data_dir()` at import
# time (STORE_PATH-style constants) capture it before any fixture can run,
# so the env has to be set here — ahead of test-module imports. Per-test
# tmp_path isolation still happens in _isolate_file_backed_test_stores for
# stores that resolve data_dir() lazily; this session dir only catches
# import-time snapshots. The session-end guard below fails the run if
# anything still lands in apps/api/data.
#
# Set the fallback brand var (DATAFLOW_DATA_DIR), never DATAWRAP_DATA_DIR:
# `data_dir()` resolves DATAWRAP_ first, so a permanently-set DATAWRAP_
# value would shadow every test that monkeypatches DATAFLOW_DATA_DIR to
# isolate itself — those tests would all share one dir and pollute each
# other. With only the fallback var set, a test patching either var wins.
import tempfile  # noqa: E402

os.environ.setdefault(
    "DATAFLOW_DATA_DIR",
    tempfile.mkdtemp(prefix="df-suite-data-"))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
# Engine fixtures stage file sources under pytest's tmp_path.
os.environ.setdefault("DATAFLOW_SOURCE_PATH_ROOTS", tempfile.gettempdir())
# Local proof dials Postgres and MySQL on loopback. Production deploy does
# not set this. Metadata and link-local stay refused even when it is set.
os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")

# Hermetic env: Railway markers exported by `railway run`, a linked dev
# shell or CI agents with the CLI installed must not flip the suite into
# production policy (test_verify_legacy_sha256_still_works failed on such
# machines). Tests pin the environment to test and strip RAILWAY_*.
for _k in [k for k in os.environ if k.startswith("RAILWAY_")]:
    del os.environ[_k]
os.environ["DATAWRAP_ENV"] = "test"
os.environ["DATAFLOW_ENV"] = "test"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "fake_mongo: leftover MERGE uses an in-memory collection (no live Mongo)",
    )
    config.addinivalue_line("markers", "live: optional live vendor smoke test")


@pytest.fixture(autouse=True)
def _oracle_transaction_detection_grant_for_opt_out_live_tests(request):
    module_name = Path(str(request.node.path)).stem
    supported_modules = {
        "test_cdc_oracle_ddl_schema_history",
        "test_cdc_oracle_logminer_lob_live",
        "test_cdc_oracle_logminer_transfer_e2e",
        "test_cdc_poll_reconnects_live",
        "test_oracle_logminer_log_files",
    }
    if module_name not in supported_modules:
        yield
        return

    callspec = getattr(request.node, "callspec", None)
    params = getattr(callspec, "params", {})
    always_opt_out = (
        module_name == "test_cdc_oracle_ddl_schema_history"
        and request.node.name == "test_oracle_logminer_records_live_ddl_without_emitting_it_as_data"
    ) or (
        module_name == "test_oracle_logminer_log_files"
        and request.node.name.startswith(
            (
                "test_live_mid_mine_failure_ends_session_and_retry_has_no_loss_or_duplicates",
                "test_live_archived_online_sequence_registers_without_ora_01289",
            )
        )
    )
    if params.get("txn_buffer") is not False and not always_opt_out:
        yield
        return

    from test_cdc_oracle_logminer_transfer_e2e import (
        _oracle_cfg,
        _oracle_logminer_ready,
    )
    from test_cdc_oracle_logminer_txn_live import (
        _oracle_v_transaction_grant_count,
        _oracle_v_transaction_grant_user,
        _set_oracle_v_transaction_grant,
    )

    if not _oracle_logminer_ready():
        yield
        return

    user = _oracle_v_transaction_grant_user(_oracle_cfg())
    already_granted = _oracle_v_transaction_grant_count(user) > 0
    if not already_granted:
        _set_oracle_v_transaction_grant(user, enabled=True)
    try:
        yield
    finally:
        if not already_granted:
            _set_oracle_v_transaction_grant(user, enabled=False)

# fakesnow keeps the emulated warehouse in a DuckDB file, and DuckDB allows a
# single writer per file. Under `pytest -n` every worker would open the same
# default path and all but one would die on "Conflicting lock is held", so the
# Snowflake matrices only pass when the suite runs serially. Give each xdist
# worker its own catalog; the product already reads this override.
_xdist_worker = os.environ.get("PYTEST_XDIST_WORKER")
if _xdist_worker:
    _worker_data_dir = Path(
        os.environ.get("DATAFLOW_DATA_DIR") or tempfile.mkdtemp(prefix="df-suite-data-")
    )
    os.environ.setdefault(
        "FAKESNOW_DB_PATH",
        str(_worker_data_dir / f"fakesnow_data_{_xdist_worker}"),
    )
    # Load-history JSON must not share the dev data dir's quality_profiles across
    # workers — leftover 1M / prior-pytest runs inflate rows_written_total.
    # Do not rewrite DATAFLOW_DATA_DIR here (connectors/tenants live there).
    os.environ.setdefault(
        "DATAFLOW_QUALITY_PROFILES_DIR",
        str(_worker_data_dir / f"quality_profiles_{_xdist_worker}"),
    )

# Slim CI images omit sentence-transformers; use a deterministic hash embedder
# so vector destination matrices still exercise the write path.
try:
    import sentence_transformers  # noqa: F401
except ImportError:
    os.environ.setdefault("DATAFLOW_EMBEDDING_MODEL", "hash/32")


#: Bucket the object-store matrix routes read and write.
LOCAL_OBJECT_STORE_BUCKET = "dataflow-matrix"


@pytest.fixture()
def local_object_store(_local_object_store_server: str) -> str:
    """Session endpoint with the matrix bucket guaranteed to exist *now*.

    ``moto.mock_aws()`` resets moto's in-process backends, so any test that opens
    that context manager wipes the bucket this server was started with. The
    object-store routes then failed with ``NoSuchBucket`` in a full-suite run and
    passed in isolation — a harness artefact that looks exactly like a product
    defect, which is worse than a skip. Re-assert the bucket per test.
    """
    if not _local_object_store_server:
        return ""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=_local_object_store_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket=LOCAL_OBJECT_STORE_BUCKET)
    except ClientError:
        try:
            client.create_bucket(Bucket=LOCAL_OBJECT_STORE_BUCKET)
        except ClientError as exc:
            logging.getLogger(__name__).info("bucket re-create failed: %s", exc)
            return ""
    return _local_object_store_server


@pytest.fixture(scope="session")
def _local_object_store_server() -> str:
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


@pytest.fixture(scope="session")
def local_sftp():
    """A real local SFTP server, or ``None`` when paramiko is unavailable.

    SFTP was the one named connector with no live route: it could read and
    write but declared ``introspect: False`` / ``preflight: False``, so every
    test it had patched ``connect_sftp`` and asserted on the mock. paramiko
    ships the server half of the protocol, so the routes run for real here —
    with the generated host key pinned, which exercises the verification path
    instead of the ``insecure_ignore`` escape.
    """
    import tempfile

    try:
        from tests.sftp_test_server import start_sftp_server
    except ImportError:
        try:
            from sftp_test_server import start_sftp_server
        except ImportError:
            yield None
            return

    root = tempfile.mkdtemp(prefix="df_sftp_")
    try:
        details, runner = start_sftp_server(root)
    except Exception as exc:  # noqa: BLE001 — an unstartable server is a skip
        logging.getLogger(__name__).info("local sftp unavailable: %s", exc)
        yield None
        return
    try:
        yield details
    finally:
        try:
            runner.stop()
        except Exception as exc:  # noqa: BLE001 — teardown must not fail a run
            logging.getLogger(__name__).info("sftp stop failed: %s", exc)


@pytest.fixture(autouse=True)
def _isolate_process_env(monkeypatch, request):
    """Undo ``os.environ`` mutations a test makes outside monkeypatch.

    ``apply_railway_defaults()`` and startup/bootstrap code write env
    defaults straight into ``os.environ`` during a ``with TestClient(app)``
    startup; monkeypatch never recorded them, so they leak into every
    later test (production config 503s, secret-vault ``SecretVaultError``s).
    Snapshot the whole environment and restore it exactly so a test can
    only affect the process env through monkeypatch.
    """
    snapshot = dict(os.environ)
    yield
    patched = {
        name
        for (target, name, _prev) in getattr(monkeypatch, "_setitem", ())
        if target is os.environ
    }
    added = sorted(
        k for k in os.environ
        if k not in snapshot and k not in patched and k != "PYTEST_CURRENT_TEST"
    )
    changed = sorted(
        k for k in snapshot
        if os.environ.get(k) != snapshot[k]
        and k not in patched
        and k != "PYTEST_CURRENT_TEST"
    )
    for key in added:
        os.environ.pop(key, None)
    for key in changed:
        os.environ[key] = snapshot[key]
    if (added or changed) and os.environ.get("DATAFLOW_REPORT_ENV_LEAKS"):
        report = os.environ["DATAFLOW_REPORT_ENV_LEAKS"]
        if report != "1":
            with open(report, "a") as fh:
                fh.write(
                    f"{request.node.nodeid} added={added} changed={changed}\n"
                )
        else:
            print(
                f"\n[env-leak] {request.node.nodeid} "
                f"added={added} changed={changed}"
            )


@pytest.fixture(autouse=True)
def _isolate_parked_snowflake_logins():
    """A Snowflake plan login parked by one test is never taken by another (D187)."""
    yield
    try:
        from connectors.snowflake_warm_login import reset_for_tests
    except ImportError:
        return
    reset_for_tests()


@pytest.fixture(autouse=True)
def _isolate_app_state():
    """Restore FastAPI ``app.state`` fields written by a test's startup.

    ``with TestClient(app)`` runs the lifespan block, which sets
    ``app.state.config_errors``. Under a leaked production env that leaves
    a non-empty list on the shared app object and
    ``ProductionConfigMiddleware`` 503s every later request in the suite.
    Snapshot the state dict and restore it per test.
    """
    try:
        from src.main import app
    except Exception:
        yield
        return
    state = app.state._state
    snapshot = dict(state)
    yield
    for key in list(state):
        if key not in snapshot:
            del state[key]
    for key, val in snapshot.items():
        if state.get(key) is not val:
            state[key] = val


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


_SUITE_DATA_DIR = Path(os.environ["DATAFLOW_DATA_DIR"]).resolve()
_DEV_DATA_DIR = (_api_root / "data").resolve()
_PATH_ATTR_HINTS = ("PATH", "DIR", "STORE", "FILE", "LEDGER", "REGISTRY", "DB")
_TRACKED_ASSETS: set[Path] | None = None
_MODULE_DATA_DIRS: dict[str, Path] = {}


def _tracked_data_assets() -> set[Path]:
    """Files under ``apps/api/data`` tracked in git — read-only assets like
    ``connector_form_schema.json`` that must never be rebound to a tmp dir."""
    global _TRACKED_ASSETS
    if _TRACKED_ASSETS is None:
        import subprocess

        try:
            out = subprocess.run(
                ["git", "ls-files", "-z", "data"],
                cwd=_api_root,
                capture_output=True,
                check=True,
            ).stdout
            _TRACKED_ASSETS = {
                (_api_root / p.decode()).resolve()
                for p in out.split(b"\0")
                if p
            }
        except Exception:  # noqa: BLE001 — no git means nothing is tracked
            _TRACKED_ASSETS = set()
    return _TRACKED_ASSETS


def _rebind_import_time_store_paths(monkeypatch, per_test_dir: Path) -> None:
    """Rebind module-level ``Path`` constants captured under a data dir.

    A dozen-plus stores do ``STORE_PATH = data_dir() / "x.json"`` at import —
    before any fixture can run — so they all share the session data dir and
    leak state across tests (the suite-order pollution class). Point every
    such constant at the per-test dir instead. Only attributes in
    services.*/src.*/connectors.* modules whose Path value already lives
    under a known data dir are touched — config paths outside data dirs
    (fixtures, package data) are left alone.
    """
    for mod in list(sys.modules.values()):
        mod_name = getattr(mod, "__name__", "") or ""
        if not mod_name.startswith(("services", "src", "connectors")):
            continue
        for attr, val in list(vars(mod).items()):
            if not isinstance(val, Path) or attr.startswith("__"):
                continue
            if not any(h in attr.upper() for h in _PATH_ATTR_HINTS):
                continue
            try:
                resolved = val.resolve()
            except OSError:
                continue
            if resolved in _tracked_data_assets():
                continue
            for root in (_SUITE_DATA_DIR, _DEV_DATA_DIR):
                try:
                    rel = resolved.relative_to(root)
                except ValueError:
                    continue
                monkeypatch.setattr(mod, attr, per_test_dir / rel)
                break


@pytest.fixture(scope="module", autouse=True)
def _isolate_module_data_dir(tmp_path_factory, request):
    """Keep module-scoped fixture state out of the developer's data dir.

    Runs BEFORE any module-scoped fixture in the test file (autouse module
    fixtures instantiate first), so a live seed like ``live_orders`` or the
    studio/adls connector fixtures writes under the module's tmp dir — the
    same dir the function-scoped fixture below points every test at.
    """
    stem = request.module.__name__.rsplit(".", 1)[-1]
    module_dir = _MODULE_DATA_DIRS.get(stem)
    if module_dir is None:
        module_dir = tmp_path_factory.mktemp("dw-data-" + stem, numbered=False)
        _MODULE_DATA_DIRS[stem] = module_dir
    mp = pytest.MonkeyPatch()
    mp.delenv("DATAWRAP_DATA_DIR", raising=False)
    mp.setenv("DATAFLOW_DATA_DIR", str(module_dir))
    _rebind_import_time_store_paths(mp, module_dir)
    try:
        yield module_dir
    finally:
        mp.undo()


@pytest.fixture(autouse=True)
def _isolate_file_backed_test_stores(monkeypatch, _isolate_module_data_dir, request):
    """Keep test persistence out of the developer's default data directory."""
    # The master dial: every store that resolves ``platform_config.data_dir()``
    # (users, challenges, resets, workspaces, quota, ...) lands in a per-module
    # tmp dir (set by _isolate_module_data_dir so module-scoped fixtures land
    # there too), so no test can touch apps/api/data or another file's state.
    # Drop DATAWRAP_DATA_DIR (the higher-priority brand var) and set the
    # fallback DATAFLOW_DATA_DIR instead, so a test that monkeypatches either
    # variable to point at its own directory is never shadowed by this one.
    module_dir = _isolate_module_data_dir
    monkeypatch.delenv("DATAWRAP_DATA_DIR", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(module_dir))
    from services import audit_log, auth_sessions, mcp_invocation_log

    monkeypatch.setattr(
        audit_log, "STORE_PATH", module_dir / "audit_events.jsonl"
    )
    monkeypatch.setattr(
        auth_sessions, "_path", lambda: module_dir / "auth_sessions.json"
    )
    monkeypatch.setenv(
        "DATAFLOW_AUDIT_ANCHOR_STORE",
        str(module_dir / "audit_tip_anchors.jsonl"),
    )
    monkeypatch.setattr(
        mcp_invocation_log, "STORE_PATH", module_dir / "mcp_invocations.jsonl"
    )
    from services import append_continuity

    monkeypatch.setattr(append_continuity, "data_dir", lambda: module_dir)
    monkeypatch.setattr(append_continuity, "_collection", lambda: None)

    # Signup feature stores — file fallbacks land in module_dir (shared per file).
    from services import denylist_store, org_store, signup_store

    monkeypatch.setattr(org_store, "_orgs_path", lambda: module_dir / "organizations.json")
    monkeypatch.setattr(org_store, "_plans_path", lambda: module_dir / "plan_entitlements.json")
    monkeypatch.setattr(signup_store, "_pending_path", lambda: module_dir / "pending_signups.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: module_dir / "otp_challenges.json")
    monkeypatch.setattr(denylist_store, "_path", lambda: module_dir / "email_denylist.json")

    test_name = request.node.path.name
    if test_name.startswith("test_mcp"):
        from services import contract_store, preflight_run_store, schema_registry
        from src.ai.copilot import result_store

        monkeypatch.setenv(
            "DATAFLOW_CONTRACTS_PATH", str(module_dir / "contracts.json")
        )
        monkeypatch.setattr(contract_store, "_store_instance", None)
        monkeypatch.setenv(
            "DATAFLOW_PILOT_RESULTS_PATH", str(module_dir / "pilot_results.json")
        )
        monkeypatch.setattr(result_store, "_store", None)
        monkeypatch.setattr(
            preflight_run_store, "STORE_PATH", module_dir / "preflight_runs.jsonl"
        )
        monkeypatch.setenv(
            "DATAFLOW_QUALITY_PROFILES_DIR", str(module_dir / "quality_profiles")
        )
        monkeypatch.setattr(
            schema_registry,
            "_SCHEMAS_PATH",
            module_dir / "schema_registry_schemas.json",
        )
        monkeypatch.setattr(
            schema_registry,
            "_LINEAGE_PATH",
            module_dir / "schema_registry_lineage.json",
        )

    if test_name.startswith(("test_mcp", "test_scim")):
        from services import team_store

        monkeypatch.setattr(team_store, "data_dir", lambda: module_dir)


@pytest.fixture(autouse=True)
def _isolate_mcp_rate_limit_buckets():
    from services.mcp_rate_limit import reset_mcp_rate_limits

    reset_mcp_rate_limits()
    try:
        yield
    finally:
        reset_mcp_rate_limits()


def pg_test_cfg(port: int | None = None) -> dict:
    """PG connection params for live tests, from DATAFLOW_PG_TEST_DSN.

    Defaults to the local 127.0.0.1:5432 dataflow/dataflow instance —
    ``localhost`` is refused by the egress guard even under a loopback
    allowlist, so tests must never hardcode it. ``port`` overrides the
    DSN port for tests that need a second server (e.g. wal_level matrix).
    """
    import os
    from urllib.parse import urlparse

    u = urlparse(
        os.environ.get("DATAFLOW_PG_TEST_DSN")
        or "postgresql://dataflow:dataflow@127.0.0.1:5432/dataflow"
    )
    return {
        "host": u.hostname or "127.0.0.1",
        "port": port if port is not None else (u.port or 5432),
        "database": (u.path or "/dataflow").lstrip("/"),
        "username": u.username or "dataflow",
        "password": u.password or "",
    }


@pytest.fixture(scope="session", autouse=True)
def _drop_leaked_pg_replication_slots():
    """CDC tests create logical replication slots on the local PG and do not
    always drop them — 20 leaked ``df_dataflow_mt_*``/``s_probe`` slots once
    filled ``max_replication_slots`` and failed unrelated CDC tests with
    ``replication_slot_quota: all replication slots are in use``. Best-effort
    session teardown: drop inactive slots matching the test prefixes.
    """
    yield
    try:
        import psycopg2

        cfg = pg_test_cfg()
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["username"],
            password=cfg["password"],
            connect_timeout=3,
        )
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT slot_name FROM pg_replication_slots "
                    "WHERE NOT active AND ("
                    "slot_name LIKE 'df_dataflow_mt_%' "
                    "OR slot_name LIKE 's_probe%' "
                    "OR slot_name LIKE 'x29_%' "
                    "OR slot_name LIKE 'df_test_%')"
                )
                for (slot_name,) in cur.fetchall():
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        finally:
            conn.close()
    except Exception:
        # PG absent / creds differ — cleanup is best-effort hygiene.
        pass


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
            if "mongodb" in item.nodeid.lower() and "fake_mongo" not in item.keywords:
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


def spend_pilot_ack(ack_id: str, actor: str) -> dict:
    """Consume a Pilot approval the way the HTTP route does.

    ``/copilot/confirm`` re-checks permission from the request object, so a test
    calling it with the ack alone exercises a signature production does not have.
    One helper keeps every confirm test on the real call shape.
    """
    import asyncio

    from fastapi import BackgroundTasks

    from src.routers.copilot_router import ConfirmActionRequest, copilot_confirm

    class _State:
        pass

    class _HttpRequest:
        state = _State()
        headers: dict[str, str] = {}

    return asyncio.run(
        copilot_confirm(
            ConfirmActionRequest(ack_id=ack_id, actor=actor), _HttpRequest(), BackgroundTasks()
        )
    )


def _data_dir_fingerprint() -> dict[str, tuple[int, int]]:
    """relpath -> (mtime_ns, size) for everything under the dev data dir."""
    root = _api_root / "data"
    if not root.is_dir():
        return {}
    out: dict[str, tuple[int, int]] = {}
    for p in root.rglob("*"):
        if p.is_file():
            st = p.stat()
            out[str(p.relative_to(root))] = (st.st_mtime_ns, st.st_size)
    return out


@pytest.fixture(scope="session", autouse=True)
def _guard_no_test_writes_dev_data_dir():
    """The suite must never write into apps/api/data — per-test stores live
    under tmp_path via DATAWRAP_DATA_DIR. Fails the run listing every file a
    test created or modified in the shared dev directory."""
    before = _data_dir_fingerprint()
    yield
    after = _data_dir_fingerprint()
    changed = sorted(p for p, sig in after.items() if before.get(p) != sig)
    removed = sorted(p for p in before if p not in after)
    if changed or removed:
        pytest.fail(
            "tests wrote into apps/api/data — route the store through "
            "data_dir() (DATAWRAP_DATA_DIR is tmp_path in tests). "
            f"changed/created: {changed[:20]}; removed: {removed[:20]}"
        )
