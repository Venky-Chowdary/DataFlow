"""QA round 11 (build 9c75ada4), Addendum A — Ensembl MySQL preflight hang.

``Ensembl MySQL → PG overwrite help_record``: Validate ran for more than five
minutes (twice) and had to be cancelled. Outside a copilot tool call there was
no deadline: ``remaining_seconds()`` was ``None``, so neither ``bounded_step``
nor the statement budget applied, the REST route ran its blocking gates on the
event loop, and the optional catalog probes (physical storage, partitions,
CHECK, indexes) scanned ``information_schema`` server-wide with no cap — plus
a host-wide ``information_schema.tables`` fallback across every schema.

Now REST and engine preflight run under the preflight deadline (the existing
interactive budget, not a raise), REST off the event loop, every catalog
statement of the optional probes is capped server-side, and a probe that hits
its cap is a warn finding naming it.

Real MySQL 8 with ~300 throwaway schemas (and MariaDB 11), one deliberately
slow catalog statement injected into the physical-storage probe: the preflight
must answer within its budget with that finding, and leave no statement
running on the server.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

from tests.test_r11_mysql_pg_fk_ddl_e2e import (  # noqa: F401 — fixture re-export
    KINDS,
    _connector,
    _my,
    _my_cfg,
    _sql,
    client,
    pymysql,
)

BUDGET_SECONDS = 30.0
SLOW_MARK = "r11_injected_slow_catalog_statement"
SLOW_SQL = (
    f"SELECT /* {SLOW_MARK} */ COUNT(*) FROM information_schema.columns a "
    "CROSS JOIN information_schema.columns b CROSS JOIN information_schema.columns c"
)
N_SCHEMAS = 300


def _admin_cfg(kind: str) -> dict:
    """The connector's credentials, or a local admin when one is configured.

    The admin login only matters for seeing the throwaway schemas through
    ``information_schema`` the way a public read-everything user (Ensembl's
    ``anonymous``) does.
    """
    cfg = dict(_my_cfg(kind))
    admin_pw = os.environ.get(f"DATAFLOW_TEST_{kind.upper()}_ROOT_PASSWORD")
    if admin_pw:
        cfg.update(user="root", password=admin_pw)
    return cfg


def _admin(kind: str, *statements: str) -> list[tuple]:
    conn = pymysql.connect(autocommit=True, connect_timeout=3, **_admin_cfg(kind))
    try:
        with conn.cursor() as cur:
            rows: list[tuple] = []
            for stmt in statements:
                cur.execute(stmt)
                rows = list(cur.fetchall() or [])
            return rows
    finally:
        conn.close()


def _kill_slow(kind: str) -> None:
    try:
        rows = _admin(
            kind,
            "SELECT id FROM information_schema.processlist "
            f"WHERE info LIKE '%{SLOW_MARK}%' AND info NOT LIKE '%processlist%'",
        )
        for (pid,) in rows:
            _admin(kind, f"KILL QUERY {int(pid)}")
    except Exception:  # noqa: BLE001 — best-effort cleanup
        pass


@pytest.fixture
def ensembl_like(request):
    kind = request.param
    _my(kind, "SELECT 1")  # skips when the server is not running here
    created: list[str] = []
    if kind == "mysql":
        have = int(
            _admin(
                kind,
                "SELECT COUNT(*) FROM information_schema.schemata "
                "WHERE schema_name LIKE 'r11ens\\_%'",
            )[0][0]
        )
        try:
            for i in range(have, N_SCHEMAS):
                name = f"r11ens_{i:03d}"
                _admin(
                    kind,
                    f"CREATE DATABASE IF NOT EXISTS {name}",
                    f"CREATE TABLE IF NOT EXISTS {name}.help_record "
                    "(id INT PRIMARY KEY, v VARCHAR(20)) ENGINE=MyISAM",
                )
                created.append(name)
        except Exception as exc:  # noqa: BLE001 — no CREATE DATABASE grant here
            print(f"throwaway schemas not created ({type(exc).__name__}); slow statement only")
    tag = uuid.uuid4().hex[:6]
    names = {"kind": kind, "src": f"help_record_{tag}", "dst": f"r11hr_{tag}"}
    _my(
        kind,
        f"CREATE TABLE {names['src']} (help_record_id INT UNSIGNED PRIMARY KEY, "
        "keyword VARCHAR(255), description TEXT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        f"INSERT INTO {names['src']} VALUES (1,'gene','Ünïcødé ✓'),(2,NULL,NULL),"
        "(3,'',''),(4,'  ','日本')",
    )
    try:
        yield names
    finally:
        _kill_slow(kind)
        _my(kind, f"DROP TABLE IF EXISTS {names['src']}")
        _sql(f"DROP TABLE IF EXISTS {names['dst']}")
        for name in created:
            try:
                _admin(kind, f"DROP DATABASE IF EXISTS {name}")
            except Exception:  # noqa: BLE001
                pass


@pytest.fixture
def slow_catalog(monkeypatch):
    """One catalog statement of the physical-storage probe that never ends by itself."""
    import services.physical_storage_metadata as psm

    original = psm._probe_mysql

    def slow_probe(cursor, schema, table):
        cursor.execute(SLOW_SQL, ())
        cursor.fetchall()
        return original(cursor, schema, table)

    monkeypatch.setitem(psm._PROBES, "mysql", slow_probe)
    monkeypatch.setenv("DATAFLOW_PREFLIGHT_DEADLINE_SECONDS", str(BUDGET_SECONDS))


def _assert_no_statement_left(kind: str) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        left = _admin(
            kind,
            "SELECT COUNT(*) FROM information_schema.processlist "
            f"WHERE info LIKE '%{SLOW_MARK}%' AND info NOT LIKE '%processlist%'",
        )
        if int(left[0][0]) == 0:
            return
        time.sleep(0.5)
    raise AssertionError("the slow catalog statement is still running on the server")


@pytest.mark.timeout(240)
@pytest.mark.parametrize("ensembl_like", KINDS, indirect=True)
def test_rest_preflight_answers_within_budget_with_a_clear_finding(
    client, ensembl_like, slow_catalog
):
    from services.connector_store import create_connector

    cfg = _admin_cfg(ensembl_like["kind"])
    src = str(
        create_connector(
            {
                "name": f"r11-ens-{uuid.uuid4().hex[:6]}", "type": ensembl_like["kind"],
                "role": "source", "host": cfg["host"], "port": cfg["port"],
                "database": cfg["database"], "username": cfg["user"],
                "password": cfg["password"], "ssl": False, "workspace_id": "",
            }
        ).id
    )
    dst = _connector("postgresql", "destination")
    cols = ["help_record_id", "keyword", "description"]
    started = time.monotonic()
    resp = client.post(
        "/api/v1/preflight/run",
        json={
            "dataset_name": ensembl_like["src"],
            "columns": cols,
            "column_types": {"help_record_id": "INT UNSIGNED", "keyword": "VARCHAR(255)", "description": "TEXT"},
            "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in cols],
            "source_connector_id": src, "source_kind": "database",
            "source_type": ensembl_like["kind"], "source_table": ensembl_like["src"],
            "connector_id": dst, "dest_kind": "database", "dest_type": "postgresql",
            "dest_table": ensembl_like["dst"], "sync_mode": "full_refresh_overwrite",
            "validation_mode": "strict",
        },
    )
    elapsed = time.monotonic() - started
    assert resp.status_code == 200, resp.text
    assert elapsed < BUDGET_SECONDS + 5, elapsed
    body = resp.json()
    budget = [
        w for w in body.get("warnings") or []
        if isinstance(w, dict) and w.get("id") == "catalog_probe_budget"
    ]
    assert budget, body.get("warnings")
    assert "physical-storage" in budget[0]["message"]
    assert "per-statement catalog budget" in budget[0]["message"]
    _assert_no_statement_left(ensembl_like["kind"])


@pytest.mark.timeout(300)
@pytest.mark.parametrize("ensembl_like", KINDS, indirect=True)
def test_engine_preflight_bounds_the_probe_and_the_run_lands(ensembl_like, slow_catalog, monkeypatch, tmp_path):
    """Execute's own preflight: the slow optional probe no longer holds the job."""
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    from tests.test_r11_mysql_pg_fk_ddl_e2e import PG

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *a, **k):
            return None

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    cfg = _admin_cfg(ensembl_like["kind"])
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format=ensembl_like["kind"], host=cfg["host"], port=cfg["port"],
            database=cfg["database"], username=cfg["user"], password=cfg["password"],
            table=ensembl_like["src"],
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", host=PG["host"], port=PG["port"],
            database=PG["dbname"], username=PG["user"], password=PG["password"],
            schema="public", table=ensembl_like["dst"],
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    for _run_no in (1, 2):
        started = time.monotonic()
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        elapsed = time.monotonic() - started
        assert result.success, result.error
        # Two bounded probe calls per run at most, never an unbounded scan.
        assert elapsed < 2 * BUDGET_SECONDS + 30, elapsed
        assert _sql(
            f"SELECT help_record_id, keyword, description FROM {ensembl_like['dst']} "
            "ORDER BY help_record_id"
        ) == [(1, "gene", "Ünïcødé ✓"), (2, None, None), (3, "", ""), (4, "  ", "日本")]
    _assert_no_statement_left(ensembl_like["kind"])
