"""QA1 retest of #372 (S7, S11) on real PostgreSQL / MySQL.

S7: "Use sku as the key" on a CSV create-new left the table with no key. The
CSV identity COPY fast path created the table itself and never received the
declared key, so it now hands keyed create-new loads to the row path, whose
fidelity planner renders the key.

S11: Studio's create-new ran CREATE TABLE with no approve control. Studio's
multipart Execute and its persisted plan never said the policy was declared,
so the create-new DDL gate (which only gates a declared policy) never applied.
"""

from __future__ import annotations

import inspect
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

CSV = b"sku,name,price\nA-1,Alpha,1.50\nB-2,Beta,2.25\nC-3,Gamma,3.00\n"

DESTS = {
    "pg": dict(format="postgresql", host="127.0.0.1", port=5432, username="dataflow",
               password="dataflow", database="dataflow", schema="public"),
    "mysql": dict(format="mysql", host="127.0.0.1", port=3306, username="root",
                  password="dataflow", database="dataflow"),
}


def _conn(dest: str):
    try:
        if dest == "pg":
            import psycopg2

            return psycopg2.connect(host="127.0.0.1", port=5432, user="dataflow",
                                    password="dataflow", dbname="dataflow", connect_timeout=3)
        import pymysql

        return pymysql.connect(host="127.0.0.1", port=3306, user="root",
                               password="dataflow", database="dataflow", connect_timeout=3,
                               autocommit=True)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"{dest} unreachable: {exc}")


def _drop(dest: str, table: str) -> None:
    c = _conn(dest)
    with c.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    c.commit()
    c.close()


def _exists(dest: str, table: str) -> bool:
    c = _conn(dest)
    with c.cursor() as cur:
        if dest == "pg":
            cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table}",))
        else:
            cur.execute("SELECT COUNT(*) > 0 FROM information_schema.tables "
                        "WHERE table_schema='dataflow' AND table_name=%s", (table,))
        out = bool(cur.fetchone()[0])
    c.close()
    return out


def _keys(dest: str, table: str) -> list[tuple[str, str]]:
    c = _conn(dest)
    schema = "public" if dest == "pg" else "dataflow"
    with c.cursor() as cur:
        cur.execute(
            "SELECT tc.constraint_type, kcu.column_name FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name "
            "AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name "
            "WHERE tc.table_schema=%s AND tc.table_name=%s "
            "AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE')",
            (schema, table),
        )
        rows = sorted((str(a), str(b)) for a, b in cur.fetchall())
    c.close()
    return rows


def _count(dest: str, table: str) -> int:
    c = _conn(dest)
    with c.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = int(cur.fetchone()[0])
    c.close()
    return n


def _request(dest: str, table: str, *, key: str, policy: str, declared: bool = False,
             ack: bool = False):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", table=table, **DESTS[dest]),
        source_filename="products.csv",
        source_content=CSV,
        schema_policy=policy,
        schema_policy_declared=declared,
        ddl_changes_acknowledged=ack,
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "products", "selected": True, "sync_mode": "full_refresh_overwrite",
            "primary_key": key, "schema_policy": policy,
        }],
    )


def _execute(req):
    os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    from src.transfer.engine import get_transfer_engine

    return get_transfer_engine().execute(req)


# --- S7: the declared key lands in CREATE TABLE ------------------------------------


@pytest.mark.parametrize("dest", ["pg", "mysql"])
def test_csv_create_new_carries_the_declared_key(dest):
    table = f"qa1_s7_key_{dest}"
    _drop(dest, table)
    res = _execute(_request(dest, table, key="sku", policy="propagate_columns"))
    assert res.success, res.error
    assert _count(dest, table) == 3
    assert [c for _t, c in _keys(dest, table)] == ["sku"]


@pytest.mark.parametrize("dest", ["pg", "mysql"])
def test_csv_create_new_without_a_key_declares_none(dest):
    table = f"qa1_s7_nokey_{dest}"
    _drop(dest, table)
    res = _execute(_request(dest, table, key="", policy="propagate_columns"))
    assert res.success, res.error
    assert _count(dest, table) == 3
    assert _keys(dest, table) == []


# --- S11: Studio declares its policy, so create-new DDL is gated -------------------


def test_studio_run_form_accepts_the_declared_policy():
    from src.routers.transfer_router import run_universal_transfer

    assert "schema_policy_declared" in inspect.signature(run_universal_transfer).parameters


def _plan_preflight_kwargs(policies: dict) -> dict:
    from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
    from services.transfer_plan_store import create_plan

    plan = create_plan({
        "name": "csv-pg-create",
        "source": {"kind": "file", "format": "csv"},
        "destination": {"kind": "database", "format": "postgresql",
                        "connector_id": "dst", "table": "qa1_s11_new"},
        "source_columns": ["sku", "name"],
        "source_schema": {"sku": "VARCHAR", "name": "VARCHAR"},
        "target_columns": [],
        "target_schema": {},
        "sample_rows": [{"sku": "A-1", "name": "Alpha"}],
        "policies": policies,
    })
    sync_plan_mappings(plan.id, [
        {"source": "sku", "target": "sku", "confidence": 0.99},
        {"source": "name", "target": "name", "confidence": 0.99},
    ])
    captured: dict = {}

    def fake_run_file_preflight(**kw):
        captured.update(kw)
        return {"passed": False, "passed_count": 0, "total_gates": 0, "gates": [], "blockers": []}

    with patch("services.transfer_plan_service._preflight") as mock_pf, patch(
        "src.transfer.adapters.read_source_database", side_effect=Exception("skip"),
    ):
        mock_pf.return_value = (
            lambda pf, *_a, **_k: pf,
            lambda mode: 0.85,
            lambda **_k: {"connected": True, "table_exists": False, "can_create_table": True,
                          "db_type": "postgresql", "column_types": {}, "message": "ok"},
            fake_run_file_preflight,
            lambda **_k: [],
        )
        run_plan_preflight(plan.id)
    return captured


@pytest.fixture
def isolated_plan_store(tmp_path, monkeypatch):
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")


def test_studio_plan_validate_gates_create_new_ddl(isolated_plan_store):
    declared = _plan_preflight_kwargs(
        {"schema_policy": "manual_review", "schema_policy_declared": True}
    )
    assert declared["schema_policy_explicit"] is True
    default = _plan_preflight_kwargs({"schema_policy": "manual_review"})
    assert default.get("schema_policy_explicit", False) is False


@pytest.mark.parametrize("dest", ["pg", "mysql"])
def test_declared_manual_review_execute_refuses_unapproved_create(dest):
    table = f"qa1_s11_refuse_{dest}"
    _drop(dest, table)
    res = _execute(_request(dest, table, key="", policy="manual_review", declared=True))
    assert not res.success
    assert "CREATE TABLE was not approved" in str(res.error)
    assert not _exists(dest, table)
    ok = _execute(_request(dest, table, key="", policy="manual_review", declared=True, ack=True))
    assert ok.success, ok.error
    assert _count(dest, table) == 3
