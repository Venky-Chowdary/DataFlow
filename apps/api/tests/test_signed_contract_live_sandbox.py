"""Signed DataContract + Migration Risk Contract on live Postgres dest COUNT.

Honesty
-------
* Only Postgres ``:5432`` is a real desktop engine in this VM. MySQL / Mongo skip.
* ``require_signed_contract`` must fail-close on ``execute_tracked`` itself, not
  only at the HTTP stamp — skip_preflight / SDK must not invent a DRAFT and write.
* QUARANTINE_ROW hold-out is dest COUNT of good rows, not a finding-only claim.
* ``100%`` is not claimed. Named fixtures only.
"""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

PG = dict(
    host="localhost",
    port=5432,
    database="dataflow",
    username="dataflow",
    password="dataflow",
)


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def _pg_connect():
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        user=PG["username"],
        password=PG["password"],
    )
    conn.autocommit = True
    return conn


def _pg_exec(sql: str, params: tuple | None = None) -> None:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
    finally:
        conn.close()


def _pg_fetch(sql: str, params: tuple | None = None) -> list:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
    finally:
        conn.close()


def _pg_count(table: str) -> int:
    rows = _pg_fetch(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    if not rows or int(rows[0][0]) == 0:
        return 0
    return int(_pg_fetch(f'SELECT COUNT(*) FROM public."{table}"')[0][0])


def _pg_ids(table: str) -> list[int]:
    return [int(r[0]) for r in _pg_fetch(f'SELECT id FROM public."{table}" ORDER BY id')]


def _pg_drop(*tables: str) -> None:
    for table in tables:
        _pg_exec(f'DROP TABLE IF EXISTS public."{table}" CASCADE')


def _pg_ep(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        schema="public",
        table=table,
        ssl=False,
        **PG,
    )


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, "0" * 24)


def _identity_maps() -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "INTEGER",
            "confidence": 0.99,
        },
        {
            "source": "label",
            "target": "label",
            "source_type": "TEXT",
            "target_type": "TEXT",
            "confidence": 0.99,
        },
    ]


def _patch_contract_store(monkeypatch):
    from services import contract_store as cstore
    from services.data_contract import ContractStatus, DataContract

    cstore.reset_contract_store()
    backend = cstore.InMemoryContractStore()
    monkeypatch.setattr(cstore, "get_contract_store", lambda: backend)
    import src.transfer.contract_engine as ce

    monkeypatch.setattr(ce, "get_contract_store", lambda: backend)
    return backend, DataContract, ContractStatus


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="PostgreSQL :5432 not reachable")
def test_live_pg_signed_datacontract_execute_tracked_dest_count(monkeypatch) -> None:
    backend, DataContract, ContractStatus = _patch_contract_store(monkeypatch)
    src_t, dst_t = "ctr_s_" + uuid.uuid4().hex[:8], "ctr_d_" + uuid.uuid4().hex[:8]
    signed = DataContract(
        name="pg-sandbox-signed",
        status=ContractStatus.SIGNED,
        source={"format": "postgresql"},
        destination={"format": "postgresql"},
    )
    backend.save_contract(signed)
    try:
        _pg_drop(src_t, dst_t)
        _pg_exec(f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, label TEXT)')
        _pg_exec(f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, label TEXT)')
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'a'),(2,'b')""")
        result = _run(
            TransferRequest(
                source=_pg_ep(src_t),
                destination=_pg_ep(dst_t),
                mappings=_identity_maps(),
                sync_mode="full_refresh",
                validation_mode="strict",
                skip_preflight=True,
                contract_id=signed.id,
                enforce_contract=True,
                require_signed_contract=True,
            )
        )
        assert result.success, result.error or result.reconciliation
        assert _pg_count(dst_t) == 2, _pg_ids(dst_t)
        assert _pg_ids(dst_t) == [1, 2]
        recon = result.reconciliation or {}
        assert recon.get("passed") is True, recon
    finally:
        _pg_drop(src_t, dst_t)


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="PostgreSQL :5432 not reachable")
def test_live_pg_draft_require_signed_refuses_dest_count_zero(monkeypatch) -> None:
    backend, DataContract, ContractStatus = _patch_contract_store(monkeypatch)
    src_t, dst_t = "ctr_s_" + uuid.uuid4().hex[:8], "ctr_d_" + uuid.uuid4().hex[:8]
    draft = DataContract(
        name="pg-sandbox-draft",
        status=ContractStatus.DRAFT,
        source={"format": "postgresql"},
        destination={"format": "postgresql"},
    )
    backend.save_contract(draft)
    try:
        _pg_drop(src_t, dst_t)
        _pg_exec(f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, label TEXT)')
        _pg_exec(f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, label TEXT)')
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'a'),(2,'b')""")
        result = _run(
            TransferRequest(
                source=_pg_ep(src_t),
                destination=_pg_ep(dst_t),
                mappings=_identity_maps(),
                sync_mode="full_refresh",
                validation_mode="strict",
                skip_preflight=True,
                contract_id=draft.id,
                enforce_contract=True,
                require_signed_contract=True,
            )
        )
        assert result.success is False
        err = (result.error or "").lower()
        assert "signed" in err or "contract" in err, result.error
        assert _pg_count(dst_t) == 0
    finally:
        _pg_drop(src_t, dst_t)


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="PostgreSQL :5432 not reachable")
def test_live_pg_require_signed_without_id_refuses_invented_draft(monkeypatch) -> None:
    _patch_contract_store(monkeypatch)
    src_t, dst_t = "ctr_s_" + uuid.uuid4().hex[:8], "ctr_d_" + uuid.uuid4().hex[:8]
    try:
        _pg_drop(src_t, dst_t)
        _pg_exec(f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, label TEXT)')
        _pg_exec(f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, label TEXT)')
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'a')""")
        result = _run(
            TransferRequest(
                source=_pg_ep(src_t),
                destination=_pg_ep(dst_t),
                mappings=_identity_maps(),
                sync_mode="full_refresh",
                validation_mode="strict",
                skip_preflight=True,
                contract_id="",
                enforce_contract=True,
                require_signed_contract=True,
            )
        )
        assert result.success is False
        assert "no contract_id" in (result.error or "").lower() or "contract" in (
            result.error or ""
        ).lower(), result.error
        assert _pg_count(dst_t) == 0
    finally:
        _pg_drop(src_t, dst_t)


def _signed_age_contract() -> dict[str, Any]:
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column="age",
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Named TEXT→INTEGER hold-out sandbox on live Postgres",
        execution_policy="QUARANTINE_ROW",
        transform="integer",
    ).to_dict()


def _age_maps(contract: dict[str, Any] | None) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = [
        {
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "INTEGER",
            "confidence": 0.99,
        },
        {
            "source": "age",
            "target": "age",
            "source_type": "TEXT",
            "target_type": "INTEGER",
            "transform": "integer",
            "confidence": 0.7,
        },
    ]
    if contract is not None:
        mapped[1]["risk_contract"] = contract
        mapped[1]["risk_acknowledged"] = True
    return mapped


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="PostgreSQL :5432 not reachable")
def test_live_pg_signed_quarantine_row_holdout_dest_count() -> None:
    src_t, dst_t = "mrc_s_" + uuid.uuid4().hex[:8], "mrc_d_" + uuid.uuid4().hex[:8]
    q_table = f"{dst_t}_df_quarantine"
    try:
        _pg_drop(src_t, dst_t, q_table)
        _pg_exec(f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, age TEXT)')
        _pg_exec(f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, age INT)')
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'30'),(2,'nope'),(3,'40')""")
        result = _run(
            TransferRequest(
                source=_pg_ep(src_t),
                destination=_pg_ep(dst_t),
                mappings=_age_maps(_signed_age_contract()),
                sync_mode="full_refresh",
                validation_mode="strict",
                skip_preflight=True,
            )
        )
        assert result.success, result.error or result.reconciliation
        assert _pg_ids(dst_t) == [1, 3], _pg_ids(dst_t)
        assert _pg_count(dst_t) == 2
        ages = {
            int(r[0]): int(r[1])
            for r in _pg_fetch(f'SELECT id, age FROM public."{dst_t}" ORDER BY id')
        }
        assert ages == {1: 30, 3: 40}
        assert _pg_count(q_table) >= 1
    finally:
        _pg_drop(src_t, dst_t, q_table)


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="PostgreSQL :5432 not reachable")
def test_live_pg_unsigned_lossy_refuses_partial_table() -> None:
    src_t, dst_t = "mrc_s_" + uuid.uuid4().hex[:8], "mrc_d_" + uuid.uuid4().hex[:8]
    try:
        _pg_drop(src_t, dst_t)
        _pg_exec(f'CREATE TABLE public."{src_t}" (id INT PRIMARY KEY, age TEXT)')
        _pg_exec(f'CREATE TABLE public."{dst_t}" (id INT PRIMARY KEY, age INT)')
        _pg_exec(f"""INSERT INTO public."{src_t}" VALUES (1,'30'),(2,'nope'),(3,'40')""")
        result = _run(
            TransferRequest(
                source=_pg_ep(src_t),
                destination=_pg_ep(dst_t),
                mappings=_age_maps(None),
                sync_mode="full_refresh",
                validation_mode="strict",
                skip_preflight=True,
            )
        )
        assert result.success is False
        assert _pg_count(dst_t) == 0
    finally:
        _pg_drop(src_t, dst_t)


def test_mysql_signed_contract_sandbox_skipped_when_closed() -> None:
    if _reachable("127.0.0.1", 3306):
        pytest.skip(
            "MySQL :3306 is open — signed-contract dest COUNT belongs on the MySQL "
            "sandbox, not this Postgres-only file"
        )
    pytest.skip("MySQL :3306 closed — no invented binlog / dest COUNT proof")
