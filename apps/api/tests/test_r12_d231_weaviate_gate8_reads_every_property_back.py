"""D231: Gate-8 reads every Weaviate property back and compares it to the source.

Weaviate Gate-8 stopped at COUNT(DISTINCT source_id) ("cell fidelity
unproven"), so a 62k-char cell or a changed value could not fail the job, and
a rerun into the class the first run filled failed "keyed conservation is
unproven". Every source row must now be an object whose properties equal it;
the report names rows -> objects and any capped 'content' copy. Live Weaviate
on 127.0.0.1:8080; the live legs skip when it is absent.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests

import src.transfer.engine as engine_mod
from services.pgvector_document_verify import _canon_instant
from services.weaviate_document_verify import gate8_weaviate_report, object_expansion
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

BASE = "http://127.0.0.1:8080"
LONG = "lorem ipsum dolor sit amet " * 2300  # 62,100 chars


def _weaviate_up() -> bool:
    try:
        return requests.get(f"{BASE}/v1/meta", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def _seed(path: Path) -> list[dict]:
    rows = []
    for i in range(1, 31):
        rows.append({
            "id": str(i),
            "name": "名前 – 日本語 😀" if i == 2 else f"name_{i}",
            "long_txt": LONG if i == 7 else f"short {i}",
            "qty": str(10 * i),
            "tstz": (datetime(2024, 11, 3, 5, tzinfo=timezone.utc) + timedelta(minutes=37 * i)).isoformat(),
            "note": None if i % 3 == 0 else ("" if i % 3 == 2 else f"note {i}"),
        })
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, long_txt TEXT, qty TEXT, tstz TEXT, note TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (:id, :name, :long_txt, :qty, :tstz, :note)", rows
        )
    conn.close()
    return rows


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    return fake


def _run(fake, src: Path, cls: str):
    job_id = "d231g8" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="weaviate", host="127.0.0.1", port=8080, table=cls),
        sync_mode="full_refresh_append",
        skip_preflight=True,
        validation_mode="balanced",
    )
    return UniversalTransferEngine().execute_tracked(req, job_id)


def _count(cls: str) -> int:
    q = "{ Aggregate { %s { meta { count } } } }" % cls
    body = requests.post(f"{BASE}/v1/graphql", json={"query": q}, timeout=10).json()
    return int(body["data"]["Aggregate"][cls][0]["meta"]["count"])


def test_object_expansion_names_rows_to_objects():
    one = [{"source_id": str(i), "_df_object_count": 1} for i in range(1, 31)]
    assert object_expansion(30, one) == "30 rows -> 30 objects (one per row)"
    chunked = [
        {"source_id": str(i), "_df_object_count": 135 if i == 7 else 1} for i in range(1, 31)
    ]
    assert object_expansion(30, chunked) == "30 rows -> 164 objects (row 7 = 135 chunks)"


def test_weaviate_date_instant_compares_as_utc_to_the_microsecond():
    assert _canon_instant("2024-11-03T14:15:00+00:00") == _canon_instant("2024-11-03T14:15:00Z")
    assert _canon_instant("2024-11-03T09:15:00-05:00") == _canon_instant("2024-11-03T14:15:00Z")
    assert _canon_instant("2024-11-03T14:15:00.000001Z") != _canon_instant("2024-11-03T14:15:00Z")


@pytest.mark.skipif(not _weaviate_up(), reason="Weaviate not reachable on 127.0.0.1:8080")
def test_weaviate_gate8_proves_every_property_and_rerun_stays_verified(tmp_path: Path, isolated):
    src = tmp_path / "src.sqlite"
    rows = _seed(src)
    cls = "D231G8" + uuid.uuid4().hex[:8]
    try:
        for run in (1, 2):
            result = _run(isolated, src, cls)
            recon = result.reconciliation if isinstance(result.reconciliation, dict) else result.reconciliation.model_dump()
            assert result.success, f"run {run}: {result.error}"
            assert _count(cls) == 30, f"run {run}"
            assert recon.get("unproven") is False, f"run {run}: {recon.get('message')}"
            msg = recon.get("message") or ""
            assert "every source value of 30 row(s)" in msg, msg
            assert "30 rows -> 30 objects (one per row)" in msg, msg
            assert "row 7: stored 'content'" in msg and "62,100 chars" in msg, msg
            assert "every mapped source property is stored complete" in msg, msg

        # A stored value that differs from the source fails the proof.
        q = '{ Get { %s(where:{path:["source_id"],operator:Equal,valueText:"7"}) { _additional { id } } } }' % cls
        oid = requests.post(f"{BASE}/v1/graphql", json={"query": q}, timeout=10).json()["data"]["Get"][cls][0]["_additional"]["id"]
        patched = requests.patch(
            f"{BASE}/v1/objects/{cls}/{oid}", json={"class": cls, "properties": {"long_txt": LONG[:-1]}}, timeout=10
        )
        assert patched.status_code in (200, 204), patched.text
        report = gate8_weaviate_report(
            cfg={"host": "127.0.0.1", "port": 8080},
            table_name=cls,
            mappings=[{"source": k, "target": k} for k in rows[0]],
            source_rows=30,
            source_endpoint=None,
            read_sample=lambda *a, **k: [],
            run_records=rows,
            base={},
        )
        assert report is not None and report["passed"] is False
        assert "do not hold 1 of 30" in report["message"], report["message"]
    finally:
        requests.delete(f"{BASE}/v1/schema/{cls}", timeout=10)
