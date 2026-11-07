"""E1-025 — a FAILED MongoDB overwrite keeps the existing collection, and says so.

QA (build 6a2bae9aaa00): a collection holding {_id: 999, label: "SENTINEL"};
an overwrite from a PostgreSQL fixture failed (decimal.Inexact) and the
original collection no longer existed.

On 3e258e90f the overwrite renames the collection aside (the same rename-aside
backup the SQL engines use since round 12) and a failure renames it back, so
the sentinel survives. The job's message still said "Destination already held
1 row(s) before this run. The failed batch was left in place" — the stream's
undo judged the live object before the engine restored the backup, so the
operator was told the bad batch was live when it had been rolled back. The
restore now restamps the note.
"""

from __future__ import annotations

import decimal
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import connectors.mongodb_writer as mongodb_writer  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def _mongo_uri() -> str:
    return os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI") or (
        "mongodb://localhost:27017/?directConnection=true"
    )


@pytest.fixture()
def live(monkeypatch):
    try:
        import psycopg2
        from pymongo import MongoClient

        client = MongoClient(_mongo_uri(), serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        pg = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                              password="dataflow", connect_timeout=3)
        pg.autocommit = True
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"engine unavailable: {exc}")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    tag = uuid.uuid4().hex[:8]
    db = client["dataflow_r13"]
    coll, src = f"r13_e1025_{tag}", f"r13_e1025_s_{tag}"
    db[coll].insert_one({"_id": 999, "label": "SENTINEL", "note": "", "pad": "  ", "nil": None, "u": "東京"})
    with pg.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{src}" (id int PRIMARY KEY, amount numeric, name text)')
        cur.executemany(
            f'INSERT INTO public."{src}" VALUES (%s,%s,%s)',
            [(i, "1.5", ["", "  ", None, "ü"][i % 4]) for i in range(1, 1201)],
        )
    yield db, coll, src
    for name in list(db.list_collection_names()):
        if name.startswith(coll):
            db.drop_collection(name)
    with pg.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
    pg.close()
    client.close()


def _request(coll: str, src: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", host="localhost", port=5432,
                              database="dataflow", username="dataflow", password="dataflow",
                              schema="public", table=src),
        destination=EndpointConfig(kind="database", format="mongodb", connection_string=_mongo_uri(),
                                   database="dataflow_r13", table=coll),
        mappings=[{"source": "id", "target": "_id"}, {"source": "amount", "target": "amount"},
                  {"source": "name", "target": "name"}],
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )


def _assert_kept(db, coll: str, result) -> None:
    assert result.success is False
    assert list(db[coll].find()) == [
        {"_id": 999, "label": "SENTINEL", "note": "", "pad": "  ", "nil": None, "u": "東京"}
    ]
    assert [n for n in db.list_collection_names() if n.startswith(coll)] == [coll]
    err = result.error or ""
    assert "was restored" in err
    assert "left in place" not in err


def test_failure_mid_write_restores_the_collection(live, monkeypatch):
    db, coll, src = live
    real = mongodb_writer._idempotent_insert_many
    calls = {"n": 0}

    def _fails_on_second_batch(collection, docs):
        calls["n"] += 1
        if calls["n"] > 1:
            raise decimal.Inexact("[<class 'decimal.Inexact'>]")
        return real(collection, docs)

    monkeypatch.setattr(mongodb_writer, "_idempotent_insert_many", _fails_on_second_batch)
    result = UniversalTransferEngine().execute_tracked(_request(coll, src), uuid.uuid4().hex[:24])
    assert calls["n"] >= 2  # the replacement had started landing documents
    _assert_kept(db, coll, result)


def test_failed_verification_restores_the_collection(live, monkeypatch):
    db, coll, src = live
    real = engine_mod.run_reconciliation

    def _refuted(**kwargs):
        out = real(**kwargs)
        out["passed"] = False
        out["message"] = "Read-back refuted the load"
        return out

    monkeypatch.setattr(engine_mod, "run_reconciliation", _refuted)
    result = UniversalTransferEngine().execute_tracked(_request(coll, src), uuid.uuid4().hex[:24])
    _assert_kept(db, coll, result)
