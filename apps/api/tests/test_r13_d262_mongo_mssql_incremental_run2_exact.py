"""D262 — Mongo -> SQL Server incremental run 2 lands the source and proves it.

QA (prod 85fd48e7): run 1 31/31; tamper ids 4, 8, 12 (``$unset`` note), 5
(untracked), new 32 (ratio -0.0); run 2 wrote 4 rows then FAILED "Transform-
applied proof failed … changed: 32; extra_in_dest: 31"; the box showed id 12
still holding 'note 12' after the ``$unset``.

Measured on 3e258e90f (same shape, a trim recipe): FAILED "Destination drift on
column note, ratio … changed: 12, 32; extra_in_dest: 31". Three causes:

* key 12 — a field missing from a full Mongo document was the missing sentinel
  ("left alone on update"), so the SQL upsert kept the stale value; absent is
  NULL in a relational destination outside CDC;
* key 32 — Gate-8's canonical number kept the sign of the text "-0.0" while
  the float -0.0 (and SQL Server's 0.0) folded to "0";
* key 31 — the seed-inclusive window re-read the cursor tie the run did not
  rewrite, and a key the recipe's expected population holds was named
  "extra_in_dest" because the write's pin ledger lacked it.
"""

from __future__ import annotations

import datetime
import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def test_canonical_zero_has_one_spelling():
    from services.reconciliation import normalize_cell

    for v in ("-0.0", -0.0, "-0", "0.00", 0):
        assert normalize_cell(v, ddl_type="FLOAT", engine="sqlserver") == "0"
    assert normalize_cell("-0.5", ddl_type="FLOAT", engine="sqlserver") == "-0.5"


def test_mongo_to_sqlserver_run2_completes_and_lands_unset_as_null(tmp_path: Path, monkeypatch):
    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI") or ""
    pw = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD") or os.environ.get("MSSQL_SA_PASSWORD") or ""
    user = os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa")
    try:
        import pyodbc
        from pymongo import MongoClient

        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        cn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=master;"
            f"UID={user};PWD={pw};TrustServerCertificate=yes", timeout=5,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"engine unavailable: {exc}")
    import services.sync_cursor as sc

    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    tag = uuid.uuid4().hex[:6]
    coll, dst = f"r13_d262_{tag}", f"r13_d262_d_{tag}"
    db = client["dataflow_r13"]
    base = datetime.datetime(2026, 1, 1)
    names = ["n", "", "  ", "東京"]
    db[coll].insert_many([
        {"_id": i, "name": names[i % 4], "note": f"note {i}", "ratio": i / 10,
         "updated_at": base + datetime.timedelta(minutes=i)}
        for i in range(1, 32)
    ])
    src = EndpointConfig(kind="database", format="mongodb", connection_string=uri,
                         database="dataflow_r13", table=coll)
    dest = EndpointConfig(kind="database", format="sqlserver", host="localhost", port=1433,
                          database="master", username=user, password=pw, schema="dbo",
                          table=dst, extra={"trust_server_certificate": True})

    def _req():
        return TransferRequest(
            source=src, destination=dest, sync_mode="incremental_deduped",
            skip_preflight=True, validation_mode="strict",
            shape_recipe={"steps": [{"op": "trim", "column": "name"}]},
            schema_policy="propagate_columns",
            stream_contracts=[{"name": coll, "primary_key": "_id", "cursor_field": "updated_at",
                               "cursor_semantics": "modification_timestamp",
                               "sync_mode": "incremental_deduped", "selected": True}],
        )

    try:
        first = UniversalTransferEngine().execute_tracked(_req(), uuid.uuid4().hex[:24])
        assert first.success is True, first.error
        later = base + datetime.timedelta(days=1)
        for i in (4, 8):
            db[coll].update_one({"_id": i}, {"$set": {"name": f"  N{i}  ", "updated_at": later}})
        db[coll].update_one({"_id": 12}, {"$unset": {"note": ""}, "$set": {"updated_at": later}})
        db[coll].update_one({"_id": 5}, {"$set": {"name": "untracked"}})
        db[coll].insert_one({"_id": 32, "name": "n32", "note": None, "ratio": -0.0, "updated_at": later})
        second = UniversalTransferEngine().execute_tracked(_req(), uuid.uuid4().hex[:24])
        assert second.success is True, second.error
        assert second.records_transferred == 4
        cur = cn.cursor()
        cur.execute(f"SELECT _id, name, note, ratio FROM dbo.[{dst}] WHERE _id IN (4, 8, 12, 31, 32) ORDER BY _id")
        got = {int(r[0]): (r[1], r[2], float(r[3])) for r in cur.fetchall()}
        assert got[4][0] == "N4" and got[8][0] == "N8"
        assert got[12][1] is None  # $unset -> NULL
        assert got[31] == ("東京", "note 31", 3.1)  # untouched (31 % 4 == 3)
        assert got[32][1] is None and got[32][2] == 0.0
    finally:
        db.drop_collection(coll)
        try:
            cur = cn.cursor()
            cur.execute(f"IF OBJECT_ID('dbo.[{dst}]') IS NOT NULL DROP TABLE dbo.[{dst}]")
            cn.commit()
        finally:
            cn.close()
            client.close()
