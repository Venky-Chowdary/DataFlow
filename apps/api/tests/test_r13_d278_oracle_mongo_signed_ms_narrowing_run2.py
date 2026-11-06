"""D278 — Oracle TIMESTAMP(6) -> existing Mongo collection, incremental upsert, run 2.

QA (prod 3e258e90f): run 1 completed 30/30 with the carriers labelled
"date (BSON instant, 3 fractional digits)"; every later run FAILED its read-back
on a sub-millisecond timestamp (.123456 / .654321) although the microsecond ->
BSON-millisecond narrowing was signed (TRANSFORM_AND_CONTINUE) and declared.

Reproduced live. Two causes, both in the one seam each belongs to:

* the Mongo ``date`` token is BSON's instant, not a calendar day. Resolving it
  to the calendar ``date`` transform refused every cell carrying a clock
  (D291's refusal, shared with Devin's D232 test) or bound it to midnight;
  ``resolve_transform`` now knows the destination kind and the Mongo writer
  resolves through it;
* on run 2 the existing collection's live type is spelled ``TIMESTAMPTZ(6)``,
  so Gate-8 quantized at 6 digits and compared the Oracle microseconds against
  the stored milliseconds. A BSON date keeps 3 digits whatever its spelling:
  ``carrier_instant_digits`` caps a document-instant engine at 3.

Rows cover '' / ' ' / NULL / unicode names, a zoned TIMESTAMP WITH TIME ZONE,
and the tamper of QA's log (ID 5 updated, ID 40 inserted, .123456 / .654321).
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

pytestmark = pytest.mark.timeout(600)


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


def _oracle():
    pw = os.environ.get("DATAFLOW_TEST_ORACLE_PASSWORD", "")
    if not pw:
        pytest.skip("Oracle not configured (DATAFLOW_TEST_ORACLE_PASSWORD, :1521)")
    oracledb = pytest.importorskip("oracledb")
    cfg = dict(
        host=os.environ.get("DATAFLOW_TEST_ORACLE_HOST", "127.0.0.1"), port=1521,
        service=os.environ.get("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1"),
        user=os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system"), password=pw,
    )
    try:
        conn = oracledb.connect(user=cfg["user"], password=pw, dsn=f"{cfg['host']}:1521/{cfg['service']}")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Oracle unavailable: {exc}")
    conn.autocommit = True
    return cfg, conn


def _contract(column: str, source_type: str, table: str) -> dict:
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column=column, source_type=source_type, destination_type="date",
        approved_by="qa@dataflow.app", reason="accept the microsecond -> BSON millisecond narrowing",
        execution_policy="TRANSFORM_AND_CONTINUE", target=column, table=table,
        quarantine_policy="holdout_rejected_rows", expected_precision_loss=True,
    ).to_dict()


def test_run2_after_a_signed_ms_narrowing_completes_and_lands_the_tamper(tmp_path: Path, monkeypatch):
    cfg, oc = _oracle()
    try:
        from pymongo import MongoClient

        uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI") or (
            "mongodb://localhost:27017/?directConnection=true"
        )
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Mongo unavailable: {exc}")
    import services.sync_cursor as sc

    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    tag = uuid.uuid4().hex[:6].upper()
    tbl, coll = f"Q9_ORA_{tag}", f"q9_or2mg_{tag.lower()}"
    db = client["dataflow_r13"]
    cur = oc.cursor()
    cur.execute(f"CREATE TABLE {tbl} (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(200), QTY NUMBER(10), "
                "TS TIMESTAMP(6), UPDATED_AT TIMESTAMP(6), TSTZ TIMESTAMP(6) WITH TIME ZONE)")
    base = datetime.datetime(2026, 1, 1, 8, 0, 0)
    rows = []
    for i in range(1, 31):
        # Bound as text: python-oracledb binds a datetime as Oracle DATE (whole
        # seconds), which would silently drop the microseconds this test is about.
        ts = base + datetime.timedelta(minutes=i, microseconds=123456 if i % 3 else 0)
        text = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        rows.append((i, ["n", "", " ", None, "日本語 ü"][i % 5], i * 10, text, text, f"{text} +02:00"))
    cur.executemany(
        f"INSERT INTO {tbl} VALUES (:1, :2, :3, TO_TIMESTAMP(:4, 'YYYY-MM-DD HH24:MI:SS.FF6'), "
        "TO_TIMESTAMP(:5, 'YYYY-MM-DD HH24:MI:SS.FF6'), "
        "TO_TIMESTAMP_TZ(:6, 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'))",
        rows,
    )
    cur.execute(f"SELECT TO_CHAR(TS, 'FF6') FROM {tbl} WHERE ID = 1")
    assert cur.fetchone()[0] == "123456"  # the source really carries microseconds
    src = EndpointConfig(kind="database", format="oracle", host=cfg["host"], port=1521, database=cfg["service"],
                         username=cfg["user"], password=cfg["password"], schema=cfg["user"].upper(), table=tbl)
    dest = EndpointConfig(kind="database", format="mongodb", connection_string=uri, database="dataflow_r13", table=coll)
    maps = [
        {"source": "ID", "target": "ID", "source_type": "NUMBER(10)", "target_type": "int"},
        {"source": "NAME", "target": "NAME", "source_type": "VARCHAR2(200)", "target_type": "string"},
        {"source": "QTY", "target": "QTY", "source_type": "NUMBER(10)", "target_type": "int"},
        {"source": "TS", "target": "TS", "source_type": "TIMESTAMP(6)", "target_type": "date"},
        {"source": "UPDATED_AT", "target": "UPDATED_AT", "source_type": "TIMESTAMP(6)", "target_type": "date"},
        {"source": "TSTZ", "target": "TSTZ", "source_type": "TIMESTAMP(6) WITH TIME ZONE", "target_type": "date"},
    ]
    for m in maps:
        m.update({"confidence": 0.99, "approved": True})
        if m["target_type"] == "date":
            m["risk_contract"] = _contract(m["source"], m["source_type"], coll)
            m["risk_acknowledged"] = True

    def _req() -> TransferRequest:
        return TransferRequest(
            source=src, destination=dest, mappings=maps, sync_mode="incremental_deduped",
            skip_preflight=True, validation_mode="strict",
            stream_contracts=[{"name": tbl, "primary_key": "ID", "cursor_field": "UPDATED_AT",
                               "cursor_semantics": "modification_timestamp",
                               "sync_mode": "incremental_deduped", "selected": True}],
        )

    try:
        first = UniversalTransferEngine().execute_tracked(_req(), uuid.uuid4().hex[:24])
        assert first.success is True, first.error
        assert first.records_transferred == 30
        assert db[coll].find_one({"ID": 1})["TS"] == datetime.datetime(2026, 1, 1, 8, 1, 0, 123000)
        cur.execute(f"UPDATE {tbl} SET NAME = 'tamper 改 😀', QTY = 777, "
                    f"UPDATED_AT = TIMESTAMP '2026-11-03 09:00:00.123456' WHERE ID = 5")
        cur.execute(f"INSERT INTO {tbl} VALUES (40, 'new', 40, TIMESTAMP '2026-11-03 09:00:00.654321', "
                    "TIMESTAMP '2026-11-03 09:00:00.654321', TIMESTAMP '2026-11-03 09:00:00.654321 +02:00')")
        second = UniversalTransferEngine().execute_tracked(_req(), uuid.uuid4().hex[:24])
        assert second.success is True, second.error
        assert second.records_transferred == 2
        doc5 = db[coll].find_one({"ID": 5})
        assert (doc5["NAME"], doc5["QTY"]) == ("tamper 改 😀", 777)
        assert doc5["UPDATED_AT"] == datetime.datetime(2026, 11, 3, 9, 0, 0, 123000)
        doc40 = db[coll].find_one({"ID": 40})
        assert doc40["TS"] == datetime.datetime(2026, 11, 3, 9, 0, 0, 654000)
        assert doc40["TSTZ"] == datetime.datetime(2026, 11, 3, 7, 0, 0, 654000)  # +02:00 -> UTC
        assert db[coll].count_documents({}) == 31
        names = {d["ID"]: d["NAME"] for d in db[coll].find({"ID": {"$in": [1, 2, 3, 4]}})}
        # Oracle stores VARCHAR2 '' as NULL (D210): the destination holds the source.
        assert names == {1: None, 2: " ", 3: None, 4: "日本語 ü"}
    finally:
        try:
            cur.execute(f"DROP TABLE {tbl} PURGE")
        finally:
            oc.close()
        for name in list(db.list_collection_names()):
            if name.startswith(coll):
                db.drop_collection(name)
        client.close()
