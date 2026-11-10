"""D315: Mongo -> Postgres CDC, documents that lack a nested field.

Repro (QA): 28 documents carry ``payload`` = {k: int, ...}, two do not (one
``payload: null``, one without the field). The plan flattens ``payload`` into
typed columns (``payload_k`` INTEGER, ...). A full refresh loads all 30 with
NULL for the two; the first CDC copy failed with "column 'payload_k': Null
sentinel '__DF_SQL_NULL__' cannot coerce to integer" (exactly-once route) or
quarantined both documents (at-least-once route), and the failed run dropped
the table it had created.

Root cause: ``MongodbChangeStreamCdc.snapshot`` re-serialized the reader's
page rows, which are already wire cells, so the NULL / missing sentinels
became escaped *text*. With that fixed, the exactly-once apply also bound the
missing-field sentinel itself into an INSERT.

Live MongoDB replica set + live PostgreSQL; skipped when either is absent.
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.r13_mapping_harness import (  # noqa: E402
    drop,
    endpoint,
    isolated,  # noqa: F401 - fixture
    run_sql,
)

CFG = {
    "host": "localhost",
    "port": 27017,
    "database": "dataflow_cdc",
    "connection_string": "mongodb://localhost:27017/dataflow_cdc?directConnection=true",
    "ssl": False,
}
NO_PAYLOAD = {7: "null", 9: "absent"}
COLS = ["payload_k", "payload_amount", "payload_day", "payload_flag", "payload_s"]


def _ready() -> bool:
    try:
        from connectors.mongodb_change_stream import MongodbChangeStreamCdc

        return MongodbChangeStreamCdc(CFG, collection="probe", primary_key="_id").is_available()
    except Exception:  # noqa: BLE001 - skip reason below
        return False


pytestmark = pytest.mark.skipif(
    not _ready(), reason="MongoDB replica set (change streams) not reachable"
)


def _doc(i: int) -> dict:
    from bson.decimal128 import Decimal128

    doc = {"_id": i, "name": f"n{i}"}
    if NO_PAYLOAD.get(i) == "null":
        doc["payload"] = None
    elif NO_PAYLOAD.get(i) != "absent":
        doc["payload"] = {
            "k": i,
            "amount": Decimal128(f"{i}.25"),
            "day": dt.datetime(2024, 1, i, tzinfo=dt.timezone.utc),
            "flag": i % 2 == 0,
            "s": f"s{i}",
        }
    return doc


def _expected(doc: dict) -> tuple:
    p = doc.get("payload")
    if not p:
        return (None,) * len(COLS)
    return (p["k"], Decimal(str(p["amount"].to_decimal())), p["day"].date(), p["flag"], p["s"])


def _landed(table: str) -> dict[int, tuple]:
    rows = run_sql(
        "postgresql",
        f'SELECT "_id", payload_k, payload_amount, payload_day, payload_flag, payload_s '  # nosec B608
        f'FROM "{table}"',
    )
    out = {}
    for r in rows:
        day = r[3].date() if isinstance(r[3], dt.datetime) else r[3]
        amount = Decimal(str(r[2])) if r[2] is not None else None
        flag = None if r[4] is None else str(r[4]).lower() in {"true", "t", "1"}
        k = None if r[1] is None else int(r[1])
        out[int(r[0])] = (k, amount, day, flag, r[5])
    return out


def _run(fake, coll: str, table: str, semantics: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    contract = {"name": coll, "selected": True, "sync_mode": "cdc", "primary_key": "_id"}
    if semantics:
        contract["cursor_semantics"] = semantics
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="mongodb", table=coll, **CFG),
        destination=endpoint("postgresql", table),
        sync_mode="cdc",
        validation_mode="balanced",
        stream_contracts=[contract],
        ddl_changes_acknowledged=True,
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id), job_id


@pytest.mark.parametrize("semantics", ["", "cdc_position"], ids=["at_least_once", "exactly_once"])
def test_first_cdc_copy_lands_missing_nested_fields_as_null(isolated, semantics):  # noqa: F811
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc

    endpoint("postgresql", "probe")  # skips when PostgreSQL is not configured
    name = "d315_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="_id")
    coll = cdc.coll
    docs = [_doc(i) for i in range(1, 31)]
    coll.insert_many(docs)
    try:
        result, job_id = _run(isolated, name, name, semantics)
        assert result.success, result.error
        job = isolated.get_job(job_id)
        assert int(job.get("rejected_rows") or 0) == 0, job.get("rejected_rows")
        assert _landed(name) == {d["_id"]: _expected(d) for d in docs}
        if semantics:
            # A second exactly-once run on this route fails on base already
            # ("redelivery checksum mismatch"), independent of missing fields.
            return

        # Change tracking keeps applying documents that lack the field.
        coll.insert_one({"_id": 31, "name": "n31"})
        coll.update_one({"_id": 5}, {"$set": {"name": "renamed"}})
        coll.delete_one({"_id": 6})
        result, _ = _run(isolated, name, name, semantics)
        assert result.success, result.error
        landed = _landed(name)
        assert 6 not in landed
        assert landed[31] == (None,) * len(COLS)
        assert landed[5] == _expected(docs[4])
        names = dict(run_sql("postgresql", f'SELECT "_id", name FROM "{name}"'))  # nosec B608
        assert names[5] == "renamed" and names[31] == "n31"
    finally:
        coll.drop()
        cdc.client.close()
        drop("postgresql", name)
        drop("postgresql", f"{name}_df_quarantine")


def test_failed_first_cdc_copy_says_the_created_table_was_dropped(isolated, monkeypatch):  # noqa: F811
    """A first exactly-once copy that cannot convert a cell fails before any row
    lands, and the engine drops the table it created (D177). The job record
    named a missing destination column and said rows stay "until you address
    capacity"; it must name the conversion and say nothing was kept."""
    import connectors.cdc_eos_sa as eos
    from connectors.mongodb_change_stream import MongodbChangeStreamCdc

    endpoint("postgresql", "probe")
    real = eos._apply_mapped_transforms

    def refuse_k(values, mappings, column_types):
        if str(values.get("payload_k")) == "3":
            raise ValueError(
                "column 'payload_k' value could not be converted: Invalid integer: '3'"
            )
        return real(values, mappings, column_types)

    monkeypatch.setattr(eos, "_apply_mapped_transforms", refuse_k)
    name = "d315f_" + uuid.uuid4().hex[:8]
    cdc = MongodbChangeStreamCdc(CFG, collection=name, primary_key="_id")
    cdc.coll.insert_many([_doc(i) for i in range(1, 6)])
    try:
        result, job_id = _run(isolated, name, name, "cdc_position")
        assert not result.success
        assert run_sql(
            "postgresql",
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{name}'",  # nosec B608
        )[0][0] == 0
        message = str(isolated.get_job(job_id).get("message") or "")
        assert "payload_k" in message and "could not be converted" in message, message
        assert "not on the destination" not in message, message
        assert "capacity" not in message and "stay on the destination" not in message, message
        assert "dropped it again" in message and name in message, message
    finally:
        cdc.coll.drop()
        cdc.client.close()
        drop("postgresql", name)
