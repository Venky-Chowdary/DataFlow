"""D398: assume_timezone on MySQL DATETIME into a new MongoDB collection is
proven, or refused before the run, and correct data is never dropped.

QA job 6afbe66d: MySQL -> new Mongo collection with ``assume_timezone:UTC``
on ``ts`` / ``updated_at`` wrote 21 rows, then failed "Transform-applied proof
failed ... cast_column on ts did not apply" and dropped the collection.

Live route (``start_transfer`` -> confirm), strict, on current code: the
single transform and a pipeline step, in UTC and in America/New_York (a
fall-back hour is resolved to the later instant, the PostgreSQL ``AT TIME
ZONE`` convention, ``transform_engine.attach_timezone``). A wall clock that
does not exist (spring-forward gap) is refused at Validate, before the run.

Live MySQL + MongoDB.
"""

from __future__ import annotations

import datetime as dt
import uuid

import pytest

# Build Settings before ``transfer_client`` blanks DATAFLOW_SEED_DEMO.
import services.config  # noqa: F401
from tests import value_fidelity_route as r
from tests.test_d190_d194_d197_declared_string_types_e2e import (
    _run,
    transfer_client,  # noqa: F401 - fixture
)
from tests.test_d355_mask_pii_json_document_mongo import mongo_db  # noqa: F401

pytestmark = pytest.mark.timeout(300)

UTC = dt.timezone.utc


def _seed(table: str, gap: bool = False) -> None:
    r.execute(
        "mysql",
        f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(20), ts DATETIME, "
        "updated_at DATETIME(3))",
    )
    for i in range(1, 12):
        ts = None if i == 4 else dt.datetime(2024, 3, 10, 1, 30, i)
        if gap and i == 2:
            ts = dt.datetime(2024, 3, 10, 2, 30, 0)  # does not exist in New York
        r.execute(
            "mysql",
            f"INSERT INTO `{table}` VALUES (%s, %s, %s, %s)",  # nosec B608
            (i, f"n{i}", ts, dt.datetime(2024, 11, 3, 1, 30, i, 123000)),
        )


def _connectors(table: str) -> tuple[str, str]:
    from services.connector_store import create_connector

    src = create_connector({
        "name": "p2-my-" + table, "role": "source", "workspace_id": "", "ssl": False,
        **r.dest_config("mysql"),
    })
    dst = create_connector({
        "name": "p2-mongo-" + table, "type": "mongodb", "role": "destination",
        "host": "127.0.0.1", "port": 27017, "database": "dataflow_test",
        "ssl": False, "workspace_id": "",
    })
    return src.id, dst.id


@pytest.mark.parametrize("pipeline", [False, True], ids=["transform", "pipeline_step"])
@pytest.mark.parametrize(
    "zone,ts_utc,upd_utc",
    [
        ("UTC", dt.datetime(2024, 3, 10, 1, 30, 1), dt.datetime(2024, 11, 3, 1, 30, 1, 123000)),
        # 01:30 on 2024-11-03 happens twice in New York; the later (EST) instant.
        ("America/New_York", dt.datetime(2024, 3, 10, 6, 30, 1),
         dt.datetime(2024, 11, 3, 6, 30, 1, 123000)),
    ],
    ids=["utc", "new_york"],
)
def test_assume_timezone_lands_as_bson_dates_and_is_verified(
    transfer_client, mongo_db, zone, ts_utc, upd_utc, pipeline  # noqa: F811
):
    db, created = mongo_db
    table = f"p2d398_{uuid.uuid4().hex[:8]}"
    collection = table + "_m"
    created.append(collection)
    _seed(table)
    try:
        src, dst = _connectors(table)
        tf = f"assume_timezone:{zone}"
        chain = ["trim", tf] if pipeline else tf
        _plan, job = _run(
            transfer_client,
            source_connector_id=src, source_table=table,
            dest_connector_id=dst, dest_table=collection,
            sync_mode="full_refresh_overwrite", upsert_key="id",
            column_transforms={"ts": chain, "updated_at": chain},
        )
        assert int(job.get("records_processed") or job.get("rows_written") or 11) == 11
        docs = {d["id"]: d for d in db[collection].find({}, {"_id": 0})}
        assert sorted(docs) == list(range(1, 12))
        assert docs[1]["ts"] == ts_utc
        assert docs[1]["updated_at"] == upd_utc
        assert docs[4]["ts"] is None
    finally:
        r.drop("mysql", table)


def test_a_wall_clock_that_does_not_exist_is_refused_before_the_run(
    transfer_client, mongo_db  # noqa: F811
):
    from src.ai.copilot.transfer_tools import start_transfer

    db, created = mongo_db
    table = f"p2d398g_{uuid.uuid4().hex[:8]}"
    collection = table + "_m"
    created.append(collection)
    _seed(table, gap=True)
    try:
        src, dst = _connectors(table)
        staged = start_transfer(
            validation_mode="strict",
            source_connector_id=src, source_table=table,
            dest_connector_id=dst, dest_table=collection,
            sync_mode="full_refresh_overwrite", upsert_key="id",
            column_transforms={"ts": "assume_timezone:America/New_York"},
        )
        assert not staged.success, "a DST-gap wall clock passed Validate"
        assert "does not exist" in str(staged.error), staged.error
        assert collection not in db.list_collection_names()
    finally:
        r.drop("mysql", table)
