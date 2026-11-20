"""D349 (non-PG) and D147 on the QA route into MySQL.

D349: ``column_transforms {payload: redact}`` on a MongoDB field that the
reader flattens into ``payload_k`` / ``payload_nested`` / ``payload_nested_a``
/ ``payload_s``. On Mongo -> PostgreSQL every child inherits the protection;
the story asks for the same on non-PG destinations, where the DDL and writer
paths differ. MySQL is the non-PG engine available locally; SQL Server and
Oracle share the same mapping-stage inheritance (``services.flatten_protection``)
but are not run here.

D147: BSON dates and an empty ``{}`` sub-document into a NEW MySQL table on
the strict plan -> confirm route: dates land typed, the ``{}`` document lands
with NULLs, and the flattened columns are created NULLable.

Live MongoDB + MySQL; skipped when either is absent.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid

import pytest

# Build Settings before ``transfer_client`` blanks DATAFLOW_SEED_DEMO: a blank
# is not a boolean, and in a single-file run nothing else imported it first.
import services.config  # noqa: F401,E402
from tests.test_d190_d194_d197_declared_string_types_e2e import (
    _mongo_source,
    _run,
    transfer_client,  # noqa: F401 - fixture
)
from tests.test_d349_flatten_children_inherit_protection import (
    _CHILDREN,
    _assert_no_clear_payload,
    _docs,
    mongo_coll,  # noqa: F401 - fixture
)

pytestmark = pytest.mark.timeout(300)


def _my():
    pymysql = pytest.importorskip("pymysql")
    from tests.helpers.live_env import mysql_creds

    c = mysql_creds()
    try:
        return pymysql.connect(
            host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
            database=c["database"], autocommit=True, connect_timeout=3,
        ), c
    except Exception as exc:  # environmental
        pytest.skip(f"MySQL not reachable: {exc}")


def _my_sql(sql: str, params=None):
    conn, _c = _my()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


def _my_connector(tag: str) -> str:
    from services.connector_store import create_connector

    conn, c = _my()
    conn.close()
    return str(
        create_connector(
            {
                "name": f"{tag}-my-{uuid.uuid4().hex[:6]}", "type": "mysql",
                "role": "destination", "host": c["host"], "port": int(c["port"]),
                "database": c["database"], "username": c["username"],
                "password": c["password"], "ssl": False, "workspace_id": "",
            }
        ).id
    )


def _columns(table: str) -> dict[str, tuple[str, str]]:
    return {
        r[0]: (str(r[1]).lower(), r[2])
        for r in _my_sql(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s",
            (table,),
        )
    }


def _rows(table: str, cols: list[str]) -> list[dict]:
    quoted = ", ".join(f"`{c}`" for c in cols)
    return [
        dict(zip(cols, r))
        for r in _my_sql(f"SELECT {quoted} FROM `{table}` ORDER BY `_id`")  # nosec B608
    ]


def _drop(table: str) -> None:
    _my_sql(f"DROP TABLE IF EXISTS `{table}`")
    _my_sql(f"DROP TABLE IF EXISTS `{table}_df_quarantine`")


@pytest.mark.parametrize("parent_tf", ["redact", "hash_pii", "mask_pii", "omit"])
def test_mongo_to_mysql_protects_every_flattened_child(
    transfer_client, mongo_coll, parent_tf  # noqa: F811
):
    docs = _docs()
    mongo_coll.insert_many([dict(d) for d in docs])
    table = mongo_coll.name + "_my"
    _drop(table)
    try:
        plan, _job = _run(
            transfer_client,
            source_connector_id=_mongo_source(mongo_coll),
            source_table=mongo_coll.name,
            dest_connector_id=_my_connector("d349"),
            dest_table=table,
            sync_mode="full_refresh_overwrite",
            column_transforms={"payload": parent_tf},
            pii_acknowledgement={"approved_by": "qa@example.com", "reason": "d349"},
        )
        cols = _columns(table)
        if parent_tf == "omit":
            assert not any(c.startswith("payload") and c != "payload_id" for c in cols), cols
            return
        assert set(cols) >= _CHILDREN | {"payload"}, cols
        names = ["_id"] + sorted(c for c in cols if c.startswith("payload"))
        rows = _rows(table, names)
        assert len(rows) == len(docs)
        _assert_no_clear_payload(rows, docs, parent_tf)
        if parent_tf == "redact":
            for row in rows:
                for col in _CHILDREN | {"payload"}:
                    value = row[col]
                    # A JSON carrier holds the token as a JSON string.
                    text = json.loads(value) if str(cols[col][0]) == "json" else value
                    assert text == "<redacted>", (col, value)
        inherited = {i["column"] for i in plan.get("inherited_protection") or []}
        assert inherited == _CHILDREN
    finally:
        _drop(table)


def test_mongo_dates_and_empty_subdocument_land_in_a_new_mysql_table(
    transfer_client, mongo_coll  # noqa: F811
):
    docs = []
    for i in range(1, 11):
        doc = {
            "_id": i,
            "ts": dt.datetime(2024, 1, 1, 10, 0, i, 123000),
            "payload": {"k": i, "nested": {"a": [i, i + 1]}, "s": f"s{i}"},
        }
        if i == 7:
            doc["payload"] = {}
        docs.append(doc)
    mongo_coll.insert_many(docs)
    table = mongo_coll.name + "_d147"
    _drop(table)
    try:
        _run(
            transfer_client,
            source_connector_id=_mongo_source(mongo_coll),
            source_table=mongo_coll.name,
            dest_connector_id=_my_connector("d147"),
            dest_table=table,
            sync_mode="full_refresh_overwrite",
        )
        cols = _columns(table)
        assert cols["ts"][0] in {"datetime", "timestamp"}, cols["ts"]
        for child in ("payload_k", "payload_nested_a", "payload_s"):
            assert cols[child][1] == "YES", (child, cols[child])
        rows = {r["_id"]: r for r in _rows(table, ["_id", "ts", "payload_k", "payload_s"])}
        assert len(rows) == 10
        assert rows[1]["ts"] == dt.datetime(2024, 1, 1, 10, 0, 1, 123000)
        assert rows[7]["payload_k"] is None and rows[7]["payload_s"] is None
        assert int(rows[8]["payload_k"]) == 8
    finally:
        _drop(table)
