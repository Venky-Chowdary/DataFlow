"""D279 - a unique Oracle key does not "collapse" into one in-file duplicate.

QA (prod 3e258e90f): Oracle ``Q9_ORA`` (ID NUMBER(10,0) PRIMARY KEY, 30
distinct ids) -> Mongo, incremental_upsert. Preflight warned "In-file
duplicate keys on ID: 30 extra row(s) collapsed to one per key (None) (last
wins)" and then wrote all 30 documents.

Root cause (services/data_integrity._check_duplicate_keys): the in-file
collapse read the key with ``row.get("ID")`` while the sample rows carried
the column in another case, so every row read ``None``; and
``unique_equality_key(None)`` is ``''``, so every keyless row shared one key
and "collapsed". The key cell is now looked up case-insensitively (exact
name first), and a row with no key cell keeps its own identity, as
``collapse_keep_last`` already does for NULL keys.
"""

from __future__ import annotations

from services.data_integrity import _check_duplicate_keys

_MAPPINGS = [{"source": "ID", "target": "id"}, {"source": "QTY", "target": "qty"}]


def _check(rows, **kw):
    return _check_duplicate_keys(
        _MAPPINGS, rows, "strict", dest_kind="mongodb", primary_key="ID",
        primary_key_columns=["ID"], sync_mode="incremental_upsert", **kw,
    )


def _texts(result) -> str:
    return repr(result)


def test_unique_ids_in_other_case_do_not_collapse():
    rows = [{"id": str(i), "qty": str(i * 10)} for i in range(1, 31)]
    result = _check(rows)
    assert "collapsed to one per key" not in _texts(result), result
    assert "(None)" not in _texts(result)


def test_rows_without_the_key_cell_are_not_one_key():
    rows = [{"qty": str(i)} for i in range(1, 31)]
    assert "collapsed to one per key" not in _texts(_check(rows))


def test_real_duplicates_in_other_case_are_still_counted_and_named():
    rows = [{"id": "7", "qty": "1"}, {"id": "7", "qty": "2"}, {"id": "8", "qty": "3"}]
    text = _texts(_check(rows))
    assert "1 extra row(s) collapsed to one per key (7)" in text, text


def test_exact_case_unique_and_duplicate_keys():
    unique = [{"ID": str(i)} for i in range(1, 6)]
    assert "collapsed to one per key" not in _texts(_check(unique))
    dup = [{"ID": "1"}, {"ID": "1"}, {"ID": "Łukasz"}, {"ID": "Łukasz"}, {"ID": ""}, {"ID": " "}]
    assert "2 extra row(s) collapsed to one per key" in _texts(_check(dup))


# ------------------------------------------------- BSON types (D279 part 2)
# Root cause: services/create_new_risk_stamp re-invented the create-new stamp
# with the *source* engine bound. The stamp is a destination token: MongoDB's
# integer carrier ``long``. Read as Oracle's spelling it is the text LOB LONG,
# so every Oracle NUMBER(p,0) landed in Mongo as a BSON string and the second
# run blocked below the mapping floor. The stamp is now re-read in the
# destination's own dialect.


def test_oracle_integers_map_to_bson_long_and_oracle_long_stays_text():
    from services.semantic_mapper import map_columns

    schemas = [
        {"name": "ID", "inferred_type": "BIGINT", "samples": ["1"]},
        {"name": "FLAG", "inferred_type": "BIGINT", "samples": ["0"]},
        {"name": "AMOUNT", "inferred_type": "DECIMAL(18,6)", "samples": ["1.5"]},
        {"name": "NOTE", "inferred_type": "LONG", "samples": ["text"]},
    ]
    out = map_columns([s["name"] for s in schemas], [], source_schemas=schemas,
                      destination_db_type="mongodb", source_db_type="oracle",
                      destination_table_exists=False)
    assert {m["source"]: m["target_type"] for m in out} == {
        "ID": "long", "FLAG": "long", "AMOUNT": "decimal", "NOTE": "string"}


import os  # noqa: E402
import socket  # noqa: E402
import uuid  # noqa: E402

import pytest  # noqa: E402

_ORA = dict(
    host="127.0.0.1", port=1521,
    database=os.getenv("DATAFLOW_ORACLE_SERVICE") or "FREEPDB1",
    username=os.getenv("DATAFLOW_ORACLE_USER") or "system",
    password=os.getenv("DATAFLOW_ORACLE_PASSWORD") or "oracle",
)


def _live() -> bool:
    for port in (1521, 27017):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass
        except OSError:
            return False
    try:
        import oracledb

        oracledb.connect(user=_ORA["username"], password=_ORA["password"],
                         dsn=f"127.0.0.1:1521/{_ORA['database']}").close()
        return True
    except Exception:  # noqa: BLE001 - listener up, service not open yet
        return False


@pytest.mark.timeout(180)
def test_oracle_to_mongo_upsert_keeps_numeric_bson_types_over_two_runs(monkeypatch, tmp_path):
    if not _live():
        pytest.skip("live Oracle and MongoDB required")
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import oracledb
    import pymongo
    from bson.decimal128 import Decimal128

    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *a, **k):
            return None

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    table = ("R13D279_" + uuid.uuid4().hex[:8]).upper()
    coll = "r13d279_" + uuid.uuid4().hex[:8]
    oc = oracledb.connect(user=_ORA["username"], password=_ORA["password"],
                          dsn=f"127.0.0.1:1521/{_ORA['database']}")
    mongo = pymongo.MongoClient("mongodb://127.0.0.1:27017", directConnection=True)
    names = ["x", "", " ", "Łukasz 山田 🎉", None]
    try:
        cur = oc.cursor()
        cur.execute(f"CREATE TABLE {table} (ID NUMBER(10,0) PRIMARY KEY, QTY NUMBER(10,0), "
                    "FLAG NUMBER(1), AMOUNT NUMBER(18,6), NAME VARCHAR2(40 CHAR))")
        for i in range(1, 31):
            cur.execute(f"INSERT INTO {table} VALUES (:1, :2, :3, TO_NUMBER(:4), :5)",
                        [i, None if i == 5 else i * 10, i % 2,
                         "99999999999.999999" if i == 9 else f"{i}.5", names[i % 5]])
        oc.commit()
        mode = "incremental_upsert"
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="oracle", schema=_ORA["username"].upper(),
                                  table=table, **_ORA),
            destination=EndpointConfig(kind="database", format="mongodb", host="localhost", port=27017,
                                       database="dataflow_test", collection=coll, table=coll),
            sync_mode=mode, validation_mode="strict",
            stream_contracts=[{"name": table, "primary_key": ["ID"], "cursor_field": "ID",
                               "cursor_semantics": "insert_only", "sync_mode": mode, "selected": True}],
        )
        for run in (1, 2):  # run 2 writes into the collection run 1 created
            res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
            assert res.success, (run, res.error)
            docs = {d["ID"]: d for d in mongo["dataflow_test"][coll].find({}, {"_id": 0})}
            assert len(docs) == 30, run
            assert all(isinstance(k, int) for k in docs), run
            assert docs[9]["QTY"] == 90 and isinstance(docs[9]["QTY"], int)
            assert docs[5]["QTY"] is None
            assert docs[1]["FLAG"] == 1 and docs[2]["FLAG"] == 0
            assert docs[9]["AMOUNT"] == Decimal128("99999999999.999999")
            # Oracle stores '' as NULL, so row 1 is NULL at the source too.
            assert [docs[i]["NAME"] for i in (1, 2, 3, 4, 5)] == [None, " ", "Łukasz 山田 🎉", None, "x"]
    finally:
        mongo["dataflow_test"][coll].drop()
        mongo.close()
        try:
            oc.cursor().execute(f"DROP TABLE {table} PURGE")
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
        oc.close()
