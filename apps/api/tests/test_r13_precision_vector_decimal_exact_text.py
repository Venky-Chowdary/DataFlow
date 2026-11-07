"""D290 / D285 / D265 - a DECIMAL into a vector store is the exact JSON text the plan names.

QA (prod 3e258e90f): PG ``NUMERIC(18,6)`` 99999999999.999999 -> new Qdrant
collection landed as the JSON *number* 100000000000.0 and the job COMPLETED
(D290); the same value into a new Weaviate class created ``amount: number``
and stored 100000000000 (D265, D285). The plan said the column is stored as
exact JSON text.

Root cause: every vector writer serialised its HTTP body with
``json_dumps_exact_numbers`` (the *file* encoder), which writes a Decimal as
an unquoted JSON number. The digits were exact on the wire, but Qdrant,
Weaviate, Milvus and Pinecone parse a JSON number as IEEE float64. Weaviate's
auto-schema then typed the property ``number``.

Fix: ``json_dumps_exact_text`` (Decimal -> exact JSON string, ints/bools/
floats keep their JSON type) is the one body encoder for the vector writers;
a class the Weaviate writer creates declares the mapped properties from the
plan (``weaviate_mapped_property_types``), so auto-schema never invents a
carrier; the Confirm note no longer says integers/booleans are "stored as
text".

No Qdrant/Weaviate server here: recording fakes stand in for the REST API
(they parse the body the way the real server does: ``json.loads``). The
source side of the e2e is the live PostgreSQL.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_EMBEDDING_MODEL", "hash/32")

import connectors.qdrant_writer as qdrant_writer  # noqa: E402
import connectors.weaviate_writer as weaviate_writer  # noqa: E402

BIG = "99999999999.999999"


class _Resp:
    def __init__(self, status: int, payload):
        self.status_code = status
        self.text = json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        return json.loads(self.text)


def _body(data, js):
    if js is not None:
        return js
    if not data:
        return {}
    return json.loads(data if isinstance(data, str) else data.decode())


class QdrantFake:
    """Qdrant REST: collections, upsert, scroll, count, retrieve, delete."""

    def __init__(self):
        self.collections: dict[str, dict] = {}
        self.points: dict[str, dict[str, dict]] = {}
        self.upsert_bodies: list[str] = []

    @staticmethod
    def _coll(url):
        m = re.search(r"/collections/([^/?]+)", url)
        return m.group(1) if m else None

    def get(self, url, headers=None, timeout=None, params=None):
        c = self._coll(url)
        if url.rstrip("/").endswith("/collections"):
            return _Resp(200, {"result": {"collections": [{"name": n} for n in self.collections]}})
        if c in self.collections:
            return _Resp(200, {"result": {"status": "green", "points_count": len(self.points.get(c, {})),
                                          "config": self.collections[c], "payload_schema": {}}})
        return _Resp(404, {"status": {"error": f"Not found: Collection `{c}` doesn't exist!"}})

    def put(self, url, data=None, headers=None, timeout=None, json=None):
        body = _body(data, json)
        c = self._coll(url)
        if url.endswith(f"/collections/{c}"):
            self.collections[c] = {"params": body}
            self.points.setdefault(c, {})
            return _Resp(200, {"result": True, "status": "ok"})
        if "/points" in url:
            self.upsert_bodies.append(data if isinstance(data, str) else data.decode())
            for p in body.get("points", []):
                self.points.setdefault(c, {})[str(p["id"])] = p
            return _Resp(200, {"result": {"status": "completed"}, "status": "ok"})
        return _Resp(200, {"result": True})

    def post(self, url, data=None, headers=None, timeout=None, json=None):
        body = _body(data, json)
        c = self._coll(url)
        pts = list(self.points.get(c, {}).values())
        if url.endswith("/points/scroll"):
            limit = int(body.get("limit") or 100)
            off = body.get("offset")
            ids = [str(p["id"]) for p in pts]
            start = ids.index(str(off)) if off is not None and str(off) in ids else 0
            page = pts[start:start + limit]
            nxt = pts[start + limit]["id"] if start + limit < len(pts) else None
            return _Resp(200, {"result": {"points": [{"id": p["id"], "payload": p.get("payload", {})} for p in page],
                                          "next_page_offset": nxt}, "status": "ok"})
        if url.endswith("/points/count"):
            return _Resp(200, {"result": {"count": len(pts)}, "status": "ok"})
        if url.endswith("/points") and "ids" in body:
            want = {str(i) for i in body["ids"]}
            return _Resp(200, {"result": [{"id": p["id"], "payload": p.get("payload", {})}
                                          for p in pts if str(p["id"]) in want], "status": "ok"})
        if url.endswith("/points/delete"):
            for i in {str(i) for i in body.get("points", [])}:
                self.points.get(c, {}).pop(i, None)
            return _Resp(200, {"result": {"status": "completed"}, "status": "ok"})
        return _Resp(200, {"result": {}, "status": "ok"})

    def delete(self, url, headers=None, timeout=None, data=None, json=None):
        c = self._coll(url)
        self.collections.pop(c, None)
        self.points.pop(c, None)
        return _Resp(200, {"result": True})

    def close(self):
        pass


class WeaviateFake:
    """Weaviate REST + GraphQL with the server's auto-schema rule for new properties."""

    def __init__(self):
        self.classes: dict[str, dict] = {}
        self.objects: dict[str, dict[str, dict]] = {}
        self.batch_bodies: list[str] = []

    @staticmethod
    def _cls(url):
        m = re.search(r"/v1/schema/([^/?]+)", url)
        return m.group(1) if m else None

    def _autoschema(self, cls, props):
        names = {p["name"] for p in self.classes[cls]["properties"]}
        for k, v in props.items():
            if k in names:
                continue
            dt = "boolean" if isinstance(v, bool) else "number" if isinstance(v, (int, float)) else "text"
            self.classes[cls]["properties"].append({"name": k, "dataType": [dt]})
            names.add(k)

    def _store(self, cls, obj):
        self.classes.setdefault(cls, {"class": cls, "properties": []})
        self.objects.setdefault(cls, {})
        props = dict(obj.get("properties", {}))
        self._autoschema(cls, props)
        numeric = {p["name"] for p in self.classes[cls]["properties"] if p["dataType"] == ["number"]}
        for k in numeric & set(props):
            if isinstance(props[k], (int, float)) and not isinstance(props[k], bool):
                props[k] = float(props[k])  # float64 storage, as the server does
        self.objects[cls][obj["id"]] = {"class": cls, "id": obj["id"], "properties": props}

    def get(self, url, headers=None, timeout=None, params=None):
        if url.endswith("/v1/schema"):
            return _Resp(200, {"classes": list(self.classes.values())})
        c = self._cls(url)
        if c:
            return _Resp(200, self.classes[c]) if c in self.classes else _Resp(404, {"error": [{"message": "not found"}]})
        m = re.search(r"/v1/objects/([^/?]+)/([^/?]+)$", url)
        if m:
            o = self.objects.get(m.group(1), {}).get(m.group(2))
            return _Resp(200, o) if o else _Resp(404, {"error": [{"message": "not found"}]})
        if "/v1/objects" in url:
            cls = (params or {}).get("class")
            lim = int((params or {}).get("limit") or 25)
            after = (params or {}).get("after")
            objs = list(self.objects.get(cls, {}).values())
            start = 0
            if after:
                ids = [o["id"] for o in objs]
                start = ids.index(after) + 1 if after in ids else len(objs)
            return _Resp(200, {"objects": objs[start:start + lim]})
        return _Resp(200, {"version": "1.25.0"})

    def post(self, url, data=None, headers=None, timeout=None, json=None):
        body = _body(data, json)
        if url.endswith("/v1/schema"):
            self.classes[body["class"]] = {"class": body["class"], "properties": list(body.get("properties", []))}
            self.objects.setdefault(body["class"], {})
            return _Resp(200, body)
        if url.endswith("/v1/batch/objects"):
            self.batch_bodies.append(data if isinstance(data, str) else data.decode())
            out = []
            for o in body.get("objects", []):
                self._store(o["class"], o)
                out.append({"id": o["id"], "class": o["class"], "result": {"status": "SUCCESS"}})
            return _Resp(200, out)
        if url.endswith("/v1/objects"):
            self._store(body["class"], body)
            return _Resp(200, body)
        if url.endswith("/v1/graphql"):
            q = body.get("query", "")
            m = re.search(r"Aggregate\s*\{\s*(\w+)", q)
            if m:
                cls = m.group(1)
                return _Resp(200, {"data": {"Aggregate": {cls: [{"meta": {"count": len(self.objects.get(cls, {}))}}]}}})
            m = re.search(r"Get\s*\{\s*(\w+)", q)
            if m:
                cls = m.group(1)
                objs = list(self.objects.get(cls, {}).values())
                return _Resp(200, {"data": {"Get": {cls: [{**o["properties"], "_additional": {"id": o["id"]}} for o in objs]}}})
            return _Resp(200, {"data": {}})
        return _Resp(200, {})

    def delete(self, url, headers=None, timeout=None, data=None, json=None):
        c = self._cls(url)
        if c:
            self.classes.pop(c, None)
            self.objects.pop(c, None)
        m = re.search(r"/v1/objects/([^/]+)/([^/?]+)", url)
        if m:
            self.objects.get(m.group(1), {}).pop(m.group(2), None)
        return _Resp(204, {})

    def close(self):
        pass


HEADERS = ["id", "qty", "amount", "label", "note"]
COLUMN_TYPES = {"id": "INT4", "qty": "INT4", "amount": "DECIMAL(18,6)", "label": "TEXT", "note": "TEXT"}
# The plan stamps TEXT for every column of a schemaless destination.
MAPPINGS = [{"source": h, "target": h, "target_type": "TEXT"} for h in HEADERS]
ROWS = [
    ["1", "10", "1.500000", "a", "__DF_SQL_NULL__"],
    ["2", "0", BIG, "", "x"],
    ["3", "-5", "-0.000001", "  ", "Łukasz 山田 🎉"],
    ["4", "__DF_SQL_NULL__", "__DF_SQL_NULL__", "n", ""],
]
EXPECT_AMOUNT = {"1": "1.500000", "2": BIG, "3": "-0.000001"}


def _common(table: str) -> dict:
    return dict(host="127.0.0.1", port=1, database="", username="", password="", schema="",
                connection_string="", ssl=False, table_name=table, headers=HEADERS, data_rows=ROWS,
                mappings=MAPPINGS, column_types=COLUMN_TYPES, embedding_model="hash/32",
                error_policy="strict")


def _check_payloads(payloads: dict[str, dict]) -> None:
    assert set(payloads) == {"1", "2", "3", "4"}
    for sid, p in payloads.items():
        if sid in EXPECT_AMOUNT:
            assert p["amount"] == EXPECT_AMOUNT[sid], (sid, p["amount"])
            assert isinstance(p["amount"], str)
        else:
            assert "amount" not in p  # SQL NULL: omitted, never 0 / "" / null-as-float
        assert isinstance(p["qty"], int) if "qty" in p else True
    assert payloads["2"]["label"] == "" and payloads["3"]["label"] == "  "
    assert payloads["3"]["note"] == "Łukasz 山田 🎉" and payloads["4"]["note"] == ""


def test_json_dumps_exact_text_keeps_decimal_digits_as_one_json_type():
    from services.value_serializer import json_dumps_exact_text

    out = json_dumps_exact_text({"a": Decimal(BIG), "b": Decimal("1.5"), "i": 7, "f": 0.5, "t": True})
    parsed = json.loads(out)
    assert parsed["a"] == BIG and parsed["b"] == "1.5"  # one JSON type per field: text
    assert parsed["i"] == 7 and parsed["f"] == 0.5 and parsed["t"] is True
    assert json.loads(out)["a"] != 100000000000.0


def test_qdrant_writer_sends_decimal_as_exact_json_text(monkeypatch):
    fake = QdrantFake()
    monkeypatch.setattr(qdrant_writer, "_requests_session", lambda: fake)
    result = qdrant_writer.write_mapped_rows(**_common("q15_pg2qd"))
    assert result.ok, result.error
    assert result.rows_written == 4
    assert any(f'"amount": "{BIG}"' in b for b in fake.upsert_bodies), fake.upsert_bodies
    payloads = {p["payload"]["source_id"]: p["payload"] for p in fake.points["q15_pg2qd"].values()}
    _check_payloads(payloads)


def test_weaviate_writer_declares_plan_carriers_and_sends_decimal_as_text(monkeypatch):
    fake = WeaviateFake()
    monkeypatch.setattr(weaviate_writer, "_requests_session", lambda: fake)
    result = weaviate_writer.write_mapped_rows(**_common("PvOraWvFr"))
    assert result.ok, result.error
    assert result.rows_written == 4
    props = {p["name"]: p["dataType"] for p in fake.classes["PvOraWvFr"]["properties"]}
    assert props["amount"] == ["text"], props
    assert props["qty"] == ["int"] and props["label"] == ["text"] and props["note"] == ["text"]
    stored = {o["properties"]["source_id"]: o["properties"] for o in fake.objects["PvOraWvFr"].values()}
    _check_payloads(stored)


def test_weaviate_mapped_property_types_follow_the_payload_rule():
    types = weaviate_writer.weaviate_mapped_property_types(
        [{"source": "ID", "target": "ID", "target_type": "TEXT"},
         {"source": "AMOUNT", "target": "AMOUNT", "target_type": "TEXT"},
         {"source": "FLAG", "target": "FLAG", "target_type": "TEXT"},
         {"source": "RATIO", "target": "RATIO", "target_type": "TEXT"},
         {"source": "TS", "target": "TS", "target_type": "TEXT"},
         {"source": "id", "target": "id", "target_type": "TEXT"}],
        {"ID": "NUMBER(10,0)", "AMOUNT": "NUMBER(18,6)", "FLAG": "BOOLEAN", "RATIO": "BINARY_DOUBLE",
         "TS": "TIMESTAMP(6) WITH TIME ZONE", "id": "INTEGER"},
    )
    assert types == {"ID": "int", "AMOUNT": "text", "FLAG": "text", "RATIO": "text", "TS": "text"}


def test_confirm_note_names_only_the_columns_stored_as_text():
    from src.ai.copilot.transfer_tools import _stored_as_text

    notes = _stored_as_text(
        [{"source": "id", "target": "id", "source_type": "INT4"},
         {"source": "ok", "target": "ok", "source_type": "BOOLEAN"},
         {"source": "amount", "target": "amount", "source_type": "NUMERIC(18,6)"},
         {"source": "ts", "target": "ts", "source_type": "TIMESTAMPTZ"}],
        "qdrant",
    )
    assert [n["column"] for n in notes] == ["amount", "ts"]


# ---------------------------------------------------------------- live PG e2e
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402


@pytest.fixture()
def pg_table():
    if not pg_up():
        pytest.skip("PostgreSQL 127.0.0.1:5432 not reachable")
    psycopg2 = pytest.importorskip("psycopg2")
    c = pg_creds()
    conn = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                            user=c["username"], password=c["password"])
    conn.autocommit = True
    name = "r13_q15_src_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, qty INTEGER, amount NUMERIC(18,6), label TEXT, note TEXT)")
        cur.executemany(
            f"INSERT INTO {name} VALUES (%s,%s,%s,%s,%s)",
            [(1, 10, "1.500000", "a", None), (2, 0, BIG, "", "x"),
             (3, -5, "-0.000001", "  ", "Łukasz 山田 🎉"), (4, None, None, "n", "")],
        )
    try:
        yield c, name
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {name}")
        conn.close()


def _run_engine(pg, table, dest_format, dest_table, fake, module, monkeypatch, runs=2):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *a, **k):
            return None

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    monkeypatch.setattr(module, "_requests_session", lambda: fake)
    src = EndpointConfig(kind="database", format="postgresql", host=pg["host"], port=pg["port"],
                         database=pg["database"], username=pg["username"], password=pg["password"],
                         schema="public", table=table)
    dst = EndpointConfig(kind="database", format=dest_format, host="127.0.0.1", port=1,
                         table=dest_table, extra={"embedding_model": "hash/32"})
    req = TransferRequest(source=src, destination=dst, sync_mode="full_refresh_overwrite",
                          skip_preflight=True, validation_mode="strict")
    results = []
    for _ in range(runs):  # the second run writes into the collection DataFlow created
        res = UniversalTransferEngine().execute_tracked(req, "job" + uuid.uuid4().hex[:8])
        assert res.success, getattr(res, "error", None)
        results.append(res)
    return results


@pytest.mark.timeout(120)
def test_pg_numeric_to_qdrant_is_exact_text_over_two_runs(pg_table, monkeypatch):
    pg, table = pg_table
    fake = QdrantFake()
    _run_engine(pg, table, "qdrant", "q15_pg2qd", fake, qdrant_writer, monkeypatch)
    payloads = {p["payload"]["source_id"]: p["payload"] for p in fake.points["q15_pg2qd"].values()}
    _check_payloads(payloads)
    assert all(f'"amount": {BIG}' not in b for b in fake.upsert_bodies)  # never an unquoted literal


@pytest.mark.timeout(120)
def test_pg_numeric_to_weaviate_class_is_text(pg_table, monkeypatch):
    # One run: an unchanged rerun into a Weaviate class depends on the live
    # GraphQL document-state read, which this HTTP fake does not reproduce
    # faithfully; the live rerun is tests/test_r12_d231_weaviate_rerun_with_mapped_id.py.
    pg, table = pg_table
    fake = WeaviateFake()
    _run_engine(pg, table, "weaviate", "PvPgWv", fake, weaviate_writer, monkeypatch, runs=1)
    props = {p["name"]: p["dataType"] for p in fake.classes["PvPgWv"]["properties"]}
    assert props["amount"] == ["text"] and props["qty"] == ["int"], props
    stored = {o["properties"]["source_id"]: o["properties"] for o in fake.objects["PvPgWv"].values()}
    _check_payloads(stored)


def test_weaviate_plan_stamps_name_the_carrier_the_class_declares():
    """Round-13 review: the plan said TEXT for every column while the class
    declared int / boolean / number. One rule now, from the stored value."""
    from services.decision_kernel import InventContext, invent_dest_type

    stamps = {
        src: invent_dest_type(src, dest_db="weaviate", context=InventContext.CREATE_NEW)
        for src in ("BIGINT", "INT4", "NUMERIC(18,6)", "DOUBLE PRECISION", "REAL", "BOOLEAN",
                    "TIMESTAMPTZ", "UUID", "TEXT", "JSONB")
    }
    assert stamps == {"BIGINT": "int", "INT4": "int", "NUMERIC(18,6)": "text", "DOUBLE PRECISION": "text",
                      "REAL": "text", "BOOLEAN": "text", "TIMESTAMPTZ": "text", "UUID": "text",
                      "TEXT": "text", "JSONB": "text"}, stamps
    declared = weaviate_writer.weaviate_mapped_property_types(
        [{"source": c, "target": c, "target_type": stamps[c]} for c in stamps],
        {c: c for c in stamps},
    )
    assert declared == {c: ("int" if c in ("BIGINT", "INT4") else "text") for c in stamps}
    # Weaviate's ``int`` is the engine's int64 carrier, and the runtime-fit SSOT
    # says so; a bare ``int`` on a SQL engine stays a width-unknown logical stamp.
    from services.type_system import integer_storage_bounds

    assert integer_storage_bounds("int", dest_db="weaviate") == (-(1 << 63), (1 << 63) - 1)
    assert integer_storage_bounds("int", dest_db="postgresql") is None
    assert integer_storage_bounds("INTEGER", dest_db="sqlite") == (-(1 << 63), (1 << 63) - 1)
