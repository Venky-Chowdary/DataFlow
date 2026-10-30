"""D206 (provisional) - a JSON number the destination's native JSON type
cannot hold exactly is refused at preflight, never rounded on write.

QA: PG jsonb ``{"n": 12345678901234567890.123}`` into MySQL. MySQL ``JSON``
stores numbers as IEEE doubles (docs/ZERO_LOSS_PROPERTIES.md, Property 8,
"JSON number spelling"), so the document landed as ``1.2345678901234567e19``
and Gate-8 failed the run *after* the write; preflight said nothing.

Now preflight refuses before the write, naming the column, the value and the
remedy (a text carrier, or an explicit risk-contract acceptance). With the
text-carrier remedy the document loads exactly; with a signed quarantine
contract the row is held out and never stored rounded.

Route: ``start_transfer`` (strict) -> ``/api/v1/copilot/confirm`` -> job,
PG -> MySQL, twice for the loading cases.
"""

from __future__ import annotations

import json
import uuid
from decimal import Decimal

import pytest

from tests import value_fidelity_route as r

BIG = "12345678901234567890.123"
# id -> jsonb literal (None = SQL NULL)
_ROWS = {
    1: '{"n": %s}' % BIG,
    2: '{"n": 1.5, "s": "ok"}',
    3: '{"n": 42}',
    4: "null",
    5: None,
    6: '{"u": "Łukasz 山田", "e": "", "w": "  "}',
    7: '[1, 2.25, -3]',
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


@pytest.fixture
def route(client):
    r.dest_config("mysql")
    token = uuid.uuid4().hex[:8]
    table = f"d206_{token}"
    r.execute("postgresql", f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, doc JSONB, note TEXT)")
    for i, doc in _ROWS.items():
        note = {5: None, 6: "", 7: "   "}.get(i, f"row {i}")
        r.execute("postgresql", f"INSERT INTO {table} VALUES (%s, %s::jsonb, %s)",  # nosec B608
                  (i, doc, note))
    src_id, dst_id = r.connectors(token, "mysql")
    dest = f"{table}_out"
    try:
        yield {"src": src_id, "dst": dst_id, "table": table, "dest": dest}
    finally:
        r.execute("postgresql", f"DROP TABLE IF EXISTS {table}")  # nosec B608
        r.drop("mysql", dest)
        r.drop("mysql", f"{dest}_df_quarantine")


def _args(route, **extra):
    return dict(source_connector_id=route["src"], source_table=route["table"],
                dest_connector_id=route["dst"], dest_table=route["dest"],
                sync_mode="full_refresh_overwrite", primary_key="id", **extra)


def _wait(job_id: str, timeout_s: float = 180.0) -> dict:
    import time

    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") not in {"", "pending", "queued", "running", "starting"}:
            return job
        time.sleep(0.25)
    return job


def _dest_rows(route) -> dict[int, object]:
    rows = r.query("mysql", f"SELECT id, doc FROM `{route['dest']}`")  # nosec B608
    return {int(a): b for a, b in rows}


def test_preflight_refuses_a_number_mysql_json_would_round(client, route):
    staged, _plan, blockers = r.stage(**_args(route))
    text = f"{staged.error} {blockers}"
    assert not staged.success, "preflight passed a document MySQL JSON would round"
    assert "doc" in text, text
    assert BIG in text, text  # names the value
    assert "LONGTEXT" in text, text  # names the text-carrier remedy
    assert "text" in text.lower() and ("risk" in text.lower() or "contract" in text.lower()), text
    # Nothing was written.
    assert not r.query("mysql", "SELECT table_name FROM information_schema.tables "  # nosec B608
                       f"WHERE table_schema = DATABASE() AND table_name = '{route['dest']}'")


def test_text_carrier_remedy_loads_every_document_exactly(client, route):
    # The remedy the refusal names: carry the column as text (LONGTEXT).
    r.execute("mysql", f"CREATE TABLE `{route['dest']}` (id BIGINT PRIMARY KEY, "
              "doc LONGTEXT, note LONGTEXT) CHARACTER SET utf8mb4")
    for run in (1, 2):
        r.run(client, **_args(route))
        got = _dest_rows(route)
        assert set(got) == set(_ROWS), (run, got)
        assert got[5] is None  # SQL NULL
        assert got[4] == "null"  # JSON null, distinct
        assert BIG in got[1], (run, got[1])
        for i, want in _ROWS.items():
            if want is not None:
                assert json.loads(got[i], parse_float=Decimal) == json.loads(want, parse_float=Decimal)


def test_signed_quarantine_contract_holds_the_row_out_never_rounds(client, route):
    contract = {"approved_by": "qa", "reason": "hold out what MySQL JSON would round",
                "execution_policy": "CAST_AND_CONTINUE", "on_cast_failure": "quarantine"}
    staged, _plan, blockers = r.stage(**_args(route, risk_acceptance=contract))
    assert staged.success, (staged.error, blockers)
    resp = client.post("/api/v1/copilot/confirm",
                       json={"ack_id": staged.output["ack_id"], "reason": "d206"})
    assert resp.status_code == 200 and resp.json().get("ok") is True, resp.text
    job = _wait(str(resp.json().get("job_id") or ""))
    assert str(job.get("status")) in {"completed", "completed_with_quarantine"}, job.get("error")
    got = _dest_rows(route)
    assert 1 not in got, got[1] if 1 in got else None  # never stored rounded
    assert set(got) == set(_ROWS) - {1}
    assert json.loads(got[2]) == {"n": 1.5, "s": "ok"} and got[4] == "null" and got[5] is None


@pytest.mark.parametrize("engine,ddl,token,lossy", [
    ("mysql", "JSON", BIG, True),
    ("mysql", "JSON", "1.10", False),                 # value-equal: 1.1 is the same number
    ("mysql", "JSON", "18446744073709551615", False),  # uint64 is exact
    ("mysql", "JSON", "18446744073709551616", True),
    ("mysql", "LONGTEXT", BIG, False),                # text carrier
    ("mariadb", "JSON", BIG, False),                  # MariaDB JSON is LONGTEXT
    ("postgresql", "JSONB", BIG, False),
    ("snowflake", "VARIANT", BIG, False),             # 23 digits fit NUMBER(38)
    ("snowflake", "VARIANT", "1" * 39, True),
    ("snowflake", "VARIANT", "1.5e400", True),
    ("mongodb", "OBJECT", BIG, False),                # Decimal128 holds 34 digits
    ("mongodb", "OBJECT", "1" * 35 + ".5", True),
    ("mongodb", "OBJECT", "9223372036854775807", False),
    ("mongodb", "OBJECT", "9223372036854775808", True),  # BSON has no int past int64
])
def test_capacity_rule_per_engine(engine, ddl, token, lossy):
    from services.json_number_capacity import json_numbers_not_held

    found = json_numbers_not_held('{"a": [%s]}' % token, engine=engine, ddl_type=ddl)
    assert bool(found) is lossy, found


def test_mongodb_destination_refuses_a_number_bson_cannot_hold(client):
    """Same rule into MongoDB: 36 significant digits fit neither a double nor
    Decimal128 (the writer used to fall back to a *string*). Real local Mongo."""
    import os

    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or os.environ.get("MONGO_URI") or ""
    if not uri.startswith("mongodb"):
        pytest.skip("no local MongoDB")
    from services.connector_store import create_connector

    wide = "1" * 35 + ".5"
    token = uuid.uuid4().hex[:8]
    table = f"d206m_{token}"
    r.execute("postgresql", f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, doc JSONB)")
    r.execute("postgresql", f"INSERT INTO {table} VALUES (1, %s::jsonb), (2, %s::jsonb)",  # nosec B608
              ('{"n": %s}' % wide, '{"n": %s}' % BIG))
    try:
        src = create_connector({"name": f"d206m-src-{token}", "role": "source", "workspace_id": "",
                                "ssl": False, **r.dest_config("postgresql")})
        dst = create_connector({"name": f"d206m-dst-{token}", "role": "destination",
                                "workspace_id": "", "type": "mongodb", "connection_string": uri,
                                "database": "dataflow", "ssl": False})
        staged, _plan, blockers = r.stage(
            source_connector_id=src.id, source_table=table, dest_connector_id=dst.id,
            dest_table=f"{table}_out", sync_mode="full_refresh_overwrite", primary_key="id")
        text = f"{staged.error} {blockers}"
        assert not staged.success, text
        assert wide[:30] in text and "JSON_NUMBER_NOT_EXACT" in text, text
        assert BIG not in text, text  # 23 digits fit Decimal128: not refused
    finally:
        r.execute("postgresql", f"DROP TABLE IF EXISTS {table}")  # nosec B608
