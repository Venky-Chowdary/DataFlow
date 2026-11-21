"""Mapping Witness: likely swaps, unit and format mismatches, wrong targets — and no false flags.

The four fixtures the witness must catch (first/last swap, cents vs dollars,
dd/mm vs mm/dd, email vs username) are each checked twice: keyed against
destination rows another system loaded correctly, and source-only before any
load. The correct mapping must come back without a single flag in every mode.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from services.mapping_witness import (
    KIND_FORMAT,
    KIND_SWAP,
    KIND_UNIT,
    KIND_WRONG_TARGET,
    MODE_KEYED,
    MODE_PROFILE,
    MODE_SOURCE_ONLY,
    STATUS_DIFFERS,
    STATUS_FLAGGED,
    STATUS_NO_EVIDENCE,
    STATUS_WITNESSED,
    norm,
    witness_mappings,
)
from tests.helpers import witness_fixtures as F


@pytest.fixture(scope="module")
def data():
    return F.customers()


def _flags(report):
    return {p["source"]: p["flag"] for p in report["pairs"] if p["flag"]}


def _keyed(maps, data, locale="DMY"):
    src, dst = data
    return witness_mappings(maps, src, dst, key=F.KEY, date_locale=locale)


def _source_only(maps, data, locale="DMY"):
    return witness_mappings(maps, data[0], None, date_locale=locale)


@pytest.mark.parametrize("locale", ["DMY", ""])
def test_correct_mapping_is_witnessed_on_every_key_and_never_flagged(data, locale):
    report = _keyed(F.CORRECT_MAPPING, data, locale)
    assert report["mode"] == MODE_KEYED
    assert report["summary"]["flagged"] == 0
    assert report["summary"]["witnessed"] == len(F.CORRECT_MAPPING)
    joined = next(p for p in report["pairs"] if p["source"] == "joined")
    # Per-value day/month detection leaves ambiguous dates out rather than guessing.
    assert joined["evidence"]["compared"] <= 60


@pytest.mark.parametrize("locale", ["DMY", ""])
def test_correct_mapping_has_no_flags_before_load(data, locale):
    report = _source_only(F.CORRECT_MAPPING, data, locale)
    assert report["mode"] == MODE_SOURCE_ONLY
    assert report["summary"]["flagged"] == 0
    assert report["summary"]["witnessed"] == 0  # nothing to witness against before load


def test_first_last_swap_is_flagged_on_keys_with_the_other_column(data):
    flags = _flags(_keyed(F.SWAPPED_NAMES, data))
    assert set(flags) == {"given", "family"}
    assert flags["given"]["kind"] == KIND_SWAP and flags["given"]["suggested_target"] == "first_name"
    assert flags["family"]["kind"] == KIND_SWAP and flags["family"]["suggested_target"] == "last_name"
    assert flags["given"]["action"] == "review"


def test_first_last_swap_is_flagged_before_load_from_name_profiles(data):
    flags = _flags(_source_only(F.SWAPPED_NAMES, data))
    assert set(flags) == {"given", "family"}
    assert {f["kind"] for f in flags.values()} == {KIND_SWAP}


def test_one_sided_name_evidence_is_not_a_swap(data):
    """Surnames headed for first_name alone (no given names going to last_name) prove nothing."""
    maps = [m for m in F.SWAPPED_NAMES if m["source"] != "given"]
    assert _flags(_source_only(maps, data)) == {}


def test_dollars_into_cents_is_a_unit_mismatch_on_keys_and_before_load(data):
    keyed = _flags(_keyed(F.DOLLARS_INTO_CENTS, data))
    assert set(keyed) == {"balance"} and keyed["balance"]["kind"] == KIND_UNIT
    assert "100 times" in keyed["balance"]["reason"]
    before = _flags(_source_only(F.DOLLARS_INTO_CENTS, data))
    assert set(before) == {"balance"} and before["balance"]["kind"] == KIND_UNIT


def test_cents_source_into_a_currency_column_is_flagged_before_load():
    rows = [{"id": i, "total_cents": 1000 + i * 37} for i in range(20)]
    flags = _flags(witness_mappings([{"source": "total_cents", "target": "order_total",
                                      "target_type": "DECIMAL(12,2)"}], rows))
    assert flags["total_cents"]["kind"] == KIND_UNIT


def test_day_first_dates_read_month_first_are_a_format_mismatch(data):
    keyed = _flags(_keyed(F.CORRECT_MAPPING, data, "MDY"))
    assert set(keyed) == {"joined"} and keyed["joined"]["kind"] == KIND_FORMAT
    assert "day first" in keyed["joined"]["reason"]
    before = _flags(_source_only(F.CORRECT_MAPPING, data, "MDY"))
    assert set(before) == {"joined"} and before["joined"]["kind"] == KIND_FORMAT


def test_mixed_day_month_order_in_one_column_is_flagged():
    rows = [{"d": v} for v in ["13/02/2024", "25/12/2023", "02/14/2024", "12/31/2023", "01/05/2024", "17/07/2022"]]
    flags = _flags(witness_mappings([{"source": "d", "target": "event_date"}], rows))
    assert flags["d"]["kind"] == KIND_FORMAT and "mixed" in flags["d"]["reason"]


def test_usernames_into_email_are_flagged_and_point_at_username(data):
    keyed = _flags(_keyed(F.USERNAME_INTO_EMAIL, data))
    assert set(keyed) == {"login", "mail"}
    assert keyed["login"]["kind"] == KIND_SWAP and keyed["login"]["suggested_target"] == "username"
    only_login = _flags(_keyed(F.LOGIN_INTO_EMAIL_ONLY, data))
    assert only_login["login"]["kind"] == KIND_WRONG_TARGET
    assert only_login["login"]["suggested_target"] == "username"
    before = _flags(_source_only(F.LOGIN_INTO_EMAIL_ONLY, data))
    assert before["login"]["kind"] == KIND_FORMAT and "user names" in before["login"]["reason"]


def test_values_never_leave_the_witness(data):
    src, dst = data
    for maps in (F.SWAPPED_NAMES, F.DOLLARS_INTO_CENTS, F.USERNAME_INTO_EMAIL, F.CORRECT_MAPPING):
        for report in (_keyed(maps, data, "MDY"), _source_only(maps, data, "MDY")):
            text = json.dumps(report)
            for row in src[:20]:
                for col in ("given", "family", "mail", "login", "joined", "balance"):
                    assert f'"{row[col]}"' not in text and f" {row[col]} " not in text, col


def test_same_inputs_same_report(data):
    a = _keyed(F.SWAPPED_NAMES, data)
    b = _keyed(F.SWAPPED_NAMES, data)
    a.pop("generated_at"), b.pop("generated_at")
    assert a == b


def test_low_cardinality_columns_never_contradict_each_other_by_chance():
    rows_src = [{"id": i, "active": i % 2 == 0, "verified": i % 2 == 0} for i in range(40)]
    rows_dst = [{"id": i, "is_active": i % 3 == 0, "is_verified": i % 2 == 0} for i in range(40)]
    report = witness_mappings([{"source": "id", "target": "id"}, {"source": "active", "target": "is_active"}],
                              rows_src, rows_dst, key=("id", "id"))
    active = next(p for p in report["pairs"] if p["source"] == "active")
    # Disagreeing but explained by another column that agrees perfectly: a real contradiction.
    assert active["status"] == STATUS_FLAGGED
    rows_dst2 = [{"id": i, "is_active": i % 3 == 0, "flag": True} for i in range(40)]
    report2 = witness_mappings([{"source": "active", "target": "is_active"}], rows_src, rows_dst2, key=("id", "id"))
    active2 = report2["pairs"][0]
    # A constant column "agrees" half the time by chance only: κ keeps it from contradicting.
    assert active2["status"] == STATUS_DIFFERS and active2["flag"] is None


def test_stale_destination_values_are_reported_as_differs_not_flagged(data):
    src, dst = data
    stale = [{**d, "username": d["username"] + "_old"} for d in dst]
    report = witness_mappings(F.CORRECT_MAPPING, src, stale, key=F.KEY)
    login = next(p for p in report["pairs"] if p["source"] == "login")
    assert login["status"] == STATUS_DIFFERS and login["flag"] is None


def test_too_few_values_is_no_evidence_not_a_pass():
    report = witness_mappings([{"source": "e", "target": "email"}], [{"e": "bob"}, {"e": "ann"}])
    assert report["pairs"][0]["status"] == STATUS_NO_EVIDENCE


def test_unkeyed_destination_rows_use_value_overlap(data):
    src, dst = data
    report = witness_mappings(F.LOGIN_INTO_EMAIL_ONLY, src, dst, key=None)
    assert report["mode"] == MODE_PROFILE
    flags = _flags(report)
    assert flags["login"]["kind"] in (KIND_FORMAT, KIND_WRONG_TARGET)
    assert _flags(witness_mappings(F.CORRECT_MAPPING, src, dst, key=None)) == {}


def test_normalization_matches_the_same_value_across_spellings():
    from datetime import date, datetime, timezone
    from decimal import Decimal

    assert norm(Decimal("12.50")) == norm("12.5") == norm(12.5)
    assert norm(True) == norm("t") == norm("TRUE")
    assert norm(date(2024, 1, 2)) == norm("2024-01-02") == norm(datetime(2024, 1, 2, tzinfo=timezone.utc))
    assert norm("2024-01-02 10:00:00") == norm("2024-01-02T10:00:00Z")
    assert norm("  ") is None and norm(None) is None


# ── API ──────────────────────────────────────────────────────────────────────


@pytest.fixture()
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    from src.main import app

    return TestClient(app)


def test_witness_route_returns_evidence_and_review_records_the_person(client, data):
    src, _ = data
    body = {"mappings": F.LOGIN_INTO_EMAIL_ONLY, "source_rows": src, "date_locale": "DMY",
            "destination_table_exists": False, "session_id": "map-session-0001",
            "source_ref": {"kind": "file", "name": "customers.csv"}}
    resp = client.post("/api/v1/transfer/mapping-witness", json=body)
    assert resp.status_code == 200, resp.text
    out = resp.json()
    flagged = [p for p in out["report"]["pairs"] if p["status"] == STATUS_FLAGGED]
    assert [p["source"] for p in flagged] == ["login"]
    assert out["report"]["notes"] == ["The destination table does not exist yet, so only source values were checked."]
    review = client.post("/api/v1/transfer/mapping-witness/review",
                         json={"mapping_digest": out["mapping_digest"], "source": "login", "decision": "changed",
                               "new_target": "username", "session_id": "map-session-0001"})
    assert review.status_code == 200, review.text
    assert review.json()["decision"]["decision"] == "changed"
    missing = client.post("/api/v1/transfer/mapping-witness/review",
                          json={"mapping_digest": out["mapping_digest"], "source": "given", "decision": "kept",
                                "session_id": "map-session-0001"})
    assert missing.status_code == 404


def test_map_and_job_name_the_same_destination_however_it_was_spelled(monkeypatch, tmp_path, data):
    """Review finding 2: Map hashed the raw DTO, the job the resolved endpoint; they never met."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    from services.mapping_witness_service import destination_identity, snapshot_for_request
    from src.main import app
    from src.transfer.models import EndpointConfig

    src, _ = data
    maps = F.LOGIN_INTO_EMAIL_ONLY + [{"source": "legacy_flag", "target": "legacy_flag", "intentional_omit": True}]
    # Map: blank schema, port 0 (Studio's defaults), an omitted row still carrying its target.
    map_side = {"kind": "database", "format": "postgresql", "host": "db.internal.test", "port": 0,
                "database": "crm", "schema": "", "table": "customers"}
    out = TestClient(app).post("/api/v1/transfer/mapping-witness", json={
        "mappings": maps, "source_rows": src, "destination": map_side, "destination_table_exists": False,
        "session_id": "map-session-0001", "source_ref": {"kind": "file", "name": "customers.csv"},
    })
    assert out.status_code == 200, out.text
    # Job: the resolved endpoint (schema public, port 5432); the omitted row sent as Execute sends it.
    job_dest = EndpointConfig(kind="database", format="postgresql", host="db.internal.test", port=5432,
                              database="crm", schema="public", table="customers")
    job_maps = F.LOGIN_INTO_EMAIL_ONLY + [{"source": "legacy_flag", "target": "", "transform": "omit",
                                           "intentional_omit": True}]
    snap = snapshot_for_request(SimpleNamespace(
        workspace_id="", destination=job_dest, mappings=job_maps, witness_session_id="map-session-0001",
        source=SimpleNamespace(kind="file", table="", collection=""), source_filename="customers.csv"))
    assert snap["status"] == "ok" and snap["mapping_digest"] == out.json()["mapping_digest"]
    assert snap["report"]["summary"]["flagged"] == 1
    # The same table spelled other ways is one identity; another table is not.
    sf = dict(kind="database", format="snowflake", host="acme.snowflakecomputing.com", database="DW",
              table="ORDERS")
    assert destination_identity({**sf, "schema": ""}) == destination_identity({**sf, "schema": "PUBLIC"})
    mongo_uri = {"kind": "database", "format": "mongodb", "collection": "orders",
                 "connection_string": "mongodb://app:pw@mongo.internal.test:27017/shop"}
    mongo_fields = {"kind": "database", "format": "mongodb", "collection": "orders", "host": "mongo.internal.test",
                    "port": 27017, "database": "shop"}
    assert destination_identity(mongo_uri) == destination_identity(mongo_fields)
    assert destination_identity({**map_side, "table": "orders"}) != destination_identity(map_side)


# ── live PostgreSQL: the destination is read by the server, keyed ────────────

PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"), "port": int(os.environ.get("PGPORT", "5432")),
    "database": os.environ.get("PGDATABASE", "dataflow"), "username": os.environ.get("PGUSER", "dataflow"),
    "password": os.environ.get("PGPASSWORD", "dataflow"),
}


def test_live_postgres_destination_is_read_by_key_and_the_swap_is_flagged(data):
    import psycopg2

    try:
        conn = psycopg2.connect(host=PG["host"], port=PG["port"], dbname=PG["database"], user=PG["username"],
                                password=PG["password"], connect_timeout=3)
    except Exception as exc:  # noqa: BLE001 — a foreign PG on :5432 refuses these credentials
        pytest.skip(f"project PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    table = f"witness_{uuid.uuid4().hex[:8]}"
    src, dst = data
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE "{table}" (customer_id INT PRIMARY KEY, first_name TEXT, last_name TEXT, '
                'email TEXT, username TEXT, joined_on DATE, balance NUMERIC(12,2), balance_cents BIGINT)')
    try:
        cur.executemany(
            f'INSERT INTO "{table}" VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            [(d["customer_id"], d["first_name"], d["last_name"], d["email"], d["username"], d["joined_on"],
              d["balance"], d["balance_cents"]) for d in dst[:40]],
        )
        from services.mapping_witness_service import run_witness
        from src.transfer.models import EndpointConfig

        endpoint = EndpointConfig(kind="database", format="postgresql", schema="public", table=table, **PG)
        report = run_witness(mappings=F.SWAPPED_NAMES + [], source_rows=src, destination_endpoint=endpoint,
                             key=F.KEY, destination_table_exists=True, date_locale="DMY")
        assert report["mode"] == MODE_KEYED, report["notes"]
        assert report["sample"]["matching_keys"] == 40
        flags = _flags(report)
        assert set(flags) == {"given", "family"} and {f["kind"] for f in flags.values()} == {KIND_SWAP}
        good = run_witness(mappings=F.CORRECT_MAPPING, source_rows=src, destination_endpoint=endpoint,
                           key=F.KEY, destination_table_exists=True, date_locale="DMY")
        assert good["summary"]["flagged"] == 0 and good["summary"]["witnessed"] == len(F.CORRECT_MAPPING)
        assert {p["status"] for p in good["pairs"]} == {STATUS_WITNESSED}
    finally:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


def test_live_witness_dial_obeys_the_workspace_egress_allowlist(data, monkeypatch):
    """The destination read binds the workspace's allowlist; no process-wide allowlist is set."""
    import psycopg2

    import services.egress_guard as egress_guard

    try:
        conn = psycopg2.connect(host=PG["host"], port=PG["port"], dbname=PG["database"], user=PG["username"],
                                password=PG["password"], connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"project PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    for env in ("DATAFLOW_EGRESS_ALLOWLIST", "DATAWRAP_EGRESS_ALLOWLIST"):
        monkeypatch.setenv(env, "")
    monkeypatch.setattr(egress_guard, "_workspace_egress_entries",
                        lambda ws: ["127.0.0.0/8", "::1/128"] if ws == "ws-allowed" else [])
    table = f"witness_{uuid.uuid4().hex[:8]}"
    src, dst = data
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE "{table}" (customer_id INT PRIMARY KEY, first_name TEXT, last_name TEXT)')
    try:
        cur.executemany(f'INSERT INTO "{table}" VALUES (%s,%s,%s)',
                        [(d["customer_id"], d["first_name"], d["last_name"]) for d in dst[:30]])
        from services.mapping_witness_service import run_witness
        from src.transfer.models import EndpointConfig

        endpoint = EndpointConfig(kind="database", format="postgresql", schema="public", table=table, **PG)
        maps = [m for m in F.SWAPPED_NAMES if m["source"] in ("cust_id", "given", "family")]
        allowed = run_witness(mappings=maps, source_rows=src, destination_endpoint=endpoint, key=F.KEY,
                              destination_table_exists=True, workspace_id="ws-allowed")
        assert allowed["mode"] == MODE_KEYED, allowed["notes"]
        denied = run_witness(mappings=maps, source_rows=src, destination_endpoint=endpoint, key=F.KEY,
                             destination_table_exists=True, workspace_id="ws-denied")
        assert denied["mode"] == MODE_SOURCE_ONLY
        assert denied["notes"] == ["The destination could not be read, so only source values were checked."]
    finally:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


def test_a_warehouse_is_read_only_when_asked(data, monkeypatch):
    import services.mapping_witness_service as svc
    from src.transfer.models import EndpointConfig

    reads: list[list[str]] = []
    monkeypatch.setattr(svc, "read_rows", lambda *a, **kw: reads.append(kw["columns"]) or [])
    src, _ = data
    wh = EndpointConfig(kind="database", format="snowflake", host="acme.snowflakecomputing.com", database="DW",
                        schema="PUBLIC", table="CUSTOMERS")
    auto = svc.run_witness(mappings=F.CORRECT_MAPPING, source_rows=src, destination_endpoint=wh, key=F.KEY,
                           destination_table_exists=True)
    assert reads == [] and "only when you choose Check mapping" in auto["notes"][0]
    svc.run_witness(mappings=F.CORRECT_MAPPING, source_rows=src, destination_endpoint=wh, key=F.KEY,
                    destination_table_exists=True, trigger="explicit",
                    candidate_columns=["customer_id", "first_name", "last_name", "email", "username", "joined_on",
                                       "balance", "region", "x; DROP"])
    # Named columns only (never SELECT *): the key, the mapped targets, the catalog's candidates.
    assert reads == [["customer_id", "first_name", "last_name", "email", "username", "joined_on", "balance",
                      "region"]]


def test_submit_freezes_the_evidence_onto_the_job(monkeypatch, tmp_path, data):
    """Review finding 1: the job carries its own copy; later reports for the same mapping change nothing."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import src.transfer.engine as engine_mod
    from services.mapping_witness_service import destination_identity, mapping_digest, save_report
    from src.transfer.job_submission import create_pending_job
    from src.transfer.models import EndpointConfig, TransferRequest

    created: list[dict] = []
    monkeypatch.setattr(engine_mod, "get_mongodb_service",
                        lambda: SimpleNamespace(create_transfer_job=lambda doc: created.append(doc) or "job-1"))
    engine = SimpleNamespace(_resolve_saved_connectors=lambda r: None, _claim_idempotency=lambda r, d: None,
                             _claim_destination=lambda r, d: None, _bind_idempotency_claim=lambda *a: None)
    dest = EndpointConfig(kind="database", format="postgresql", host="db.internal.test", port=5432,
                          database="crm", schema="public", table="customers")
    src, dst = data
    identity = destination_identity(dest, "ws1")
    digest = mapping_digest(F.SWAPPED_NAMES, identity)
    save_report(witness_mappings(F.SWAPPED_NAMES, src, dst, key=F.KEY), workspace_id="ws1", session="plan-0001-x",
                identity=identity, digest=digest, source="database|legacy")
    request = TransferRequest(source=EndpointConfig(kind="database", format="postgresql", table="legacy"),
                              destination=dest, mappings=list(F.SWAPPED_NAMES), workspace_id="ws1",
                              skip_preflight=True)
    request.witness_session_id = "plan-0001-x"
    monkeypatch.setattr("services.data_residency.enforce_request_residency", lambda r: None)
    monkeypatch.setattr("services.callable_intake.apply_callable_intake", lambda r: None)
    assert create_pending_job(engine, request) == "job-1"
    snap = created[0]["mapping_witness"]
    assert snap["status"] == "ok" and snap["mapping_digest"] == digest
    assert snap["report"]["summary"]["flagged"] == 2


def test_the_route_answers_within_its_deadline(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import time as _time

    from fastapi.testclient import TestClient

    import services.mapping_witness_service as svc
    import src.routers.verified_ai_router as router_mod
    from src.main import app

    monkeypatch.setattr(router_mod, "WITNESS_DEADLINE_SECONDS", 0.5)
    monkeypatch.setattr(svc, "run_witness", lambda **kw: _time.sleep(3) or {})
    started = _time.monotonic()
    res = TestClient(app).post("/api/v1/transfer/mapping-witness", json={"mappings": F.CORRECT_MAPPING})
    assert res.status_code == 504 and _time.monotonic() - started < 2.5


def test_hostile_cells_through_the_route_are_a_200(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    from src.main import app

    rows = [{"amount_cents": "1e1000000"} for _ in range(50)] + [{"amount_cents": "1e999999"} for _ in range(50)]
    res = TestClient(app).post("/api/v1/transfer/mapping-witness", json={
        "mappings": [{"source": "amount_cents", "target": "amount_cents"}], "source_rows": rows})
    assert res.status_code == 200, res.text


# ── Round 2 (N2, N6, N7, N8) ─────────────────────────────────────────────────


def _wide(n_cols=80, rows=50):
    cols = [f"c{i:02d}" for i in range(n_cols)] + ["amount (usd)"]
    src = [{"id": r, **{c: f"{c}-{r}" for c in cols}} for r in range(rows)]
    return cols, src


def test_a_wide_table_is_read_in_batches_and_unread_columns_never_read_as_differs(monkeypatch, tmp_path):
    import services.mapping_witness_service as svc
    from services.mapping_witness import STATUS_NOT_READ
    from src.transfer.models import EndpointConfig

    cols, src = _wide()
    dest_table = {r["id"]: dict(r) for r in src}
    calls: list[list[str]] = []

    def fake_read(endpoint, *, workspace_id, key_column, keys, columns):
        calls.append(list(columns))
        if any(not svc._addressable(c) for c in columns):
            raise ValueError("unaddressable column reached the reader")
        return [{c: dest_table[k].get(c) for c in columns} for k in keys]

    monkeypatch.setattr(svc, "read_rows", fake_read)
    maps = [{"source": "id", "target": "id"}] + [{"source": c, "target": c} for c in cols]
    maps.append({"source": "later", "target": "added_later"})  # not in the catalog yet (createNew lagging)
    src = [{**r, "later": "x"} for r in src]
    endpoint = EndpointConfig(kind="database", format="postgresql", host="db.internal.test", table="wide")
    report = svc.run_witness(mappings=maps, source_rows=src, destination_endpoint=endpoint, key=("id", "id"),
                             destination_table_exists=True, candidate_columns=["id", *cols])
    assert len(calls) == 2 and all(len(c) <= svc.READ_BATCH_COLUMNS + 1 for c in calls)
    statuses = {p["source"]: p["status"] for p in report["pairs"]}
    assert statuses["amount (usd)"] == STATUS_NOT_READ
    assert statuses["later"] == STATUS_NOT_READ
    assert "differs" not in statuses.values()
    assert sum(1 for v in statuses.values() if v == "witnessed") == 81


def test_a_failed_read_batch_leaves_its_columns_not_read(monkeypatch):
    import services.mapping_witness_service as svc
    from src.transfer.models import EndpointConfig

    cols, src = _wide(n_cols=70)
    state = {"n": 0}

    def flaky(endpoint, *, workspace_id, key_column, keys, columns):
        state["n"] += 1
        if state["n"] == 2:
            raise OSError("batch two failed")
        return [{c: f"{c}-{k}" if c != "id" else k for c in columns} for k in keys]

    monkeypatch.setattr(svc, "read_rows", flaky)
    maps = [{"source": "id", "target": "id"}] + [{"source": c, "target": c} for c in cols[:70]]
    endpoint = EndpointConfig(kind="database", format="postgresql", host="db.internal.test", table="wide")
    report = svc.run_witness(mappings=maps, source_rows=src, destination_endpoint=endpoint, key=("id", "id"),
                             destination_table_exists=True, candidate_columns=["id", *cols])
    by = {p["source"]: p for p in report["pairs"]}
    assert by["c69"]["status"] == "not_read" and "could not be read" in by["c69"]["reason"]
    assert by["c00"]["status"] == "witnessed"


def test_stores_are_capped_and_pruned(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc

    monkeypatch.setattr(svc, "MAX_RECORDS_PER_WORKSPACE", 5)
    monkeypatch.setattr(svc, "MAX_LIVE_RECORDS_PER_WORKSPACE", 6)
    real_now = svc._now
    monkeypatch.setattr(svc, "_now", lambda: "2020-01-01T00:00:00+00:00")
    for i in range(4):  # records past the evidence window go first
        svc.save_report({"pairs": [], "n": i}, workspace_id="ws1", session=f"session-{i:04d}", identity="x",
                        digest=f"{i:064d}")
    monkeypatch.setattr(svc, "_now", real_now)
    for i in range(4, 11):
        svc.save_report({"pairs": [], "n": i}, workspace_id="ws1", session=f"session-{i:04d}", identity="x",
                        digest=f"{i:064d}")
    data = svc._DOC.load(part="ws1")
    kept = [k for k, r in data.items() if isinstance(r, dict) and r.get("workspace_id") == "ws1"
            and k.startswith("witness::")]
    assert len(kept) == 6, kept  # the hard bound
    assert not any(k.endswith(f"{i:064d}") for k in kept for i in range(5))
    many = [{"source": f"s{i}", "target": f"t{i}", "approved": True} for i in range(1200)]
    for _ in range(3):
        svc.record_map_decisions(workspace_id="ws2", session="session-big1", identity="x", mappings=many,
                                 actor="a@example.com")
    _d, _k, record, _n = svc._activity("ws2", "session-big1", "x")
    assert len(record["map_decisions"]) <= svc.MAX_SOURCES


def test_identity_tells_files_projects_and_quoted_case_apart(monkeypatch):
    from services.mapping_witness_service import destination_identity

    a = {"kind": "file_export", "format": "csv", "output_path": "exports/a.csv"}
    b = {"kind": "file_export", "format": "csv", "output_path": "exports/b.csv"}
    assert destination_identity(a) != destination_identity(b)
    bq = dict(kind="database", format="bigquery", database="sales", table="orders")
    assert destination_identity({**bq, "extra": {"project": "p-one"}}) != \
        destination_identity({**bq, "extra": {"project": "p-two"}})
    pg = dict(kind="database", format="postgresql", host="db.internal.test", database="crm", schema="public")
    assert destination_identity({**pg, "table": "Orders"}) != destination_identity({**pg, "table": "orders"})
    sf = dict(kind="database", format="snowflake", host="acme.snowflakecomputing.com", database="DW", schema="PUBLIC")
    assert destination_identity({**sf, "table": "ORDERS"}) == destination_identity({**sf, "table": "orders"})


def test_identity_is_the_same_with_or_without_a_connector_id(monkeypatch):
    import src.transfer.adapters as adapters
    from services.mapping_witness_service import destination_identity

    cfg = {"type": "postgresql", "host": "db.internal.test", "port": 5432, "database": "crm", "schema": "public",
           "table": "customers"}
    monkeypatch.setattr(adapters, "resolve_connector_config", lambda ep, ws=None: dict(cfg))
    via_id = {"kind": "database", "format": "postgresql", "connector_id": "conn-1", "table": "customers"}
    inline = {"kind": "database", "format": "postgresql", "host": "db.internal.test", "table": "customers"}
    assert destination_identity(via_id) == destination_identity(inline)


def test_the_warehouse_guard_reads_the_resolved_engine(monkeypatch):
    import services.mapping_witness_service as svc
    import src.transfer.adapters as adapters
    from src.transfer.models import EndpointConfig

    monkeypatch.setattr(adapters, "resolve_connector_config", lambda ep, ws=None: {
        "type": "snowflake", "host": "acme.snowflakecomputing.com", "database": "DW", "table": "T"})
    monkeypatch.setattr(svc, "read_rows", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("read a warehouse")))
    saved = EndpointConfig(kind="database", format="", connector_id="conn-sf", table="T")
    report = svc.run_witness(mappings=F.CORRECT_MAPPING, source_rows=F.customers()[0], destination_endpoint=saved,
                             key=F.KEY, destination_table_exists=True)
    assert "only when you choose Check mapping" in report["notes"][0]


def test_floats_beyond_the_bound_are_compared_as_written_not_missing():
    assert norm(1.5e61) == "1.5e+61"
    assert norm(1.5e61) is not None and norm(1.5e61) != norm(2.5e61)


def test_pruning_never_drops_an_open_sessions_evidence_and_says_expired_when_forced(monkeypatch, tmp_path):
    """Round 3 item 1: newer records elsewhere in the workspace must not erase an open session's check or Keep."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc
    from services.mapping_witness import witness_mappings
    from tests.helpers import witness_fixtures as F

    monkeypatch.setattr(svc, "MAX_RECORDS_PER_WORKSPACE", 5)
    maps = F.LOGIN_INTO_EMAIL_ONLY
    src, dst = F.customers()
    report = witness_mappings(maps, src, dst, key=F.KEY)
    [flag] = [p for p in report["pairs"] if p["status"] == "flagged"]
    digest = svc.mapping_digest(maps, "dest-1")
    saved = svc.save_report(report, workspace_id="ws1", session="session-open", identity="dest-1", digest=digest,
                            source="file|a.csv", actor="ann@example.com")
    svc.record_review(workspace_id="ws1", session="session-open", identity="dest-1", digest=digest,
                      source=flag["source"], decision="kept", flag_id=flag["flag"]["id"], actor="ann@example.com")
    for i in range(12):  # other people's checks in the same workspace
        svc.save_report({"pairs": []}, workspace_id="ws1", session=f"session-other-{i:03d}", identity="x",
                        digest=f"{i:064d}")
    snap = svc.build_snapshot(workspace_id="ws1", session="session-open", identity="dest-1", source="file|a.csv",
                              mappings=maps)
    assert snap["report_sha256"] == saved["report_sha256"]
    assert [r["decision"] for r in snap["flag_reviews"]] == ["kept"]

    monkeypatch.setattr(svc, "MAX_LIVE_RECORDS_PER_WORKSPACE", 3)  # the hard bound forces it out
    svc.save_report({"pairs": []}, workspace_id="ws1", session="session-other-last", identity="x", digest="f" * 64)
    snap = svc.build_snapshot(workspace_id="ws1", session="session-open", identity="dest-1", source="file|a.csv",
                              mappings=maps)
    assert snap["report"] is None
    assert snap["not_checked_reason"].startswith("Evidence expired")


# ── Map → job through the real routes (round 3) ──────────────────────────────

CASES = json.loads((Path(__file__).parent / "fixtures" / "witness" / "source_ref_cases.json").read_text(encoding="utf-8"))
MAP_DEST = {"kind": "database", "format": "postgresql", "host": "db.internal.test", "port": 5432, "database": "crm",
            "schema": "public", "table": "customers"}
RUN_DEST = {"dest_kind": "database", "dest_format": "postgresql", "dest_host": "db.internal.test", "dest_port": "5432",
            "dest_database": "crm", "dest_schema": "public", "dest_table": "customers"}


@pytest.fixture()
def submitted(monkeypatch):
    """Requests /transfer/run hands the engine, after the route's own parsing and intake."""
    import src.transfer.engine as engine_mod

    caught: list = []

    class _Stop(Exception):
        pass

    def _create(request):
        caught.append(request)
        raise _Stop()

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: SimpleNamespace(_create_pending_job=_create))
    return caught


def _run(client, caught, form, *, session, mappings, file_name=None, source_extra=None, headers=None):
    data = {**form, **RUN_DEST, "mappings_json": json.dumps(mappings), "witness_session_id": session,
            "skip_preflight": "true", "async_mode": "true", "schema_policy": "manual_review"}
    if source_extra:
        data["source_extra_json"] = json.dumps(source_extra)
    files = {"file": (file_name, b"cust_id,given\n1,Ann\n", "text/csv")} if file_name else None
    before = len(caught)
    client.post("/api/v1/transfer/run", data=data, files=files, headers=headers or {})
    assert len(caught) == before + 1, "the run never reached job creation"
    return caught[-1]


def _map_check(client, src, *, session, source_ref, mappings=None, trigger="auto", headers=None):
    body = {"mappings": mappings or F.LOGIN_INTO_EMAIL_ONLY, "source_rows": src, "destination": MAP_DEST,
            "destination_table_exists": False, "session_id": session, "source_ref": source_ref, "trigger": trigger}
    res = client.post("/api/v1/transfer/mapping-witness", json=body, headers=headers or {})
    assert res.status_code == 200, res.text
    return res.json()


@pytest.mark.parametrize("case", CASES["cases"], ids=[c["id"] for c in CASES["cases"]])
def test_map_payload_and_run_submission_name_the_same_source(client, submitted, data, monkeypatch, case):
    """Round 3 item 3: the source Map names and the source the job names meet, through both real routes."""
    from services.mapping_witness_service import snapshot_for_request

    monkeypatch.setattr("services.data_residency.enforce_request_residency", lambda r: None)
    src, _ = data
    out = _map_check(client, src, session="map-session-src1", source_ref=case["map_source_ref"])
    request = _run(client, submitted, case["run_form"], session="map-session-src1", mappings=F.LOGIN_INTO_EMAIL_ONLY,
                   file_name=case.get("run_file"), source_extra=case.get("run_source_extra"))
    snap = snapshot_for_request(request)
    if case["attaches"]:
        assert snap["report_sha256"] == out["report_sha256"], snap.get("not_checked_reason")
    else:
        assert snap["report"] is None


def test_a_rechecked_flag_keeps_its_keep_through_the_routes(client, submitted, data, monkeypatch):
    """Round 3 item 2: the same inputs checked again (a new report) keep the person's Keep, as the UI shows it."""
    from services.ai_decision_ledger import KIND_WITNESS_FLAG, build_ledger
    from services.mapping_witness_service import snapshot_for_request

    monkeypatch.setattr("services.data_residency.enforce_request_residency", lambda r: None)
    src, _ = data
    ref = {"kind": "file", "name": "customers.csv"}
    first = _map_check(client, src, session="map-session-keep1", source_ref=ref)
    [flag] = [p for p in first["report"]["pairs"] if p["status"] == STATUS_FLAGGED]
    kept = client.post("/api/v1/transfer/mapping-witness/review", json={
        "mapping_digest": first["mapping_digest"], "session_id": "map-session-keep1", "destination": MAP_DEST,
        "source": flag["source"], "decision": "kept", "flag_id": flag["flag"]["id"]})
    assert kept.status_code == 200, kept.text
    again = _map_check(client, src, session="map-session-keep1", source_ref=ref, trigger="explicit")
    assert again["report_sha256"] != first["report_sha256"]
    [same] = [p for p in again["report"]["pairs"] if p["status"] == STATUS_FLAGGED]
    assert same["flag"]["id"] == flag["flag"]["id"]
    request = _run(client, submitted, {"source_kind": "file", "source_format": "csv"}, session="map-session-keep1",
                   mappings=F.LOGIN_INTO_EMAIL_ONLY, file_name="customers.csv")
    snap = snapshot_for_request(request)
    assert snap["report_sha256"] == again["report_sha256"]
    [row] = [e for e in build_ledger(_job_doc(monkeypatch, request))["entries"] if e["kind"] == KIND_WITNESS_FLAG]
    assert row["decision"]["status"] == "kept"


def test_approvals_and_changes_do_not_carry_over_to_another_source_in_one_session(monkeypatch, tmp_path, data):
    """Round 3 item 4: one session, the source swapped: Map approvals and changed reviews stay with their source."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc

    src, dst = data
    maps = F.LOGIN_INTO_EMAIL_ONLY
    identity = "dest-1"
    digest = svc.mapping_digest(maps, identity)
    report = witness_mappings(maps, src, dst, key=F.KEY)
    svc.save_report(report, workspace_id="ws1", session="session-swap", identity=identity, digest=digest,
                    source="file|a.csv", actor="ann@example.com")
    svc.record_map_decisions(workspace_id="ws1", session="session-swap", identity=identity, source="file|a.csv",
                             mappings=[{"source": "given", "target": "first_name", "approved": True}],
                             actor="ann@example.com")
    [flag] = [p for p in report["pairs"] if p["status"] == STATUS_FLAGGED]
    svc.record_review(workspace_id="ws1", session="session-swap", identity=identity, digest=digest,
                      source=flag["source"], decision="changed", new_target="username", actor="ann@example.com")
    changed = [{**m, "target": "username"} if m["source"] == flag["source"] else m for m in maps]
    same = svc.build_snapshot(workspace_id="ws1", session="session-swap", identity=identity, source="file|a.csv",
                              mappings=changed)
    assert same["map_decisions"] and same["flag_reviews"]
    # The same session, now on b.csv with its own check: nothing approved on a.csv comes along.
    svc.save_report(witness_mappings(changed, src, dst, key=F.KEY), workspace_id="ws1", session="session-swap",
                    identity=identity, digest=svc.mapping_digest(changed, identity), source="file|b.csv")
    other = svc.build_snapshot(workspace_id="ws1", session="session-swap", identity=identity, source="file|b.csv",
                               mappings=changed)
    assert other["report"] is not None
    assert other["map_decisions"] == {} and other["flag_reviews"] == []


def _job_doc(monkeypatch, request):
    """The job document create_pending_job writes for a request the run route built."""
    import src.transfer.engine as engine_mod
    from src.transfer.job_submission import create_pending_job

    created: list[dict] = []
    monkeypatch.setattr(engine_mod, "get_mongodb_service",
                        lambda: SimpleNamespace(create_transfer_job=lambda doc: created.append(doc) or "job-1"))
    engine = SimpleNamespace(_resolve_saved_connectors=lambda r: None, _claim_idempotency=lambda r, d: None,
                             _claim_destination=lambda r, d: None, _bind_idempotency_claim=lambda *a: None)
    monkeypatch.setattr("services.callable_intake.apply_callable_intake", lambda r: None)
    create_pending_job(engine, request)
    return {"_id": "job-1", "status": "completed", **created[0]}


def test_a_job_run_by_b_in_as_session_credits_a_for_the_check_and_b_for_the_run(client, submitted, data, monkeypatch):
    """Round 3 item 4: a session id is only a key; the people are those who acted."""
    from services.ai_decision_ledger import KIND_MAPPING_PROPOSAL, build_ledger

    monkeypatch.setattr("services.data_residency.enforce_request_residency", lambda r: None)
    monkeypatch.setattr("src.routers.verified_ai_router.actor_from_request", lambda request: "ann@example.com")
    monkeypatch.setattr("src.routers.transfer_router._actor_email", lambda request: "bob@example.com")
    src, _ = data
    ref = {"kind": "file", "name": "customers.csv"}
    _map_check(client, src, session="map-session-ab01", source_ref=ref, mappings=F.CORRECT_MAPPING)
    res = client.post("/api/v1/transfer/mapping-witness/decisions", json={
        "session_id": "map-session-ab01", "destination": MAP_DEST, "source_ref": ref,
        "mappings": [{"source": "mail", "target": "email", "approved": True}]})
    assert res.status_code == 200 and res.json()["recorded"] == 1
    request = _run(client, submitted, {"source_kind": "file", "source_format": "csv"}, session="map-session-ab01",
                   mappings=F.CORRECT_MAPPING, file_name="customers.csv")
    assert request.triggered_by == "bob@example.com"
    ledger = build_ledger(_job_doc(monkeypatch, request))
    assert ledger["witness"]["checked_by"] == "ann@example.com"
    row = next(e for e in ledger["entries"] if e["kind"] == KIND_MAPPING_PROPOSAL and e["subject"] == "mail → email")
    assert row["decision"]["by"] == "ann@example.com" and row["decision"]["run_by"] == "bob@example.com"
