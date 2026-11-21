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
            "destination_table_exists": False}
    resp = client.post("/api/v1/transfer/mapping-witness", json=body)
    assert resp.status_code == 200, resp.text
    out = resp.json()
    flagged = [p for p in out["report"]["pairs"] if p["status"] == STATUS_FLAGGED]
    assert [p["source"] for p in flagged] == ["login"]
    assert out["report"]["notes"] == ["The destination table does not exist yet, so only source values were checked."]
    review = client.post("/api/v1/transfer/mapping-witness/review",
                         json={"mapping_digest": out["mapping_digest"], "source": "login", "decision": "changed",
                               "new_target": "username"})
    assert review.status_code == 200, review.text
    assert review.json()["decision"]["decision"] == "changed"
    missing = client.post("/api/v1/transfer/mapping-witness/review",
                          json={"mapping_digest": out["mapping_digest"], "source": "given", "decision": "kept"})
    assert missing.status_code == 404


def test_job_finds_the_witness_for_exactly_the_mapping_it_ran(monkeypatch, tmp_path, data):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from services.mapping_witness_service import mapping_digest, record_for_job, save_report

    src, dst = data
    dest = {"kind": "database", "format": "postgresql", "database": "crm", "schema": "public", "table": "customers"}
    report = witness_mappings(F.SWAPPED_NAMES, src, dst, key=F.KEY)
    save_report(report, workspace_id="ws1", digest=mapping_digest(F.SWAPPED_NAMES, dest))
    job = {"_id": "j1", "transfer_request": {"workspace_id": "ws1", "destination": dest,
                                             "mappings": list(reversed(F.SWAPPED_NAMES))}}
    record = record_for_job(job)
    assert record and record["report"]["summary"]["flagged"] == 2
    other = {**job, "transfer_request": {**job["transfer_request"], "mappings": F.CORRECT_MAPPING}}
    assert record_for_job(other) is None
    foreign = {**job, "transfer_request": {**job["transfer_request"], "workspace_id": "ws2"}}
    assert record_for_job(foreign) is None


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
