"""A numeric count column is not a phone number (QA round 11, P4).

QA (production 9c75ada4, Addendum A): copying RNAcentral's ``rnc_database``
needed a PII acknowledgment because ``num_sequences`` was "detected as a phone
number". ``compliance_guard._is_phone`` matched any bare 7-15 digit run, and
one hit flagged the column.

Bare digits now count as a phone only under a phone-hinting column name, or
when most non-empty values (>= 80%) look like one *and* the column is not a
numeric type. Formatted numbers (``+1 (555) 123-4567``) still count on their
own.

Through MCP ``plan_transfer`` → ``start_transfer`` → ``/copilot/confirm`` on a
live PostgreSQL source and destination, twice into the table the first run
created, with NULL / '' / whitespace / unicode in the text columns.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    return TestClient(app)


# Shaped like RNAcentral's rnc_database: counts in the millions, no PII.
_ROWS = [
    (1, "ENA", 12_345_678, 1_234_567, "European Nucleotide Archive"),
    (2, "RefSeq", 8_901_234, 7_654_321, None),
    (3, "Rfam", 3_456_789, 2_345_678, ""),
    (4, "miRBase", 48_860_000, 1_000_001, "   "),
    (5, "GtRNAdb ✓", 5_550_123, 9_999_999, "naïve 中文 😀"),
]


def _source(table: str) -> None:
    h.execute(
        "postgresql",
        f'CREATE TABLE "{table}" (id BIGINT PRIMARY KEY, db_label TEXT, '
        "num_sequences BIGINT, num_organisms INTEGER, description TEXT)",
    )
    conn = h.pg_conn()
    try:
        conn.cursor().executemany(
            f'INSERT INTO "{table}" VALUES (%s, %s, %s, %s, %s)', _ROWS  # nosec B608
        )
    finally:
        conn.close()


def _pii(plan: dict) -> tuple[dict[str, list[str]], dict]:
    """(column -> PII categories, transfer decision) as the plan reports them."""
    body = plan.get("plan") or plan
    preflight = body.get("preflight") or {}
    found: dict[str, list[str]] = {}
    for warning in preflight.get("warnings") or []:
        if isinstance(warning, dict) and warning.get("id") == "pii_detected":
            for item in (warning.get("details") or {}).get("findings") or []:
                found[item["column"]] = list(item.get("categories") or [])
    decision = (preflight.get("proof_bundle") or {}).get("transfer_decision") or {}
    return found, decision


def test_counts_in_the_millions_are_not_phones_and_need_no_pii_ack(client):
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    src_t, dst_t = f"r11pii_s_{token}", f"r11pii_d_{token}"
    cfg = h.dest_config("postgresql")
    try:
        _source(src_t)
        src = create_connector({"name": f"r11pii-s-{token}", "role": "source", "ssl": False, **cfg})
        dst = create_connector({"name": f"r11pii-d-{token}", "role": "destination", "ssl": False,
                                **cfg})
        args = {"source_connector_id": src.id, "source_table": src_t,
                "dest_connector_id": dst.id, "dest_table": dst_t,
                "sync_mode": "full_refresh_overwrite"}
        ok, plan = h.mcp_call(client, "plan_transfer", dict(args))
        assert ok, plan
        found, decision = _pii(plan)
        assert "num_sequences" not in found and "num_organisms" not in found, found
        assert found == {}, found
        assert decision.get("decision") != "review", decision
        assert "PII" not in str(decision.get("reason") or ""), decision
        assert not h.blockers_of(plan), h.blockers_of(plan)

        for _ in range(2):  # second run into the table the first run created
            job = h.stage_and_run(client, "start_transfer", args)
            assert str(job.get("status")) == "completed", (job.get("status"), job.get("error"))
            rows = h.query("postgresql", f'SELECT id, num_sequences, description FROM "{dst_t}" '
                           "ORDER BY id")  # nosec B608
            assert [(int(a), int(b), c) for a, b, c in rows] == [
                (r[0], r[2], r[4]) for r in _ROWS
            ]
    finally:
        h.drop_table("postgresql", src_t)
        h.drop_table("postgresql", dst_t)


def test_real_phone_columns_are_still_detected():
    from services.compliance_guard import detect_pii_fields

    rows = [
        {"mobile": 5551234567, "contact": "5551234567", "callback": "+1 (555) 123-4567",
         "num_sequences": 12345678, "order_ref": "12345678", "note": None},
        {"mobile": 5559876543, "contact": "5559876543", "callback": "", "num_sequences": 8901234,
         "order_ref": "87654321", "note": ""},
        {"mobile": None, "contact": "   ", "callback": None, "num_sequences": None,
         "order_ref": "ref-中文", "note": "naïve 中文"},
        {"mobile": 5550001111, "contact": "5550001111", "callback": "555.000.1111",
         "num_sequences": 5550123, "order_ref": "x", "note": "   "},
    ]
    report = detect_pii_fields(list(rows[0]), rows)
    risk = report["field_risk"]
    # Name hint: digits under "mobile" are phones even when the column is numeric.
    assert "phone" in risk.get("mobile", []), risk
    # Text column, every non-empty value phone-shaped: still a phone.
    assert "phone" in risk.get("contact", []), risk
    # Formatted numbers count on their own.
    assert "phone" in risk.get("callback", []), risk
    # A numeric count, and a text column where most values are not phones.
    assert "num_sequences" not in risk and "order_ref" not in risk and "note" not in risk, risk
    # Declared numeric type wins for string-carried samples too (CSV via inference).
    typed = detect_pii_fields(
        ["num_sequences"], [{"num_sequences": "12345678"}, {"num_sequences": "8901234"}],
        column_types={"num_sequences": "BIGINT"},
    )
    assert typed["field_risk"] == {}, typed
