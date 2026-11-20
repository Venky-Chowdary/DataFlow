"""D94 on the original route: a table of direct identifiers into SQLite, strict.

QA staged ``hq1410_em`` (id, email, phone, contact holding emails) from
PostgreSQL into SQLite as ``safe_to_start`` with no PII finding. On current
code the plan (``start_transfer``) and ``create_schedule`` must both hold it
for PII review, naming the columns by name and by value, and a card-number
column must be named as payment-card data. Live PostgreSQL; SQLite in-process.
"""

from __future__ import annotations

import uuid

import pytest

from tests import value_fidelity_route as r

_ROWS = [
    (1, "ann@example.com", "+1 415 555 0100", "ann.lee@example.org", "4111111111111111"),
    (2, "ben@example.net", "(212) 555-0199", "ben@example.com", "5500005555555559"),
    (3, "cy@example.io", "+44 20 7946 0958", "cy@example.co.uk", "340000000000009"),
    (4, None, None, None, None),
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


def _connectors(tmp_path, token: str) -> tuple[str, str]:
    from services.connector_store import create_connector

    src = create_connector(
        {"name": f"d94-src-{token}", "role": "source", "workspace_id": "", "ssl": False,
         **r.dest_config("postgresql")}
    )
    db = str(tmp_path / f"d94_{token}.sqlite")
    dst = create_connector(
        {"name": f"d94-lite-{token}", "type": "sqlite", "host": db, "database": db,
         "port": 0, "workspace_id": "", "role": "destination"}
    )
    return src.id, dst.id


def _hold_text(result) -> str:
    out = result.output or {}
    plan = out.get("plan") or out
    blockers = (plan.get("preflight") or {}).get("blockers") or []
    return " ".join(
        [str(result.error or "")] + [str(b.get("message") or b) for b in blockers]
    )


def test_identifier_table_into_sqlite_is_held_at_plan_and_create_schedule(client, tmp_path):
    token = uuid.uuid4().hex[:8]
    table = f"p2d94_{token}"
    r.execute(
        "postgresql",
        f'CREATE TABLE "{table}" (id BIGINT PRIMARY KEY, email TEXT, phone TEXT, '
        "contact TEXT, card_number TEXT)",
    )
    for row in _ROWS:
        r.execute("postgresql", f'INSERT INTO "{table}" VALUES (%s, %s, %s, %s, %s)', row)  # nosec B608
    try:
        src_id, dst_id = _connectors(tmp_path, token)
        staged, _plan, _blockers = r.stage(
            source_connector_id=src_id, source_table=table,
            dest_connector_id=dst_id, dest_table=table + "_out",
            sync_mode="full_refresh_overwrite", primary_key="id",
        )
        assert not staged.success, "a table of direct identifiers staged safe_to_start"
        hold = _hold_text(staged)
        assert "PII/compliance review required" in hold, hold
        assert "contact (email by sample value)" in hold, hold
        assert "email (email by column name, email by sample value)" in hold, hold
        assert "phone (phone by column name, phone by sample value)" in hold, hold
        assert "card_number (pci_pan by column name, pci_pan by sample value)" in hold, hold

        from src.ai.copilot.schedule_tools import create_schedule

        scheduled = create_schedule(
            source_connector_id=src_id, source_table=table,
            dest_connector_id=dst_id, dest_table=table + "_sched",
            sync_mode="full_refresh_overwrite", validation_mode="strict",
            cadence="daily at 02:17", primary_key="id",
        )
        assert not scheduled.success, scheduled.output
        sched_hold = _hold_text(scheduled)
        assert "PII/compliance review required" in sched_hold, sched_hold
        assert "card_number (pci_pan by column name, pci_pan by sample value)" in sched_hold
        assert "contact (email by sample value)" in sched_hold, sched_hold
    finally:
        r.drop("postgresql", table)
