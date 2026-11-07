"""D94 - the PII gate must detect email / phone / IP by CONTENT, not by column name.

QA: on a table with ``contact`` (emails), ``phone``, ``ip`` (IPv4) and
``passport`` the gate's hold read only "passport (passport by column name)":
the email, phone and IP columns the content scan had found were left out of
the message, and without the passport column nothing about PII was said at
all, because email / phone / IP are medium risk and do not force a review.

Root cause: the content scan in ``compliance_guard`` worked, but its result
was reported by name only - ``_compliance_review_message`` listed just the
high-risk columns, the evidence dropped ``sample_value`` whenever the name
also matched, and a medium-risk finding produced no gate output whatsoever.

Route: ``start_transfer`` (plan + preflight) on PG and MySQL sources, read
through the real connectors. Cells hold NULL, '', whitespace and unicode
beside the PII values.
"""

from __future__ import annotations

import uuid

import pytest

from tests import value_fidelity_route as r

_ROWS = [
    (1, "alice@example.com", "+1 415 555 0100", "192.168.1.20", "K1234567", "Łukasz 山田"),
    (2, "bob@example.org", "(212) 555-0199", "10.0.0.7", "X9876543", ""),
    (3, "carol@example.net", "+44 20 7946 0958", "8.8.8.8", None, None),
    (4, "", "", "", "", "   "),
    (5, None, None, None, None, "x"),
    (6, "dan@example.io", "+81 3 1234 5678", "172.16.5.4", "P1234567", "🎉"),
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


def _source(kind: str, table: str, with_passport: bool) -> None:
    text = "TEXT" if kind == "postgresql" else "VARCHAR(200)"
    cols = ["id BIGINT PRIMARY KEY", f"contact {text}", f"phone {text}", f"ip {text}"]
    if with_passport:
        cols.append(f"passport {text}")
    cols.append(f"note {text}")
    tail = " CHARACTER SET utf8mb4" if kind == "mysql" else ""
    r.execute(kind, f"CREATE TABLE {r.quote(kind, table)} ({', '.join(cols)}){tail}")
    ph = "%s"
    for row in _ROWS:
        vals = row if with_passport else row[:4] + row[5:]
        r.execute(
            kind,
            f"INSERT INTO {r.quote(kind, table)} VALUES ({', '.join([ph] * len(vals))})",  # nosec B608
            vals,
        )


def _assert_content_evidence(text: str) -> None:
    assert "contact (email by sample value)" in text, text
    assert "phone (phone by column name, phone by sample value)" in text, text
    assert "ip (ip_address by column name, ip_address by sample value)" in text, text
    assert "note" not in text.split("Consider")[0], text


@pytest.mark.parametrize("src_kind", ["postgresql", "mysql"])
def test_review_hold_names_every_column_the_content_scan_found(client, src_kind):
    token = uuid.uuid4().hex[:8]
    table = f"d94_{src_kind[:2]}_{token}"
    _source(src_kind, table, with_passport=True)
    try:
        src_id, dst_id = r.connectors(token, "postgresql", src_kind=src_kind)
        staged, _plan, blockers = r.stage(
            source_connector_id=src_id, source_table=table,
            dest_connector_id=dst_id, dest_table=table + "_out",
            sync_mode="full_refresh_overwrite", primary_key="id",
        )
        assert not staged.success
        hold = " ".join(blockers) + " " + str(staged.error)
        assert "passport (passport by column name)" in hold, hold
        _assert_content_evidence(hold)

        # Acknowledged, the same plan runs and every value lands as it was.
        job = r.run(
            client, source_connector_id=src_id, source_table=table,
            dest_connector_id=dst_id, dest_table=table + "_out",
            sync_mode="full_refresh_overwrite", primary_key="id",
            pii_acknowledgement={"approved_by": "qa@example.com", "reason": "d94 test"},
        )
        assert job
        got = r.query("postgresql", f'SELECT id, contact, note FROM "{table}_out" ORDER BY id')  # nosec B608
        assert [(int(a), b, c) for a, b, c in got] == [(x[0], x[1], x[5]) for x in _ROWS]
    finally:
        r.drop(src_kind, table)
        r.drop("postgresql", table + "_out")


@pytest.mark.parametrize("src_kind", ["postgresql", "mysql"])
def test_medium_risk_pii_found_by_content_is_reported_not_silent(client, src_kind):
    token = uuid.uuid4().hex[:8]
    table = f"d94m_{src_kind[:2]}_{token}"
    _source(src_kind, table, with_passport=False)
    try:
        src_id, dst_id = r.connectors(token, "postgresql", src_kind=src_kind)
        staged, plan, blockers = r.stage(
            source_connector_id=src_id, source_table=table,
            dest_connector_id=dst_id, dest_table=table + "_out",
            sync_mode="full_refresh_overwrite", primary_key="id",
            column_transforms={"contact": "mask_pii"},
        )
        assert staged.success and not blockers, (staged.error, blockers)
        warnings = (plan.get("preflight") or {}).get("warnings") or []
        pii = [w for w in warnings if isinstance(w, dict) and w.get("id") == "pii_detected"]
        assert pii, warnings
        _assert_content_evidence(pii[0]["message"])
        # contact is masked by its mapping; phone and ip are still open.
        assert "Consider mask_pii / hash_pii / redact on ip, phone." in pii[0]["message"]
    finally:
        r.drop(src_kind, table)
