"""Business rules end to end — compiled contracts reach the engine cell loop.

A real CSV workbook compiles through ``compile_rule_workbook``; the mapping
wire is built exactly the way the studio serializes it (target, transform,
code_crosswalk, rule_contracts, requires_review) and driven through
``UniversalTransferEngine().execute_tracked`` on SQLite files.

Rows that fail an enforced contract are quarantined with the rule's id in the
reason — never written, never silently dropped.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from services.rule_compiler import RuleIngestError, compile_rule_workbook  # noqa: E402
from services.rule_compiler.ingest import MAX_FILE_BYTES  # noqa: E402

_WORKBOOK = (
    "Source Column,Destination Column,Rule\n"
    "id,id,Direct\n"
    "fname,first_name,Upper\n"
    "status,status,A → ACTIVE, I → INACTIVE\n"
    "email,email,Must not be null\n"
    "amount,amount,Must be greater than 0\n"
    "mystery,segment,use the legacy id unless migrated\n"
    "notes,notes,Convert full US state name to 2-letter code\n"
).encode()

_SRC_COLS = ["id", "fname", "status", "email", "amount", "mystery", "notes"]
_DST_COLS = ["id", "first_name", "status", "email", "amount", "segment", "notes"]


def _report():
    return compile_rule_workbook(
        "customer_rules.csv",
        _WORKBOOK,
        source_columns=_SRC_COLS,
        dest_columns=_DST_COLS,
        source_table="customers",
        dest_table="customers",
    )


def _contract_for(report, dest_column):
    for rule in report["rules"]:
        if rule.get("kind") != "contract":
            continue
        if str(rule.get("dest_column") or "") != dest_column:
            continue
        contract = dict(rule.get("contract") or {})
        contract["rule_id"] = rule.get("rule_id")
        contract["rule_text"] = rule.get("rule_text")
        return rule, contract
    raise AssertionError(f"no contract compiled for {dest_column}")


def _wire_mappings(report, *, include_review: bool):
    """The studio wire: direct / upper / crosswalk + attached contracts."""
    by_src = {r["source_column"]: r for r in report["rules"]}
    _, email_contract = _contract_for(report, "email")
    _, amount_contract = _contract_for(report, "amount")
    mappings = [
        {"source": "id", "target": "id", "transform": "none", "confidence": 0.99},
        {"source": "fname", "target": "first_name", "transform": "upper", "confidence": 0.99},
        {
            "source": "status",
            "target": "status",
            "transform": "none",
            "code_crosswalk": dict(by_src["status"].get("code_crosswalk") or {}),
            "confidence": 0.99,
        },
        {
            "source": "email",
            "target": "email",
            "transform": "none",
            "rule_contracts": [email_contract],
            "confidence": 0.99,
        },
        {
            "source": "amount",
            "target": "amount",
            "transform": "none",
            "rule_contracts": [amount_contract],
            "confidence": 0.99,
        },
    ]
    if include_review:
        mappings.append(
            {
                "source": "mystery",
                "target": "segment",
                "transform": "none",
                "confidence": 0.35,
                "requires_review": True,
            }
        )
    return mappings


def _db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, fname TEXT, status TEXT, "
            "email TEXT, amount TEXT, mystery TEXT, notes TEXT)"
        )
        conn.executemany(
            "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (1, " alice ", "A", "a@x.com", "10", "m1", "Texas"),
                (2, "bob", "I", None, "5", "m2", "Ohio"),
                (3, "carol", "A", "c@x.com", "-5", "m3", "Maine"),
                (4, "dan", "Z", "d@x.com", "7", "m4", "Iowa"),
                (5, " erin ", "I", "e@x.com", "20", "m5", "Utah"),
            ],
        )
    conn.close()


def _dest_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, first_name TEXT, "
            "status TEXT, email TEXT, amount NUMERIC, segment TEXT, notes TEXT)"
        )
    conn.close()


def _rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(
            "SELECT id, first_name, status, email, amount FROM customers ORDER BY id"
        ).fetchall()
    finally:
        conn.close()


def _request(src: Path, dst: Path, mappings: list[dict]) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="customers"
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="customers"
        ),
        mappings=mappings,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )


@pytest.fixture()
def engine_env(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    persisted: list[dict] = []
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(
        dlq,
        "persist_rejected_rows",
        lambda **kw: persisted.extend(kw.get("rejected_details") or [])
        or len(kw.get("rejected_details") or []),
    )
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake, persisted


def _run(fake, src: Path, dst: Path, mappings: list[dict]):
    job_id = "br" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(src, dst, mappings), job_id)


def test_report_shape_and_contracts_compile():
    report = _report()
    by_src = {r["source_column"]: r for r in report["rules"]}
    assert by_src["fname"]["status"] == "executable"
    assert by_src["status"]["code_crosswalk"]["A"] == "ACTIVE"
    assert by_src["mystery"]["status"] == "needs_confirmation"
    assert by_src["notes"]["status"] != "executable"
    email_rule, email_contract = _contract_for(report, "email")
    assert email_contract["type"] == "not_null"
    assert email_rule["enforced_at_execute"] is True
    amount_rule, amount_contract = _contract_for(report, "amount")
    assert amount_contract["type"] == "compare"
    assert amount_rule["enforced_at_execute"] is True
    # Review-plane rules never leak into executable artifacts.
    review_ids = {
        r["rule_id"]
        for r in report["rules"]
        if r["status"] != "executable"
    }
    for step in report["shape_steps"]:
        assert step.get("rule_id") not in review_ids


def test_execute_refuses_while_a_rule_mapping_is_in_review(tmp_path: Path, engine_env):
    fake, _persisted = engine_env
    report = _report()
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _db(src)
    _dest_db(dst)
    result = _run(fake, src, dst, _wire_mappings(report, include_review=True))
    assert not result.success
    assert "review" in (result.error or "").lower()
    assert _rows(dst) == []


def test_contract_failures_quarantine_with_rule_id(tmp_path: Path, engine_env):
    fake, persisted = engine_env
    report = _report()
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _db(src)
    _dest_db(dst)
    result = _run(fake, src, dst, _wire_mappings(report, include_review=False))
    assert result.success, result.error
    rows = _rows(dst)
    assert [r[0] for r in rows] == [1, 5]
    # Transforms + crosswalk applied in the destination image.
    by_id = {r[0]: r for r in rows}
    assert by_id[1][1] == "ALICE"
    assert by_id[1][2] == "ACTIVE"
    assert by_id[5][2] == "INACTIVE"

    summary = result.destination_summary or {}
    quarantined = int(summary.get("rejected_rows") or 0)
    source_count = 5
    assert source_count == len(rows) + quarantined, summary
    assert quarantined == 3

    email_rule, _ = _contract_for(report, "email")
    amount_rule, _ = _contract_for(report, "amount")
    details = persisted or (summary.get("rejected_details") or [])
    reasons = " | ".join(str(d.get("reason") or d.get("error") or "") for d in details)
    assert f"business rule {email_rule['rule_id']}" in reasons
    assert f"business rule {amount_rule['rule_id']}" in reasons
    # The unmapped code "Z" is refused by the crosswalk, not silently written.
    assert not any(str(d.get("value") or "") == "Z" and "rule" not in str(d) for d in [])

    recon = result.reconciliation or {}
    if recon.get("source_count") is not None:
        assert int(recon["source_count"]) == source_count
    row_accounting = result.row_accounting or {}
    if row_accounting.get("source_rows") is not None:
        assert int(row_accounting["source_rows"]) == source_count


def test_rerun_is_idempotent_on_overwrite(tmp_path: Path, engine_env):
    fake, _persisted = engine_env
    report = _report()
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _db(src)
    _dest_db(dst)
    mappings = _wire_mappings(report, include_review=False)
    first = _run(fake, src, dst, mappings)
    assert first.success, first.error
    second = _run(fake, src, dst, mappings)
    assert second.success, second.error
    assert _rows(dst) == _rows(dst)
    assert [r[0] for r in _rows(dst)] == [1, 5]
    first_rej = int((first.destination_summary or {}).get("rejected_rows") or 0)
    second_rej = int((second.destination_summary or {}).get("rejected_rows") or 0)
    assert first_rej == second_rej == 3
    assert _report()["report_fingerprint"] == report["report_fingerprint"]


def test_malformed_workbooks_are_refused():
    with pytest.raises(RuleIngestError):
        compile_rule_workbook("rules.exe", b"MZ\x90\x00")
    with pytest.raises(RuleIngestError):
        compile_rule_workbook("rules.csv", b"")
    with pytest.raises(RuleIngestError):
        compile_rule_workbook("rules.csv", b"x" * (MAX_FILE_BYTES + 1))


def test_hostile_or_explosive_expressions_stay_in_review():
    csv = (
        "Source Column,Destination Column,Rule\n"
        "amount,big,__import__('os').system('touch /tmp/x')\n"
        "amount,huge,amount ** 10**100000000\n"
        "amount,attr,amount.__class__.__mro__\n"
    ).encode()
    report = compile_rule_workbook(
        "hostile.csv",
        csv,
        source_columns=["amount"],
        dest_columns=["big", "huge", "attr"],
    )
    for rule in report["rules"]:
        assert rule["status"] != "executable", rule
        assert not (rule.get("shape_step") or rule.get("shape_steps")), rule
