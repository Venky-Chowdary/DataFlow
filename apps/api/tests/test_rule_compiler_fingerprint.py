"""Compile report fingerprint — same inputs, same report; any drift shows."""

from __future__ import annotations

from services.rule_compiler.compile import RULE_COMPILER_VERSION, compile_rule_workbook

_CSV = (
    "Source Column,Destination Column,Rule\n"
    "fname,first_name,Direct\n"
    "status,status,A → ACTIVE, I → INACTIVE\n"
    "email,email,Must not be null\n"
    "tier,tier,Must be GOLD or SILVER\n"
).encode()

_SRC = ["fname", "status", "email", "tier"]
_DST = ["first_name", "status", "email", "tier"]


def _compile(**kw):
    args = dict(
        source_columns=_SRC,
        dest_columns=_DST,
        source_table="Customer",
        dest_table="Customer",
    )
    args.update(kw)
    return compile_rule_workbook("rules.csv", _CSV, **args)


def test_report_carries_version_and_fingerprints():
    report = _compile()
    assert report["compiler_version"] == RULE_COMPILER_VERSION
    assert len(report["input_sha256"]) == 64
    assert len(report["report_fingerprint"]) == 64
    for rule in report["rules"]:
        assert len(rule["rule_id"]) == 16
        int(rule["rule_id"], 16)


def test_fingerprint_is_deterministic_across_calls():
    first = _compile()
    second = _compile()
    assert first["report_fingerprint"] == second["report_fingerprint"]
    assert first["input_sha256"] == second["input_sha256"]
    assert [r["rule_id"] for r in first["rules"]] == [r["rule_id"] for r in second["rules"]]


def test_fingerprint_changes_on_payload_or_schema_drift():
    base = _compile()
    changed_payload = compile_rule_workbook(
        "rules.csv",
        _CSV.replace(b"Direct", b"Trim"),
        source_columns=_SRC,
        dest_columns=_DST,
        source_table="Customer",
        dest_table="Customer",
    )
    assert changed_payload["input_sha256"] != base["input_sha256"]
    assert changed_payload["report_fingerprint"] != base["report_fingerprint"]

    changed_schema = _compile(source_columns=_SRC + ["extra_col"])
    assert changed_schema["report_fingerprint"] != base["report_fingerprint"]


def test_rule_ids_are_unique_within_a_report():
    report = _compile()
    ids = [r["rule_id"] for r in report["rules"]]
    assert len(ids) == len(set(ids))


def test_contracts_carry_enforced_at_execute():
    csv = (
        "Source Column,Destination Column,Rule\n"
        "email,email,Must not be null\n"
        "tier,tier,Must be GOLD or SILVER\n"
        "cust_id,cust_id,must be unique\n"
    ).encode()
    report = compile_rule_workbook(
        "rules.csv",
        csv,
        source_columns=["email", "tier", "cust_id"],
        dest_columns=["email", "tier", "cust_id"],
    )
    contracts = [r for r in report["rules"] if r.get("kind") == "contract"]
    assert contracts, "expected contract rows"
    by_type = {r["contract"]["type"]: r for r in contracts}
    assert by_type["not_null"]["enforced_at_execute"] is True
    assert by_type["in_set"]["enforced_at_execute"] is True
    assert by_type["unique"]["enforced_at_execute"] is False
    expected = sum(1 for r in contracts if r["enforced_at_execute"])
    assert report["coverage"]["contracts_enforced_at_execute"] == expected
