"""Rule workbook compiler — closed forms execute, the rest stay in review."""

from __future__ import annotations

import io
import json

import pytest

from services.rule_compiler.classify import classify_rule, parse_lookup
from services.rule_compiler.compile import compile_rule_workbook
from services.rule_compiler.ingest import RuleIngestError, ingest_rule_file
from services.rule_compiler.normalize import canonical_header, resolve_name


def test_direct_and_empty_are_the_same_closed_form():
    for text in ("", "Direct", "1:1", "passthrough", "as-is", "preserve"):
        assert classify_rule(text)["kind"] == "direct", text


def test_lookup_accepts_a_single_code_pair():
    assert parse_lookup("A → ACTIVE") == {"A": "ACTIVE"}
    assert parse_lookup("YYYY → YYYY-MM-DD") == {}
    pairs = parse_lookup("A → ACTIVE, I → INACTIVE, P → PENDING")
    assert pairs == {"A": "ACTIVE", "I": "INACTIVE", "P": "PENDING"}
    assert classify_rule("A=ACTIVE; I=INACTIVE")["kind"] == "lookup"


def test_derive_salary_times_twelve():
    got = classify_rule("salary × 12")
    assert got["kind"] == "derive"
    assert "*" in got["expression"]


def test_unknown_prose_is_review_not_a_guess():
    got = classify_rule("for legacy customers use the old number unless migrated")
    assert got["kind"] == "unknown"
    assert got["plane"] == "review"


def test_spoken_headers_fold_onto_compiler_columns():
    assert canonical_header("Source Column") == "source_column"
    assert canonical_header("destination_column") == "dest_column"
    assert canonical_header("Join From") == "join_from"
    assert canonical_header("Business Rule") == "rule"


def test_resolve_name_fails_closed_on_short_or_ambiguous():
    cols = ["customer_id", "order_id", "first_name"]
    assert resolve_name("first_name", cols) == "first_name"
    assert resolve_name("FIRST NAME", cols) == "first_name"
    assert resolve_name("id", cols) == ""
    assert resolve_name("customer", ["customer_id", "customer_name"]) == ""


def test_csv_compiles_customer_fixture():
    csv = (
        "Source,Source Column,Destination,Destination Column,Rule\n"
        "Customer,fname,Customer,first_name,Direct\n"
        "Customer,lname,Customer,last_name,Direct\n"
        "Customer,dob,Customer,birth_date,Convert MM/DD/YYYY → YYYY-MM-DD\n"
        "Customer,status,Customer,status,\"A → ACTIVE, I → INACTIVE\"\n"
        "Customer,salary,Customer,annual_salary,salary × 12\n"
        "Customer,email,Customer,email,Lowercase + validate email\n"
        "Customer,notes,Customer,notes,omit\n"
        "Customer,mystery,Customer,segment,use legacy id unless migrated\n"
    ).encode()
    src = ["fname", "lname", "dob", "status", "salary", "email", "notes", "mystery"]
    dst = ["first_name", "last_name", "birth_date", "status", "annual_salary", "email", "notes", "segment", "unused_flag"]
    report = compile_rule_workbook(
        "customer_rules.csv",
        csv,
        source_columns=src,
        dest_columns=dst,
        source_table="Customer",
        dest_table="Customer",
    )
    by_src = {r["source_column"]: r for r in report["rules"]}
    assert report["buckets"]["executable"] >= 6
    assert report["buckets"]["needs_confirmation"] >= 1
    assert by_src["fname"]["status"] == "executable"
    assert by_src["fname"]["dest_column"] == "first_name"
    assert by_src["status"]["code_crosswalk"]["A"] == "ACTIVE"
    assert by_src["email"]["transform"] == "email"
    assert by_src["notes"]["transform"] == "omit"
    assert by_src["dob"]["transform"] == "date_iso"
    salary = [r for r in report["rules"] if r["kind"] == "derive"][0]
    assert salary["shape_step"]["op"] == "derive_column"
    assert salary["map_source"] == "annual_salary"
    mystery = by_src["mystery"]
    assert mystery["status"] == "needs_confirmation"
    assert mystery["kind"] == "unknown"
    assert report["unused_dest_count"] == 1
    assert "unused_flag" in report["unused_dest_columns"]
    # Unbound dest is not invented, unused dest is not written.
    assert all(r["dest_column"] != "unused_flag" or r["status"] != "executable" for r in report["rules"])


def test_json_array_is_accepted():
    payload = json.dumps([
        {"source_column": "fname", "destination_column": "first_name", "rule": "Direct"},
        {"source_column": "status", "dest_column": "status", "rule": "A → ACTIVE, I → INACTIVE"},
    ]).encode()
    report = compile_rule_workbook(
        "rules.json",
        payload,
        source_columns=["fname", "status"],
        dest_columns=["first_name", "status"],
    )
    assert report["rule_count"] == 2
    assert report["buckets"]["executable"] == 2


def test_xlsx_roundtrip():
    openpyxl = pytest.importorskip("openpyxl")
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Rules"
    ws.append(["Source Column", "Destination Column", "Rule"])
    ws.append(["fname", "first_name", "Direct"])
    ws.append(["status", "status", "A → ACTIVE, I → INACTIVE"])
    buf = io.BytesIO()
    wb.save(buf)
    report = compile_rule_workbook(
        "rules.xlsx",
        buf.getvalue(),
        source_columns=["fname", "status"],
        dest_columns=["first_name", "status"],
    )
    assert report["buckets"]["executable"] == 2
    assert report["rules"][0]["provenance"]["sheet"] == "Rules"
    assert report["rules"][0]["provenance"]["row"] == 2


def test_unbound_source_is_not_executed():
    csv = b"Source Column,Destination Column,Rule\nbogus,first_name,Direct\n"
    report = compile_rule_workbook(
        "bad.csv",
        csv,
        source_columns=["fname"],
        dest_columns=["first_name"],
    )
    assert report["rules"][0]["status"] == "needs_confirmation"
    assert report["shape_steps"] == []


def test_concat_and_hash_and_join_are_closed_forms():
    concat = classify_rule("concat first_name and last_name")
    assert concat["kind"] == "concat"
    assert "first_name" in concat["columns"]
    assert classify_rule("hash PII")["kind"] == "hash"
    assert classify_rule("cast integer")["kind"] == "cast_integer"
    join = classify_rule("left join customers on customer_id")
    assert join["kind"] == "join"
    assert join["plane"] == "review"


def test_concat_csv_emits_shape_step():
    csv = (
        "Source Column,Destination Column,Rule\n"
        "first_name,full_name,concat first_name and last_name\n"
        "ssn,ssn_hash,hash PII\n"
        "orders,fact,left join customers on customer_id\n"
    ).encode()
    report = compile_rule_workbook(
        "more.csv",
        csv,
        source_columns=["first_name", "last_name", "ssn", "orders"],
        dest_columns=["full_name", "ssn_hash", "fact"],
    )
    by_kind = {r["kind"]: r for r in report["rules"]}
    assert by_kind["concat"]["status"] == "executable"
    assert by_kind["concat"]["shape_step"]["op"] == "concat_columns"
    assert by_kind["hash"]["transform"] == "hash_pii"
    assert by_kind["join"]["status"] == "needs_confirmation"
    assert by_kind["join"]["shape_step"] is None


def test_join_keys_on_a_direct_row_stay_in_review():
    csv = (
        "Source Column,Destination Column,Rule,Join From,Join On\n"
        "email,email,Direct,customers,customer_id\n"
    ).encode()
    report = compile_rule_workbook(
        "joins.csv",
        csv,
        source_columns=["email"],
        dest_columns=["email"],
    )
    kinds = [r["kind"] for r in report["rules"]]
    assert "direct" in kinds
    assert "join" in kinds
    join = next(r for r in report["rules"] if r["kind"] == "join")
    assert join["status"] == "needs_confirmation"


def test_empty_file_is_refused():
    with pytest.raises(RuleIngestError):
        ingest_rule_file("empty.csv", b"")


def test_legacy_xls_is_refused():
    with pytest.raises(RuleIngestError):
        ingest_rule_file("old.xls", b"not-empty")
