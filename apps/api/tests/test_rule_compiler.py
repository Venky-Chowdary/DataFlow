"""Rule workbook compiler — closed forms execute, the rest stay in review."""

from __future__ import annotations

import io
import json

import pytest

from services.rule_compiler.classify import classify_rule, parse_lookup
from services.rule_compiler.compile import compile_rule_workbook
from services.rule_compiler.ingest import RuleIngestError, ingest_rule_file
from services.rule_compiler.match import name_similarity, unique_linguistic_match
from services.rule_compiler.normalize import canonical_header, resolve_name, resolve_name_ex
from services.rule_compiler.roles import infer_header_roles


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
    assert classify_rule("Lowercase + validate email")["kind"] == "email"
    assert classify_rule("cast number")["kind"] == "cast_number"


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


def test_closed_form_matrix():
    cases = {
        "trim + lowercase": "case_lower",
        "title case": "title",
        "collapse whitespace": "collapse",
        "strip controls": "strip_controls",
        "hh:mm:ss": "time",
        "parse json": "json",
        "base64": "binary",
        "absolute value": "absolute",
        "round to 2": "round",
        "truncate to 4": "truncate",
        "clamp 0 to 100": "clamp",
        "left(code, 3)": "substr",
        "prefix \"US-\"": "prefix",
        "suffix \"-EUR\"": "suffix",
        "default 0": "default",
        "if null then N/A": "default",
        "null if : n/a, NA": "null_if",
        "keep if amount > 0": "filter",
        "exclude rows where status = X": "filter",
        "divert rows if amount < 0": "divert",
        "required": "contract",
        "must be unique": "contract",
        "=UPPER(A2)": "case_upper",
        "=TRIM(A2)": "trim",
        "=VLOOKUP(A2,Sheet2!A:B,2,FALSE)": "join",
        "if(status = \"A\", \"ACTIVE\", \"INACTIVE\")": "derive",
        "assume timezone UTC": "timezone",
    }
    for text, kind in cases.items():
        got = classify_rule(text)
        assert got["kind"] == kind, f"{text!r} → {got['kind']} (want {kind})"


def test_qualified_source_and_title_row_csv():
    csv = (
        "Customer mapping spec\n"
        "Source Column,Destination Column,Rule\n"
        "Customer.fname,first_name,Direct\n"
        "# comment,ignored,ignored\n"
        "status,status,Direct\n"
    ).encode()
    report = compile_rule_workbook(
        "titled.csv",
        csv,
        source_columns=["fname", "status"],
        dest_columns=["first_name", "status"],
    )
    by_src = {r["source_column"]: r for r in report["rules"]}
    assert by_src["fname"]["status"] == "executable"
    assert by_src["fname"]["source_table"] == "Customer"


def test_tsv_and_semicolon_and_ndjson():
    tsv = b"Source Column\tDestination Column\tRule\nfname\tfirst_name\tDirect\n"
    report = compile_rule_workbook("rules.tsv", tsv, source_columns=["fname"], dest_columns=["first_name"])
    assert report["buckets"]["executable"] == 1

    semi = "Source Column;Destination Column;Rule\nfname;first_name;Direct\n".encode()
    report = compile_rule_workbook("rules.csv", semi, source_columns=["fname"], dest_columns=["first_name"])
    assert report["buckets"]["executable"] == 1

    ndjson = (
        '{"source_column":"fname","dest_column":"first_name","rule":"Direct"}\n'
        '{"source_column":"status","dest_column":"status","rule":"A → ACTIVE, I → INACTIVE"}\n'
    ).encode()
    report = compile_rule_workbook(
        "rules.ndjson",
        ndjson,
        source_columns=["fname", "status"],
        dest_columns=["first_name", "status"],
    )
    assert report["buckets"]["executable"] == 2


def test_lookup_sheet_pairs_and_dest_collision():
    csv = (
        "Source Column,Destination Column,From Code,To Value\n"
        "status,status,A,ACTIVE\n"
        "status,status,I,INACTIVE\n"
        "Source Column,Destination Column,Rule\n"
    ).encode()
    # The second header-looking row is data? From Code sheet only:
    csv = (
        "Source Column,From Code,To Value\n"
        "status,A,ACTIVE\n"
        "status,I,INACTIVE\n"
    ).encode()
    report = compile_rule_workbook(
        "codes.csv",
        csv,
        source_columns=["status"],
        dest_columns=["status"],
    )
    lookup = next(r for r in report["rules"] if r.get("code_crosswalk"))
    assert lookup["code_crosswalk"]["A"] == "ACTIVE"
    assert lookup["code_crosswalk"]["I"] == "INACTIVE"

    collide = (
        "Source Column,Destination Column,Rule\n"
        "fname,name,Direct\n"
        "lname,name,Direct\n"
    ).encode()
    report = compile_rule_workbook(
        "collide.csv",
        collide,
        source_columns=["fname", "lname"],
        dest_columns=["name"],
    )
    statuses = {r["source_column"]: r["status"] for r in report["rules"]}
    assert statuses["fname"] == "executable"
    assert statuses["lname"] == "conflict"


def test_same_source_two_dests_and_unmapped_source():
    csv = (
        "Source Column,Destination Column,Rule\n"
        "fname,first_name,Direct\n"
        "fname,display_name,Direct\n"
    ).encode()
    report = compile_rule_workbook(
        "fanout.csv",
        csv,
        source_columns=["fname", "extra"],
        dest_columns=["first_name", "display_name"],
    )
    dests = [r["dest_column"] for r in report["rules"] if r["status"] == "executable"]
    assert set(dests) == {"first_name", "display_name"}
    assert report["unmapped_source_count"] == 1
    assert "extra" in report["unmapped_source_columns"]


def test_filter_and_compound_trim_emit_shape_steps():
    csv = (
        "Source Column,Destination Column,Rule\n"
        "amount,amount,keep if amount > 0\n"
        "email,email,trim + lowercase\n"
        "code,code_prefix,\"left(code, 3)\"\n"
    ).encode()
    report = compile_rule_workbook(
        "shape.csv",
        csv,
        source_columns=["amount", "email", "code"],
        dest_columns=["amount", "email", "code_prefix"],
    )
    ops = [s["op"] for s in report["shape_steps"]]
    assert "filter_rows" in ops
    assert "trim" in ops
    assert "derive_column" in ops
    email = next(r for r in report["rules"] if r["source_column"] == "email")
    assert email["transform"] == "lower"
    assert email["status"] == "executable"


def test_exclude_rows_is_not_omit():
    assert classify_rule("exclude rows where status = X")["kind"] == "filter"
    assert classify_rule("omit")["kind"] == "omit"


def test_prose_still_fails_closed():
    assert classify_rule("for legacy customers use the old number unless migrated")["kind"] == "unknown"
    assert classify_rule("maybe use the other id")["kind"] == "unknown"


def test_title_row_does_not_steal_unusual_headers():
    csv = (
        "Customer mapping spec\n"
        "Orig Field,Target Name,How to convert\n"
        "fname,first_name,Direct\n"
    ).encode()
    report = compile_rule_workbook(
        "titled-unusual.csv",
        csv,
        source_columns=["fname"],
        dest_columns=["first_name"],
    )
    roles = {item["header"]: item["role"] for item in report["header_roles"]}
    assert roles["Orig Field"] == "source_column"
    assert report["rules"][0]["source_column"] == "fname"
    assert report["rules"][0]["status"] == "executable"


def test_unusual_headers_are_inferred_from_schema_not_aliases():
    """A customer sheet does not have to say 'Source Column'."""
    assert canonical_header("Orig Field") == ""
    assert canonical_header("How to convert") == ""
    csv = (
        "Orig Field,Target Name,How to convert\n"
        "fname,first_name,Direct\n"
        "status,status,trim + lowercase\n"
        "mystery,segment,use the legacy id unless migrated\n"
    ).encode()
    src = ["fname", "status", "mystery"]
    dst = ["first_name", "status", "segment"]
    report = compile_rule_workbook(
        "unusual.csv",
        csv,
        source_columns=src,
        dest_columns=dst,
    )
    roles = {item["header"]: item["role"] for item in report["header_roles"]}
    assert roles["Orig Field"] == "source_column"
    assert roles["Target Name"] == "dest_column"
    assert roles["How to convert"] == "rule"
    assert any(item["method"] in {"schema", "rule_pattern", "hint"} for item in report["header_roles"])
    by_src = {r["source_column"]: r for r in report["rules"]}
    assert by_src["fname"]["status"] == "executable"
    assert by_src["fname"]["dest_column"] == "first_name"
    assert by_src["status"]["transform"] == "lower"
    assert by_src["mystery"]["status"] == "needs_confirmation"
    assert by_src["mystery"]["kind"] == "unknown"


def test_opaque_headers_bind_from_cell_values_against_schema():
    csv = (
        "ColA,ColB,ColC\n"
        "fname,first_name,Direct\n"
        "lname,last_name,Direct\n"
        "status,status,A → ACTIVE, I → INACTIVE\n"
    ).encode()
    report = compile_rule_workbook(
        "opaque.csv",
        csv,
        source_columns=["fname", "lname", "status"],
        dest_columns=["first_name", "last_name", "status"],
    )
    roles = {item["header"]: (item["role"], item["method"]) for item in report["header_roles"]}
    assert roles["ColA"][0] == "source_column"
    assert roles["ColA"][1] == "schema"
    assert roles["ColB"][0] == "dest_column"
    assert roles["ColB"][1] == "schema"
    assert roles["ColC"][0] == "rule"
    assert report["buckets"]["executable"] == 3
    status = next(r for r in report["rules"] if r["source_column"] == "status")
    assert status["code_crosswalk"]["A"] == "ACTIVE"


def test_hint_only_headers_without_schema_stay_in_review():
    csv = (
        "Orig Field,Landing col,Instruction\n"
        "fname,first_name,Direct\n"
    ).encode()
    report = compile_rule_workbook("hints-only.csv", csv)
    assert report["header_roles"]
    assert report["rules"][0]["status"] == "needs_confirmation"
    assert any("inferred" in issue.lower() for issue in report["rules"][0]["issues"])


def test_infer_header_roles_prefers_schema_over_name_hints():
    inferred = infer_header_roles(
        ["Legacy attr", "Outbound name", "Instruction"],
        [
            {"Legacy attr": "fname", "Outbound name": "first_name", "Instruction": "Direct"},
            {"Legacy attr": "lname", "Outbound name": "last_name", "Instruction": "trim"},
        ],
        source_columns=["fname", "lname"],
        dest_columns=["first_name", "last_name"],
    )
    assert inferred.roles["Legacy attr"] == "source_column"
    assert inferred.roles["Outbound name"] == "dest_column"
    assert inferred.roles["Instruction"] == "rule"
    methods = {item["header"]: item["method"] for item in inferred.evidence}
    assert methods["Legacy attr"] == "schema"
    assert methods["Outbound name"] == "schema"


def test_linguistic_bind_is_cupid_unique_winner():
    cols = ["customer_id", "order_id", "first_name"]
    assert resolve_name("cust_id", cols) == "customer_id"
    name, method, score = resolve_name_ex("cust_id", cols)
    assert name == "customer_id"
    assert method == "linguistic"
    assert score >= 0.72
    # Ambiguous and short names stay unbound — Similarity Flooding honesty.
    assert resolve_name("customer", ["customer_id", "customer_name"]) == ""
    assert resolve_name("id", cols) == ""
    assert resolve_name("name", ["first_name", "last_name"]) == ""
    assert unique_linguistic_match("fname", ["first_name", "last_name"])[0] == ""
    assert name_similarity("customer_id", "customer_id") == 1.0


def test_structural_sql_and_nested_excel_are_closed_forms():
    assert classify_rule("CAST(amount AS INTEGER)")["kind"] == "cast_integer"
    assert classify_rule("amount::numeric")["kind"] == "cast_number"
    assert classify_rule("COALESCE(status, N/A)")["kind"] == "default"
    assert classify_rule("NULLIF(code, '')")["kind"] == "null_if"
    assert classify_rule("LEN(code)")["kind"] == "derive"
    assert classify_rule("to_date(dob)")["kind"] == "date"
    nested = classify_rule("=TRIM(UPPER(A2))")
    assert nested["kind"] == "case_upper"
    assert any(extra.get("op") == "trim" for extra in nested.get("extras") or [])
    outer = classify_rule("=UPPER(TRIM(A2))")
    assert outer["kind"] == "case_upper"
    assert any(extra.get("op") == "trim" for extra in outer.get("extras") or [])


def test_linguistic_and_sql_compile_onto_engines():
    csv = (
        "Source Column,Destination Column,Rule\n"
        "cust_id,customer_id,Direct\n"
        "amount,amount,CAST(amount AS INTEGER)\n"
        "status,status,\"COALESCE(status, N/A)\"\n"
        "Customer.First Name,first_name,=TRIM(UPPER(A2))\n"
    ).encode()
    report = compile_rule_workbook(
        "linguistic.csv",
        csv,
        source_columns=["customer_id", "amount", "status", "first_name"],
        dest_columns=["customer_id", "amount", "status", "first_name"],
    )
    by_src = {r["source_column"]: r for r in report["rules"]}
    assert by_src["customer_id"]["status"] == "executable"
    assert by_src["customer_id"]["bind_method"] == "linguistic"
    assert by_src["amount"]["transform"] == "cast_integer"
    assert by_src["status"]["shape_step"]["op"] == "default_if_null"
    assert by_src["first_name"]["transform"] == "upper"
    assert any(s["op"] == "trim" for s in report["shape_steps"])
    assert report["matcher"] == "cupid-linguistic+instance"
    assert report["bind_methods"].get("linguistic", 0) >= 1


def test_duplicate_headers_and_notes_sheet_are_not_silent():
    csv = (
        "Col,Col,Rule\n"
        "fname,first_name,Direct\n"
    ).encode()
    report = compile_rule_workbook(
        "dup.csv",
        csv,
        source_columns=["fname"],
        dest_columns=["first_name"],
    )
    roles = {item["header"]: item["role"] for item in report["header_roles"]}
    assert "Col" in roles and "Col_2" in roles
    assert report["buckets"]["executable"] == 1

    openpyxl = pytest.importorskip("openpyxl")
    from openpyxl import Workbook

    wb = Workbook()
    notes = wb.active
    notes.title = "Instructions"
    notes.append(["Read this first"])
    notes.append([
        "Please map customer fields using the Rules tab. "
        "Contact the data team for exceptions that are not listed here. "
        "Historical codes stay in the archive until legal signs off."
    ])
    rules = wb.create_sheet("Rules")
    rules.append(["Source Column", "Destination Column", "Rule"])
    rules.append(["fname", "first_name", "Direct"])
    buf = io.BytesIO()
    wb.save(buf)
    report = compile_rule_workbook(
        "mixed.xlsx",
        buf.getvalue(),
        source_columns=["fname"],
        dest_columns=["first_name"],
    )
    kinds = {item["sheet"]: item["kind"] for item in report["sheet_kinds"]}
    assert kinds.get("Instructions") == "notes"
    assert kinds.get("Rules") == "rules"
    executable = [r for r in report["rules"] if r["status"] == "executable"]
    assert len(executable) == 1
    assert executable[0]["source_column"] == "fname"
    assert any("commentary" in (r.get("rule_text") or "").lower() for r in report["rules"])
