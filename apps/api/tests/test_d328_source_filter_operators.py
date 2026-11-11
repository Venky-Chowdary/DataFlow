"""D328: source_filter accepts the symbol spellings of its operators and the
MCP tool schema lists the operator set.

QA: ``source_filter={"column": "qty", "op": ">", "value": 1}`` (and ``>=``)
failed with "Unsupported filter operator: >" while ``gt`` worked, and the tool
description only showed ``"op": "="`` — which itself was refused.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services import row_filter as rf  # noqa: E402
from services.row_filter import _FILTER_OPS, apply_row_filter  # noqa: E402

ROWS = [{"id": i, "qty": q, "region": r} for i, (q, r) in enumerate(
    [(0, "EU"), (1, "EU"), (2, "US"), (3, "EU"), (10, "APAC")], start=1
)]

SYMBOL_TO_WORD = [
    (">", "gt"), (">=", "gte"), ("<", "lt"), ("<=", "lte"),
    ("=", "eq"), ("==", "eq"), ("!=", "ne"), ("<>", "ne"),
]


def _ids(rows):
    return [r["id"] for r in rows]


@pytest.mark.parametrize(("symbol", "word"), SYMBOL_TO_WORD)
def test_symbol_operator_filters_like_its_word_form(symbol, word):
    by_symbol = apply_row_filter(ROWS, {"column": "qty", "op": symbol, "value": 1})
    by_word = apply_row_filter(ROWS, {"column": "qty", "op": word, "value": 1})
    assert _ids(by_symbol) == _ids(by_word)


def test_gt_and_gte_symbols_give_exact_rows():
    assert _ids(apply_row_filter(ROWS, {"column": "qty", "op": ">", "value": 1})) == [3, 4, 5]
    assert _ids(apply_row_filter(ROWS, {"column": "qty", "op": ">=", "value": 1})) == [2, 3, 4, 5]
    nested = {"and": [
        {"column": "qty", "operator": ">", "value": 0},
        {"column": "region", "op": "=", "value": "EU"},
    ]}
    assert _ids(apply_row_filter(ROWS, nested)) == [2, 4]


@pytest.mark.parametrize("bad", ["~~", "=>", "; DROP TABLE t", "like"])
def test_unknown_operator_is_refused_with_the_accepted_list(bad):
    with pytest.raises(ValueError) as exc:
        apply_row_filter(ROWS, {"column": "qty", "op": bad, "value": 1})
    message = str(exc.value)
    assert "Unsupported filter operator" in message
    assert "gt (>)" in message and "is_not_null" in message


def test_canonical_spec_rewrites_symbols_to_word_forms():
    spec = {"or": [
        {"column": "qty", "op": ">=", "value": 1},
        {"and": [{"column": "region", "op": "<>", "value": "EU"}, {"column": "qty", "op": "IS NULL"}]},
    ]}
    out = rf.canonical_filter_spec(spec)
    assert out["or"][0]["op"] == "gte"
    assert out["or"][1]["and"][0]["op"] == "ne"
    assert out["or"][1]["and"][1]["op"] == "is_null"
    # The input is not mutated.
    assert spec["or"][0]["op"] == ">="


def test_plan_grounding_carries_the_word_form_and_refuses_unknown():
    from src.ai.copilot.transfer_tools import _ground_data_rules

    rules, err = _ground_data_rules(
        source_filter={"column": "QTY", "op": ">", "value": 1},
        upsert_key="", dedupe_key="", source_columns=["id", "qty"],
        source_label="src.t", mode="overwrite",
    )
    assert err == ""
    assert rules["source_filter"] == {"column": "qty", "op": "gt", "value": 1}
    assert rules["filter_description"] == "qty gt 1"

    _rules, err = _ground_data_rules(
        source_filter={"column": "qty", "op": "=>", "value": 1},
        upsert_key="", dedupe_key="", source_columns=["id", "qty"],
        source_label="src.t", mode="overwrite",
    )
    assert "Unsupported filter operator: =>" in err and "gte (>=)" in err


def test_tool_schema_documents_every_operator():
    from src.ai.copilot.tools import _ROW_RULE_PROPS

    text = _ROW_RULE_PROPS["source_filter"]["description"]
    for op in _FILTER_OPS:
        assert op in text, op
    for symbol in (">", ">=", "<", "<=", "=", "!=", "<>"):
        assert f"{symbol}" in text
    assert all(word in _FILTER_OPS for word in rf.FILTER_OP_ALIASES.values())


def test_cdc_snapshot_filter_shares_the_vocabulary_and_binds_values():
    from services.cdc_snapshot_filter import (
        compile_snapshot_filter_sql,
        normalize_snapshot_filter,
    )

    spec = normalize_snapshot_filter({"column": "qty", "op": ">=", "value": "1; DROP TABLE t"})
    assert spec["op"] == "gte"
    sql, params = compile_snapshot_filter_sql(spec, dialect="postgresql")
    assert "DROP" not in sql
    assert list(params) == ["1; DROP TABLE t"]


# --- the transfer itself: CSV -> SQLite through the engine ------------------


def _csv() -> bytes:
    lines = ["id,qty,region"] + [f"{r['id']},{r['qty']},{r['region']}" for r in ROWS]
    return "\n".join(lines).encode("utf-8")


@pytest.mark.parametrize(("op", "expected"), [(">", [3, 4, 5]), (">=", [2, 3, 4, 5])])
def test_transfer_with_symbol_source_filter_moves_exact_rows(tmp_path, op, expected):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    db_path = tmp_path / "d328.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv(),
        source_filename="qty.csv",
        destination=EndpointConfig(
            kind="database", format="sqlite", connection_string=str(db_path), table="t"
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        source_filter={"column": "qty", "op": op, "value": 1},
    )
    result = UniversalTransferEngine().execute_tracked(request, "d328" + uuid.uuid4().hex[:16])
    assert result.success, result.error

    conn = sqlite3.connect(str(db_path))
    try:
        landed = [int(r[0]) for r in conn.execute("SELECT id FROM t ORDER BY id")]
    finally:
        conn.close()
    assert landed == expected
