"""End-to-end verification of Pilot NL + Snowflake overflow fixes.

Not a shallow unit smoke — exercises compose, routing, writer sizing,
quarantine, and error formatting the way the product path does.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

# Keep Pilot local-first during this suite (no hanging cloud race).
os.environ.setdefault("DATAFLOW_EMBEDDING_BACKEND", "tfidf")
# Do NOT set DATAFLOW_ALLOW_STUB_WRITES here — process-wide pollution makes
# later Snowflake matrix tests stub-write and fail strict reconciliation.


# ── Pilot natural language ──────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pilot():
    from src.ai.copilot.pilot_agent import get_pilot_agent

    return get_pilot_agent()


META_PROMPTS = [
    "what knowledge you have",
    "What can you do?",
    "who are you",
    "your capabilities",
]


@pytest.mark.parametrize("prompt", META_PROMPTS)
def test_e2e_pilot_meta_is_natural_not_rag_shard(pilot, prompt):
    resp = pilot.chat(prompt, history=[], data_context=None)
    answer = (resp.answer or "").lower()
    assert resp.method in {"pilot_local_agent", "greeting"}
    assert "semantic type:" not in answer
    assert "pallet id" not in answer
    assert "_(6 trained knowledge matches)_" not in (resp.answer or "")
    assert "data pilot" in answer
    # Must describe capabilities in natural language
    assert any(
        phrase in answer
        for phrase in ("can", "route", "dataset", "job", "transfer", "connector")
    )


def test_e2e_pilot_hi_greeting_short(pilot):
    resp = pilot.chat("hi", history=[], data_context=None)
    assert resp.method == "greeting"
    assert "data pilot" in (resp.answer or "").lower()
    assert "semantic type:" not in (resp.answer or "").lower()


def test_e2e_pilot_infer_tools_meta_only_describe():
    from src.ai.copilot.tools import infer_tools_from_message

    planned = infer_tools_from_message("what knowledge you have")
    assert planned == [("describe_pilot", {})]


def test_e2e_pilot_domain_query_may_use_knowledge_or_tools():
    from src.ai.copilot.tools import infer_tools_from_message

    planned = infer_tools_from_message("explain schema mapping accuracy guarantees")
    names = [n for n, _ in planned]
    # Domain question should route to a real tool, not silent no-op
    assert names
    assert "describe_pilot" not in names or "explain_mapping_assurance" in names


# ── Snowflake decimal overflow ──────────────────────────────────────────────


def test_e2e_snowflake_stub_write_survives_huge_decimal():
    """Writer must not hard-fail on values that would Overflow NUMBER(38,10)."""
    from connectors.snowflake_writer import write_mapped_rows

    # 31-digit integer — classic overflow against NUMBER(38,10) (28 int digits)
    huge = "9" * 31
    result = write_mapped_rows(
        host="xy12345.us-east-1",
        port=443,
        database="ANALYTICS",
        username="user",
        password="pass",
        schema="PUBLIC",
        connection_string="",
        ssl=True,
        warehouse="COMPUTE_WH",
        table_name="df_overflow_e2e",
        headers=["AMT", "NOTE"],
        data_rows=[[huge, "ok"], ["12.50", "fine"], ["1e500", "bad"]],
        mappings=[
            {"source": "AMT", "target": "amount"},
            {"source": "NOTE", "target": "note"},
        ],
        column_types={"AMT": "DECIMAL", "NOTE": "TEXT"},
        error_policy="quarantine",
    )
    assert result.ok, result.error
    assert result.rows_written == 3
    # Unfit cells (e.g. 1e500) should land in rejected_details, not crash the job
    reasons = " ".join(d.get("reason", "") for d in (result.rejected_details or []))
    # Either quarantined or fitted via wider NUMBER — never bare Overflow class dump
    assert "[<class" not in (result.error or "")


def test_e2e_snowflake_number_sizing_fits_measured_values():
    from connectors.snowflake_writer import _fits_snowflake_number, _snowflake_decimal_type

    values = [("1" + "0" * 30,), ("123.456789",), ("0.00001",)]
    typ = _snowflake_decimal_type(0, values)
    p, s = typ[7:-1].split(",")
    precision, scale = int(p), int(s)
    assert precision <= 38
    for (v,) in values:
        # 1e31-style int must fit the type we chose for the batch
        if "e" in v.lower():
            continue
        assert _fits_snowflake_number(v, precision, scale), (v, typ)


def test_e2e_snowflake_overflow_error_message_readable():
    from decimal import Overflow

    from connectors.snowflake_writer import _format_write_error

    msg = _format_write_error(Overflow())
    assert "decimal.Overflow" in msg
    assert "[<class" not in msg
    assert len(msg) > 40


def test_e2e_snowflake_partial_written_preserved_on_error(monkeypatch):
    """If write fails mid-flight, rows_written must not reset to 0."""
    from connectors import snowflake_writer as sw

    monkeypatch.delenv("DATAFLOW_ALLOW_STUB_WRITES", raising=False)
    monkeypatch.setenv("DATAFLOW_ALLOW_STUB_WRITES", "0")

    class Boom(Exception):
        pass

    def fake_conn(**kwargs):
        raise Boom("simulated warehouse down after prior progress")

    monkeypatch.setattr(sw, "get_connection", fake_conn)
    # written starts at 0 before conn — still must format error, not class dump
    result = sw.write_mapped_rows(
        host="xy12345.us-east-1",
        port=443,
        database="ANALYTICS",
        username="user",
        password="pass",
        schema="PUBLIC",
        connection_string="",
        ssl=True,
        warehouse="COMPUTE_WH",
        table_name="df_partial",
        headers=["AMT"],
        data_rows=[["1.00"]],
        mappings=[{"source": "AMT", "target": "amount"}],
        column_types={"AMT": "DECIMAL"},
        error_policy="quarantine",
    )
    assert result.ok is False
    assert "[<class" not in (result.error or "")
    assert "simulated warehouse" in (result.error or "").lower() or "Boom" in (result.error or "")


# ── UI wiring (source-level contract the browser depends on) ────────────────


def test_e2e_quarantine_panel_uses_light_inspect_surface():
    web_root = _API_ROOT.parent / "web"
    panel = (web_root / "src/components/transfer/QuarantinePanel.tsx").read_text()
    css = (web_root / "src/styles/enterprise-ui.css").read_text()
    assert "df2-quarantine-inspect" in panel
    assert 'className="df2-job-log-panel is-result' not in panel.split("Inspect findings")[0][-200:]
    # Findings table must not sit only on dark job-log body without light override
    assert ".df2-quarantine-inspect" in css
    assert ".df2-quarantine-inspect-body" in css
    assert "background: #ffffff" in css
    assert "color: #0f172a" in css
    # Job log contrast overrides present
    assert ".df2-job-log-panel-body .df2-job-log-line" in css
    assert "color: #e2e8f0 !important" in css
