"""Data Pilot get_job / ID routing — not a chatbot stub."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot.tools import DataPilotTools, infer_tools_from_message  # noqa: E402


def test_infer_tools_routes_object_id_to_get_job():
    planned = infer_tools_from_message("why did job 507f1f77bcf86cd799439011 fail?")
    names = [n for n, _ in planned]
    assert "get_job" in names
    args = dict(planned)["get_job"]
    assert args["job_id"] == "507f1f77bcf86cd799439011"


def test_infer_tools_routes_pf_id():
    planned = infer_tools_from_message("explain pf_a1b2c3d4e5f6 please")
    assert ("get_preflight_run", {"run_id": "pf_a1b2c3d4e5f6"}) in planned


def test_infer_tools_meta_knowledge_not_rag_dump():
    planned = infer_tools_from_message("what knowledge you have")
    names = [n for n, _ in planned]
    assert names == ["describe_pilot"]
    assert "search_knowledge" not in names


def test_infer_tools_greeting_fluff_skips_rag():
    planned = infer_tools_from_message("thanks a lot")
    assert planned == []


def test_describe_pilot_returns_capabilities():
    tools = DataPilotTools()
    tr = tools.execute("describe_pilot", {})
    assert tr.success is True
    assert "can" in (tr.output or {})
    assert len(tr.output["can"]) >= 3


def test_get_job_missing_returns_error():
    tools = DataPilotTools()
    tr = tools.execute("get_job", {"job_id": "000000000000000000000000"})
    assert tr.success is False
    assert "not found" in (tr.error or "").lower()
