"""D58 — every MCP tool schema matches the parameters its handler really takes.

QA: ``start_dataset_transfer`` upsert had no key parameter. The earlier fix
added ``upsert_key`` to that one schema. The family is wider: the schema an MCP
client (and the model) reads is hand-written beside the handler, so any handler
parameter left out of it cannot be named by a client, and any schema property
the handler does not take is refused at call time. This diffs the two for every
tool, from the same dispatch table ``DataPilotTools.execute`` uses and the same
descriptors ``tools/list`` serves.
"""

from __future__ import annotations

import inspect
import re

import pytest


def _dispatch() -> dict[str, str]:
    """tool name -> handler method name, read from ``DataPilotTools.execute``."""
    from src.ai.copilot.tools import DataPilotTools

    source = inspect.getsource(DataPilotTools.execute)
    found = dict(re.findall(r'"(\w+)":\s*self\.(\w+)', source))
    assert len(found) > 40, "dispatch table not found in DataPilotTools.execute"
    return found


def _listed_schemas() -> dict[str, dict]:
    """The inputSchema each tool advertises on ``tools/list``."""
    from services.mcp_protocol import handle_jsonrpc

    reply = handle_jsonrpc(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, authenticated=True
    )
    return {t["name"]: t["inputSchema"] for t in reply["result"]["tools"]}


def _handler_params(method: str) -> tuple[set[str], set[str], bool]:
    from src.ai.copilot.tools import DataPilotTools

    params = inspect.signature(getattr(DataPilotTools, method)).parameters
    named = {
        k for k, p in params.items()
        if k != "self" and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
    }
    required = {k for k in named if params[k].default is inspect.Parameter.empty}
    var_kw = any(p.kind == p.VAR_KEYWORD for p in params.values())
    return named, required, var_kw


def test_every_listed_tool_has_a_handler_and_every_handler_is_listed():
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    defined = {t["name"] for t in TOOL_DEFINITIONS}
    handled = set(_dispatch())
    assert defined - handled == set(), "advertised tools with no handler"
    assert handled - defined == set(), "handlers no schema advertises"


@pytest.mark.parametrize("tool", sorted(_dispatch()))
def test_tool_schema_matches_handler_signature(tool):
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    method = _dispatch()[tool]
    named, required, var_kw = _handler_params(method)
    schema = next(t["input_schema"] for t in TOOL_DEFINITIONS if t["name"] == tool)
    props = set(schema.get("properties") or {})
    schema_required = set(schema.get("required") or [])

    assert named - props == set(), (
        f"{tool}: the handler takes parameters its schema never advertises"
    )
    if not var_kw:
        assert props - named == set(), (
            f"{tool}: the schema advertises parameters the handler refuses"
        )
    assert schema_required <= props, f"{tool}: required names a missing property"
    assert required <= schema_required, (
        f"{tool}: the handler requires parameters the schema marks optional"
    )


def test_tools_list_serves_the_same_schemas():
    """What an MCP client reads is the definition the test above checked."""
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    listed = _listed_schemas()
    for tool in TOOL_DEFINITIONS:
        if tool["name"] in listed:
            assert listed[tool["name"]] == tool["input_schema"], tool["name"]
    assert "upsert_key" in listed["start_dataset_transfer"]["properties"]
