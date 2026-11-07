"""D58 — ``start_dataset_transfer`` offered upsert but no way to name the key.

The handler always accepted ``upsert_key``, but the tool schema the model and
MCP clients read did not list it, so a file upsert could only be staged with
no key (and then refused for having none).
"""

from __future__ import annotations

import inspect


def test_start_dataset_transfer_advertises_its_upsert_key():
    from src.ai.copilot.tools import DataPilotTools, _tool_schema_properties

    advertised = set(_tool_schema_properties("start_dataset_transfer"))
    assert "upsert_key" in advertised
    # Everything advertised is something the handler really takes.
    handler = inspect.signature(DataPilotTools._start_dataset_transfer).parameters
    assert advertised <= set(handler) - {"self"}
