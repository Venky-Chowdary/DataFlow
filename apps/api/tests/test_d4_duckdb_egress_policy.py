"""D4 — one egress policy per connector across all tools.

Repro (prod c6c16128): a saved DuckDB connector whose host field is a
label ("localhost" or a file path) was refused with a loopback egress
error at create/update/probe/MCP-list time, while the same connector ran
fine as a transfer source. File engines never dial a host; the same
exemption _build_engine already applies must hold at every gate.
"""
from __future__ import annotations


def test_file_engine_host_label_not_refused():
    from services.egress_guard import metadata_endpoint_refusal

    for engine in ("duckdb", "sqlite", "csv_local", "file", "excel"):
        assert metadata_endpoint_refusal("localhost", db_type=engine) == "", engine
        assert metadata_endpoint_refusal("127.0.0.1", db_type=engine) == "", engine
        assert (
            metadata_endpoint_refusal(
                "", "duckdb:///tmp/lab.db", db_type=engine
            )
            == ""
        ), engine


def test_network_engine_still_refused():
    from services.egress_guard import metadata_endpoint_refusal

    # allowlist=() isolates the policy itself from the process env list
    # (DATAFLOW_EGRESS_ALLOWLIST permits loopback for the local test rig).
    assert metadata_endpoint_refusal("127.0.0.1", db_type="postgresql", allowlist=())
    assert metadata_endpoint_refusal("localhost", db_type="mysql", allowlist=())
    # No db_type: behaviour unchanged (fail closed on literal loopback).
    assert metadata_endpoint_refusal("127.0.0.1", allowlist=())


def test_probe_passes_type_to_egress_check():
    import inspect

    from src.transfer import connector_registry

    source = inspect.getsource(connector_registry._run_probe_impl)
    assert 'db_type=str(cfg.get("type")' in source


def test_create_and_update_pass_type():
    import inspect
    import pathlib

    tools_src = (
        pathlib.Path(__file__).resolve().parents[1]
        / "src/ai/copilot/tools.py"
    ).read_text()
    assert 'db_type=str(draft.get("type")' in tools_src
    life_src = (
        pathlib.Path(__file__).resolve().parents[1]
        / "src/ai/copilot/lifecycle_tools.py"
    ).read_text()
    assert 'db_type=str(\n                changes.get("type")' in life_src
