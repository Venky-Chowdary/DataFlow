"""D90: an append-only destination refuses non-append sync modes at Validate.

The refusal is read from the connector capability registry (append yes; upsert,
merge and overwrite no), so every append-only destination gets it - not only the
Kafka family the writer happens to refuse at run time.
"""
from __future__ import annotations

import pytest

from services import connector_capability_registry as registry
from services.preflight_service import run_transfer_policy_gates

GATE_ID = "g_dest_append_only"
_NON_APPEND = ("full_refresh_overwrite", "incremental_deduped", "upsert", "mirror", "scd2", "reverse_etl", "cdc")
_APPEND = ("full_refresh_append", "incremental_append")


def _gate(sync_mode: str, dest: str):
    gates = run_transfer_policy_gates(
        sync_mode=sync_mode,
        dest_type=dest,
        source_type="postgresql",
        source_kind="database",
        stream_contracts=[],
        mappings=[],
    )
    return next((g for g in gates if g["id"] == GATE_ID), None)


@pytest.fixture
def append_sink(monkeypatch):
    """A destination that is append-only by registry record, with no Kafka name."""
    real = registry.get_connector_capability

    def fake(key):
        if key == "append_sink":
            return {**registry.DEFAULT_CAPABILITY, "supports_upsert": False,
                    "supports_overwrite": False, "supports_merge": False}
        return real(key)

    monkeypatch.setattr(registry, "get_connector_capability", fake)


@pytest.mark.parametrize("mode", _NON_APPEND)
def test_registry_append_only_destination_blocks_non_append_modes(append_sink, mode):
    gate = _gate(mode, "append_sink")
    assert gate is not None and gate["status"] == "block", gate
    assert "append" in gate["message"].lower() and "append_sink" in gate["message"]


@pytest.mark.parametrize("mode", _APPEND)
def test_registry_append_only_destination_passes_append_modes(append_sink, mode):
    gate = _gate(mode, "append_sink")
    assert gate is not None and gate["status"] == "pass", gate


@pytest.mark.parametrize("dest", ["postgresql", "s3", "snowflake", "mongodb"])
def test_destinations_that_can_overwrite_or_upsert_get_no_append_only_gate(dest):
    assert _gate("incremental_deduped", dest) is None


def test_kafka_is_append_only_by_the_same_registry_rule():
    assert registry.is_append_only_destination("kafka")
    assert not registry.is_append_only_destination("postgresql")
    assert not registry.is_append_only_destination("not-a-connector")
