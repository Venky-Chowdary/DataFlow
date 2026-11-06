"""X-29 regression guard: the CDC snapshot leftover gate binds the catalog key.

An API-created CDC schedule declares no ``primary_key`` on its stream contract;
the run binds the source catalog key. The E3-006 leftover gate read only the
contract key and raised ``ValueError("primary_key is required")`` out of the
policy pass, so the run failed before the snapshot (and a keyless table lost
its named refusal). Live coverage: ``test_x29_api_cdc_schedule.py``.
"""

from __future__ import annotations

from services.cdc_snapshot_sweep import build_snapshot_leftover_preflight_gate

_SNAPSHOT = {"id": "g18", "status": "pass", "details": {"run_snapshot": True}}
_CONTRACT = [{"selected": True, "name": "orders", "stream": "orders", "sync_mode": "cdc",
              "primary_key": ""}]


def _gate(**kw):
    return build_snapshot_leftover_preflight_gate(
        snapshot_gate=_SNAPSHOT, stream_contracts=_CONTRACT, source_endpoint=None,
        destination_endpoint=None, source_config={}, mappings=[], dest_type="postgresql", **kw,
    )


def test_contract_without_key_and_catalog_key_does_not_raise():
    assert _gate(catalog_primary_key_columns=["id"]) is None


def test_keyless_contract_and_catalog_leaves_the_refusal_to_the_capture_gate():
    assert _gate(catalog_primary_key_columns=[]) is None
    assert _gate() is None
