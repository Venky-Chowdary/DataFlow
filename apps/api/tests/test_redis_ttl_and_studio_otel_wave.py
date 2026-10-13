"""Redis TTL honesty + studio.map/validate span helpers."""

from __future__ import annotations


from services.preflight_service import run_transfer_policy_gates
from services.tracing import start_span


def test_redis_destination_emits_ttl_soft_warning():
    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_overwrite",
        dest_type="redis",
        source_type="postgresql",
        source_kind="database",
    )
    ttl = next((g for g in gates if g.get("id") == "redis_ttl_semantics"), None)
    assert ttl is not None
    assert ttl.get("blocks_transfer") is False
    assert "TTL" in (ttl.get("message") or "")
    assert ttl.get("details", {}).get("honesty") == "ttl_not_productized"


def test_redis_source_emits_ttl_soft_warning():
    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_overwrite",
        dest_type="postgresql",
        source_type="redis",
        source_kind="database",
    )
    assert any(g.get("id") == "redis_ttl_semantics" for g in gates)


def test_start_span_studio_map_name_is_stable():
    """Span helper accepts studio.map / studio.validate names (fail-open)."""
    with start_span("studio.map", attributes={"dataflow.phase": "map"}) as span:
        assert span is not None
    with start_span("studio.validate", attributes={"dataflow.phase": "validate"}) as span:
        assert span is not None


def test_skip_preflight_policy_requires_reason():
    from tests.helpers.skip_preflight_policy import require_skip_reason
    import pytest

    assert require_skip_reason(skip_preflight=False) is False
    with pytest.raises(ValueError, match="reason"):
        require_skip_reason(skip_preflight=True, reason="short")
    assert require_skip_reason(
        skip_preflight=True,
        reason="perf matrix — no live dest probe",
    ) is True
