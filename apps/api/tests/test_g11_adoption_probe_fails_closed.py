"""G11 re-review L2: adoption fails closed when the destination cannot be read.

When a target's spelling changed (a long name, a mixed-case BigQuery target)
Execute reads the destination's columns to adopt an earlier run's legacy
column. A failed read adopted nothing and carried on: with an approved hash
the run then failed with a misleading "DDL identity mismatch"; without one it
wrote the full name next to the legacy column. It now refuses with what
happened.
"""

from __future__ import annotations

from unittest.mock import patch

LONG = "p" * 70


def _request(stream_contracts=None):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=[{"source": "id", "target": "id"}, {"source": "v", "target": LONG}],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        sync_mode="full_refresh_append",
        stream_contracts=stream_contracts or [],
    )


def test_failed_column_read_refuses_with_the_reason():
    from services.source_filter_gate import request_refusal

    request = _request()
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint",
               side_effect=RuntimeError("connection refused")):
        refused = request_refusal(request)
    assert "Could not read the destination's existing columns" in refused, refused
    assert "connection refused" in refused
    assert [m["target"] for m in request.mappings] == ["id", LONG]


def test_failed_column_read_refuses_each_stream():
    from services.source_filter_gate import request_refusal

    request = _request([{"name": "s1", "selected": True}, {"name": "s2", "selected": True}])
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint",
               side_effect=RuntimeError("connection refused")):
        refused = request_refusal(request)
    assert "Could not read the destination's existing columns" in refused, refused


def test_no_probe_needed_no_refusal():
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        mappings=[{"source": "id", "target": "id"}],
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format="postgresql", database="d", table="t"),
        sync_mode="full_refresh_append",
    )
    with patch("src.transfer.endpoint_intelligence.introspect_endpoint",
               side_effect=AssertionError("no probe needed")):
        assert request_refusal(request) == ""
