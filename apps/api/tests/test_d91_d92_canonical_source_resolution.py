"""D91/D92 — one canonical schema-qualified name resolved once at staging.

A transfer staging read must resolve the object the operator named, in the
namespace the connector declared. Fuzzy cross-schema/cross-database recovery
(matching a same-named table elsewhere) is interactive-browse behavior only:
inside a transfer it silently reads or shapes the wrong object.
"""

from __future__ import annotations

from unittest.mock import patch

from src.transfer.endpoint_intelligence import _attach_db_sample
from src.transfer.models import EndpointConfig


def _out(fmt: str) -> dict:
    return {
        "kind": "database",
        "format": fmt,
        "connected": True,
        "objects": [{"name": "orders", "type": "table"}],
        "columns": [],
        "schema": {},
        "row_estimate": 0,
        "auto_create": [],
        "message": "connected",
    }


def _endpoint(*, strict: bool, purpose: str = "source") -> EndpointConfig:
    extra = {"introspect_purpose": purpose}
    if strict:
        extra["introspect_strict"] = True
    return EndpointConfig(
        kind="database",
        format="mysql",
        database="railway",
        table="users",
        extra=extra,
    )


def _captured_strict(endpoint: EndpointConfig) -> bool | None:
    out = _out(endpoint.format)
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "mysql", "database": "railway", "host": "h", "port": 3306},
    ), patch(
        "src.transfer.endpoint_intelligence._introspect_table_schema_rich",
        return_value=({}, {}, {}),
    ) as intro:
        _attach_db_sample(out, endpoint)
    assert intro.called
    return intro.call_args.kwargs.get("strict_namespace")


def test_staging_source_introspection_is_namespace_strict():
    # A transfer staging read (introspect_strict) resolves canonically — no
    # cross-schema stealing when the named namespace lacks the object.
    assert _captured_strict(_endpoint(strict=True)) is True


def test_interactive_source_introspection_stays_fuzzy():
    # Interactive browse/search keeps the fuzzy cross-schema recovery.
    assert _captured_strict(_endpoint(strict=False)) is False


def test_destination_purpose_is_strict_without_flag():
    assert _captured_strict(_endpoint(strict=False, purpose="destination")) is True


def test_transfer_path_callers_set_introspect_strict():
    """Every source introspection inside the run/authority path carries the flag."""
    captured: list[dict] = []

    class _E:
        kind = "database"
        format = "postgresql"
        extra: dict = {}

    def _fake_introspect(endpoint, *a, **k):
        captured.append(dict(getattr(endpoint, "extra", None) or {}))
        return {"schema": {"id": "INTEGER"}, "schema_nullability": {"id": True}}

    endpoint = _E()
    with patch(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        side_effect=_fake_introspect,
    ):
        from services.source_schema_authority import endpoint_source_column_types

        endpoint_source_column_types(endpoint)

    assert captured, "endpoint_schema_map never introspected"
    assert all(c.get("introspect_strict") is True for c in captured), captured
    assert all(c.get("introspect_purpose") == "source" for c in captured), captured
