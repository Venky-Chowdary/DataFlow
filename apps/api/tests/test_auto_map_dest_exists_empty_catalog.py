"""Execute auto-map must not invent create-new when dest exists but columns unread.

destination_exists_for_typing collapses exists=True + empty catalog for *type*
invent. Dest-exists shape (Map / G15) must keep the probe boolean.
"""

from __future__ import annotations

from src.transfer.engine import _auto_map
from src.transfer.models import EndpointConfig, TransferRequest
from services.sync_cursor import destination_exists_for_shape, destination_exists_for_typing


def test_shape_keeps_existing_empty_catalog_typing_does_not() -> None:
    assert destination_exists_for_shape(True, dest_format="postgresql") is True
    assert (
        destination_exists_for_typing(
            "full_refresh_append",
            True,
            has_live_column_types=False,
            dest_format="postgresql",
        )
        is False
    )
    assert destination_exists_for_shape(None, dest_format="postgresql") is None
    assert destination_exists_for_shape(True, dest_format="redis") is False
    assert destination_exists_for_shape(False, dest_format="postgresql") is False


def test_auto_map_existing_empty_catalog_is_pending_not_create_new(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.transfer.engine._destination_schema_probe",
        lambda dest, sync_mode="": ({}, True),
    )
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql"),
        destination=EndpointConfig(kind="database", format="postgresql", table="orders"),
        sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    mappings = _auto_map(
        request,
        ["id", "title"],
        {"id": "INTEGER", "title": "TEXT"},
    )
    assert mappings
    assert all(m.get("create_new") is not True for m in mappings)
    assert all(m.get("assignment_strategy") == "pending_dest_schema" for m in mappings)


def test_auto_map_unknown_catalog_is_pending_not_create_new(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.transfer.engine._destination_schema_probe",
        lambda dest, sync_mode="": ({}, None),
    )
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql"),
        destination=EndpointConfig(kind="database", format="postgresql", table="orders"),
        sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    mappings = _auto_map(
        request,
        ["id", "title"],
        {"id": "INTEGER", "title": "TEXT"},
    )
    assert mappings
    assert all(m.get("create_new") is not True for m in mappings)
    assert all(m.get("assignment_strategy") == "pending_dest_schema" for m in mappings)


def test_auto_map_missing_table_still_invents_create_new(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.transfer.engine._destination_schema_probe",
        lambda dest, sync_mode="": ({}, False),
    )
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql"),
        destination=EndpointConfig(kind="database", format="postgresql", table="orders"),
        sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    mappings = _auto_map(
        request,
        ["id", "title"],
        {"id": "INTEGER", "title": "TEXT"},
    )
    assert mappings
    assert any(m.get("create_new") is True for m in mappings)
    assert all(m.get("assignment_strategy") != "pending_dest_schema" for m in mappings)
