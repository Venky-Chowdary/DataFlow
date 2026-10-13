"""Shared source-timezone declaration policy tests."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.timezone_policy import (
    declare_zone_on_schema,
    stamp_zone_transform,
    validate_source_timezone,
)


def test_offset_bearing_column_is_untouched():
    rows = [
        {
            "name": "created",
            "inferred_type": "TIMESTAMPTZ",
            "samples": ["2024-06-01T12:00:00+05:30"],
        }
    ]
    declared_rows, columns = declare_zone_on_schema(rows, "Asia/Kolkata")
    mappings = stamp_zone_transform(
        [{"source": "created", "target": "created", "transform": "none"}],
        "Asia/Kolkata",
        columns,
    )
    assert columns == set()
    assert declared_rows == rows
    assert mappings[0]["transform"] == "none"


def test_operator_transform_is_not_replaced():
    rows, columns = declare_zone_on_schema(
        [
            {
                "name": "created",
                "inferred_type": "TIMESTAMP_NTZ",
                "samples": ["2024-06-01 12:00:00"],
            }
        ],
        "Asia/Kolkata",
    )
    mappings = stamp_zone_transform(
        [{
            "source": "created",
            "target": "created",
            "transform": "assume_timezone:America/New_York",
        }],
        "Asia/Kolkata",
        columns,
    )
    assert "created" in columns
    assert mappings[0]["transform"] == "assume_timezone:America/New_York"
    assert rows[0]["inferred_type"] == "TIMESTAMPTZ"


def test_invalid_source_timezone_is_refused_with_field_name():
    zone, error = validate_source_timezone("Not/AZone")
    assert zone == ""
    assert "source_timezone" in error
    assert "Not/AZone" in error


def test_execute_api_returns_422_for_invalid_source_timezone():
    from src.routers.transfer_router import router

    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).post(
        "/transfer/execute",
        json={
            "source": {"kind": "database", "format": "mongodb"},
            "destination": {"kind": "database", "format": "postgresql"},
            "source_timezone": "Not/AZone",
        },
    )
    assert response.status_code == 422
    assert "source_timezone" in response.text


def test_skip_preflight_mongo_create_new_mapping_is_stamped(monkeypatch):
    from src.transfer import engine as engine_module
    from src.transfer.engine_automap import _auto_map
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setattr(
        engine_module,
        "_destination_schema_probe",
        lambda *_args, **_kwargs: ({}, False),
    )
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="mongodb",
            database="dataflow",
            table="events",
        ),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            database="dataflow",
            table="events_out",
        ),
        source_timezone="Asia/Kolkata",
        skip_preflight=True,
        sync_mode="full_refresh_overwrite",
    )
    mappings = _auto_map(
            request,
            ["id", "created"],
            {"id": "INTEGER", "created": "DATE"},
        sample_rows=[
            {"id": 1, "created": "2024-01-01T00:00:00Z"},
            {"id": 2, "created": "2024-06-01"},
        ],
    )
    created = next(mapping for mapping in mappings if mapping.get("source") == "created")
    assert created["transform"] == "assume_timezone:Asia/Kolkata"
    assert request.mappings == mappings
