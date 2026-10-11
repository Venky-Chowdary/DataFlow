"""Rule workbook import router — bounded read, strict form-field JSON."""

from __future__ import annotations

import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.rule_compiler.ingest import MAX_FILE_BYTES
from src.routers.rule_compiler_router import router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _csv_file(payload: bytes = b"Source Column,Destination Column,Rule\nfname,first_name,Direct\n"):
    return {"file": ("rules.csv", payload, "text/csv")}


def test_oversize_upload_is_413_before_compile():
    client = _client()
    payload = b"x" * (MAX_FILE_BYTES + 1)
    response = client.post("/api/v1/transfer/rules/import", files=_csv_file(payload))
    assert response.status_code == 413
    assert "MB" in response.json()["detail"] or "limited" in response.json()["detail"]


def test_malformed_json_form_field_is_400_naming_the_field():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files=_csv_file(),
        data={"source_columns": "{not json"},
    )
    assert response.status_code == 400
    assert "source_columns" in response.json()["detail"]


def test_wrong_type_json_form_field_is_400():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files=_csv_file(),
        data={"source_catalog": "[\"not\", \"a\", \"dict\"]"},
    )
    assert response.status_code == 400
    assert "source_catalog" in response.json()["detail"]


def test_valid_and_empty_form_fields_still_compile():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files=_csv_file(),
        data={
            "source_columns": json.dumps(["fname"]),
            "dest_columns": json.dumps(["first_name"]),
            "source_types": json.dumps({"fname": "text"}),
            "source_catalog": "",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["buckets"]["executable"] == 1


def test_bad_extension_is_400():
    client = _client()
    response = client.post(
        "/api/v1/transfer/rules/import",
        files={"file": ("rules.exe", b"MZ", "application/octet-stream")},
    )
    assert response.status_code == 400
