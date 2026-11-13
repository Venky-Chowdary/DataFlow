"""An unknown formula_protection / tsv_null is refused when the request is made (422).

Before: ``/transfer/run`` took writer options from ``dest_extra_json`` unchecked
(the writer refused them only when the job ran), and plan, contract, preflight,
shape and fidelity bodies took a destination dict with no check at all. One
shared validator (``services.dest_writer_options``) now guards every entry point
that accepts a destination or its extras. Schedules carry no destination extras
(their destination is a saved connector; unknown fields are dropped).
"""

from __future__ import annotations

import json

import pytest

BAD = [{"formula_protection": "strip"}, {"tsv_null": "NULL"}]


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient

    from src.main import app

    return TestClient(app)


def test_shared_validator_vocabulary():
    from services.dest_writer_options import validate_dest_writer_options

    validate_dest_writer_options({"formula_protection": "prefix", "extra": {"tsv_null": "\\N"}}, None, "x")
    validate_dest_writer_options({"extra": {"formula_protection": "OFF", "tsv_null": "empty"}})
    for bad in BAD:
        with pytest.raises(ValueError, match=next(iter(bad))):
            validate_dest_writer_options(bad)
        with pytest.raises(ValueError, match=next(iter(bad))):
            validate_dest_writer_options({"kind": "file_export", "extra": bad})


@pytest.mark.parametrize("bad", BAD)
def test_transfer_run_multipart_refuses_bad_dest_extra(client, bad):
    response = client.post(
        "/api/v1/transfer/run",
        data={
            "source_kind": "file",
            "dest_kind": "file_export",
            "dest_format": "tsv",
            "dest_extra_json": json.dumps(bad),
        },
        files={"file": ("a.csv", b"id\n1\n", "text/csv")},
    )
    assert response.status_code == 422, response.text
    assert next(iter(bad)) in response.text


def test_valid_options_pass_every_entry_point_model():
    """The check refuses only unknown values."""
    from services.dest_writer_options import refuse_invalid_dest_writer_options
    from src.routers.transfer_router import AnalyzeRequest, CreatePlanRequest

    refuse_invalid_dest_writer_options({"formula_protection": "prefix", "tsv_null": "\\N"})
    good = {"kind": "file_export", "format": "tsv", "extra": {"formula_protection": "off", "tsv_null": "\\N"}}
    assert CreatePlanRequest(destination=good).destination == good
    assert AnalyzeRequest(source={"kind": "file"}, destination=good).destination.extra["tsv_null"] == "\\N"


@pytest.mark.parametrize("bad", BAD)
@pytest.mark.parametrize(
    "path, body",
    [
        ("/api/v1/transfer/plans", lambda d: {"source": {"kind": "file"}, "destination": d}),
        ("/api/v1/contracts/from-transfer", lambda d: {"name": "c", "source": {"kind": "file"}, "destination": d}),
        ("/api/v1/contracts/test", lambda d: {"source": {"kind": "file"}, "destination": d}),
        ("/api/v1/transfer/analyze", lambda d: {"source": {"kind": "file"}, "destination": d}),
    ],
)
def test_json_entry_points_refuse_bad_destination_options(client, path, body, bad):
    for destination in ({"kind": "file_export", "format": "tsv", "extra": bad}, {"kind": "file_export", "format": "tsv", **bad}):
        response = client.post(path, json=body(destination))
        assert response.status_code == 422, (path, response.text)
        assert next(iter(bad)) in response.text, response.text


@pytest.mark.parametrize("bad", BAD)
def test_request_models_refuse_bad_destination_options(bad):
    from pydantic import ValidationError

    from src.routers.contracts_router import _ContractTestRequest, _CreateFromTransferRequest
    from src.routers.fidelity_router import _FidelityCheckRequest
    from src.routers.preflight_router import PreflightRequest
    from src.routers.shape_router import _PreviewBody
    from src.routers.transfer_router import AnalyzeRequest, CreatePlanRequest, UpdatePlanRequest

    dest = {"kind": "file_export", "format": "tsv", "extra": bad}
    for model, kwargs in (
        (CreatePlanRequest, {"destination": dest}),
        (UpdatePlanRequest, {"destination": dest}),
        (_CreateFromTransferRequest, {"name": "c", "source": {}, "destination": dest}),
        (_ContractTestRequest, {"source": {}, "destination": dest}),
        (_FidelityCheckRequest, {"source": {}, "destination": dest}),
        (_PreviewBody, {"destination": dest}),
        (PreflightRequest, {"dest_extra": bad}),
        (AnalyzeRequest, {"source": {"kind": "file"}, "destination": {"kind": "file_export", "format": "tsv", **bad}}),
    ):
        fields = {k: v for k, v in kwargs.items() if k in model.model_fields}
        with pytest.raises(ValidationError, match=next(iter(bad))):
            model.model_validate(fields)


def test_schedules_carry_no_destination_extras():
    from src.routers.schedules_router import ScheduleCreate, ScheduleUpdate

    for model in (ScheduleCreate, ScheduleUpdate):
        assert not {"extra", "dest_extra", "formula_protection", "tsv_null"} & set(model.model_fields)
        assert model.model_config.get("extra") in (None, "ignore")


# ── review L: \N on a non-TSV file, and saved connectors ────────────────────


@pytest.mark.parametrize(
    "dest",
    [
        {"kind": "file_export", "format": "csv", "extra": {"tsv_null": "\\N"}},
        {"kind": "file_export", "format": "json", "tsv_null": "\\N"},
        {"kind": "database", "format": "s3", "table": "out/data.csv", "extra": {"tsv_null": "\\N"}},
        {"kind": "database", "format": "sftp", "table": "x", "file_format": "jsonl", "extra": {"tsv_null": "\\N"}},
    ],
)
def test_backslash_n_on_a_non_tsv_file_is_refused_at_request_time(client, dest):
    from pydantic import ValidationError

    from services.dest_writer_options import validate_dest_writer_options
    from src.routers.transfer_router import AnalyzeRequest, CreatePlanRequest

    with pytest.raises(ValueError, match="TSV files only"):
        validate_dest_writer_options(dest)
    with pytest.raises(ValidationError, match="TSV files only"):
        CreatePlanRequest(destination=dest)
    with pytest.raises(ValidationError, match="TSV files only"):
        AnalyzeRequest(source={"kind": "file"}, destination=dest)
    for path, body in (
        ("/api/v1/transfer/plans", {"source": {"kind": "file"}, "destination": dest}),
        ("/api/v1/contracts/from-transfer", {"name": "c", "source": {"kind": "file"}, "destination": dest}),
    ):
        response = client.post(path, json=body)
        assert response.status_code == 422, (path, response.text)


def test_backslash_n_on_a_tsv_file_is_accepted():
    from services.dest_writer_options import validate_dest_writer_options

    validate_dest_writer_options(
        {"kind": "file_export", "format": "tsv", "extra": {"tsv_null": "\\N"}},
        {"kind": "database", "format": "s3", "table": "out/data.tsv", "tsv_null": "\\N"},
        {"kind": "database", "format": "postgresql", "table": "t", "extra": {"tsv_null": "\\N"}},  # no file: run time
        {"tsv_null": "\\N"},  # extra-only payload: the format is not known here
    )


def test_multipart_run_refuses_backslash_n_on_a_csv_file_export(client):
    response = client.post(
        "/api/v1/transfer/run",
        data={"source_kind": "file", "dest_kind": "file_export", "dest_format": "csv",
              "dest_extra_json": json.dumps({"tsv_null": "\\N"})},
        files={"file": ("a.csv", b"id\n1\n", "text/csv")},
    )
    assert response.status_code == 422, response.text
    assert "TSV files only" in response.text


@pytest.mark.parametrize("bad", BAD)
def test_saved_connector_create_and_patch_validate_writer_options(client, bad):
    from pydantic import ValidationError

    from src.routers.saved_connectors_router import ConnectorPatchDTO, ConnectorSaveDTO

    with pytest.raises(ValidationError, match=next(iter(bad))):
        ConnectorSaveDTO(name="s3 out", type="s3", host="s3.amazonaws.com", port=443, extra=bad)
    with pytest.raises(ValidationError, match=next(iter(bad))):
        ConnectorPatchDTO(extra=bad)
    response = client.post("/api/v1/connectors/saved", json={
        "name": "s3 out", "type": "s3", "host": "s3.amazonaws.com", "port": 443, "extra": bad,
    })
    assert response.status_code == 422, response.text
    ConnectorSaveDTO(name="s3 out", type="s3", host="s3.amazonaws.com", port=443, extra={"tsv_null": "\\N"})



# ── re-review: the 422 uses the writer's own format rule; sources are not checked ──


@pytest.mark.parametrize(
    "dest, ok",
    [
        # Object store: the key's extension wins; file_format only for an extensionless key.
        ({"kind": "database", "format": "s3", "table": "out/a.tsv", "file_format": "csv"}, True),
        ({"kind": "database", "format": "s3", "table": "out/a.csv", "file_format": "tsv"}, False),
        ({"kind": "database", "format": "gcs", "table": "out/folder", "file_format": "tsv"}, True),
        ({"kind": "database", "format": "adls", "table": "out/folder", "file_format": "csv"}, False),
        ({"kind": "database", "format": "s3", "table": "out/folder"}, False),  # the writer writes export.json
        ({"kind": "database", "format": "s3"}, True),  # no object name yet: decided at run time
        # SFTP: the name's extension, else file_format.
        ({"kind": "database", "format": "sftp", "table": "in/x", "file_format": "tsv"}, True),
        ({"kind": "database", "format": "sftp", "table": "in/x.csv", "file_format": "tsv"}, False),
        # Email: the attachment format (extra.format).
        ({"kind": "database", "format": "email", "extra_format": "tsv"}, True),
        ({"kind": "database", "format": "email", "extra_format": "csv"}, False),
        ({"kind": "database", "format": "email"}, True),
    ],
)
def test_backslash_n_format_follows_the_writer(dest, ok):
    from services.dest_writer_options import validate_dest_writer_options

    dest = dict(dest)
    extra = {"tsv_null": "\\N"}
    if "extra_format" in dest:
        extra["format"] = dest.pop("extra_format")
    payload = {**dest, "extra": extra}
    if ok:
        validate_dest_writer_options(payload)
    else:
        with pytest.raises(ValueError, match="TSV files only"):
            validate_dest_writer_options(payload)


def test_source_endpoints_are_not_validated_for_writer_options(client):
    from src.routers.transfer_router import AnalyzeRequest

    body = AnalyzeRequest(
        source={"kind": "file", "format": "csv", "extra": {"formula_protection": "strip", "tsv_null": "NULL"}},
        destination={"kind": "file_export", "format": "tsv"},
    )
    assert body.source.extra["tsv_null"] == "NULL"
    response = client.post("/api/v1/transfer/plans", json={
        "source": {"kind": "file", "extra": {"tsv_null": "NULL"}},
        "destination": {"kind": "file_export", "format": "tsv"},
    })
    assert response.status_code != 422, response.text
