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
    from src.routers.transfer_router import CreatePlanRequest, EndpointDTO

    refuse_invalid_dest_writer_options({"formula_protection": "prefix", "tsv_null": "\\N"})
    good = {"kind": "file_export", "format": "tsv", "extra": {"formula_protection": "off", "tsv_null": "\\N"}}
    assert CreatePlanRequest(destination=good).destination == good
    assert EndpointDTO(**good).extra["tsv_null"] == "\\N"


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
    from src.routers.transfer_router import CreatePlanRequest, EndpointDTO, UpdatePlanRequest

    dest = {"kind": "file_export", "format": "tsv", "extra": bad}
    for model, kwargs in (
        (CreatePlanRequest, {"destination": dest}),
        (UpdatePlanRequest, {"destination": dest}),
        (_CreateFromTransferRequest, {"name": "c", "source": {}, "destination": dest}),
        (_ContractTestRequest, {"source": {}, "destination": dest}),
        (_FidelityCheckRequest, {"source": {}, "destination": dest}),
        (_PreviewBody, {"destination": dest}),
        (PreflightRequest, {"dest_extra": bad}),
        (EndpointDTO, {"kind": "file_export", "format": "tsv", **bad}),
    ):
        fields = {k: v for k, v in kwargs.items() if k in model.model_fields or model is EndpointDTO}
        with pytest.raises(ValidationError, match=next(iter(bad))):
            model.model_validate(fields)


def test_schedules_carry_no_destination_extras():
    from src.routers.schedules_router import ScheduleCreate, ScheduleUpdate

    for model in (ScheduleCreate, ScheduleUpdate):
        assert not {"extra", "dest_extra", "formula_protection", "tsv_null"} & set(model.model_fields)
        assert model.model_config.get("extra") in (None, "ignore")
