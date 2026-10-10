from __future__ import annotations

import importlib

import pytest

from services import connector_form_schema
from services import connector_truth_audit as truth_audit
from services.connector_form_schema import AuthMode, ConnectorForm
from services.document_chunking import DOCUMENT_TYPES, extract_document_chunks
from src.transfer.adapters import write_destination_file
from src.transfer.connector_capabilities import _FILE_CAPS
from src.transfer.file_stream import STREAMABLE_TYPES, iter_source_rows
from src.transfer.models import EndpointConfig


_OWNED_ELSEWHERE = {
    ("form_secret_field", "sftp"): "SFTP form owned by defect group D",
}


def test_file_driver_roles_follow_file_capabilities() -> None:
    truth_audit.driver_role_reality.cache_clear()

    for driver, caps in _FILE_CAPS.items():
        reader, writer = truth_audit.driver_role_reality(driver)
        if caps.get("file_source"):
            assert reader["kind"] == "real", driver
        else:
            assert reader == {
                "exists": False,
                "kind": "absent",
                "detail": "file caps declare no file_source",
            }
        if caps.get("file_export"):
            assert writer["kind"] == "real", driver
        else:
            assert writer == {
                "exists": False,
                "kind": "absent",
                "detail": "file caps declare no file_export",
            }


@pytest.mark.parametrize(
    "file_format",
    sorted(fmt for fmt, caps in _FILE_CAPS.items() if caps.get("file_export")),
)
def test_declared_file_exports_run_and_streamable_formats_round_trip(
    file_format: str,
) -> None:
    records = [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}]
    extra = {}
    if file_format == "fixed_width":
        extra = {"fixed_width_layout": [["id", 8], ["label", 12]]}
    endpoint = EndpointConfig(
        kind="file_export",
        format=file_format,
        table="sample",
        extra=extra,
    )

    content, filename, _summary = write_destination_file(
        endpoint,
        records,
        ["id", "label"],
        source_format="postgresql",
        column_types={"id": "INTEGER", "label": "TEXT"},
    )

    assert content, file_format
    if file_format in STREAMABLE_TYPES and _FILE_CAPS[file_format].get("file_source"):
        round_trip = list(iter_source_rows(content, filename, batch_size=1))
        assert len(round_trip) == 2, file_format
        assert [str(row["id"]) for row in round_trip] == ["1", "2"], file_format
        assert [row["label"] for row in round_trip] == ["alpha", "beta"], file_format


def test_read_only_document_formats_have_document_extraction_paths() -> None:
    assert {"pdf", "docx", "html"} <= DOCUMENT_TYPES
    html_rows = extract_document_chunks(
        b"<html><body><h1>Contacts</h1><p>Alpha and Beta</p></body></html>",
        "sample.html",
    )
    assert html_rows
    assert any("Alpha and Beta" in row["content"] for row in html_rows)


def test_explicit_probe_branch_matches_callable() -> None:
    module, fn_name = truth_audit.EXPLICIT_PROBE_BRANCHES["mongodb"]
    assert truth_audit.callable_reality(module, fn_name)["kind"] == "real"


def test_catalog_advertises_only_implemented_capabilities() -> None:
    violations = truth_audit.audit_catalog_truth()

    assert {(v.rule, v.connector_id) for v in violations} == set(_OWNED_ELSEWHERE)


def test_planted_form_auth_mode_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        connector_form_schema,
        "_load",
        lambda: {
            "postgresql": ConnectorForm(
                type="postgresql",
                label="PostgreSQL",
                default_auth_mode="oauth_magic",
                auth_modes=(AuthMode(value="oauth_magic", label="Magic OAuth"),),
            )
        },
    )

    violations = truth_audit.audit_catalog_truth()

    assert ("form_auth_mode", "postgresql") in {
        (v.rule, v.connector_id) for v in violations
    }


def test_planted_write_mode_claim_without_writer_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services import connector_capability_registry
    from src.transfer import connector_capabilities

    original_resolver = connector_capabilities.resolve_driver_type
    monkeypatch.setitem(
        connector_capability_registry.CAPABILITY_REGISTRY,
        "truth_audit_fake_writer",
        {
            "transfer_ready": False,
            "supports_upsert": True,
            "supports_append": False,
            "supports_overwrite": False,
            "supports_merge": False,
        },
    )
    monkeypatch.setattr(
        connector_capabilities,
        "resolve_driver_type",
        lambda connector_id: (
            "truth_audit_no_writer"
            if connector_id == "truth_audit_fake_writer"
            else original_resolver(connector_id)
        ),
    )
    truth_audit.driver_role_reality.cache_clear()

    violations = truth_audit.audit_catalog_truth()

    assert ("write_modes", "truth_audit_fake_writer") in {
        (v.rule, v.connector_id) for v in violations
    }


@pytest.mark.parametrize("connector_id", ["stripe", "cassandra", "dremio"])
def test_unimplemented_write_modes_are_downgraded(connector_id: str) -> None:
    from services.connector_capability_registry import get_connector_capability

    capability = get_connector_capability(connector_id)
    write_fields = (
        "supports_upsert",
        "supports_append",
        "supports_overwrite",
        "supports_merge",
    )

    assert not any(capability.get(field) for field in write_fields)
    downgrades = capability.get("capability_downgrades")
    assert downgrades
    assert all(
        downgrade.get("reason", "").strip()
        for downgrade in downgrades
    )


@pytest.mark.parametrize("kind", ["absent", "missing_function", "refusal", "stub"])
def test_write_mode_downgrade_reason_for_unusable_writer(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    monkeypatch.setattr(
        truth_audit,
        "driver_role_reality",
        lambda _driver: ({}, {"kind": kind, "detail": "writer evidence"}),
    )

    assert truth_audit.write_mode_downgrade_reason(
        "connector", "driver", {"write": True}
    ) == "No destination writer for driver: writer evidence"


def test_write_mode_downgrade_reason_for_driver_without_write_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        truth_audit,
        "driver_role_reality",
        lambda _driver: ({}, {"kind": "real", "detail": "1 source line"}),
    )

    assert truth_audit.write_mode_downgrade_reason(
        "connector", "driver", {"write": False}
    ) == "Driver capability table declares no write for driver"


def test_write_mode_downgrade_reason_for_uncertified_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        truth_audit,
        "driver_role_reality",
        lambda _driver: ({}, {"kind": "real", "detail": "1 source line"}),
    )

    assert truth_audit.write_mode_downgrade_reason(
        "connector", "driver", {"write": True, "certified_dest": False}
    ) == (
        "Destination not certified: writer code exists but no production "
        "destination COUNT proof (certified_dest=False)"
    )


def test_write_mode_downgrade_reason_treats_import_error_as_environment_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        truth_audit,
        "driver_role_reality",
        lambda _driver: ({}, {"kind": "import_error", "detail": "ImportError"}),
    )

    assert truth_audit.write_mode_downgrade_reason(
        "connector", "driver", {"write": True}
    ) is None


def test_write_mode_downgrade_reason_accepts_real_certified_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        truth_audit,
        "driver_role_reality",
        lambda _driver: ({}, {"kind": "real", "detail": "1 source line"}),
    )

    assert truth_audit.write_mode_downgrade_reason(
        "connector", "driver", {"write": True, "certified_dest": True}
    ) is None


def test_readiness_audit_script_keeps_its_legacy_import_names() -> None:
    script = importlib.import_module("scripts.connector_readiness_audit")

    assert script._NOT_SUPPORTED_FN is truth_audit._NOT_SUPPORTED_FN
    assert script._NON_REGISTRY_PATHS is truth_audit.NON_REGISTRY_PATHS
    assert script._FILE_FORMAT_PATH is truth_audit.FILE_FORMAT_PATH
    assert script._FILE_FORMATS is truth_audit.FILE_FORMATS
    assert script._callable_reality is truth_audit.callable_reality
    importlib.import_module("scripts.connector_readiness_matrix_doc")
