"""Confirm builds the request the ack named, and a file ack cannot leave the upload tree."""

from __future__ import annotations

from pathlib import Path

import pytest

from services.confirmed_transfer import transfer_request_from_ack


def _dest() -> dict:
    return {"kind": "database", "connector_id": "dest-1", "format": "postgresql", "table": "pay"}


def test_a_database_ack_stays_a_database_and_cannot_skip_preflight() -> None:
    request = transfer_request_from_ack({
        "source": {"kind": "database", "connector_id": "src-1", "format": "postgresql", "table": "orders"},
        "destination": _dest(),
        "mappings": [{"source": "id", "target": "id"}],
        "skip_preflight": True,
        "sync_mode": "full_refresh_append",
    })
    assert request.source.kind == "database"
    assert request.source.connector_id == "src-1"
    assert request.destination.connector_id == "dest-1"
    assert request.skip_preflight is False
    assert request.triggered_by == "data-pilot"


def test_a_database_ack_without_a_source_connector_is_refused() -> None:
    with pytest.raises(ValueError, match="source connector"):
        transfer_request_from_ack({
            "source": {"kind": "database", "format": "postgresql", "table": "orders"},
            "destination": _dest(),
        })


def test_a_file_ack_keeps_the_upload_and_refuses_a_path_outside_it(tmp_path, monkeypatch) -> None:
    import services.dataset_file as dataset_file

    monkeypatch.setattr(dataset_file, "dataset_roots", lambda: [tmp_path])
    csv_path = tmp_path / "payments.csv"
    csv_path.write_text("id,amount\n1,2\n", encoding="utf-8")
    spill = tmp_path / "xfer_job_payments.csv"
    spill.write_text("id\n1\n", encoding="utf-8")

    request = transfer_request_from_ack({
        "source": {"kind": "file", "format": "csv", "table": "payments"},
        "destination": _dest(),
        "source_path": str(csv_path),
        "source_filename": "payments.csv",
        "mappings": [{"source": "id", "target": "id"}],
        "skip_preflight": True,
    })
    assert request.source.kind == "file"
    assert request.source.format == "csv"
    assert Path(request.source_path) == csv_path.resolve()
    assert request.skip_preflight is False
    assert request.destination.kind == "database"

    with pytest.raises(ValueError, match="outside the upload"):
        transfer_request_from_ack({
            "source": {"kind": "file", "format": "csv", "table": "shadow"},
            "destination": _dest(),
            "source_path": "/etc/passwd",
        })
    with pytest.raises(ValueError, match="one transfer"):
        transfer_request_from_ack({
            "source": {"kind": "file", "format": "csv", "table": "spill"},
            "destination": _dest(),
            "source_path": str(spill),
        })


def test_a_file_ack_without_a_file_is_refused() -> None:
    with pytest.raises(ValueError, match="uploaded file"):
        transfer_request_from_ack({
            "source": {"kind": "file", "format": "csv", "table": "payments"},
            "destination": _dest(),
        })
