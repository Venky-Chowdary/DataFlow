"""Quarantine API and rejected-details persistence tests."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient


def _client():
    from src.main import app
    return TestClient(app)


def _build_age_rows(error_policy: str):
    from connectors.writer_common import build_mapped_rows_with_details

    return build_mapped_rows_with_details(
        headers=["id", "age"],
        data_rows=[["1", "30"], ["2", "not-a-number"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
        ],
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
        error_policy=error_policy,
        # Opt-in path only — job coerce_null is gated without staging/Risk Contract.
        allow_job_coerce_null=(error_policy == "coerce_null"),
    )


def test_quarantine_holds_bad_row_out_of_primary_and_surfaces_it():
    """``quarantine`` never writes a NULL in place of a bad cell on the primary.

    The whole row is held out, and it must still be fully recoverable: original
    value, reason, source row number, replay payload, and identity stamp.
    """
    mapped, errors, details = _build_age_rows("quarantine")

    assert mapped == [("1", 30)]
    assert len(details) == 1
    detail = details[0]
    assert detail["value"] == "not-a-number"
    assert "age" in (detail["column"], detail["target"])
    assert detail["row"] == 2
    assert detail["policy"] == "quarantine"
    # Replay must not need to re-read the source.
    assert detail["values"] == {"id": "2", "age": "not-a-number"}
    assert detail["source_values"] == {"id": "2", "age": "not-a-number"}
    # Identity stamp so replay can upsert the row back.
    assert detail["primary_key"] == ["id"]
    assert detail["pk_value"] == {"id": "2"}
    assert detail["source_pk"] == "2"
    assert any("not-a-number" in e for e in errors)


def test_coerce_null_keeps_row_with_null_cell_instead_of_holding_it_out():
    """``coerce_null`` is the opt-in that alters the primary row rather than drop it."""
    mapped, _errors, details = _build_age_rows("coerce_null")

    assert mapped == [("1", 30), ("2", None)]
    assert len(details) == 1
    assert details[0]["value"] == "not-a-number"
    assert details[0]["policy"] == "coerce_null"


def test_no_policy_silently_discards_a_bad_row():
    """Whatever the policy, a bad row is either written or surfaced — never lost."""
    for policy in ("quarantine", "coerce_null", "fail"):
        mapped, _errors, details = _build_age_rows(policy)
        rows_in = 2
        assert len(mapped) + len({d["row"] for d in details}) >= rows_in, (
            f"policy={policy} lost a row: mapped={mapped} details={details}"
        )


def test_job_quarantine_endpoint(monkeypatch):
    from services import connector_store
    from src.transfer import engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    monkeypatch.setattr(engine_mod, "_enforce_ddl_identity", lambda *a, **k: None)

    # Create a tiny CSV that fails integer coercion for one row.
    csv = b"id,age\n1,30\n2,not-a-number\n"
    dest_path = Path(_API_ROOT) / "exports" / "quarantine_test.db"
    try:
        connector_store.create_connector({
            "name": "Quarantine SQLite",
            "type": "sqlite",
            "role": "destination",
            "connection_string": f"sqlite:///{dest_path}",
            "workspace_id": "",
        })
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(kind="database", format="sqlite", table="users"),
            source_filename="users.csv",
            source_content=csv,
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
            mappings=[
                {"source": "id", "target": "id", "confidence": 0.95},
                {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
            ],
        )
        engine = UniversalTransferEngine()
        result = engine.execute(request)
        job_id = result.job_id
        assert result.success is True
        assert result.destination_summary.get("rejected_rows", 0) >= 1

        client = _client()
        resp = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
        if resp.status_code != 200:
            print("QUARANTINE RESP", resp.status_code, resp.text)
        assert resp.status_code == 200
        data = resp.json()
        assert data["rejected_rows"] >= 1
        assert any("not-a-number" in str(q.get("value", "")) for q in data["quarantine"])

        export_resp = client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/export")
        assert export_resp.status_code == 200
        export = export_resp.json()
        assert export["success"] is True
        assert export["row_count"] >= 1
        assert export["download_url"]
    finally:
        if dest_path.exists():
            dest_path.unlink()
