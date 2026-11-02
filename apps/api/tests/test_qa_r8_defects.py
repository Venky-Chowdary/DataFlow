"""Round-8 QA regressions.

Plan and the writer share one transform registry. A schema-drift attestation
survives Confirm. Updating a connector that was just created does not re-decrypt
plaintext as Fernet. An empty counted table is not blocked for lack of a sample
row. S3 is a record batch, so one bad cast quarantines. The metadata address is
refused before a socket. API responses carry CSP, and HSTS when the request is
HTTPS. Unauthenticated routes are rate limited when that limiter is on.
"""

from __future__ import annotations

import time

from cryptography.fernet import Fernet

from services.confirmed_transfer import transfer_request_from_ack
from services.egress_guard import metadata_endpoint_refusal
from services.transform_engine import apply_transform, normalize_engine_transform
from services.transform_resolver import resolve_transform, write_plan_transform


def test_plan_and_runtime_share_one_transform_registry(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    aliases = {
        "cast_decimal": "decimal",
        "hash": "hash_pii",
        "replace:-:_": "replace:-:_",
        "concat:@masked": "concat:@masked",
        "default:missing": "default:missing",
    }
    for raw, canon in aliases.items():
        assert normalize_engine_transform(raw) == canon
        mapping = {
            "source": "name",
            "target": "name",
            "transform": raw,
            "target_type": "VARCHAR",
        }
        assert resolve_transform(mapping) == canon
        assert write_plan_transform(mapping) == canon

    assert apply_transform("a-b", "replace:-:_") == ("a_b", None)
    assert apply_transform("user", "concat:@masked") == ("user@masked", None)
    assert apply_transform("", "default:missing") == ("missing", None)
    assert apply_transform("1.5", "cast_decimal")[0] == apply_transform("1.5", "decimal")[0]
    hashed, herr = apply_transform("ada@example.com", "hash")
    assert herr is None
    assert hashed == apply_transform("ada@example.com", "hash_pii")[0]
    # Explicit SQL NULL is empty for default:, and must not land as sentinel text.
    assert apply_transform("__df_sql_null__", "default:missing") == ("missing", None)
    assert apply_transform("__df_sql_null__", "concat:@masked") == (None, None)
    assert apply_transform("__DF_MISSING__", "replace:-:_") == ("__DF_MISSING__", None)
    assert normalize_engine_transform("to_integer") == "integer"
    _zvalue, zerr = apply_transform("2024-01-15 12:00:00", "assume_timezone:")
    assert zerr and "needs a zone" in zerr
    direct = {
        "source": "id",
        "target": "id",
        "transform": "direct",
        "target_type": "INTEGER",
    }
    resolved = resolve_transform(direct, column_types={"id": "INTEGER"})
    assert resolved != "direct"
    assert "Unknown" not in (apply_transform("7", resolved)[1] or "")

    unknown = {"source": "name", "target": "name", "transform": "not_a_transform", "target_type": "VARCHAR"}
    assert resolve_transform(unknown) == "not_a_transform"
    assert write_plan_transform(unknown) == "not_a_transform"
    _value, err = apply_transform("hello", "not_a_transform")
    assert err and "Unknown transform" in err


def test_unknown_transform_blocks_preflight_instead_of_passing_as_identity():
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["name"],
        column_types={"name": "VARCHAR"},
        row_count=1,
        mappings=[
            {
                "source": "name",
                "target": "name",
                "confidence": 0.99,
                "transform": "not_a_transform",
                "target_type": "VARCHAR",
            }
        ],
        destination_connected=True,
        sample_rows=[{"name": "ada"}],
        destination_column_types={"name": "VARCHAR"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        skip_population_fit=True,
    )
    blob = str(result.get("blockers") or "") + str(result.get("gates") or "")
    assert "Unknown transform" in blob


def test_schema_drift_ack_survives_confirm_onto_job_governance():
    from src.ai.copilot.job_reads import job_governance
    from src.transfer.models import transfer_request_to_dict

    request = transfer_request_from_ack({
        "source": {
            "kind": "database",
            "connector_id": "src-1",
            "format": "postgresql",
            "table": "orders",
        },
        "destination": {
            "kind": "database",
            "connector_id": "dest-1",
            "format": "postgresql",
            "table": "orders",
        },
        "mappings": [{"source": "id", "target": "id"}],
        "schema_drift_acknowledged": True,
        "acknowledgment_actor": "qa",
        "acknowledgment_reason": "accepted the new column",
    })
    assert request.schema_drift_acknowledged is True
    gov = job_governance({"transfer_request": transfer_request_to_dict(request)})
    assert gov["acknowledgments"]["schema_drift"] is True
    assert gov["acknowledgments"]["actor"] == "qa"


def test_update_of_a_fresh_connector_does_not_redecrypt_plaintext(monkeypatch, tmp_path):
    import services.connector_store as cs
    import services.platform_config as platform_config

    monkeypatch.setattr(platform_config, "is_production", lambda: True)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", Fernet.generate_key().decode())
    monkeypatch.setattr(cs, "_backend_choice", "file")
    monkeypatch.setattr(cs, "_store_path", lambda: tmp_path / "connectors.json")

    created = cs.create_connector({
        "name": "pg",
        "type": "postgresql",
        "host": "db1",
        "password": "s3cret",
    })
    updated = cs.update_connector(created.id, {"host": "db2"})
    assert updated is not None
    assert updated.host == "db2"
    assert updated.password == "s3cret"
    loaded = cs.get_connector(created.id)
    assert loaded is not None
    assert loaded.password == "s3cret"


def test_counted_empty_table_is_not_blocked_for_a_missing_sample():
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=0,
        mappings=[{"source": "id", "target": "id", "confidence": 1.0, "target_type": "INTEGER"}],
        sample_rows=[],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="postgresql",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        destination_table="empty_copy",
        source_population_counted=True,
        skip_population_fit=True,
    )
    g8 = next(g for g in result["gates"] if g["id"] == "g8_reconciliation")
    assert g8["status"] == "pass", g8
    assert g8["details"]["source_measured_empty"] is True
    assert "without sample" not in g8["message"]


def test_s3_to_s3_mixed_cast_is_a_holdout_not_a_block():
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["amt"],
        column_types={"amt": "INTEGER"},
        row_count=2,
        mappings=[
            {
                "source": "amt",
                "target": "amt",
                "confidence": 0.99,
                "transform": "integer",
                "target_type": "INTEGER",
            }
        ],
        destination_connected=True,
        sample_rows=[{"amt": "not-a-number"}, {"amt": "10"}],
        destination_column_types={"amt": "INTEGER"},
        destination_table_exists=True,
        destination_db_type="s3",
        source_kind="database",
        source_format="s3",
        source_config={"type": "s3"},
        skip_population_fit=True,
    )
    decision = (result.get("proof_bundle") or {}).get("transfer_decision") or {}
    assert decision.get("decision") != "block", decision
    kinds = [
        str((b.get("details") or {}).get("kind") or "")
        for b in result.get("blockers") or []
    ]
    assert "sample_transform" not in kinds


def test_metadata_address_is_refused_before_a_socket():
    assert metadata_endpoint_refusal("169.254.169.254")
    assert metadata_endpoint_refusal("http://169.254.169.254/latest/meta-data")
    assert metadata_endpoint_refusal("host=169.254.169.254;port=80")
    assert metadata_endpoint_refusal("::ffff:169.254.169.254")
    assert metadata_endpoint_refusal("10.0.0.8")
    assert "10.x" in metadata_endpoint_refusal("10.0.0.8")
    assert metadata_endpoint_refusal("host=10.0.0.8;port=5432")
    assert "192.168/16" in metadata_endpoint_refusal("192.168.1.10")
    assert "172.16/12" in metadata_endpoint_refusal("172.16.0.4")
    assert metadata_endpoint_refusal("db.internal.example") == ""

    from src.transfer.connector_registry import run_probe

    started = time.monotonic()
    ok, message = run_probe(
        "postgresql",
        {
            "host": "169.254.169.254",
            "port": 5432,
            "database": "meta",
            "username": "u",
            "password": "p",
        },
    )
    assert time.monotonic() - started < 2
    assert ok is False
    assert "Refused" in message
    assert "169.254.169.254" in message


def test_security_headers_and_unauthenticated_rate_limit(monkeypatch):
    from fastapi.testclient import TestClient

    from services.auth_rate_limit import reset_auth_rate_limits
    from src.main import app

    monkeypatch.setenv("DATAFLOW_PUBLIC_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_PUBLIC_RATE_BURST", "1")
    monkeypatch.setenv("DATAFLOW_PUBLIC_RATE_QPS", "0.01")
    reset_auth_rate_limits()
    with TestClient(app) as client:
        health = client.get("/health", headers={"X-Forwarded-Proto": "https"})
        assert health.status_code == 200
        assert "default-src 'none'" in health.headers["content-security-policy"]
        assert "max-age=31536000" in health.headers["strict-transport-security"]
        plain = client.get("/health")
        assert "strict-transport-security" not in plain.headers
        first = client.get("/api/v1/auth/sso/providers")
        second = client.get("/api/v1/auth/sso/providers")
    assert first.status_code != 429
    assert second.status_code == 429
    reset_auth_rate_limits()
