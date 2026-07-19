"""P0–P2 enterprise hardening: secrets, metrics, contracts, GitOps, reverse-ETL, composite keys."""

from __future__ import annotations

import pytest


def test_secret_vault_rejects_v0_in_production(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "test-production-key-for-unit-tests-only!!")
    from services.secret_vault import SecretVaultError, decrypt_secret, encrypt_secret

    enc = encrypt_secret("hello-world")
    assert enc.startswith("enc:v1:")
    assert decrypt_secret(enc) == "hello-world"

    with pytest.raises(SecretVaultError):
        decrypt_secret("enc:v0:aGVsbG8=")


def test_ops_metrics_prometheus_and_terminal_once():
    from services.ops_metrics import (
        prometheus_text,
        record_cdc_poll,
        record_terminal_job_transition,
        snapshot,
    )

    before = snapshot()["counters"]["dataflow_jobs_total"]
    record_terminal_job_transition(previous_status="running", status="completed", records=10, quarantined=0)
    record_terminal_job_transition(previous_status="completed", status="completed", records=10, quarantined=0)
    after = snapshot()["counters"]["dataflow_jobs_total"]
    assert after == before + 1

    record_cdc_poll(lag_seconds=1.5, used_query_fallback=True)
    text = prometheus_text()
    assert "dataflow_jobs_total" in text
    assert "dataflow_cdc_lag_seconds" in text


def test_contract_enforcer_require_signed():
    from services.data_contract import (
        ContractEnforcer,
        ContractStatus,
        ContractViolation,
        DataContract,
    )

    contract = DataContract(
        id="c1",
        name="t",
        source={"format": "csv"},
        destination={"format": "sqlite"},
        columns=[],
        status=ContractStatus.DRAFT,
    )

    class _Req:
        source = type("S", (), {"format": "csv"})()
        destination = type("D", (), {"format": "sqlite"})()

    with pytest.raises(ContractViolation):
        ContractEnforcer(contract).enforce(_Req(), require_signed=True)

    contract.status = ContractStatus.SIGNED
    ContractEnforcer(contract).enforce(_Req(), require_signed=True)


def test_composite_key_helpers_scd2_and_mirror():
    from services.mirror_engine import _compose_key as mirror_key
    from services.mirror_engine import _pk_or_clause as mirror_clause
    from services.scd2_engine import _compose_key as scd_key
    from services.scd2_engine import _pk_or_clause as scd_clause

    row = {"a": 1, "b": "x"}
    assert scd_key(row, ["a", "b"]) == mirror_key(row, ["a", "b"])
    clause, params = scd_clause(["a", "b"], {scd_key(row, ["a", "b"])}, prefix="k")
    assert "AND" in clause
    assert len(params) == 2
    clause2, params2 = mirror_clause(["a", "b"], [mirror_key(row, ["a", "b"])], prefix="a")
    assert "AND" in clause2
    assert len(params2) == 2


def test_gitops_manifest_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from importlib import reload

    import services.gitops_manifest as gm
    import services.platform_config as pc
    import services.schedule_store as ss

    reload(pc)
    reload(ss)
    reload(gm)

    ss.create_schedule(
        {
            "name": "nightly",
            "source_connector_id": "s1",
            "source_table": "t1",
            "dest_connector_id": "d1",
            "dest_table": "t2",
            "interval": "daily",
        }
    )
    manifest = gm.build_dataflow_manifest(include_contracts=False)
    assert manifest["kind"] == "DataFlowManifest"
    assert any(r["kind"] == "PipelineSchedule" for r in manifest["resources"])


def test_reverse_etl_plan_and_vector_kinds():
    from services.reverse_etl import plan_activation, supported_activation_kinds

    plan = plan_activation(
        destination_kind="pgvector",
        object_name="embeddings",
        primary_key="id",
        field_map={"text": "content"},
    )
    assert plan.mode == "upsert"
    assert plan.primary_key == ["id"]
    assert "pgvector" in supported_activation_kinds()
    assert any("vector" in n.lower() or "RAG" in n for n in plan.notes)


def test_quarantine_dlq_append(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from importlib import reload

    import services.platform_config as pc
    import services.quarantine_dlq as dlq

    reload(pc)
    reload(dlq)

    ev = dlq.append_dlq_event(job_id="j1", action="replay", rows=3, child_job_id="j2")
    assert ev["job_id"] == "j1"
    listed = dlq.list_dlq_events(job_id="j1")
    assert listed and listed[0]["action"] == "replay"
