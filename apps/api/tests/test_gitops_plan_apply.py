"""GitOps plan/apply proofs for DataFlowManifest."""

from __future__ import annotations

from importlib import reload


def test_gitops_plan_and_apply_schedule(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.gitops_manifest as gm
    import services.platform_config as pc
    import services.schedule_store as ss

    reload(pc)
    reload(ss)
    reload(gm)

    manifest = {
        "apiVersion": "dataflow.space/v1",
        "kind": "DataFlowManifest",
        "resources": [
            {
                "apiVersion": "dataflow.space/v1",
                "kind": "PipelineSchedule",
                "metadata": {"name": "gitops-nightly"},
                "spec": {
                    "name": "gitops-nightly",
                    "source_connector_id": "s1",
                    "source_table": "orders",
                    "dest_connector_id": "d1",
                    "dest_table": "orders_copy",
                    "interval": "daily",
                    "sync_mode": "incremental",
                },
            }
        ],
    }
    plan = gm.plan_manifest(manifest)
    assert plan["creates"] == 1
    assert plan["updates"] == 0

    applied = gm.apply_manifest(manifest)
    assert applied["applied"] == 1
    assert applied["failed"] == 0
    rows = ss.list_schedules()
    assert any(s.name == "gitops-nightly" for s in rows)

    # Second apply updates the same id.
    sid = next(s.id for s in rows if s.name == "gitops-nightly")
    manifest["resources"][0]["spec"]["id"] = sid
    manifest["resources"][0]["spec"]["interval"] = "hourly"
    plan2 = gm.plan_manifest(manifest)
    assert plan2["updates"] == 1
    applied2 = gm.apply_manifest(manifest)
    assert applied2["applied"] == 1
    updated = ss.get_schedule(sid)
    assert updated is not None
    assert updated.interval == "hourly"


def test_contract_artifact_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from services.data_contract import DataContract
    from services.gitops_manifest import contract_artifact

    c = DataContract(name="orders-v1", source={"type": "postgresql"}, destination={"type": "snowflake"})
    art = contract_artifact(c)
    assert art["kind"] == "DataContract"
    assert art["spec"]["name"] == "orders-v1"
    assert "metadata" in art


def test_apply_require_signed_contracts_blocks_unsigned(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.contract_store as cs
    import services.gitops_manifest as gm
    import services.platform_config as pc
    import services.schedule_store as ss
    from services.data_contract import ContractStatus, DataContract

    reload(pc)
    reload(ss)
    reload(cs)
    reload(gm)

    store = cs.get_contract_store()
    draft = DataContract(
        name="staging-orders",
        source={"type": "postgresql"},
        destination={"type": "snowflake"},
    )
    draft.status = ContractStatus.DRAFT
    store.save_contract(draft)

    manifest = {
        "apiVersion": "dataflow.space/v1",
        "kind": "DataFlowManifest",
        "resources": [
            {
                "apiVersion": "dataflow.space/v1",
                "kind": "PipelineSchedule",
                "metadata": {"name": "staging-gated"},
                "spec": {
                    "name": "staging-gated",
                    "source_connector_id": "s1",
                    "source_table": "orders",
                    "dest_connector_id": "d1",
                    "dest_table": "orders_copy",
                    "interval": "daily",
                    "sync_mode": "incremental",
                    "contract_id": draft.id,
                    "require_signed_contract": False,
                },
            }
        ],
    }
    blocked = gm.apply_manifest(manifest, require_signed_contracts=True)
    assert blocked["failed"] == 1
    assert blocked["require_signed_contracts"] is True
    assert "SIGNED" in (blocked["results"][0].get("error") or "")

    draft.status = ContractStatus.SIGNED
    store.save_contract(draft)
    ok = gm.apply_manifest(manifest, require_signed_contracts=True)
    assert ok["failed"] == 0
    assert ok["applied"] == 1
    assert any(s.name == "staging-gated" for s in ss.list_schedules())
