"""GitOps plan/apply proofs for DatawrapManifest."""

from __future__ import annotations

import uuid
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
        "kind": "DatawrapManifest",
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


def test_apply_manifest_binds_workspace_and_refuses_foreign_update(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.gitops_manifest as gm
    import services.platform_config as pc
    import services.schedule_store as ss
    from importlib import reload

    reload(pc)
    reload(ss)
    reload(gm)

    foreign = ss.create_schedule(
        {
            "name": "theirs",
            "source_connector_id": "s1",
            "source_table": "orders",
            "dest_connector_id": "d1",
            "dest_table": "orders_copy",
            "interval": "daily",
            "workspace_id": "ws-b",
        }
    )
    result = gm.apply_manifest(
        {
            "kind": "PipelineSchedule",
            "spec": {
                "id": foreign.id,
                "name": "hijack",
                "source_connector_id": "s1",
                "source_table": "orders",
                "dest_connector_id": "d1",
                "dest_table": "orders_copy",
                "interval": "hourly",
                "workspace_id": "ws-a",
            },
        },
        workspace_id="ws-a",
    )
    assert result["failed"] == 1
    still = ss.get_schedule(foreign.id)
    assert still is not None
    assert still.workspace_id == "ws-b"
    assert still.name == "theirs"


def test_contract_artifact_shape(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from services.data_contract import DataContract
    from services.gitops_manifest import contract_artifact

    c = DataContract(name="orders-v1", source={"type": "postgresql"}, destination={"type": "snowflake"})
    art = contract_artifact(c)
    assert art["kind"] == "DataContract"
    assert art["spec"]["name"] == "orders-v1"
    assert "metadata" in art


def test_mapping_bundle_plan_and_apply(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.contract_store as cs
    import services.gitops_manifest as gm
    import services.platform_config as pc
    from services.data_contract import ContractStatus

    reload(pc)
    reload(cs)
    reload(gm)

    manifest = {
        "apiVersion": "dataflow.space/v1",
        "kind": "DatawrapManifest",
        "resources": [
            {
                "apiVersion": "dataflow.space/v1",
                "kind": "MappingBundle",
                "metadata": {"name": "orders-map"},
                "spec": {
                    "name": "orders-map",
                    "source": {"format": "postgresql"},
                    "destination": {"format": "snowflake"},
                    "mappings": [
                        {"source": "id", "target": "id", "target_type": "NUMBER"},
                        {"source": "email", "target": "email", "target_type": "VARCHAR"},
                    ],
                    "columns": [],
                },
            }
        ],
    }
    plan = gm.plan_manifest(manifest)
    assert plan["creates"] == 1
    assert plan["actions"][0]["kind"] == "MappingBundle"

    applied = gm.apply_manifest(manifest)
    assert applied["applied"] == 1
    store = cs.get_contract_store()
    rows = store.list_contracts() if hasattr(store, "list_contracts") else []
    match = next((c for c in rows if c.name == "orders-map"), None)
    assert match is not None
    assert match.status == ContractStatus.DRAFT
    assert len(match.mappings) == 2
    assert (match.metadata or {}).get("imported_from") == "MappingBundle"

    art = gm.mapping_bundle_artifact(match)
    assert art["kind"] == "MappingBundle"
    assert "honesty" in art


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

    # DATAFLOW_DATA_DIR only isolates the file fallback. The contract and
    # schedule stores prefer MongoDB when it is up, which every worker shares,
    # so fixed names collide with a parallel run or a previous one's leftovers.
    unique = uuid.uuid4().hex[:8]
    schedule_name = f"staging-gated-{unique}"

    store = cs.get_contract_store()
    draft = DataContract(
        name=f"staging-orders-{unique}",
        source={"type": "postgresql"},
        destination={"type": "snowflake"},
    )
    draft.status = ContractStatus.DRAFT
    store.save_contract(draft)

    manifest = {
        "apiVersion": "dataflow.space/v1",
        "kind": "DatawrapManifest",
        "resources": [
            {
                "apiVersion": "dataflow.space/v1",
                "kind": "PipelineSchedule",
                "metadata": {"name": schedule_name},
                "spec": {
                    "name": schedule_name,
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
    assert any(s.name == schedule_name for s in ss.list_schedules())


def test_gitops_export_round_trips_advanced_write_knobs(tmp_path, monkeypatch):
    """Studio Advanced must survive Export YAML → apply. Observed source shape must not."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.gitops_manifest as gm
    import services.platform_config as pc
    import services.schedule_store as ss

    reload(pc)
    reload(ss)
    reload(gm)

    unique = uuid.uuid4().hex[:8]
    name = f"adv-export-{unique}"
    live = ss.create_schedule({
        "name": name,
        "source_connector_id": "s1",
        "source_table": "orders",
        "dest_connector_id": "d1",
        "dest_table": "orders_copy",
        "interval": "hourly",
        "sync_mode": "cdc",
        "primary_key": "id",
        "write_via_staging": True,
        "priority_column": "updated_at",
        "priority_direction": "asc",
        "row_limit": 2500,
        "snapshot_mode": "when_needed",
        "schema_policy": "propagate_columns",
        "source_schema": {"id": "INTEGER", "joining_date": "TIMESTAMP_NTZ"},
        "source_schema_fingerprint": "fp-prod",
        "source_schema_observed_at": "2026-08-01T00:00:00+00:00",
        "source_primary_key": ["id"],
    })
    artifact = gm.schedule_artifact(live)
    spec = artifact["spec"]
    assert spec["write_via_staging"] is True
    assert spec["priority_column"] == "updated_at"
    assert spec["priority_direction"] == "asc"
    assert spec["row_limit"] == 2500
    assert spec["snapshot_mode"] == "when_needed"
    assert "source_schema" not in spec
    assert "source_schema_fingerprint" not in spec
    assert "source_primary_key" not in spec
    assert "fidelity_campaign" not in spec

    ss.delete_schedule(live.id)
    applied = gm.apply_manifest(artifact)
    assert applied["applied"] == 1
    cloned = next(s for s in ss.list_schedules() if s.name == name)
    assert cloned.write_via_staging is True
    assert cloned.priority_column == "updated_at"
    assert cloned.priority_direction == "asc"
    assert cloned.row_limit == 2500
    assert cloned.snapshot_mode == "when_needed"
    assert cloned.source_schema == {}
    assert cloned.source_schema_fingerprint == ""


def test_gitops_apply_strips_yaml_source_schema_so_drift_can_still_park(tmp_path, monkeypatch):
    """A pasted YAML must not stamp a remembered source shape over the live baseline."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.gitops_manifest as gm
    import services.platform_config as pc
    import services.schedule_store as ss

    reload(pc)
    reload(ss)
    reload(gm)

    unique = uuid.uuid4().hex[:8]
    existing = ss.create_schedule({
        "name": f"drift-guard-{unique}",
        "source_connector_id": "s1",
        "source_table": "orders",
        "dest_connector_id": "d1",
        "dest_table": "orders_copy",
        "interval": "daily",
        "source_schema": {"id": "INTEGER", "email": "VARCHAR"},
        "source_schema_fingerprint": "fp-live",
    })
    result = gm.apply_manifest({
        "kind": "PipelineSchedule",
        "spec": {
            "id": existing.id,
            "name": existing.name,
            "source_connector_id": "s1",
            "source_table": "orders",
            "dest_connector_id": "d1",
            "dest_table": "orders_copy",
            "interval": "hourly",
            "write_via_staging": True,
            "row_limit": 100,
            "source_schema": {"id": "INTEGER"},
            "source_schema_fingerprint": "fp-stale-yaml",
        },
    })
    assert result["applied"] == 1
    reloaded = ss.get_schedule(existing.id)
    assert reloaded is not None
    assert reloaded.interval == "hourly"
    assert reloaded.write_via_staging is True
    assert reloaded.row_limit == 100
    assert reloaded.source_schema == {"id": "INTEGER", "email": "VARCHAR"}
    assert reloaded.source_schema_fingerprint == "fp-live"
