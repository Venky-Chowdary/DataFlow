"""SEC-13 (GitOps part): a manifest missing a field says which, not a bare KeyError."""

from __future__ import annotations


def test_a_missing_field_is_named(monkeypatch):
    from services import gitops_manifest, schedule_store

    def boom(spec, **kwargs):
        raise KeyError("source_connector_id")

    monkeypatch.setattr(schedule_store, "create_schedule", boom)
    out = gitops_manifest.apply_manifest(
        {"kind": "PipelineSchedule", "spec": {"name": "w1sec-missing", "interval": "daily"}})
    error = out["results"][0]["error"]
    assert error == "The PipelineSchedule is missing the field source_connector_id.", error
