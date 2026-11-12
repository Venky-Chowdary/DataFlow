"""Security follow-up 3: a transform's quarantine write is residency-gated like every other write.

``services.transform_runner`` held violating rows out of a mart by writing them
to the control-plane DLQ and then to a ``<model>_df_quarantine`` table in the
transform's warehouse (``dest_quarantine.write_dest_quarantine``). Nothing asked
the residency gate, so a fenced workspace's transform against an out-of-region
warehouse wrote its rejected rows there, while a transfer to the same place was
refused. The hold-out now asks ``data_residency.residency_refusal`` for the
warehouse first; refused, nothing is written and the mart is left intact.
"""

from __future__ import annotations

import pytest

from services.transform_models import DataTest, TransformModel
from services.transform_runner import ModelRunResult, TransformRunner

WS = "ws-secfu3-fenced"
_EU = "x.eu-west-1.rds.amazonaws.com"
_US = "x.us-east-1.rds.amazonaws.com"


@pytest.fixture
def fenced(tmp_path, monkeypatch):
    from services import tenant_store

    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    tenant_store.create_tenant(workspace_id=WS, name="EU", data_region="eu-west-1")

    writes: dict[str, list] = {"dlq": [], "dest": []}

    def fake_persist(**kwargs):
        writes["dlq"].append(kwargs)

    def fake_dest(endpoint, details, **_kwargs):
        writes["dest"].append(str(getattr(endpoint, "host", "")))
        return {"ok": False, "error": "stopped by the test before the DELETE"}

    monkeypatch.setattr("services.quarantine_dlq.persist_rejected_rows", fake_persist)
    monkeypatch.setattr("services.dest_quarantine.write_dest_quarantine", fake_dest)
    return writes


class _NoEngine:
    def connect(self):  # pragma: no cover - reaching it is the failure
        raise AssertionError("the mart was touched")


def _hold_out(host: str, workspace_id: str = WS) -> ModelRunResult:
    runner = TransformRunner(
        {"type": "postgresql", "host": host, "port": 5432, "database": "d", "username": "u", "password": "p"},
        dialect="postgresql",
        project_id="secfu3",
        workspace_id=workspace_id,
    )
    model = TransformModel(name="stg_orders", sql="select 1", materialization="table")
    test = DataTest(test_type="not_null", column="email")
    run = ModelRunResult(name=model.name, materialization="table")
    violators = [{"test": test, "entry": {"message": "email is null"}, "rows": [{"id": 2, "email": None}]}]
    runner._apply_transform_holdout(_NoEngine(), model, run, violators)
    return run


def test_out_of_region_warehouse_is_refused_before_any_quarantine_write(fenced):
    run = _hold_out(_US)
    assert fenced["dest"] == [], "rejected rows were written to an out-of-region warehouse"
    assert fenced["dlq"] == []
    assert "residency" in run.hold_out_warning.lower()
    assert "us-east-1" in run.hold_out_warning
    assert run.rows_held_out == 0 and not run.hold_out_persisted


def test_in_region_warehouse_still_writes(fenced):
    run = _hold_out(_EU)
    assert fenced["dest"] == [_EU]
    assert len(fenced["dlq"]) == 1
    assert "residency" not in run.hold_out_warning.lower()


def test_an_unfenced_workspace_writes_anywhere(fenced):
    _hold_out(_US, workspace_id="ws-secfu3-unfenced")
    assert fenced["dest"] == [_US]
