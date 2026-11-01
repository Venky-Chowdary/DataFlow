"""QA r12 #4a: the GitOps export YAML carried the old brand.

QA (build 8f7d246d): the schedule export contained ``apiVersion:
dataflow.space/v1`` and ``generator: dataflow-gitops-export``.

Root cause: services/gitops_manifest.py wrote those literals for every
artifact (manifest, PipelineSchedule, DataContract, MappingBundle).

Contract, through the real routes on the live MongoDB stores and a live
PostgreSQL source (``r11_live_world``):

* the export (YAML and JSON, at the web app's ``/export/manifest`` and the
  older ``/export/dataflow`` path) names ``datawrap.io/v1`` and
  ``datawrap-gitops-export`` (``services.brand``), and its text holds neither
  "dataflow" nor "railway";
* the exported manifest plans and applies back (a second run updates, never
  duplicates);
* manifests written before the rename (``apiVersion: dataflow.space/v1``, kind
  ``DataFlowManifest`` from the old CLI) still plan and apply.
"""

from __future__ import annotations

import uuid

import pytest
import yaml

from r8_ent_harness import ent  # noqa: F401
from r11_live_world import drop_pg, live, pg_table  # noqa: F401

pytestmark = pytest.mark.timeout(300)

#: services.brand GITOPS_API_VERSION / GITOPS_GENERATOR / LEGACY_GITOPS_API_VERSIONS.
GITOPS_API_VERSION = "datawrap.io/v1"
GITOPS_GENERATOR = "datawrap-gitops-export"
LEGACY_API_VERSION = "dataflow.space/v1"
PARAMS = {"none": None, "empty": "", "blank": "   ", "label": "Zoë 名前 ✓"}


def _schedule(src: str, table: str, name: str) -> dict:
    return {
        "name": name,
        "source_connector_id": src,
        "source_table": table,
        "dest_connector_id": src,
        "dest_table": f"{table}_out",
        "interval": "hourly",
        "timezone": "UTC",
        "procedure_params": PARAMS,
        "mappings": [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
    }


def _contract(name: str, api_version: str) -> dict:
    return {
        "apiVersion": api_version,
        "kind": "DataContract",
        "metadata": {"name": name},
        "spec": {
            "name": name,
            "source": {"table": "orders"},
            "destination": {"table": "orders_copy"},
            "columns": [{"source_name": "id", "target_name": "id", "source_type": "int", "target_type": "int"}],
            "mappings": [{"source": "id", "target": "id"}],
        },
    }


def test_brand_module_owns_the_gitops_names():
    from services import brand

    assert brand.GITOPS_API_VERSION == GITOPS_API_VERSION
    assert brand.GITOPS_GENERATOR == GITOPS_GENERATOR
    assert LEGACY_API_VERSION in brand.LEGACY_GITOPS_API_VERSIONS


def test_export_names_datawrap_only_and_old_manifests_still_import(live):
    table = pg_table()
    try:
        src = live.pg_connector()
        name = f"r12 gitops ünï {uuid.uuid4().hex[:6]}"
        created = live.admin.post("/api/v1/schedules/", json=_schedule(src, table, name))
        assert created.status_code == 201, created.text
        sid = created.json()["id"]

        # A manifest exported before the rename: old apiVersion on every level.
        legacy = {
            "apiVersion": LEGACY_API_VERSION,
            "kind": "DatawrapManifest",
            "resources": [_contract(f"r12 legacy contract {uuid.uuid4().hex[:6]}", LEGACY_API_VERSION)],
        }
        plan = live.admin.post("/api/v1/schedules/gitops/plan", json=legacy)
        assert plan.status_code == 200, plan.text
        assert plan.json()["creates"] == 1, plan.json()
        applied = live.admin.post("/api/v1/schedules/gitops/apply", json=legacy)
        assert applied.status_code == 200, applied.text
        assert applied.json()["applied"] == 1, applied.json()

        for path in ("/api/v1/schedules/export/dataflow", "/api/v1/schedules/export/manifest"):
            res = live.admin.get(path)
            assert res.status_code == 200, (path, res.text[:300])
            text = res.text
            assert "dataflow" not in text.lower(), [ln for ln in text.splitlines() if "dataflow" in ln.lower()][:5]
            assert "railway" not in text.lower()
            assert 'filename=datawrap.yaml' in res.headers.get("content-disposition", "")
            doc = yaml.safe_load(text)
            assert doc["apiVersion"] == GITOPS_API_VERSION
            assert doc["kind"] == "DatawrapManifest"
            assert doc["metadata"]["generator"] == GITOPS_GENERATOR
            kinds = {r["kind"] for r in doc["resources"]}
            assert {"PipelineSchedule", "DataContract"} <= kinds, kinds
            assert {r["apiVersion"] for r in doc["resources"]} == {GITOPS_API_VERSION}
            spec = next(r["spec"] for r in doc["resources"] if r.get("metadata", {}).get("id") == sid)
            assert spec["name"] == name and spec["procedure_params"] == PARAMS

            as_json = live.admin.get(path, params={"format": "json"})
            assert as_json.status_code == 200, as_json.text
            assert "dataflow" not in as_json.text.lower()
            assert as_json.json()["apiVersion"] == GITOPS_API_VERSION

        single = live.admin.get(f"/api/v1/schedules/{sid}/export")
        assert single.status_code == 200, single.text
        assert "dataflow" not in single.text.lower()
        assert yaml.safe_load(single.text)["apiVersion"] == GITOPS_API_VERSION

        # The new export imports back; applying it twice updates, never duplicates.
        manifest = yaml.safe_load(live.admin.get("/api/v1/schedules/export/manifest").text)
        before = live.db["pipeline_schedules"].count_documents({})
        for _run in range(2):
            plan = live.admin.post("/api/v1/schedules/gitops/plan", json=manifest)
            assert plan.status_code == 200, plan.text
            assert plan.json()["creates"] == 0, plan.json()
            res = live.admin.post("/api/v1/schedules/gitops/apply", json=manifest)
            assert res.status_code == 200, res.text
            assert res.json()["failed"] == 0, res.json()
        assert live.db["pipeline_schedules"].count_documents({}) == before
        assert live.admin.get(f"/api/v1/schedules/{sid}").json()["procedure_params"] == PARAMS

        # The old CLI wrapped a bare list as kind DataFlowManifest; it imports too.
        cli = {
            "apiVersion": LEGACY_API_VERSION,
            "kind": "DataFlowManifest",
            "resources": [_contract(f"r12 cli contract {uuid.uuid4().hex[:6]}", LEGACY_API_VERSION)],
        }
        plan = live.admin.post("/api/v1/schedules/gitops/plan", json=cli)
        assert plan.status_code == 200, plan.text
        assert plan.json()["creates"] == 1, plan.json()
    finally:
        drop_pg(table)
