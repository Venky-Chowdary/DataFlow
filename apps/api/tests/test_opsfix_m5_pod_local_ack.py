"""Ops review Medium 5: the chart must not acknowledge pod-local stores on the operator's behalf.

``transfer_plans`` and ``pilot_results`` have no shared backend yet. With
several API replicas on per-pod volumes (the chart's emptyDir default), a
plan saved on one pod is "not found" on the next. The chart shipped an
acknowledgement of that as its default, so nobody ever decided it. It now
refuses to render several API replicas on pod-local volumes unless the
operator either mounts one data volume shared by every pod or acknowledges
per-pod behaviour explicitly. Compose, where every service mounts the same
volume, keeps its acknowledgement.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests._helm_lite import ChartFail, render_chart

REPO = Path(__file__).resolve().parents[3]
CHART = REPO / "deploy/enterprise/helm/dataflow"
STORES = "transfer_plans,pilot_results"


def _api_env(overrides: dict) -> dict[str, str]:
    for obj in render_chart(CHART, overrides=overrides):
        if obj.get("kind") == "Deployment" and obj["metadata"]["labels"]["app.kubernetes.io/component"] == "api":
            return {e["name"]: e.get("value") for e in obj["spec"]["template"]["spec"]["containers"][0]["env"]}
    raise AssertionError("no API deployment rendered")


def test_chart_ships_no_acknowledgement():
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert not values["controlPlane"].get("podLocalStoresAck")


def test_several_api_replicas_on_pod_local_volumes_refuse_to_render():
    with pytest.raises(ChartFail, match="podLocalStoresAck") as refused:
        render_chart(CHART)
    assert "sharedAcrossPods" in str(refused.value)
    production = yaml.safe_load((CHART / "values-production.yaml").read_text(encoding="utf-8"))
    with pytest.raises(ChartFail, match="podLocalStoresAck"):
        render_chart(CHART, overrides=production)


def test_explicit_acknowledgement_is_passed_through():
    env = _api_env({"controlPlane": {"podLocalStoresAck": "transfer_plans"}})
    assert env["DATAFLOW_POD_LOCAL_STORES_ACK"] == "transfer_plans"


def test_a_data_volume_shared_by_every_pod_needs_no_acknowledgement():
    env = _api_env({"persistence": {"data": {"existingClaim": "dataflow-data-rwx", "sharedAcrossPods": True}}})
    assert env["DATAFLOW_POD_LOCAL_STORES_ACK"] == STORES
    with pytest.raises(ChartFail, match="existingClaim"):
        render_chart(CHART, overrides={"persistence": {"data": {"sharedAcrossPods": True}}})


def test_one_api_replica_keeps_one_copy_and_renders():
    env = _api_env({"api": {"replicas": 1}, "horizontalPodAutoscaler": {"enabled": False}})
    assert env["DATAFLOW_POD_LOCAL_STORES_ACK"] == STORES
    with pytest.raises(ChartFail, match="podLocalStoresAck"):
        render_chart(CHART, overrides={"api": {"replicas": 1}})  # the HPA can still add replicas


def test_compose_keeps_its_acknowledgement():
    compose = yaml.safe_load((REPO / "docker-compose.prod.yml").read_text(encoding="utf-8"))
    for name in ("api", "scheduler", "worker", "cdc-worker"):
        assert compose["services"][name]["environment"]["DATAWRAP_POD_LOCAL_STORES_ACK"] == STORES
