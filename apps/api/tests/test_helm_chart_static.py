"""Audit H-6: the enterprise Helm chart must be able to run as shipped.

Static checks on the rendered chart. They need no cluster and no ``helm``
binary (``tests/_helm_lite.py`` renders the chart); when ``helm`` is on PATH
the same invariants also run against ``helm template`` output.

This proves the manifests are internally consistent. It does not prove a
deployment works: nothing here talks to Kubernetes.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests._helm_lite import ChartFail, render_chart

REPO = Path(__file__).resolve().parents[3]
# DATAFLOW_TEST_CHART_DIR renders another checkout's chart (used to show these
# checks failing on the chart as it was before the fix).
CHART = Path(os.environ.get("DATAFLOW_TEST_CHART_DIR") or REPO / "deploy/enterprise/helm/dataflow")
NGINX_CONF = REPO / "deploy/nginx.conf"

PYTHON_COMPONENTS = ("api", "worker", "cdc-worker", "scheduler")
#: Paths each kind of container writes at runtime.
WRITABLE = {
    "python": ("/data", "/tmp"),
    "web": ("/var/cache/nginx", "/var/run", "/tmp"),
}


def _production_overrides() -> dict[str, Any]:
    return yaml.safe_load((CHART / "values-production.yaml").read_text(encoding="utf-8")) or {}


def _lite(overrides: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return render_chart(CHART, overrides=overrides)


@pytest.fixture(params=["defaults", "production", "monitoring"])
def objects(request) -> list[dict[str, Any]]:
    if request.param == "defaults":
        return _lite()
    if request.param == "production":
        return _lite(_production_overrides())
    return _lite({"serviceMonitor": {"enabled": True}})


def _kind(objects: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [o for o in objects if o.get("kind") == kind]


def _component(obj: dict[str, Any]) -> str:
    return obj["metadata"]["labels"]["app.kubernetes.io/component"]


def _deployments(objects: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_component(d): d for d in _kind(objects, "Deployment")}


def _container(deployment: dict[str, Any]) -> dict[str, Any]:
    containers = deployment["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1
    return containers[0]


def _env(container: dict[str, Any]) -> dict[str, Any]:
    return {e["name"]: e.get("value", e.get("valueFrom")) for e in container.get("env") or []}


# ---------------------------------------------------------------------------
# Invariants (shared by the built-in renderer and `helm template`)
# ---------------------------------------------------------------------------


def check_every_workload_is_rendered(objects: list[dict[str, Any]]) -> None:
    assert set(_deployments(objects)) == {"api", "web", "worker", "cdc-worker", "scheduler"}


def check_read_only_root_has_writable_volumes(objects: list[dict[str, Any]]) -> None:
    for component, deployment in _deployments(objects).items():
        pod = deployment["spec"]["template"]["spec"]
        container = _container(deployment)
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert pod["securityContext"]["runAsNonRoot"] is True
        volumes = {v["name"]: v for v in pod.get("volumes") or []}
        mounts = container.get("volumeMounts") or []
        for mount in mounts:
            assert mount["name"] in volumes, f"{component}: volumeMount {mount['name']} has no volume"
        assert {m["name"] for m in mounts} == set(volumes), f"{component}: a volume is defined but never mounted"
        writable = {m["mountPath"] for m in mounts if not m.get("readOnly")}
        needed = WRITABLE["web" if component == "web" else "python"]
        for path in needed:
            assert path in writable, f"{component}: read-only root filesystem and no writable volume at {path}"
        if component in PYTHON_COMPONENTS:
            env = _env(container)
            for name in (
                "DATAFLOW_DATA_DIR", "DATAFLOW_UPLOAD_DIR", "DATAFLOW_VECTOR_STORE_DIR", "DATAFLOW_EXPORTS_DIR", "HOME",
            ):
                value = str(env.get(name) or "")
                assert any(value == p or value.startswith(p + "/") for p in writable), (
                    f"{component}: {name}={value!r} is not on a writable volume"
                )


def check_ports_agree(objects: list[dict[str, Any]]) -> None:
    deployments = _deployments(objects)
    services = {_component(s): s for s in _kind(objects, "Service")}
    config = next(c for c in _kind(objects, "ConfigMap") if _component(c) == "web")["data"]["default.conf"]

    def ports(component: str) -> dict[str, int]:
        return {p["name"]: p["containerPort"] for p in _container(deployments[component]).get("ports") or []}

    listen = int(re.search(r"^\s*listen\s+(\d+);", config, re.M).group(1))
    assert ports("web")["http"] == listen, "web containerPort and nginx listen port differ"
    assert listen > 1024, "nginx cannot bind a privileged port as a non-root user"

    api_service = services["api"]
    api_port = api_service["spec"]["ports"][0]["port"]
    upstreams = set(re.findall(r"proxy_pass\s+http://([^/;]+)", config))
    assert upstreams == {f"{api_service['metadata']['name']}:{api_port}"}, upstreams

    for component, service in services.items():
        for port in service["spec"]["ports"]:
            assert port["targetPort"] in ports(component), f"{component} Service targets a port the pod does not name"
    assert _env(_container(deployments["api"]))["PORT"] == str(ports("api")["http"]), "uvicorn PORT != containerPort"

    for component, deployment in deployments.items():
        container = _container(deployment)
        for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
            if probe in container:
                assert container[probe]["httpGet"]["port"] in ports(component), f"{component} {probe} port"
    for component in ("worker", "cdc-worker", "scheduler"):
        env = _env(_container(deployments[component]))
        assert env["DATAFLOW_METRICS_PORT"] == str(ports(component)["metrics"])

    policy = _kind(objects, "NetworkPolicy")[0]
    allowed = {p["port"] for rule in policy["spec"]["ingress"] for p in rule["ports"]}
    for component in deployments:
        for name, number in ports(component).items():
            assert number in allowed, f"NetworkPolicy blocks {component} port {name} ({number})"

    for rule in _kind(objects, "Ingress")[0]["spec"]["rules"]:
        backend = rule["http"]["paths"][0]["backend"]["service"]
        target = next(s for s in services.values() if s["metadata"]["name"] == backend["name"])
        assert backend["port"]["number"] in {p["port"] for p in target["spec"]["ports"]}


def check_probes_and_grace_period(objects: list[dict[str, Any]]) -> None:
    from services.worker_fleet import worker_drain_seconds
    from src.worker_main import _HARD_EXIT_MARGIN

    for component, deployment in _deployments(objects).items():
        container = _container(deployment)
        assert "livenessProbe" in container, f"{component} has no liveness probe"
        assert "readinessProbe" in container, f"{component} has no readiness probe"
        pod = deployment["spec"]["template"]["spec"]
        if component == "web":
            continue
        grace = pod.get("terminationGracePeriodSeconds")
        assert isinstance(grace, int), f"{component} has no terminationGracePeriodSeconds"
        if component in ("worker", "cdc-worker"):
            drain = float(_env(container)["DATAFLOW_WORKER_DRAIN_SECONDS"])
            assert grace >= drain + _HARD_EXIT_MARGIN + 5, (
                f"{component}: SIGKILL after {grace}s would cut a {drain:.0f}s drain short"
            )
        elif component == "api":
            pre_stop = int(container["lifecycle"]["preStop"]["exec"]["command"][1])
            assert grace >= pre_stop + worker_drain_seconds() + 5


def check_secrets_are_always_wired(objects: list[dict[str, Any]]) -> None:
    for component in PYTHON_COMPONENTS:
        container = _container(_deployments(objects)[component])
        secret_refs = [ref["secretRef"]["name"] for ref in container.get("envFrom") or []]
        assert len(secret_refs) == 1 and secret_refs[0], f"{component} reads no Secret"
        defined: set[str] = set()
        for entry in container["env"]:
            for ref in re.findall(r"\$\(([A-Z0-9_]+)\)", str(entry.get("value") or "")):
                assert ref in defined, f"{component}: {entry['name']} uses $({ref}) before it is defined"
            if "valueFrom" in entry:
                assert entry["valueFrom"]["secretKeyRef"]["name"] == secret_refs[0]
            defined.add(entry["name"])
        mongo = _env(container)["MONGODB_URL"]
        assert "tls=true" in mongo and "ssl=false" not in mongo, "DocumentDB is provisioned with TLS on"
        assert "retryWrites=false" in mongo


def check_pod_monitor_is_authenticated(objects: list[dict[str, Any]]) -> None:
    monitors = _kind(objects, "PodMonitor")
    if not monitors:
        return
    spec = monitors[0]["spec"]
    selected = set(spec["selector"]["matchExpressions"][0]["values"])
    assert "web" not in selected
    deployments = _deployments(objects)
    port_names = {
        p["name"] for c in selected for p in _container(deployments[c]).get("ports") or []
    }
    for endpoint in spec["podMetricsEndpoints"]:
        assert endpoint["port"] in port_names
        credentials = endpoint["authorization"]["credentials"]
        assert credentials["name"] and credentials["key"], "/metrics needs a credential: it is not public"
    for component in selected:
        scraped = {p["name"] for p in _container(deployments[component]).get("ports") or []}
        assert scraped & {e["port"] for e in spec["podMetricsEndpoints"]}, f"{component} exposes nothing to scrape"


INVARIANTS = (
    check_every_workload_is_rendered,
    check_read_only_root_has_writable_volumes,
    check_ports_agree,
    check_probes_and_grace_period,
    check_secrets_are_always_wired,
    check_pod_monitor_is_authenticated,
)


@pytest.mark.parametrize("invariant", INVARIANTS, ids=lambda fn: fn.__name__)
def test_chart_invariant(objects, invariant):
    invariant(objects)


# ---------------------------------------------------------------------------
# Checks specific to the chart's own guards
# ---------------------------------------------------------------------------


def test_web_server_block_is_the_repo_nginx_conf_with_port_and_upstream_templated():
    objects = _lite()
    rendered = next(c for c in _kind(objects, "ConfigMap") if _component(c) == "web")["data"]["default.conf"]
    api = next(s for s in _kind(objects, "Service") if _component(s) == "api")
    expected = (
        NGINX_CONF.read_text(encoding="utf-8")
        .replace("listen 80;", "listen 8080;")
        .replace("http://api:8000", f"http://{api['metadata']['name']}:{api['spec']['ports'][0]['port']}")
    )
    strip = lambda text: [line.rstrip() for line in text.strip().splitlines()]  # noqa: E731
    assert strip(rendered) == strip(expected), "templates/web-configmap.yaml drifted from deploy/nginx.conf"


def test_grace_period_shorter_than_the_drain_is_refused_by_the_chart():
    with pytest.raises(ChartFail, match="terminationGracePeriodSeconds"):
        _lite({"worker": {"drainSeconds": 60, "terminationGracePeriodSeconds": 45}})
    with pytest.raises(ChartFail, match="terminationGracePeriodSeconds"):
        _lite({"cdcWorker": {"drainSeconds": 40}})


def test_secret_fallbacks():
    # Default: an operator-created Secret, referenced by name.
    api = _container(_deployments(_lite())["api"])
    assert api["envFrom"][0]["secretRef"]["name"] == "dataflow-secrets"
    assert not _kind(_lite(), "Secret")
    # No Secret at all is refused, not rendered with literal $(…) placeholders.
    with pytest.raises(ChartFail, match="existingSecret"):
        _lite({"secrets": {"existingSecret": ""}})
    # Chart-managed Secret: all required keys or nothing.
    with pytest.raises(ChartFail, match="MONGODB_PASSWORD"):
        _lite({"secrets": {"create": True, "stringData": {"REDIS_PASSWORD": "r", "POSTGRES_PASSWORD": "p"}}})
    data = {k: "x" for k in ("REDIS_PASSWORD", "POSTGRES_PASSWORD", "MONGODB_PASSWORD", "DATAFLOW_AUTH_SECRET", "DATAFLOW_SECRETS_KEY")}
    created = _lite({"secrets": {"create": True, "stringData": data}})
    secret = _kind(created, "Secret")[0]
    assert secret["metadata"]["name"] == "dataflow-secrets" and set(secret["stringData"]) == set(data)
    assert _container(_deployments(created)["worker"])["envFrom"][0]["secretRef"]["name"] == "dataflow-secrets"
    # External Secrets Operator: the Secret it writes.
    external = _lite(_production_overrides())
    assert _container(_deployments(external)["api"])["envFrom"][0]["secretRef"]["name"] == "dataflow-master"


@pytest.mark.parametrize("component", PYTHON_COMPONENTS)
def test_rendered_environment_passes_the_apps_own_startup_checks(component, monkeypatch):
    """The env the chart gives each role must not trip topology_errors() or miss production mode."""
    from services.platform_config import is_production
    from services.process_role import topology_errors

    # Paths stay the test's own: the pod's /data must not be created on this machine.
    for key in list(os.environ):
        if key.startswith(("DATAFLOW_", "DATAWRAP_")) and not key.endswith(("_DIR", "_ROOT")):
            monkeypatch.delenv(key, raising=False)
    for name, value in _env(_container(_deployments(_lite())[component])).items():
        if isinstance(value, str) and not value.startswith("/"):
            monkeypatch.setenv(name, value)
    assert topology_errors() == []
    assert is_production() is True


def test_ingress_annotation_is_spelled_for_the_alb_controller():
    annotations = _kind(_lite(), "Ingress")[0]["metadata"]["annotations"]
    assert "alb.ingress.kubernetes.io/listen-ports" in annotations
    assert not [key for key in annotations if key.startswith("alb.ingress.io/")]


def test_worker_disruption_and_scale_down_are_bounded():
    objects = _lite()
    budgets = {_component(b) for b in _kind(objects, "PodDisruptionBudget")}
    assert {"api", "web", "worker"} <= budgets
    hpa = next(h for h in _kind(objects, "HorizontalPodAutoscaler") if _component(h) == "worker")
    scale_down = hpa["spec"]["behavior"]["scaleDown"]
    assert scale_down["stabilizationWindowSeconds"] >= 300
    assert scale_down["policies"] == [{"type": "Pods", "value": 1, "periodSeconds": 120}]


# ---------------------------------------------------------------------------
# The real renderer, when it is installed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("values", ["defaults", "production"])
def test_helm_template_output_passes_the_same_invariants(values):
    helm = shutil.which("helm")
    if not helm:
        pytest.skip("helm is not installed on this runner; the built-in renderer covered the invariants")
    command = [helm, "template", "dataflow", str(CHART), "--set", "serviceMonitor.enabled=true"]
    if values == "production":
        command += ["-f", str(CHART / "values-production.yaml")]
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr[-2000:]
    objects = [doc for doc in yaml.safe_load_all(result.stdout) if isinstance(doc, dict)]
    for invariant in INVARIANTS:
        invariant(objects)
