"""Ops review Medium 8: exports must be writable under a read-only root filesystem.

``export_paths.exports_dir()`` defaults to ``apps/api/exports`` inside the
image, and the preflight staging check hard-coded a path of its own. The
chart now names ``DATAFLOW_EXPORTS_DIR`` on the data volume, and every
caller asks ``export_paths`` where exports go.
"""

from __future__ import annotations

from pathlib import Path

from tests._helm_lite import render_chart

CHART = Path(__file__).resolve().parents[3] / "deploy/enterprise/helm/dataflow"


def test_chart_puts_exports_on_the_writable_data_volume():
    choice = {"controlPlane": {"podLocalStoresAck": "transfer_plans,pilot_results"}}  # see Medium 5
    for overrides in (choice, {**choice, "serviceMonitor": {"enabled": True}}):
        for obj in render_chart(CHART, overrides=overrides):
            if obj.get("kind") != "Deployment":
                continue
            component = obj["metadata"]["labels"]["app.kubernetes.io/component"]
            if component == "web":
                continue
            container = obj["spec"]["template"]["spec"]["containers"][0]
            env = {e["name"]: e.get("value") for e in container.get("env") or []}
            mounts = {m["mountPath"] for m in container.get("volumeMounts") or [] if not m.get("readOnly")}
            exports = str(env.get("DATAFLOW_EXPORTS_DIR") or "")
            assert exports, f"{component}: DATAFLOW_EXPORTS_DIR is not set; exports would go to the read-only image"
            assert any(exports.startswith(m + "/") for m in mounts), f"{component}: {exports} is not on a writable volume"


def test_preflight_staging_check_uses_the_configured_exports_folder(tmp_path, monkeypatch):
    import shutil

    from services import preflight_destination

    target = tmp_path / "exports-here"
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(target))
    monkeypatch.delenv("DATAWRAP_EXPORTS_DIR", raising=False)
    seen: list[Path] = []
    real = shutil.disk_usage

    def recording(path):
        seen.append(Path(path))
        return real(tmp_path)

    monkeypatch.setattr(shutil, "disk_usage", recording)
    preflight_destination._available_staging_bytes(1024)
    assert seen == [target.resolve()]
    assert target.is_dir()
