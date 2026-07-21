"""DataFlow CLI — validate / plan / apply / export proofs."""

from __future__ import annotations

from pathlib import Path

import yaml


def test_cli_validate_and_plan_local(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from importlib import reload

    import services.platform_config as pc
    import services.schedule_store as ss

    reload(pc)
    reload(ss)

    # Import CLI after path is usable (apps/api is cwd in pytest).
    repo = Path(__file__).resolve().parents[3]
    cli_root = repo / "apps" / "cli"
    import sys

    sys.path.insert(0, str(cli_root))
    from dataflow_cli.main import main

    manifest = {
        "apiVersion": "dataflow.space/v1",
        "kind": "DataFlowManifest",
        "resources": [
            {
                "apiVersion": "dataflow.space/v1",
                "kind": "PipelineSchedule",
                "metadata": {"name": "cli-nightly"},
                "spec": {
                    "name": "cli-nightly",
                    "source_connector_id": "s1",
                    "source_table": "t1",
                    "dest_connector_id": "d1",
                    "dest_table": "t2",
                    "interval": "daily",
                },
            }
        ],
    }
    path = tmp_path / "dataflow.yaml"
    path.write_text(yaml.safe_dump(manifest), encoding="utf-8")

    assert main(["validate", "-f", str(path)]) == 0
    assert main(["plan", "-f", str(path), "--local"]) == 0
    assert main(["apply", "-f", str(path), "--local"]) == 2  # needs --yes
    assert main(["apply", "-f", str(path), "--local", "--yes"]) == 0
    assert any(s.name == "cli-nightly" for s in ss.list_schedules())

    out = tmp_path / "out.yaml"
    assert main(["export", "--local", "-o", str(out)]) == 0
    assert out.is_file()
    assert "DataFlowManifest" in out.read_text(encoding="utf-8")


def test_cli_validate_rejects_empty_manifest(tmp_path):
    import sys
    from pathlib import Path as P

    repo = P(__file__).resolve().parents[3]
    sys.path.insert(0, str(repo / "apps" / "cli"))
    from dataflow_cli.main import main

    path = tmp_path / "bad.yaml"
    path.write_text(
        "apiVersion: dataflow.space/v1\nkind: DataFlowManifest\nresources: []\n",
        encoding="utf-8",
    )
    assert main(["validate", "-f", str(path)]) == 1
