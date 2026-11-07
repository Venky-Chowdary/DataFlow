"""The CLI and the engine must agree on what a manifest is.

The product was renamed DataFlow -> Datawrap. ``export`` began writing
``kind: DatawrapManifest`` while the CLI still only knew ``DataFlowManifest``, so
``dataflow validate -f`` refused the file the product itself had just written and
the GitOps round trip (export -> validate -> plan -> apply) was dead.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

import pytest
import yaml
from services.gitops_manifest import (
    MANIFEST_KIND,
    MANIFEST_KINDS,
    RESOURCE_KINDS,
    plan_manifest,
)


def _cli() -> ModuleType:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "cli"))
    return importlib.import_module("dataflow_cli.main")


def _schedule_resource(name: str = "kinds-nightly") -> dict:
    return {
        "apiVersion": "dataflow.space/v1",
        "kind": "PipelineSchedule",
        "metadata": {"name": name},
        "spec": {
            "name": name,
            "source_connector_id": "s1",
            "source_table": "t1",
            "dest_connector_id": "d1",
            "dest_table": "t2",
            "interval": "daily",
        },
    }


def test_cli_fallback_kinds_match_the_engine():
    cli_main = _cli()
    assert cli_main.FALLBACK_MANIFEST_KINDS == tuple(MANIFEST_KINDS)
    assert cli_main.FALLBACK_RESOURCE_KINDS == tuple(RESOURCE_KINDS)


def test_cli_reads_the_kinds_from_the_engine():
    cli_main = _cli()
    manifest_kinds, resource_kinds = cli_main._manifest_kinds()
    assert manifest_kinds == tuple(MANIFEST_KINDS)
    assert resource_kinds == tuple(RESOURCE_KINDS)
    assert manifest_kinds[0] == MANIFEST_KIND


@pytest.mark.parametrize("kind", MANIFEST_KINDS)
def test_validate_accepts_every_manifest_kind_the_engine_accepts(tmp_path, kind):
    cli_main = _cli()
    path = tmp_path / "dataflow.yaml"
    path.write_text(
        yaml.safe_dump({
            "apiVersion": "dataflow.space/v1",
            "kind": kind,
            "resources": [_schedule_resource()],
        }),
        encoding="utf-8",
    )
    assert cli_main.main(["validate", "-f", str(path)]) == 0


@pytest.mark.parametrize("kind", RESOURCE_KINDS)
def test_validate_accepts_a_single_resource_of_every_kind(tmp_path, kind):
    cli_main = _cli()
    path = tmp_path / "one.yaml"
    path.write_text(
        yaml.safe_dump({"apiVersion": "dataflow.space/v1", "kind": kind, "spec": {"name": "x"}}),
        encoding="utf-8",
    )
    assert cli_main.main(["validate", "-f", str(path)]) == 0


def test_validate_refuses_a_resource_kind_apply_would_skip(tmp_path, capsys):
    """A manifest that would apply as zero resources must not validate as ok."""
    cli_main = _cli()
    path = tmp_path / "bad.yaml"
    path.write_text(
        yaml.safe_dump({
            "apiVersion": "dataflow.space/v1",
            "kind": MANIFEST_KIND,
            "resources": [{"kind": "Pipeline", "spec": {"name": "typo"}}],
        }),
        encoding="utf-8",
    )
    assert cli_main.main(["validate", "-f", str(path)]) == 1
    assert "unsupported resource kind" in capsys.readouterr().err


def test_validate_names_the_accepted_kinds_when_it_refuses(tmp_path, capsys):
    cli_main = _cli()
    path = tmp_path / "wrong.yaml"
    path.write_text(
        yaml.safe_dump({"apiVersion": "dataflow.space/v1", "kind": "Deployment"}),
        encoding="utf-8",
    )
    assert cli_main.main(["validate", "-f", str(path)]) == 1
    err = capsys.readouterr().err
    for kind in (*MANIFEST_KINDS, *RESOURCE_KINDS):
        assert kind in err


def test_a_bare_list_is_wrapped_in_the_canonical_kind(tmp_path):
    cli_main = _cli()
    path = tmp_path / "list.yaml"
    path.write_text(yaml.safe_dump([_schedule_resource()]), encoding="utf-8")
    assert cli_main._load_file(path)["kind"] == MANIFEST_KIND
    assert cli_main.main(["validate", "-f", str(path)]) == 0


@pytest.mark.parametrize("kind", MANIFEST_KINDS)
def test_plan_reads_resources_from_every_manifest_kind(kind):
    plan = plan_manifest({
        "apiVersion": "dataflow.space/v1",
        "kind": kind,
        "resources": [_schedule_resource("kinds-plan")],
    })
    assert plan["resource_count"] == 1
    assert [a["kind"] for a in plan["actions"]] == ["PipelineSchedule"]


def test_plan_refuses_an_unknown_kind_instead_of_planning_nothing():
    """Silently planning zero resources reads as 'nothing to do', not as a typo."""
    with pytest.raises(ValueError) as excinfo:
        plan_manifest({
            "apiVersion": "dataflow.space/v1",
            "kind": "DatawrapManifestt",
            "resources": [_schedule_resource()],
        })
    assert "unsupported kind" in str(excinfo.value)
