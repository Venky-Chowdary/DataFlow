"""docs/reference/connectors.md is generated from the code and cannot drift.

``docs/tools/gen_connector_matrix.py`` reads the driver tables, the single
capability function, the privilege probe and the CI workflow files. This test
regenerates the matrix and compares it with the committed page.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
GENERATOR = REPO_ROOT / "docs" / "tools" / "gen_connector_matrix.py"
PAGE = REPO_ROOT / "docs" / "reference" / "connectors.md"
REGENERATE = "run `python docs/tools/gen_connector_matrix.py` from the repository root"


@pytest.fixture(scope="module")
def gen():
    spec = importlib.util.spec_from_file_location("gen_connector_matrix", GENERATOR)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_matrix_matches_the_code(gen) -> None:
    committed = PAGE.read_text(encoding="utf-8")
    assert gen.build_document(committed) == committed, f"{PAGE.name} is stale; {REGENERATE}"


def test_every_driver_and_file_format_has_a_row(gen) -> None:
    from src.transfer import connector_capabilities as cc

    listed = gen.matrix_ids(PAGE.read_text(encoding="utf-8"))
    expected = set(cc._DRIVER_CAPS) | set(cc._FILE_CAPS) | {"generic_sql"}
    assert expected - listed == set(), f"missing rows; {REGENERATE}"
    assert listed - expected == set(), f"rows for connectors the code no longer has; {REGENERATE}"


def test_a_capability_change_is_caught(gen, monkeypatch) -> None:
    """Prove the check bites: flipping one driver's write flag changes the page."""
    from src.transfer import connector_capabilities as cc

    patched = dict(cc._DRIVER_CAPS)
    patched["qdrant"] = {**patched["qdrant"], "write": False}
    monkeypatch.setattr(cc, "_DRIVER_CAPS", patched)
    committed = PAGE.read_text(encoding="utf-8")
    assert gen.build_document(committed) != committed


def test_cdc_column_is_the_gate_list(gen) -> None:
    from services.preflight_cursor_gate import CDC_CAPABLE_SOURCES

    gen._bootstrap()
    with gen._declared_view():
        rows = {r["id"]: r for r in gen.collect_rows()}
    cdc = {rid for rid, r in rows.items() if r["cdc"] == "yes"}
    assert cdc == {"postgresql", "mysql", "mongodb", "oracle", "sqlserver"}
    assert cdc <= CDC_CAPABLE_SOURCES
    for named in ("qdrant", "pgvector", "kafka"):
        assert rows[named]["cdc"] == "no" and rows[named]["cdc_sink"] == "yes"
    assert rows["email"]["upsert"] == "no"


def test_ci_evidence_follows_the_workflow_files(gen, monkeypatch) -> None:
    """Removing an engine from CI must change its row, not leave a stale claim."""
    gen._bootstrap()
    caps = {"read": True, "write": True}
    assert gen.ci_evidence("mysql", caps, False).startswith("live container")
    assert gen.ci_evidence("airtable", {"evidence": "synthetic-fixture"}, False).startswith("fixture only")
    assert gen.ci_evidence("csv", caps, True).startswith("no service needed")
    monkeypatch.setattr(gen, "_read", lambda _rel: "")
    assert gen.ci_evidence("mysql", caps, False) == "not started by any workflow"
    assert gen.ci_evidence("snowflake", caps, False) == "not started by any workflow"


def test_page_keeps_its_hand_written_sections() -> None:
    text = PAGE.read_text(encoding="utf-8")
    for heading in ("## Read this first", "## How to read the columns", "## Not covered", "## Verified against"):
        assert heading in text
