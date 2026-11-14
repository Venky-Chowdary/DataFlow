"""docs/reference/connectors.md is generated from the code and cannot drift.

``docs/tools/gen_connector_matrix.py`` reads the driver tables, the single
capability function, the privilege probe and the committed CI evidence
(``apps/api/data/connector_ci_evidence.json``, the source of every public claim). This test
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


def test_ci_column_and_tier_follow_the_ci_evidence(gen, tmp_path, monkeypatch) -> None:
    """The page states proof only from the CI evidence (W5), never from workflow config."""
    import json

    from services import connector_proof

    gen._bootstrap()
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"engines": {}}), encoding="utf-8")
    green = tmp_path / "green.json"
    green.write_text(
        json.dumps({"engines": {"mysql": {"status": "ci_green", "live_verified": True, "emulated": False}}}),
        encoding="utf-8",
    )
    try:
        monkeypatch.setenv(connector_proof.EVIDENCE_ENV, str(empty))
        connector_proof.reset_cache()
        with gen._declared_view():
            rows = {r["id"]: r for r in gen.collect_rows()}
        assert not any(r["ci"].startswith("proven") for r in rows.values())
        assert not any(r["tier"].startswith("Certified") for r in rows.values())
        assert rows["mysql"]["tier"].startswith("Implemented")

        monkeypatch.setenv(connector_proof.EVIDENCE_ENV, str(green))
        connector_proof.reset_cache()
        with gen._declared_view():
            rows = {r["id"]: r for r in gen.collect_rows()}
        assert rows["mysql"]["ci"] == "proven"
        assert rows["mysql"]["tier"] == "Certified"
        assert rows["postgresql"]["ci"].startswith("not proven")
        # A planned driver stays planned whatever the evidence says.
        assert rows["redshift"]["tier"].startswith("Planned")
    finally:
        monkeypatch.delenv(connector_proof.EVIDENCE_ENV, raising=False)
        connector_proof.reset_cache()


def test_proven_rows_match_the_public_claims(gen) -> None:
    """The page and the generated web claims name the same proven drivers."""
    import re

    gen._bootstrap()
    claims = (REPO_ROOT / "apps" / "web" / "src" / "lib" / "connectorClaims.generated.ts").read_text(encoding="utf-8")
    block = claims[claims.index('"proven_drivers": [') + len('"proven_drivers": [') :]
    proven_claims = set(re.findall(r'"([a-z0-9_]+)"', block[: block.index("]")]))
    with gen._declared_view():
        rows = {r["id"]: r for r in gen.collect_rows()}
    proven_page = {rid for rid, r in rows.items() if r["ci"].startswith("proven")}
    assert proven_page == proven_claims


def test_page_keeps_its_hand_written_sections() -> None:
    text = PAGE.read_text(encoding="utf-8")
    for heading in ("## Read this first", "## How to read the columns", "## Not covered", "## Verified against"):
        assert heading in text
