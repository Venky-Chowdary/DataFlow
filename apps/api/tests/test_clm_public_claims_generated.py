"""CLM-5, CLM-6, CLM-7: every public connector claim is generated from CI evidence.

* ``data/connector_ci_evidence.json`` is the evidence snapshot, written by
  ``scripts/connector_claims_report.py`` from one CI run's live-engine
  results, and it names that run.
* ``apps/web/src/lib/connectorClaims.generated.ts`` (the landing, docs,
  integrations and pricing numbers and family badges) must equal what the
  generator computes from that snapshot and the product's driver set.
* The hand-typed "46 transfer-ready drivers" and the "Every figure traces to
  a recorded run" block (whose artifacts no longer ship) are gone.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
WEB = REPO / "apps" / "web" / "src"


def _report():
    path = ROOT / "scripts" / "connector_claims_report.py"
    spec = importlib.util.spec_from_file_location("connector_claims_report", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_committed_snapshot_names_the_ci_run_it_came_from() -> None:
    from services.connector_proof import SNAPSHOT

    doc = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    assert doc.get("schema_version") == 1
    assert str(doc.get("source_run") or "").startswith("https://github.com/"), doc.get("source_run")
    for engine, record in doc["engines"].items():
        assert record["live_verified"] == (record["status"] == "ci_green"), engine
        for run in record.get("runs") or []:
            if record["live_verified"]:
                assert run["status"] == "green" and not run["engine_skips"], (engine, run)


def test_generated_web_claims_match_the_evidence(monkeypatch) -> None:
    from services import connector_proof

    monkeypatch.delenv(connector_proof.EVIDENCE_ENV, raising=False)
    connector_proof.reset_cache()
    report = _report()
    expected = report.render(report.build_claims())
    current = report.CLAIMS_PATH.read_text(encoding="utf-8")
    assert current == expected, "run apps/api/scripts/connector_claims_report.py"


def test_proven_count_is_the_evidence_not_a_constant(monkeypatch) -> None:
    from services import connector_proof
    from src.transfer.connector_capabilities import transfer_live_driver_types

    monkeypatch.delenv(connector_proof.EVIDENCE_ENV, raising=False)
    connector_proof.reset_cache()
    claims = _report().build_claims()
    declared = transfer_live_driver_types(declared=True)
    assert claims["proven_count"] + claims["pending_count"] == len(declared)
    for driver in claims["proven_drivers"]:
        assert connector_proof.driver_proven(driver), driver
    # Kafka and the vector stores have no proving CI job (CLM-11).
    for driver in ("kafka", "qdrant", "weaviate", "pinecone", "milvus", "elasticsearch"):
        assert driver in claims["pending_drivers"], driver


def test_public_pages_carry_no_untraceable_evidence_block() -> None:
    files = [
        WEB / "pages" / "LandingPage.tsx",
        WEB / "pages" / "marketing" / "DocsPortal.tsx",
        WEB / "pages" / "marketing" / "MarketingSubpage.tsx",
        WEB / "lib" / "provenEvidence.ts",
    ]
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "Every figure here traces to a recorded run" not in text, path
        assert "TRANSFER_READY_DRIVERS" not in text, path
        assert "_results.json" not in text, path  # artifacts that no longer ship
    evidence = (WEB / "lib" / "provenEvidence.ts").read_text(encoding="utf-8")
    assert 'from "./connectorClaims.generated"' in evidence
