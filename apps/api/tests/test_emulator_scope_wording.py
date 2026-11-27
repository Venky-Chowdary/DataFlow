"""Snowflake and BigQuery are proven against emulators: no user-facing text says otherwise.

P0-4 / REVIEW_ORACLE finding 8: Pilot answered "Snowflake is Certified: a green
CI run proves its engine against its emulator", and the environment-gap reason
said "<id> is Certified but its driver package is not loadable here". Every
string a reader sees that calls an emulator-proven connector Certified or
proven must carry the emulator scope.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
_CLAIM = re.compile(r"\b(certified|proven)\b", re.I)
_SCOPE = re.compile(r"emulat", re.I)


def _emulated_catalog_rows() -> list[dict]:
    from services.catalog_service import load_catalog
    from src.transfer.connector_capabilities import enrich_catalog_entry

    rows = [enrich_catalog_entry(dict(c)) for c in load_catalog().get("connectors", [])]
    return [r for r in rows if r.get("proof_scope") == "emulator"]


def _strings(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for v in value.values() for s in _strings(v)]
    if isinstance(value, (list, tuple)):
        return [s for v in value for s in _strings(v)]
    return []


def test_the_evidence_still_has_emulator_proven_warehouses():
    names = {str(r.get("id")) for r in _emulated_catalog_rows()}
    assert {"snowflake", "bigquery"} <= names, names


def test_every_claim_about_an_emulator_proven_connector_states_the_scope(monkeypatch):
    from src.ai.copilot.capability_verdicts import _tier_sentence
    from src.transfer import connector_capabilities as caps

    monkeypatch.setattr(caps, "driver_available", lambda *a, **k: False)
    for row in _emulated_catalog_rows():
        cid = str(row["id"])
        said = [_tier_sentence(row), caps.driver_environment_gap(cid)]
        # Sentences and labels a reader sees; one-word machine values
        # (certification_tier "certified") are not prose.
        said += [s for k, v in row.items() if k not in {"ci_evidence"} for s in _strings(v)]
        for text in [t for t in said if " " in t.strip()]:
            # "Certified" is the real-engine tier: an emulator run never earns it.
            assert not re.search(r"\bis Certified\b", text), f"{cid}: {text!r}"
            if _CLAIM.search(text):
                assert _SCOPE.search(text), f"{cid}: {text!r}"


_SCANNED = [
    ("apps/api/src", "*.py"), ("apps/api/services", "*.py"),
    ("apps/web/src", "*.ts"), ("apps/web/src", "*.tsx"), ("docs", "*.md"),
]
#: A warehouse and a claim in one sentence (no full stop between them).
_NEAR = re.compile(
    r"\b(Snowflake|BigQuery)\b[^.\n]{0,80}\b(certified|proven)\b"
    r"|\b(certified|proven)\b[^.\n]{0,80}\b(Snowflake|BigQuery)\b",
    re.I,
)
_COMMENT = re.compile(r"^\s*(#|//|\*|/\*)")


@pytest.mark.parametrize(("folder", "pattern"), _SCANNED)
def test_no_source_line_calls_a_warehouse_certified_without_the_scope(folder, pattern):
    offenders = []
    for path in sorted((ROOT / folder).rglob(pattern)):
        if ".test." in path.name or "/tests/" in str(path) or "generated" in path.name:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if _COMMENT.match(line):
                continue
            for hit in _NEAR.finditer(line):
                sentence = line[max(0, hit.start() - 120):hit.end() + 120]
                if not _SCOPE.search(sentence):
                    offenders.append(f"{path.relative_to(ROOT)}:{number}: {hit.group(0)[:160]}")
    assert not offenders, "\n".join(offenders)
