"""D15 — G8 skipped the fingerprint for deterministic PII transforms.

hash_pii (keyed HMAC), mask_pii and redact are declared deterministic in the
engine's transform contract (``TRANSFORM_SEMANTICS``) and G8 replays the real
write-path ``apply_transform``, yet the gate kept its own one-way list and
reported them as "non-deterministic", leaving PII columns unfingerprinted.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PREFLIGHT_ROOT = Path(__file__).resolve().parents[1] / "src"
_API_ROOT = Path(__file__).resolve().parents[3] / "apps" / "api"
for _root in (_PREFLIGHT_ROOT, _API_ROOT):
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from preflight import ColumnMapping, ColumnSchema, PreflightContext  # noqa: E402
from preflight.gates import _skips_fingerprint, gate_g8_reconciliation  # noqa: E402
from preflight.models import GateStatus  # noqa: E402
from test_gates import _happy_plan  # noqa: E402


@pytest.mark.parametrize(
    "transform", ["hash_pii", "hash", "sha256", "mask_pii", "redact", "upper", "none"]
)
def test_engine_deterministic_transforms_keep_their_fingerprint(transform):
    assert _skips_fingerprint(transform) is False


@pytest.mark.parametrize("transform", ["anonymize", "encrypt", "mask", "pii_mask"])
def test_one_way_names_the_engine_does_not_know_still_skip(transform):
    assert _skips_fingerprint(transform) is True


def test_g8_fingerprints_pii_columns_instead_of_skipping_them(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "0" * 64)
    plan = _happy_plan()
    plan.mappings = [
        ColumnMapping(source="id", target="id", confidence=0.99, transform="identity"),
        ColumnMapping(source="email", target="email", confidence=0.99, transform="hash_pii"),
        ColumnMapping(source="phone", target="phone", confidence=0.99, transform="mask_pii"),
        ColumnMapping(source="ssn", target="ssn", confidence=0.99, transform="redact"),
    ]
    plan.source.columns = [
        ColumnSchema(name=c, inferred_type="TEXT") for c in ("id", "email", "phone", "ssn")
    ]
    plan.destination.target_columns = [
        ColumnSchema(name=c, inferred_type="TEXT") for c in ("id", "email", "phone", "ssn")
    ]
    rows = [
        {"id": "1", "email": "a@example.com", "phone": "+1 555 0100", "ssn": "123-45-6789"},
        {"id": "2", "email": "b@example.com", "phone": "+1 555 0101", "ssn": "987-65-4321"},
    ]
    result = gate_g8_reconciliation(PreflightContext(plan=plan, sample_rows=rows))
    assert result.status == GateStatus.PASS, result.message
    assert "non-deterministic" not in (result.message or "")
    assert not (result.details or {}).get("skipped_fingerprint_targets")
