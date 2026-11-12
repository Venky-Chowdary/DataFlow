"""Audit M-16 (label only): an anchor receipt never claims a provider that did not store it.

``AUDIT_ANCHOR_PROVIDER=s3`` only changed the label on the receipt: the write
still went to the local JSONL file, yet the receipt said ``provider: s3`` and
"Anchor provider=s3". Until the Object Lock provider exists, a receipt names
where it was really written (the local ``stub`` store) and says the requested provider is not
implemented in this build.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def anchor_store(tmp_path, monkeypatch):
    for prefix in ("DATAWRAP_", "DATAFLOW_"):
        monkeypatch.delenv(prefix + "AUDIT_ANCHOR_PROVIDER", raising=False)
        monkeypatch.delenv(prefix + "AUDIT_ANCHOR_STORE", raising=False)
    path = tmp_path / "anchors.jsonl"
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(path))
    return path


@pytest.mark.parametrize("requested", ["s3", "S3", "gcs", "tsa", "worm"])
def test_unimplemented_provider_is_not_claimed(anchor_store, monkeypatch, requested):
    from services import audit_anchor

    monkeypatch.setenv("DATAWRAP_AUDIT_ANCHOR_PROVIDER", requested)
    receipt = audit_anchor.anchor_tip("a" * 64)

    assert anchor_store.read_text(encoding="utf-8").count("\n") == 1, "the receipt was written locally"
    assert receipt["provider"] == "stub", receipt
    assert receipt["requested_provider"] == requested.lower()
    assert receipt["externally_anchored"] is False
    assert "not implemented" in receipt["honesty"]
    assert f"provider={requested.lower()}" not in receipt["honesty"].lower()
    assert audit_anchor.anchor_provider() == "stub"
    assert audit_anchor.latest_anchor()["provider"] == "stub"


@pytest.mark.parametrize("configured", [None, "stub", "local", ""])
def test_default_provider_keeps_its_plain_statement(anchor_store, monkeypatch, configured):
    from services import audit_anchor

    if configured is not None:
        monkeypatch.setenv("DATAWRAP_AUDIT_ANCHOR_PROVIDER", configured)
    receipt = audit_anchor.anchor_tip("b" * 64)
    assert receipt["provider"] == "stub"
    assert receipt["externally_anchored"] is False
    assert "requested_provider" not in receipt
    assert "not S3 Object Lock" in receipt["honesty"]
