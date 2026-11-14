"""The anchor index on the shared control-plane log (H-5) keeps the anchor semantics (M-16, S3).

PR 402 made receipts honest (``anchored`` only after an S3 read-back; an
unimplemented provider is ``failed`` and labelled ``stub``); PR 406 moved the
index to the shared control-plane log so every replica sees the same anchors.
Together: a receipt written on one pod is found and verified on another, and
an unimplemented provider names the shared stub store that really holds it.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from services.control_plane_store import ControlPlaneUnavailable
from tests._control_plane_fakes import install_shared_store

BUCKET = "anchor-bucket"


@pytest.fixture()
def anchors(tmp_path, monkeypatch):
    from services import audit_anchor

    for prefix in ("DATAWRAP_", "DATAFLOW_"):
        for name in ("AUDIT_ANCHOR_PROVIDER", "AUDIT_ANCHOR_STORE", "S3_ENDPOINT", "AUDIT_ANCHOR_S3_ENDPOINT"):
            monkeypatch.delenv(prefix + name, raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setattr(audit_anchor, "ANCHOR_STORE", tmp_path / "pod-a" / "audit_tip_anchors.jsonl")
    monkeypatch.setattr(audit_anchor, "_last_tip_anchor", 0.0)
    install_shared_store(monkeypatch)
    return audit_anchor


def _other_pod(anchors, monkeypatch, tmp_path):
    monkeypatch.setattr(anchors, "ANCHOR_STORE", tmp_path / "pod-b" / "audit_tip_anchors.jsonl")


@pytest.mark.parametrize("requested", ["tsa", "rfc3161"])
def test_unimplemented_provider_names_the_shared_stub_store(anchors, monkeypatch, tmp_path, requested):
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", requested)
    receipt = anchors.anchor_tip("c" * 64)

    assert receipt["provider"] == "stub" and receipt["requested_provider"] == requested
    assert receipt["status"] == "failed" and receipt["anchored"] is False
    assert receipt["externally_anchored"] is False
    assert "shared control-plane database" in receipt["honesty"]
    assert "on this node" not in receipt["honesty"]
    assert not (tmp_path / "pod-a" / "audit_tip_anchors.jsonl").exists(), "the shared log took the write"

    _other_pod(anchors, monkeypatch, tmp_path)
    assert anchors.latest_anchor()["id"] == receipt["id"]
    assert anchors.verify_receipt(anchors.latest_anchor())["status"] == "failed"


def test_file_mode_names_the_local_stub_store(tmp_path, monkeypatch):
    from services import audit_anchor

    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "tsa")
    receipt = audit_anchor.anchor_tip("d" * 64)
    assert "the local stub store on this node" in receipt["honesty"]
    assert receipt["status"] == "failed"


def test_an_s3_receipt_written_on_one_pod_is_found_and_verified_on_another(anchors, monkeypatch, tmp_path):
    for name, value in (("AWS_ACCESS_KEY_ID", "testing"), ("AWS_SECRET_ACCESS_KEY", "testing"),
                        ("AWS_DEFAULT_REGION", "us-east-1"), ("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "s3"),
                        ("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", BUCKET)):
        monkeypatch.setenv(name, value)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET, ObjectLockEnabledForBucket=True)
        receipt = anchors.anchor_digest("ef" * 32, kind="exit_evidence", subject="exit_evidence:exs_1:exr_1")
        assert receipt["status"] == "anchored" and receipt["provider"] == "s3"
        assert "S3 Object Lock" in receipt["honesty"] and "stub" not in receipt["honesty"]

        _other_pod(anchors, monkeypatch, tmp_path)
        found = anchors.find_receipt("ef" * 32, kind="exit_evidence")
        assert found is not None and found["id"] == receipt["id"]
        assert anchors.verify_receipt(found, digest="ef" * 32)["status"] == "anchored"
        # Evidence-pack receipts share the log but are not listed as chain-tip anchors.
        assert anchors.latest_anchor() is None


def test_an_unreachable_shared_index_still_returns_the_receipt(anchors, monkeypatch):
    from services import shared_doc_store

    def _down(_name):
        raise ControlPlaneUnavailable("control plane down")

    monkeypatch.setattr(shared_doc_store, "_collection", _down)
    receipt = anchors.anchor_tip("e" * 64)
    assert receipt is not None and receipt["status"] == "not_anchored"
    assert anchors.latest_anchor() is None
    assert anchors.list_anchors() == []
