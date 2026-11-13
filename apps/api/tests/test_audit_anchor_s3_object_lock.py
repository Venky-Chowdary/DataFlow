"""S3 Object Lock (compliance mode) anchor provider behind services.audit_anchor.

Pins the plan's guarantee "anchor evidence in a real write-once store, or say
it is not anchored": a receipt reads ``anchored`` only after the object, its
compliance-mode retention and its digest were read back; a bucket without
Object Lock, a missing bucket, an unreachable store or a tampered/expired
anchor reads ``failed`` with the reason; the stub always reads
``not_anchored``; nothing falls back to the stub.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import boto3
import pytest
from moto import mock_aws

BUCKET = "anchor-bucket"


@pytest.fixture()
def anchors(tmp_path, monkeypatch):
    from services import audit_anchor

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    for name in ("DATAFLOW_S3_ENDPOINT", "DATAWRAP_S3_ENDPOINT", "AWS_ENDPOINT_URL",
                 "DATAFLOW_AUDIT_ANCHOR_S3_ENDPOINT", "DATAWRAP_AUDIT_ANCHOR_S3_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    monkeypatch.setattr(audit_anchor, "_last_tip_anchor", 0.0)
    with mock_aws():
        yield audit_anchor


def _s3_provider(monkeypatch, *, lock: bool = True, bucket: str = BUCKET):
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "s3")
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", bucket)
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET, ObjectLockEnabledForBucket=lock)
    return s3


def test_s3_anchor_is_compliance_mode_and_confirmed_by_read_back(anchors, monkeypatch):
    s3 = _s3_provider(monkeypatch)
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_RETENTION_DAYS", "30")
    receipt = anchors.anchor_digest("ab" * 32, kind="exit_evidence", subject="exit_evidence:exs_1:exr_1")
    assert receipt["status"] == "anchored" and receipt["anchored"] is True
    assert receipt["provider"] == "s3" and receipt["retention"]["mode"] == "COMPLIANCE"
    loc = receipt["location"]
    assert loc["bucket"] == BUCKET and loc["key"].startswith("audit-anchors/exit_evidence/")
    head = s3.head_object(Bucket=BUCKET, Key=loc["key"], VersionId=loc["version_id"])
    assert head["ObjectLockMode"] == "COMPLIANCE"
    until = head["ObjectLockRetainUntilDate"]
    assert until > datetime.now(timezone.utc) + timedelta(days=29)
    # Compliance mode: the version cannot be deleted, even by the owner.
    with pytest.raises(Exception):
        s3.delete_object(Bucket=BUCKET, Key=loc["key"], VersionId=loc["version_id"])
    # Verify-on-read confirms it again; the index finds it by digest.
    found = anchors.find_receipt("ab" * 32, kind="exit_evidence")
    assert found["id"] == receipt["id"]
    assert anchors.verify_receipt(found, digest="ab" * 32)["status"] == "anchored"


def test_a_bucket_without_object_lock_fails_closed(anchors, monkeypatch):
    _s3_provider(monkeypatch, lock=False)
    receipt = anchors.anchor_digest("cd" * 32, kind="audit_tip")
    assert receipt["status"] == "failed" and receipt["anchored"] is False
    assert "Object Lock" in receipt["reason"]
    assert receipt["provider"] == "s3"  # never quietly a stub receipt


def test_a_missing_bucket_or_setting_is_visible_not_anchored(anchors, monkeypatch):
    _s3_provider(monkeypatch, bucket="no-such-bucket")
    missing = anchors.anchor_digest("ef" * 32, kind="audit_tip")
    assert missing["status"] == "failed" and "could not be read" in missing["reason"]
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", "")
    unset = anchors.anchor_digest("ef" * 32, kind="audit_tip")
    assert unset["status"] == "failed" and "not set" in unset["reason"]


def test_verify_on_read_catches_a_wrong_digest_and_an_ended_retention(anchors, monkeypatch):
    from services.audit_anchor_s3 import verify_location

    _s3_provider(monkeypatch)
    receipt = anchors.anchor_digest("12" * 32, kind="exit_evidence")
    assert anchors.verify_receipt(receipt, digest="34" * 32)["status"] == "failed"
    forged = {**receipt, "digest": "34" * 32, "tip_hash": "34" * 32}
    assert "different digest" in anchors.verify_receipt(forged)["reason"]
    later = datetime.now(timezone.utc) + timedelta(days=4000)
    ended = verify_location(receipt["location"], "12" * 32, now=later)
    assert ended["status"] == "failed" and "retention period has ended" in ended["reason"]


def test_an_unreachable_store_fails_closed(anchors, monkeypatch):
    from services import audit_anchor_s3

    _s3_provider(monkeypatch)

    class Down:
        def __getattr__(self, _name):
            def boom(**_kwargs):
                raise ConnectionError("endpoint unreachable")
            return boom

    monkeypatch.setattr(audit_anchor_s3, "client", lambda: Down())
    receipt = anchors.anchor_digest("56" * 32, kind="audit_tip")
    assert receipt["status"] == "failed" and "ConnectionError" in receipt["reason"]


def test_stub_is_never_called_anchored(anchors, monkeypatch):
    monkeypatch.delenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", raising=False)
    receipt = anchors.anchor_tip("78" * 32)
    assert receipt["provider"] == "stub" and receipt["status"] == "not_anchored"
    assert receipt["anchored"] is False and "not write-once" in receipt["honesty"]
    assert anchors.verify_receipt(receipt)["status"] == "not_anchored"
    assert anchors.verify_receipt(None, digest="00")["status"] == "not_anchored"
    assert anchors.latest_anchor()["tip_hash"] == "78" * 32


def test_s3_tip_anchoring_is_rate_limited_but_a_forced_anchor_always_runs(anchors, monkeypatch):
    _s3_provider(monkeypatch)
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_MIN_INTERVAL_SECONDS", "3600")
    first = anchors.anchor_tip("9a" * 32)
    assert first and first["status"] == "anchored"
    assert anchors.anchor_tip("9b" * 32) is None
    forced = anchors.anchor_tip("9c" * 32, force=True)
    assert forced and forced["status"] == "anchored"
    # Evidence anchors are never rate limited.
    assert anchors.anchor_digest("9d" * 32, kind="exit_evidence")["status"] == "anchored"
    assert [r["tip_hash"] for r in anchors.list_anchors()] == ["9c" * 32, "9a" * 32]
