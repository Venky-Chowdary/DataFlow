"""Checkpoint anchoring through the existing audit anchor (S3 Object Lock under moto, or "not anchored").

Pins the guarantee "anchored only after read-back": with the stub the anchor
check is "not checked" and no verdict is ``verified``; with S3 Object Lock a
checkpoint reads anchored only when the object, its compliance retention and
its digest are read back at verification time, and then (and only then) a
sound pack is ``verified``; a deleted, replaced or unreadable anchor fails; a
bucket without Object Lock is a visible failure, never a quiet stub.
"""

from __future__ import annotations

import copy

import boto3
import pytest
from moto import mock_aws

from tests.test_ledger_log import ledger  # noqa: F401
from tests.test_ledger_packs import _job, _statuses
from tools import datawrap_verify as verifier

BUCKET = "ledger-anchor-bucket"


@pytest.fixture()
def s3_anchor(ledger, monkeypatch):  # noqa: F811
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    for name in ("DATAFLOW_S3_ENDPOINT", "DATAWRAP_S3_ENDPOINT", "AWS_ENDPOINT_URL",
                 "DATAFLOW_AUDIT_ANCHOR_S3_ENDPOINT", "DATAWRAP_AUDIT_ANCHOR_S3_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    with mock_aws():
        monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", "s3")
        monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_S3_BUCKET", BUCKET)
        monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_RETENTION_DAYS", "30")
        s3 = boto3.client("s3", region_name="us-east-1")
        yield s3


def _export(job_id: str) -> dict:
    from services.signed_proof_pack import export_proof_pack_for_job

    return export_proof_pack_for_job(_job(job_id), actor="ops@example.com")


def test_the_stub_never_claims_an_anchor_and_the_verdict_is_not_verified(ledger):  # noqa: F811
    from services.ledger_verify import verify_online

    pack = _export("job-stub")
    assert pack["ledger"]["anchor"]["status"] == "not_anchored" and pack["ledger"]["anchor"]["anchored"] is False
    record = ledger.checkpoint()
    assert record["anchor"]["status"] == "not_anchored" and record["anchor"]["kind"] == "ledger_checkpoint"
    assert record["anchor"]["digest"] == record["checkpoint_sha256"]
    evidence = verify_online(pack)
    anchor = next(c for c in evidence["checks"] if c["id"] == "anchor")
    assert anchor["status"] == "not_checked" and anchor["detail"].startswith("Not anchored: ")
    assert evidence["verdict"] == "incomplete" and evidence["anchor"]["status"] == "not_anchored"
    assert "not a full verification" in evidence["summary"] and "anchor" in evidence["summary"]


def test_s3_object_lock_anchor_is_read_back_and_only_then_is_a_pack_verified(ledger, s3_anchor):  # noqa: F811
    from services.ledger_verify import verify_online

    s3_anchor.create_bucket(Bucket=BUCKET, ObjectLockEnabledForBucket=True)
    pack = _export("job-s3")
    receipt = pack["ledger"]["anchor"]
    assert receipt["status"] == "anchored" and receipt["provider"] == "s3"
    assert receipt["retention"]["mode"] == "COMPLIANCE"
    assert receipt["digest"] == verifier.checkpoint_sha256(pack["ledger"]["checkpoint"])
    location = receipt["location"]
    assert location["key"].startswith("audit-anchors/ledger_checkpoint/")
    head = s3_anchor.head_object(Bucket=BUCKET, Key=location["key"], VersionId=location["version_id"])
    assert head["ObjectLockMode"] == "COMPLIANCE"

    evidence = verify_online(copy.deepcopy(pack))
    assert set(_statuses(evidence).values()) == {"passed"}
    assert evidence["verdict"] == "verified" and evidence["counts"] == {"passed": 8, "failed": 0, "not_checked": 0}
    assert evidence["anchor"]["status"] == "anchored"
    assert "read back" in next(c for c in evidence["checks"] if c["id"] == "anchor")["detail"]

    # The receipt alone is never believed: the stored receipt still says "anchored" while the
    # object it names holds another digest, and verification reads the object.
    from services import ledger_store

    size = pack["ledger"]["checkpoint"]["tree_size"]
    other = s3_anchor.put_object(Bucket=BUCKET, Key="audit-anchors/other", Body=b'{"digest": "00"}',
                                 ObjectLockMode="COMPLIANCE",
                                 ObjectLockRetainUntilDate=head["ObjectLockRetainUntilDate"])
    forged = {**receipt, "location": {**location, "key": "audit-anchors/other", "version_id": other["VersionId"]}}
    ledger_store.set_fields(ledger_store.CHECKPOINTS, size, {"anchor": forged})
    evidence = verify_online(copy.deepcopy(pack))
    assert _statuses(evidence)["anchor"] == "failed" and evidence["verdict"] == "failed"
    assert "different digest" in next(c for c in evidence["checks"] if c["id"] == "anchor")["detail"]
    # A receipt pointing at an object that is not there fails too.
    ledger_store.set_fields(ledger_store.CHECKPOINTS, size,
                            {"anchor": {**receipt, "location": {**location, "key": "audit-anchors/missing",
                                                               "version_id": None}}})
    assert _statuses(verify_online(copy.deepcopy(pack)))["anchor"] == "failed"
    # The checkpoint record lost its receipt (another replica, a restored store): the bucket is
    # the authority and still confirms the anchor.
    ledger_store.set_fields(ledger_store.CHECKPOINTS, size, {"anchor": None})
    assert verify_online(copy.deepcopy(pack))["verdict"] == "verified"


def test_a_bucket_without_object_lock_is_a_visible_failure_never_a_stub(ledger, s3_anchor, monkeypatch):  # noqa: F811
    from services import ledger_anchor
    from services.ledger_verify import verify_online

    events: list[tuple[str, dict]] = []
    monkeypatch.setattr(ledger_anchor, "_audit",
                        lambda receipt, record: events.append((receipt["status"], record["checkpoint_sha256"])))
    s3_anchor.create_bucket(Bucket=BUCKET, ObjectLockEnabledForBucket=False)
    pack = _export("job-nolock")
    receipt = pack["ledger"]["anchor"]
    assert receipt["status"] == "failed" and receipt["provider"] == "s3" and "Object Lock" in receipt["reason"]
    assert events and events[-1][0] == "failed"
    evidence = verify_online(pack)
    assert _statuses(evidence)["anchor"] == "failed" and evidence["verdict"] == "failed"
    # The pack was still issued and signed: only the anchor is missing.
    assert _statuses(evidence)["signatures"] == _statuses(evidence)["inclusion"] == "passed"


def test_anchor_latest_retries_a_failed_anchor_and_never_reanchors_a_confirmed_one(ledger, s3_anchor):  # noqa: F811
    from services.ledger_anchor import anchor_latest

    assert anchor_latest() == {"ok": False, "anchored": False, "checkpoint_sha256": None, "anchor": None}
    pack = _export("job-retry")  # the bucket does not exist yet
    assert pack["ledger"]["anchor"]["status"] == "failed"
    still = anchor_latest()
    assert still["ok"] is True and still["anchored"] is False and still["anchor"]["status"] == "failed"
    s3_anchor.create_bucket(Bucket=BUCKET, ObjectLockEnabledForBucket=True)
    done = anchor_latest()
    assert done["anchored"] is True and done["checkpoint_sha256"] == ledger.checkpoint()["checkpoint_sha256"]
    assert ledger.checkpoint()["anchor"]["status"] == "anchored"
    again = anchor_latest()
    assert again["anchor"]["id"] == done["anchor"]["id"]  # confirmed once; not written again
    from services.ledger_verify import verify_online

    assert verify_online(pack)["verdict"] == "verified"


def test_an_anchor_that_raises_is_recorded_as_failed(ledger, monkeypatch):  # noqa: F811
    from services import audit_anchor, ledger_anchor

    monkeypatch.setattr(ledger_anchor, "_audit", lambda *_a: None)
    monkeypatch.setattr(audit_anchor, "anchor_digest",
                        lambda *_a, **_k: (_ for _ in ()).throw(ConnectionError("s3.internal:9000 refused")))
    pack = _export("job-raise")
    receipt = pack["ledger"]["anchor"]
    assert receipt["status"] == "failed" and receipt["anchored"] is False
    assert receipt["reason"] == "the anchor could not be written (ConnectionError)"
