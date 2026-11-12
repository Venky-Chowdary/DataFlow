"""Security review round 2 (H3): an S3 write lands only in a bucket in the fenced region.

Residency judged the region boto3 signs for (``host = "eu-west-1"``), but the
bucket's own region decides where the bytes land: botocore's
``S3RegionRedirectorv2`` is registered on every S3 client, and after S3's 301
it re-sent the PutObject to ``bucket.s3.us-east-1.amazonaws.com``.

Now, in a fenced workspace, the first write to each bucket through an AWS S3
client checks the bucket's real region (``GetBucketLocation``, else
``HeadBucket``'s ``x-amz-bucket-region``) and refuses a mismatch or an
unreadable location with a residency error; and a write that S3 answers with a
redirect to another region is refused instead of re-sent. Driven entirely by
botocore events (``before-send``): no network.
"""

from __future__ import annotations

import pytest
from botocore.awsrequest import AWSResponse

from tenancy_world import tenancy_world  # noqa: F401

LOC = "<?xml version='1.0'?><LocationConstraint xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>{}</LocationConstraint>"


class _Raw:
    def stream(self, **_k):
        return iter([b""])


def _response(url, status, body=b"", headers=None):
    r = AWSResponse(url, status, headers or {}, _Raw())
    r._content = body
    return r


class FakeS3:
    """Answers each operation from a script and records every request sent."""

    def __init__(self, location="eu-west-1", head_region=None, put_redirect_to=None, location_denied=False):
        self.location = location
        self.head_region = head_region
        self.put_redirect_to = put_redirect_to
        self.location_denied = location_denied
        self.sent: list[tuple[str, str]] = []

    def __call__(self, request, **kwargs):
        op = kwargs.get("event_name", "").rsplit(".", 1)[-1]
        self.sent.append((op, request.url))
        if op == "GetBucketLocation":
            if self.location_denied:
                return _response(request.url, 403, b"<Error><Code>AccessDenied</Code><Message>no</Message></Error>")
            value = "" if self.location == "us-east-1" else self.location
            return _response(request.url, 200, LOC.format(value).encode())
        if op == "HeadBucket":
            headers = {"x-amz-bucket-region": self.head_region} if self.head_region else {}
            return _response(request.url, 200 if self.head_region else 403, b"", headers)
        if op == "PutObject":
            if self.put_redirect_to and sum(1 for o, _ in self.sent if o == "PutObject") == 1:
                body = (b"<Error><Code>PermanentRedirect</Code><Message>m</Message>"
                        b"<Endpoint>b.s3.%s.amazonaws.com</Endpoint><Bucket>b</Bucket></Error>" % self.put_redirect_to.encode())
                return _response(request.url, 301, body, {"x-amz-bucket-region": self.put_redirect_to})
            return _response(request.url, 200, b"", {"ETag": '"x"'})
        return _response(request.url, 200, b"")


@pytest.fixture
def fenced(tenancy_world, monkeypatch):
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    from services.tenant_store import create_tenant

    create_tenant(workspace_id=tenancy_world.workspace_a, name="EU", data_region="eu-west-1")
    return tenancy_world


def _client(fake, cfg=None):
    from connectors.aws_common import boto3_client

    client = boto3_client("s3", cfg or {"host": "eu-west-1", "username": "AKIAEXAMPLE", "password": "x"})
    client.meta.events.register_first("before-send.s3", fake)
    return client


def _put(client, bucket="b"):
    client.put_object(Bucket=bucket, Key="rows.csv", Body=b"id\n1\n")


def _in(world):
    from services.egress_guard import egress_workspace

    return egress_workspace(world.workspace_a)


def test_a_bucket_in_another_region_is_refused_before_any_write(fenced):
    fake = FakeS3(location="us-east-1")
    with _in(fenced):
        client = _client(fake)
        with pytest.raises(Exception, match="(?i)residency.*us-east-1|us-east-1.*residency"):
            _put(client)
    assert [op for op, _ in fake.sent if op == "PutObject"] == []


def test_a_bucket_in_the_fenced_region_is_written_once_and_checked_once(fenced):
    fake = FakeS3(location="eu-west-1")
    with _in(fenced):
        client = _client(fake)
        _put(client)
        _put(client)
    ops = [op for op, _ in fake.sent]
    assert ops.count("GetBucketLocation") == 1
    assert ops.count("PutObject") == 2


@pytest.mark.parametrize("head_region,allowed", [(None, False), ("us-east-1", False), ("eu-west-1", True)])
def test_an_unreadable_location_falls_back_to_head_bucket_then_refuses(fenced, head_region, allowed):
    fake = FakeS3(location_denied=True, head_region=head_region)
    with _in(fenced):
        client = _client(fake)
        if allowed:
            _put(client)
        else:
            with pytest.raises(Exception, match="(?i)residency"):
                _put(client)
    assert ("PutObject" in [op for op, _ in fake.sent]) is allowed


def test_a_redirect_to_another_region_is_refused_not_followed(fenced):
    """The bucket moved after the check: S3 answers 301 to us-east-1, the redirector would re-send."""
    fake = FakeS3(location="eu-west-1", put_redirect_to="us-east-1")
    with _in(fenced):
        client = _client(fake)
        with pytest.raises(Exception, match="(?i)residency"):
            _put(client)
    puts = [url for op, url in fake.sent if op == "PutObject"]
    assert len(puts) == 1 and "us-east-1" not in puts[0]


def test_an_unfenced_workspace_is_unchanged(fenced):
    fake = FakeS3(location="us-east-1", put_redirect_to="us-east-1")
    from services.egress_guard import egress_workspace

    with egress_workspace(fenced.workspace_b):
        client = _client(fake)
        _put(client)
    ops = [op for op, _ in fake.sent]
    assert "GetBucketLocation" not in ops
    assert ops.count("PutObject") == 2  # the redirector still follows S3's 301 here


def test_a_custom_endpoint_is_left_to_the_region_verification(fenced):
    """An S3-compatible store has no AWS bucket region; its verified stamp speaks for it."""
    fake = FakeS3(location="us-east-1")
    with _in(fenced):
        client = _client(fake, {"host": "", "endpoint_url": "https://storage.googleapis.com",
                                "username": "k", "password": "s"})
        _put(client)
    assert "GetBucketLocation" not in [op for op, _ in fake.sent]


def test_the_review_probe_case_end_to_end(fenced):
    """host=eu-west-1, bucket in us-east-1: residency at job start allows it; the write is refused."""
    from services import data_residency as dr
    from src.transfer.models import EndpointConfig

    ep = EndpointConfig.from_dict("database", {"format": "s3", "host": "eu-west-1", "database": "bucket-in-us-east-1", "table": "k"})
    assert dr.residency_refusal(fenced.workspace_a, ep) is None
    fake = FakeS3(location="us-east-1")
    with _in(fenced):
        with pytest.raises(Exception, match="(?i)residency"):
            _put(_client(fake), bucket="bucket-in-us-east-1")
    assert not any("us-east-1" in url for op, url in fake.sent if op == "PutObject")
