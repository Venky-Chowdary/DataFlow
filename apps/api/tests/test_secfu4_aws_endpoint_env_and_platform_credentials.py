"""Security follow-up 4: connector AWS clients ignore the process's endpoint env and platform credentials.

``connectors.aws_common.boto3_client`` built every S3/DynamoDB connector client
with boto3's defaults, so ``AWS_ENDPOINT_URL`` / ``AWS_ENDPOINT_URL_<SERVICE>``
in the API's environment (an operator's LocalStack or MinIO for the platform's
own buckets) redirected a tenant connector with no endpoint, which the egress
guard judges as real AWS, to that private endpoint, signed with the tenant's
keys. And ``use_default_aws_credentials`` in a tenant's connector settings
(or its ``extra``) signed the tenant's requests with the platform's own AWS
credential chain.
"""

from __future__ import annotations

import pytest

from connectors.aws_common import boto3_client

_PRIVATE = "http://127.0.0.1:9"  # nothing listens there; the request is stopped before sending


class _Stop(Exception):
    pass


def _dialled_url(client, call) -> str:
    seen: list[str] = []

    def before_send(request, **_kwargs):
        seen.append(request.url)
        raise _Stop()

    client.meta.events.register("before-send", before_send)
    with pytest.raises(_Stop):
        call(client)
    return seen[0]


@pytest.fixture
def endpoint_env(monkeypatch):
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_DYNAMODB"):
        monkeypatch.setenv(name, _PRIVATE)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "PLATFORMKEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "platform-secret")


def _tenant(service: str, **extra) -> dict:
    return {"type": service, "host": "us-east-1", "username": "AKIATENANT", "password": "tenant-secret",
            "database": "bucket-a", **extra}


def test_a_tenant_s3_connector_with_no_endpoint_does_not_dial_the_process_endpoint(endpoint_env):
    client = boto3_client("s3", _tenant("s3"))
    assert "127.0.0.1" not in str(client.meta.endpoint_url)
    url = _dialled_url(client, lambda c: c.list_objects_v2(Bucket="bucket-a"))
    assert url.startswith("https://") and ".amazonaws.com" in url, url
    assert "127.0.0.1" not in url


def test_a_tenant_dynamodb_connector_with_no_endpoint_does_not_dial_the_process_endpoint(endpoint_env):
    client = boto3_client("dynamodb", _tenant("dynamodb"))
    url = _dialled_url(client, lambda c: c.list_tables())
    assert url.startswith("https://dynamodb.us-east-1.amazonaws.com"), url


def test_an_explicit_connector_endpoint_is_still_used(endpoint_env):
    client = boto3_client("s3", {**_tenant("s3"), "endpoint_url": "http://127.0.0.1:9100"})
    assert client.meta.endpoint_url == "http://127.0.0.1:9100"


def test_tenant_keys_sign_even_with_platform_keys_in_the_env(endpoint_env):
    client = boto3_client("s3", _tenant("s3"))
    assert client._request_signer._credentials.access_key == "AKIATENANT"


@pytest.mark.parametrize("value", [True, "true", "1", "yes"])
def test_tenant_config_cannot_switch_on_platform_credentials(endpoint_env, value):
    with pytest.raises(ValueError, match="use_default_aws_credentials") as caught:
        boto3_client("s3", _tenant("s3", use_default_aws_credentials=value))
    assert "access key" in str(caught.value)


def test_a_tenant_endpoints_extra_cannot_switch_on_platform_credentials(endpoint_env):
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database", format="dynamodb", host="us-east-1", username="AKIATENANT", password="s",
        table="t", extra={"use_default_aws_credentials": True},
    )
    with pytest.raises(ValueError, match="use_default_aws_credentials"):
        boto3_client("dynamodb", resolve_connector_config(endpoint))


@pytest.mark.parametrize("where", ["top", "extra"])
def test_saving_a_connector_with_platform_credentials_is_refused(where):
    from services.egress_guard import connector_save_refusal

    data = {"name": "s3", "type": "s3", "host": "us-east-1", "username": "AKIATENANT", "password": "s"}
    if where == "top":
        data["use_default_aws_credentials"] = True
    else:
        data["extra"] = {"use_default_aws_credentials": "true"}
    assert "use_default_aws_credentials" in connector_save_refusal(data)


@pytest.mark.parametrize("value", [False, "false", "0", "", None])
def test_an_off_value_is_not_refused_and_signs_with_the_tenant_keys(endpoint_env, value):
    client = boto3_client("s3", _tenant("s3", use_default_aws_credentials=value))
    assert client._request_signer._credentials.access_key == "AKIATENANT"


def test_platform_code_may_still_use_the_platform_chain(endpoint_env):
    import copy

    from connectors.aws_common import PLATFORM_DEFAULT_CREDENTIALS

    cfg = copy.deepcopy({"type": "s3", "host": "us-east-1", "use_default_aws_credentials": PLATFORM_DEFAULT_CREDENTIALS})
    client = boto3_client("s3", cfg)
    assert client._request_signer._credentials.access_key == "PLATFORMKEY"
    assert "127.0.0.1" not in str(client.meta.endpoint_url)
