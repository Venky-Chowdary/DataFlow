"""Give a test the platform's AWS credential chain, hermetically.

``boto3.client(...)`` with no keys signs with ``boto3.DEFAULT_SESSION``, and
botocore caches the credentials that session resolves the first time it is
asked. Any earlier test (or a third-party library: the Snowflake connector's
``platform_detection.has_aws_identity`` calls ``boto3.client("sts")`` in a
worker thread) that builds a keyless client caches whatever ``AWS_*`` the
process had then, e.g. the suite's moto ``testing`` keys. Setting
``AWS_ACCESS_KEY_ID`` afterwards does not reach that cached session, so a
test that only set the env saw ``testing`` sign instead of its platform key,
depending on test order.

``use_platform_aws_chain`` sets the env chain and gives the test a fresh
default session (monkeypatched, so the suite's session comes back afterwards
and this test's credentials are not left cached for later tests).
"""

from __future__ import annotations

import pytest

PLATFORM_ACCESS_KEY = "PLATFORMKEY"
PLATFORM_SECRET_KEY = "platform-secret"

# A named profile removes botocore's env provider from the chain; a session
# token or a credential/config file could otherwise sign or join the chain.
_SHADOWING_ENV = (
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
)


def use_platform_aws_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the process's AWS credential chain resolve to ``PLATFORMKEY`` for this test only."""
    import boto3

    for name in _SHADOWING_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", PLATFORM_ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", PLATFORM_SECRET_KEY)
    # Never sign with a default session (and its cached credentials) built
    # before this test; the suite's own session is restored at teardown.
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
