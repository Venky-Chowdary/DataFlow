"""Follow-up review 5 (L5): librdkafka OAuth settings are refused at save.

librdkafka's OAUTHBEARER builds or fetches its token from its own settings:
``sasl.oauthbearer.method=oidc`` with ``sasl.oauthbearer.token.endpoint.url``
(a token request to any URL, with the client secret), ``sasl.oauthbearer.config``
(an unsecured JWT it signs itself), assertion files and cloud metadata
authentication. No product path builds a librdkafka config today, so they are
inert, but commit 19688c90e refused only the metadata authentication type. Now
every ``sasl.oauthbearer.*`` option (any spelling) is refused: a connector's
OAUTHBEARER token is ``extra.sasl_oauth_token``.
"""

from __future__ import annotations

import pytest


def _save(extra: dict) -> str:
    from connectors.kafka_sasl_policy import settings_refusal

    return settings_refusal({"type": "kafka", "host": "broker.example.com", "extra": extra})


@pytest.mark.parametrize("key,value", [
    ("sasl.oauthbearer.method", "oidc"),
    ("sasl.oauthbearer.method", "default"),
    ("sasl.oauthbearer.token.endpoint.url", "http://169.254.169.254/token"),
    ("sasl.oauthbearer.config", "principal=admin"),
    ("sasl.oauthbearer.client.id", "zz-client"),
    ("sasl.oauthbearer.client.secret", "zz-secret"),
    ("sasl.oauthbearer.scope", "zz-scope"),
    ("sasl.oauthbearer.extensions", "zz-ext"),
    ("SASL_OAUTHBEARER_METHOD", "oidc"),
    ("sasl-oauthbearer-token-endpoint-url", "http://zz-idp"),
])
def test_librdkafka_oauth_settings_are_refused(key, value):
    refusal = _save({"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": "own", key: value})
    assert refusal and value not in refusal
    assert "sasl_oauth_token" in refusal


def test_the_connectors_own_token_still_saves():
    assert _save({"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": "own"}) == ""
