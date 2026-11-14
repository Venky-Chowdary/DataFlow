"""Follow-up review 4 (M): a Kafka connector cannot use the API host's identity or name its files.

Every product Kafka client (consumer, producer, admin, privilege probe, Gate-8
COUNT, the SASL check) authenticates through
``copy_kafka_common.kafka_sasl_kwargs``, which passed the connector's declared
``sasl_mechanism`` (``extra``, the top level or the form's ``database`` slot)
straight to kafka-python:

* ``GSSAPI`` logs in with the API host's Kerberos ticket cache / keytab;
* ``AWS_MSK_IAM`` (kafka-python 3) signs the login with the API process's AWS
  credential chain (``botocore.session.Session().get_credentials()``) and sends
  it to the tenant's broker;
* ``OAUTHBEARER`` had no token provider at all, and a provider that fetches a
  token from the environment or a cloud identity would be the host's.

Now a tenant connector may use PLAIN, SCRAM-SHA-256 / 512, and OAUTHBEARER
only with its own token (``extra.sasl_oauth_token``, a static provider).
GSSAPI needs the operator opt-in ``DATAFLOW_ALLOW_PLATFORM_KERBEROS=1``
(startup warning, ``platform_kerberos`` in the security-settings snapshot);
AWS_MSK_IAM needs ``DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS=1``; anything else
is refused. Checked at save and where the client kwargs are built.

Kafka client options that name host files (librdkafka ``ssl.*.location``,
``sasl.kerberos.keytab``; kafka-python ``ssl_cafile`` / ``ssl_certfile`` /
``ssl_keyfile`` / ``ssl_crlfile``) are refused at save unless the file is in
an area a connector may open (``tenant_host_files``); Kerberos settings need
the Kerberos opt-in; ``sasl.kerberos.kinit.cmd`` (a command the client runs)
and ``plugin.library.paths`` (libraries it loads) are always refused.
Messages name the option and the remedy, never a value.
"""

from __future__ import annotations

import logging

import pytest

_CREDS = {"username": "tenant-user", "password": "tenant-pass"}


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ALLOW_PLATFORM_KERBEROS", "ALLOW_PLATFORM_AWS_CREDENTIALS", "SOURCE_PATH_ROOTS",
                     "EXPORTS_DIR", "UPLOAD_DIR"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    (allowed / "ca.pem").write_text("ca", encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", str(allowed))
    host = tmp_path / "host"
    host.mkdir()
    (host / "krb5.keytab").write_text("platform keytab", encoding="utf-8")
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )
    return {"allowed": allowed / "ca.pem", "host": host / "krb5.keytab"}


def _kwargs(**cfg):
    from services.copy_kafka_common import kafka_client_kwargs

    return kafka_client_kwargs({"host": "broker.example.com", "port": 9093, **cfg})


def _save(**fields):
    from services.egress_guard import connector_save_refusal

    return connector_save_refusal({"name": "k", "type": "kafka", "host": "broker.example.com", "port": 9093,
                                   **fields})


# --- SASL mechanisms -----------------------------------------------------------------------------

_GSSAPI_SPELLINGS = [
    {"extra": {"sasl_mechanism": "GSSAPI"}},
    {"extra": {"sasl_mechanism": "gssapi"}},
    {"sasl_mechanism": "GSSAPI"},
    {"database": "GSSAPI"},
]


@pytest.mark.parametrize("declared", _GSSAPI_SPELLINGS)
def test_gssapi_is_refused_without_the_kerberos_opt_in(declared):
    with pytest.raises(ValueError, match="DATAFLOW_ALLOW_PLATFORM_KERBEROS") as caught:
        _kwargs(**_CREDS, **declared)
    assert "tenant-pass" not in str(caught.value)
    refusal = _save(**_CREDS, **declared)
    assert "DATAFLOW_ALLOW_PLATFORM_KERBEROS" in refusal and "tenant-pass" not in refusal


def test_gssapi_is_allowed_with_the_kerberos_opt_in(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_KERBEROS", "1")
    kwargs = _kwargs(**_CREDS, extra={"sasl_mechanism": "GSSAPI"})
    assert kwargs["sasl_mechanism"] == "GSSAPI"
    assert kwargs["sasl_kerberos_service_name"] == "kafka"
    assert _save(**_CREDS, extra={"sasl_mechanism": "GSSAPI"}) == ""


def test_aws_msk_iam_never_signs_with_the_platform_chain(monkeypatch):
    """kafka-python's AWS_MSK_IAM takes botocore's default credentials: the API's."""
    from botocore.session import Session

    used: list[str] = []
    monkeypatch.setattr(Session, "get_credentials", lambda self: used.append("platform chain") or None)
    declared = {"extra": {"sasl_mechanism": "AWS_MSK_IAM", "security_protocol": "SASL_SSL"}}
    with pytest.raises(ValueError, match="DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS"):
        kwargs = _kwargs(**_CREDS, **declared)
        from kafka.net.sasl import get_sasl_mechanism

        mechanism = get_sasl_mechanism(kwargs["sasl_mechanism"])
        try:
            mechanism(host="broker.example.com", **kwargs)
        except AttributeError:  # no credentials object in this test: the chain was asked
            pass
    assert used == []
    assert "DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS" in _save(**_CREDS, **declared)


def test_aws_msk_iam_with_the_aws_opt_in(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS", "1")
    declared = {"extra": {"sasl_mechanism": "AWS_MSK_IAM", "security_protocol": "SASL_SSL"}}
    assert _kwargs(**_CREDS, **declared)["sasl_mechanism"] == "AWS_MSK_IAM"
    assert _save(**_CREDS, **declared) == ""


def test_oauthbearer_needs_the_connectors_own_token():
    with pytest.raises(ValueError, match="sasl_oauth_token"):
        _kwargs(**_CREDS, extra={"sasl_mechanism": "OAUTHBEARER"})
    assert "sasl_oauth_token" in _save(**_CREDS, extra={"sasl_mechanism": "OAUTHBEARER"})


def test_oauthbearer_with_a_token_uses_a_static_provider():
    kwargs = _kwargs(extra={"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": "tenant-jwt"})
    assert kwargs["sasl_mechanism"] == "OAUTHBEARER"
    provider = kwargs["sasl_oauth_token_provider"]
    assert provider.token() == "tenant-jwt"
    from kafka.net.sasl.oauth import AbstractTokenProvider

    assert isinstance(provider, AbstractTokenProvider)
    assert _save(extra={"sasl_mechanism": "OAUTHBEARER", "sasl_oauth_token": "tenant-jwt"}) == ""


@pytest.mark.parametrize("mechanism", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
def test_password_mechanisms_are_unchanged(mechanism):
    kwargs = _kwargs(**_CREDS, extra={"sasl_mechanism": mechanism})
    assert kwargs["sasl_mechanism"] == mechanism
    assert kwargs["sasl_plain_username"] == "tenant-user"
    assert _save(**_CREDS, extra={"sasl_mechanism": mechanism}) == ""


def test_no_login_declared_stays_plaintext():
    assert "sasl_mechanism" not in _kwargs()


@pytest.mark.parametrize("mechanism", ["SSPI", "OAUTHBEARER-AZURE", "x"])
def test_an_unknown_mechanism_is_refused(mechanism):
    with pytest.raises(ValueError, match="(?i)sasl mechanism"):
        _kwargs(**_CREDS, extra={"sasl_mechanism": mechanism})
    assert "SASL mechanism" in _save(**_CREDS, extra={"sasl_mechanism": mechanism})


def test_a_refused_mechanism_fails_the_connection_test_with_the_message():
    from connectors.kafka_writer import test_kafka

    ok, message = test_kafka(host="broker.example.com", port=9093, **_CREDS, extra={"sasl_mechanism": "GSSAPI"})
    assert ok is False and "DATAFLOW_ALLOW_PLATFORM_KERBEROS" in message


# --- client options that name host files --------------------------------------------------------

_FILE_KEYS = [
    "ssl.ca.location", "ssl.certificate.location", "ssl.key.location", "ssl.crl.location",
    "ssl.keystore.location", "ssl_cafile", "ssl_certfile", "ssl_keyfile", "ssl_crlfile", "SSL_CAFILE",
    "ssl-ca-location",
]


@pytest.mark.parametrize("key", _FILE_KEYS)
@pytest.mark.parametrize("where", ["extra", "connect_args", "nested", "top"])
def test_a_host_file_option_is_refused_at_save(env, key, where):
    value = str(env["host"])
    if where == "extra":
        fields = {"extra": {key: value}}
    elif where == "connect_args":
        fields = {"connect_args": {key: value}}
    elif where == "nested":
        fields = {"extra": {"consumer_config": {key: value}}}
    else:
        fields = {key: value}
    refusal = _save(**fields)
    assert refusal and value not in refusal
    assert "DATAFLOW_SOURCE_PATH_ROOTS" in refusal


@pytest.mark.parametrize("key", ["ssl.ca.location", "ssl_cafile"])
def test_a_file_in_an_allowed_area_is_accepted(env, key):
    assert _save(extra={key: str(env["allowed"])}) == ""


@pytest.mark.parametrize("key", ["sasl.kerberos.kinit.cmd", "plugin.library.paths", "sasl_kerberos_kinit_cmd"])
def test_commands_and_libraries_are_always_refused(env, monkeypatch, key):
    monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_KERBEROS", "1")
    refusal = _save(extra={key: str(env["allowed"])})
    assert refusal and str(env["allowed"]) not in refusal


@pytest.mark.parametrize(
    "key", ["sasl.kerberos.service.name", "sasl.kerberos.principal", "sasl_kerberos_service_name",
            "sasl_kerberos_domain_name", "sasl.kerberos.keytab"],
)
def test_kerberos_settings_need_the_kerberos_opt_in(env, key):
    refusal = _save(extra={key: "kafka"})
    assert "DATAFLOW_ALLOW_PLATFORM_KERBEROS" in refusal


def test_a_keytab_with_the_opt_in_is_judged_as_a_file(env, monkeypatch):
    monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_KERBEROS", "1")
    assert _save(extra={"sasl.kerberos.keytab": str(env["host"])})
    assert _save(extra={"sasl.kerberos.keytab": str(env["allowed"])}) == ""
    assert _save(extra={"sasl.kerberos.service.name": "kafka"}) == ""


def test_the_client_kwargs_check_refuses_host_files(env):
    from connectors.kafka_sasl_policy import check_client_kwargs

    with pytest.raises(ValueError):
        check_client_kwargs({"bootstrap_servers": "b:9092", "ssl_cafile": str(env["host"])})
    check_client_kwargs({"bootstrap_servers": "b:9092", "ssl_cafile": str(env["allowed"])})


# --- the operator opt-in -------------------------------------------------------------------------


def test_the_opt_in_is_in_the_snapshot_and_warned(monkeypatch, caplog):
    from connectors.kafka_sasl_policy import platform_kerberos_settings, warn_if_platform_kerberos_allowed

    logger = logging.getLogger("datawrap.security.test")
    assert platform_kerberos_settings() == {"platform_kerberos": "refused"}
    with caplog.at_level(logging.WARNING, logger=logger.name):
        warn_if_platform_kerberos_allowed(logger)
    assert not caplog.records
    monkeypatch.setenv("DATAFLOW_ALLOW_PLATFORM_KERBEROS", "1")
    assert platform_kerberos_settings() == {"platform_kerberos": "allowed"}
    with caplog.at_level(logging.WARNING, logger=logger.name):
        warn_if_platform_kerberos_allowed(logger)
    assert "DATAFLOW_ALLOW_PLATFORM_KERBEROS" in caplog.text
