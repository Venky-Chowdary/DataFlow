"""Kafka logins and client options a tenant connector may use (follow-up review 4, M).

Every product Kafka client authenticates through
``services.copy_kafka_common.kafka_sasl_kwargs``. kafka-python 3 reads the
login from the mechanism alone, and three mechanisms take an identity from the
API host instead of the connector:

* ``GSSAPI``: the process's Kerberos ticket cache or keytab;
* ``AWS_MSK_IAM``: ``botocore.session.Session().get_credentials()``, the
  platform's AWS chain, signed and sent to the tenant's broker;
* ``OAUTHBEARER``: whatever the token provider fetches; the only provider here
  is a static one that holds the connector's own token.

A tenant connector may use PLAIN, SCRAM-SHA-256 / -512 (its username and
password) and OAUTHBEARER with its own token (``extra.sasl_oauth_token``).
``GSSAPI`` needs ``DATAFLOW_ALLOW_PLATFORM_KERBEROS=1`` (a single-tenant
deployment whose connectors use the host's Kerberos identity: startup warning,
``platform_kerberos`` in the security-settings snapshot); ``AWS_MSK_IAM`` needs
``DATAFLOW_ALLOW_PLATFORM_AWS_CREDENTIALS=1``. Anything else is refused.

Client options that name a file on the host (librdkafka ``ssl.*.location``,
``sasl.kerberos.keytab``, the newer OAuth assertion files; kafka-python
``ssl_cafile`` / ``ssl_certfile`` / ``ssl_keyfile`` / ``ssl_crlfile``) must
point inside an area a connector may open (``services.tenant_host_files``);
Kerberos settings need the Kerberos opt-in; ``sasl.kerberos.kinit.cmd`` (a
command the client runs) and ``plugin.library.paths`` (libraries it loads) are
always refused, as is an OAuth token fetched with a cloud identity
(``sasl.oauthbearer.metadata.authentication.type``). Option names are folded
(case, ``.``, ``_``, ``-``) so no spelling hides one. Checked at save on the
connector's fields, ``extra`` and ``connect_args`` (nested mappings too), and on
the kwargs every client is built with. Messages name the option and the remedy,
never a value.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.brand_env import getenv_brand

FLAG = "DATAFLOW_ALLOW_PLATFORM_KERBEROS"
_ON = frozenset({"1", "true", "yes", "on"})
PASSWORD_MECHANISMS = frozenset({"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})
TOKEN_KEYS = ("sasl_oauth_token", "oauth_token")

#: Folded option names whose value is a file the Kafka client reads.
_FILE_OPTIONS = frozenset({
    "sslcalocation", "sslcertificatelocation", "sslkeylocation", "sslcrllocation", "sslkeystorelocation",
    "sslenginelocation", "saslkerberoskeytab", "sasloauthbearerassertionprivatekeyfile",
    "sasloauthbearerassertionjwttemplatefile", "sslcafile", "sslcertfile", "sslkeyfile", "sslcrlfile",
})
#: Never from a tenant: the client runs this command or loads these libraries.
_NEVER = frozenset({"saslkerberoskinitcmd", "pluginlibrarypaths", "sasloauthtokenprovider"})
_KERBEROS_PREFIX = "saslkerberos"
_OAUTH_IDENTITY = "sasloauthbearermetadataauthenticationtype"


def _fold(key: Any) -> str:
    from connectors.tls_file_options import _fold as fold

    return fold(key)


def _shown(key: Any) -> str:
    return "".join(ch for ch in str(key) if ch.isprintable())[:60]


def platform_kerberos_allowed() -> bool:
    return str(getenv_brand("ALLOW_PLATFORM_KERBEROS", "") or "").strip().lower() in _ON


def platform_kerberos_settings() -> dict[str, str]:
    """The entry of the startup security-settings snapshot."""
    return {"platform_kerberos": "allowed" if platform_kerberos_allowed() else "refused"}


def warn_if_platform_kerberos_allowed(logger: Any) -> None:
    if platform_kerberos_allowed():
        logger.warning(
            "%s is on: Kafka connectors may log in with SASL GSSAPI using this API process's "
            "Kerberos ticket cache or keytab, in every workspace. Use it only for a single-tenant "
            "deployment.", FLAG,
        )


def _kerberos_message(what: str) -> str:
    return (
        f"{what} would use this server's Kerberos identity (its ticket cache or keytab). Datawrap "
        "does not log a workspace's connector in as the platform. Use SASL PLAIN, SCRAM-SHA-256 / "
        "-512 or OAUTHBEARER with this connector's own credentials, or ask the operator to set "
        f"{FLAG}=1 (for a single-tenant deployment)."
    )


def oauth_token(cfg: Mapping[str, Any]) -> str:
    """The connector's own OAUTHBEARER token (``extra.sasl_oauth_token``), else ``""``."""
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), Mapping) else {}
    for source in (extra, cfg):
        for key in TOKEN_KEYS:
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def mechanism_refusal(mechanism: Any, cfg: Mapping[str, Any]) -> str:
    """Why this connector may not log in with ``mechanism``, else ``""``."""
    name = str(mechanism or "").strip().upper()
    if name in PASSWORD_MECHANISMS:
        return ""
    if name == "OAUTHBEARER":
        if oauth_token(cfg):
            return ""
        return (
            "Kafka SASL mechanism OAUTHBEARER needs this connector's own token: set "
            "extra.sasl_oauth_token. Datawrap does not fetch a token with this server's environment "
            "or cloud identity."
        )
    if name == "GSSAPI":
        return "" if platform_kerberos_allowed() else _kerberos_message("Kafka SASL mechanism GSSAPI")
    if name == "AWS_MSK_IAM":
        from services.platform_aws_credentials import platform_aws_credentials_allowed, platform_credentials_message

        if platform_aws_credentials_allowed():
            return ""
        return platform_credentials_message("Kafka SASL mechanism AWS_MSK_IAM signs with this server's AWS identity")
    return (
        f"Kafka SASL mechanism {_shown(name)[:30]} is not supported for a connector. Use PLAIN, "
        "SCRAM-SHA-256, SCRAM-SHA-512 or OAUTHBEARER (with extra.sasl_oauth_token)."
    )


def _static_token_provider(token: str) -> Any:
    try:
        from kafka.net.sasl.oauth import AbstractTokenProvider
    except ImportError:  # kafka-python 2.x
        from kafka.sasl.oauth import AbstractTokenProvider  # type: ignore[no-redef]

    class ConnectorToken(AbstractTokenProvider):
        """The connector's own token, never one fetched from the host."""

        def __init__(self) -> None:
            super().__init__()

        def token(self) -> str:
            return token

    return ConnectorToken()


def mechanism_kwargs(mechanism: str, cfg: Mapping[str, Any]) -> dict[str, Any]:
    """kafka-python kwargs for a non-password mechanism the policy allows."""
    if mechanism == "OAUTHBEARER":
        return {"sasl_oauth_token_provider": _static_token_provider(oauth_token(cfg))}
    if mechanism == "GSSAPI":
        extra = cfg.get("extra") if isinstance(cfg.get("extra"), Mapping) else {}
        service = str(extra.get("sasl_kerberos_service_name") or "kafka").strip() or "kafka"
        return {"sasl_kerberos_service_name": service}
    return {}


def _option_refusal(key: Any, value: Any, where: str) -> str:
    folded = _fold(key)
    if folded in _NEVER:
        return (
            f"Kafka client option {_shown(key)} {where} makes the client run a command or load a "
            "library on this server. Datawrap does not accept it from a connector; remove it."
        )
    if folded == _OAUTH_IDENTITY and str(value or "").strip().lower() not in {"", "none"}:
        return (
            f"Kafka client option {_shown(key)} {where} would fetch a token with this server's cloud "
            "identity. Use this connector's own token (extra.sasl_oauth_token)."
        )
    if folded.startswith(_KERBEROS_PREFIX) and not platform_kerberos_allowed():
        return _kerberos_message(f"Kafka client option {_shown(key)} {where}")
    if folded in _FILE_OPTIONS:
        from connectors.tls_file_options import _path_refused

        if _path_refused(value):
            return (
                f"Kafka client option {_shown(key)} {where} names a file outside the folders "
                "connectors may open on this server. Upload the file in this workspace and use that "
                "upload, or ask the operator to list its folder in DATAFLOW_SOURCE_PATH_ROOTS."
            )
    return ""


def mapping_refusal(source: Any, where: str, depth: int = 0) -> str:
    """The first refused option in ``source`` (and the mappings nested in it)."""
    if not isinstance(source, Mapping) or depth > 3:
        return ""
    for key, value in source.items():
        refusal = _option_refusal(key, value, where)
        if not refusal and isinstance(value, Mapping):
            refusal = mapping_refusal(value, where, depth + 1)
        if refusal:
            return refusal
    return ""


def _text_refusal(text: Any, where: str) -> str:
    from connectors.tls_file_options import _readings

    raw = str(text or "")
    if "=" not in raw:
        return ""
    for pairs in _readings(raw):
        for key, value in pairs:
            refusal = _option_refusal(key, value, where)
            if refusal:
                return refusal
    return ""


def settings_refusal(data: Any) -> str:
    """At save: a refused login or client option in the connector's settings, else ``""``."""
    if not isinstance(data, Mapping):
        return ""
    refusal = (
        mapping_refusal(data, "in this connector's settings")
        or mapping_refusal(data.get("connect_args"), "in this connector's connect_args")
        or _text_refusal(data.get("connection_string"), "in this connector's connection string")
    )
    if refusal:
        return refusal
    from services.copy_kafka_common import kafka_family_name, kafka_security_options

    if kafka_family_name(str(data.get("type") or data.get("format") or "")) != "kafka":
        return ""
    # Read as every client reads it (top level, ``extra``, the form's ``database`` slot).
    mechanism = kafka_security_options(dict(data)).get("sasl_mechanism", "")
    return mechanism_refusal(mechanism, data) if mechanism else ""


def check_client_kwargs(kwargs: Mapping[str, Any]) -> None:
    """Raise ``ValueError`` before a Kafka client is built with a refused option."""
    refusal = mapping_refusal({k: v for k, v in kwargs.items() if k != "sasl_oauth_token_provider"},
                              "for this connection")
    mechanism = kwargs.get("sasl_mechanism")
    if not refusal and mechanism:
        refusal = mechanism_refusal(mechanism, {"extra": {"sasl_oauth_token": "provided"}}
                                    if kwargs.get("sasl_oauth_token_provider") is not None else {})
    if refusal:
        raise ValueError(refusal)
