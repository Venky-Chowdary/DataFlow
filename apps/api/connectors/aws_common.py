"""Shared AWS client helpers for S3 and DynamoDB connectors."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

_REPEATED_SCHEME = re.compile(r"^(https?://)(?=https?://)", re.IGNORECASE)
_PORT_THEN_DEFAULT_TLS = re.compile(
    r"^(https?://)([^/:]+):(\d+):443/?$",
    re.IGNORECASE,
)


def aws_credentials(cfg: dict[str, Any]) -> tuple[str, str, str]:
    region = (cfg.get("host") or "").strip() or "us-east-1"
    access_key = (cfg.get("username") or "").strip()
    secret_key = (cfg.get("password") or "").strip()
    return region, access_key, secret_key


def _explicit_service_port(cfg: dict[str, Any]) -> int | None:
    """Port the operator typed, when it names a service rather than HTTPS.

    ``0`` and ``443`` stay "no custom endpoint" so a region token such as
    ``us-east-1`` is not turned into ``http://us-east-1:443``.
    """
    raw = cfg.get("port")
    if raw in (None, "", 0, "0"):
        return None
    try:
        port = int(raw)
    except (TypeError, ValueError):
        return None
    if port in (0, 443):
        return None
    return port


def normalize_service_endpoint(
    raw: str,
    *,
    port: Any = None,
    ssl: bool = False,
) -> str:
    """One URL: no doubled scheme, no ``:20988:443``.

    A saved tunnel ``http://bore.pub:20988`` plus the DynamoDB form default
    ``443`` used to become ``http://http://bore.pub:20988:443``. The host
    already carries its port; ``443`` is not a second port.
    """
    text = (raw or "").strip()
    while True:
        nxt = _REPEATED_SCHEME.sub("", text, count=1)
        if nxt == text:
            break
        text = nxt
    doubled = _PORT_THEN_DEFAULT_TLS.match(text)
    if doubled:
        text = f"{doubled.group(1)}{doubled.group(2)}:{doubled.group(3)}"
    if "://" not in text:
        scheme = "https" if ssl else "http"
        text = f"{scheme}://{text}"
    try:
        parsed = urlparse(text)
        hostname = parsed.hostname
        url_port = parsed.port
    except ValueError:
        hostname = None
        url_port = None
    if not hostname:
        # ``http://bore.pub:20988:443`` does not parse. Peel a trailing :443.
        peeled = _PORT_THEN_DEFAULT_TLS.match(text)
        if peeled:
            return f"{peeled.group(1)}{peeled.group(2)}:{peeled.group(3)}"
        return text.rstrip("/")
    if url_port:
        scheme = parsed.scheme or ("https" if ssl else "http")
        return f"{scheme}://{hostname}:{url_port}"
    extra = _explicit_service_port({"port": port})
    scheme = parsed.scheme or ("https" if ssl else "http")
    if extra:
        return f"{scheme}://{hostname}:{extra}"
    return f"{scheme}://{hostname}"


def resolve_endpoint_url(cfg: dict[str, Any]) -> str:
    """Custom endpoint for DynamoDB Local or private AWS-compatible stacks."""
    explicit = (cfg.get("endpoint_url") or cfg.get("connection_string") or "").strip()
    if explicit.startswith("http://") or explicit.startswith("https://"):
        return normalize_service_endpoint(
            explicit,
            port=cfg.get("port"),
            ssl=bool(cfg.get("ssl")),
        )
    host = (cfg.get("host") or "").strip()
    if host.startswith("http://") or host.startswith("https://"):
        return normalize_service_endpoint(
            host,
            port=cfg.get("port"),
            ssl=bool(cfg.get("ssl")),
        )
    if host.endswith(".amazonaws.com"):
        return f"https://{host}"
    # A host with no dots is an AWS region only when no service port was
    # given (``us-east-1``). ``minio`` + ``9000`` is a Docker endpoint;
    # treating it as a region dropped the host and port.
    # A host that already carries its port (``localhost:9100``, ``minio:9100``)
    # is handled below; appending the Port field here dialled
    # ``http://localhost:9100:9000`` (D142).
    if (
        host
        and "." not in host
        and ":" not in host
        and host not in ("localhost", "127.0.0.1", "host.docker.internal")
    ):
        port = _explicit_service_port(cfg)
        if port is None:
            return ""
        ssl = cfg.get("ssl", False)
        scheme = "https" if ssl else "http"
        return f"{scheme}://{host}:{port}"
    # If host already includes a port, extract it so we don't duplicate the port param.
    if ":" in host:
        host, _, port_from_host = host.rpartition(":")
        port = int(port_from_host) if port_from_host.isdigit() else cfg.get("port")
    else:
        port = cfg.get("port")
    if host and port:
        ssl = cfg.get("ssl", False)
        scheme = "https" if ssl else "http"
        return f"{scheme}://{host}:{int(port)}"
    if host in ("localhost", "127.0.0.1", "host.docker.internal") and port:
        return f"http://{host}:{int(port)}"
    return ""


def is_local_endpoint(cfg: dict[str, Any]) -> bool:
    endpoint = resolve_endpoint_url(cfg)
    if not endpoint:
        host = (cfg.get("host") or "").strip().lower()
        return host in ("localhost", "127.0.0.1", "host.docker.internal")
    parsed = urlparse(endpoint)
    return parsed.hostname in ("localhost", "127.0.0.1", "host.docker.internal")


def resolve_region(cfg: dict[str, Any]) -> str:
    raw = (cfg.get("host") or "").strip()
    is_url = raw.startswith("http://") or raw.startswith("https://")
    # D139: ``raw.split(":")[0]`` of an endpoint URL is its scheme, so
    # ``https://dynamodb.us-east-1.amazonaws.com`` or a DynamoDB Local URL
    # signed every request for region "https"/"http" and the probe failed.
    host = (urlparse(raw).hostname or "") if is_url else raw.split(":")[0]
    if is_url or host.endswith(".amazonaws.com"):
        if host == "s3.amazonaws.com":
            return "us-east-1"
        # Extract region from virtual-hosted style endpoints like s3.us-east-1.amazonaws.com
        parts = host.split(".")
        if host.endswith(".amazonaws.com") and len(parts) >= 3 and parts[-2] == "amazonaws":
            candidate = parts[-3]
            if candidate and candidate not in ("s3", "s3-website"):
                return candidate
        return "us-east-1"
    if host and host not in ("localhost", "127.0.0.1", "host.docker.internal"):
        return host
    return "us-east-1"


class _PlatformCredentials:
    """The marker for "sign with this process's own AWS credential chain".

    Only platform code sets it (``PLATFORM_DEFAULT_CREDENTIALS``); a request
    body, a saved connector or its ``extra`` can only hold JSON values, never
    this object, so tenant settings cannot switch it on. Copies keep identity.
    """

    __slots__ = ()

    def __copy__(self) -> "_PlatformCredentials":
        return self

    def __deepcopy__(self, _memo: Any) -> "_PlatformCredentials":
        return self

    def __reduce__(self) -> str:
        return "PLATFORM_DEFAULT_CREDENTIALS"

    def __repr__(self) -> str:
        return "PLATFORM_DEFAULT_CREDENTIALS"


PLATFORM_DEFAULT_CREDENTIALS = _PlatformCredentials()
_OFF_VALUES = frozenset({"", "0", "false", "no", "off", "none"})
DEFAULT_CREDENTIALS_MESSAGE = (
    "use_default_aws_credentials is not accepted in a connector's settings: Datawrap does not "
    "sign a workspace's requests with the platform's own AWS credentials. Enter this "
    "connector's access key and secret key instead."
)


def default_credentials_refusal(cfg: Any) -> str:
    """The refusal when tenant settings (top level or ``extra``) ask for the platform's
    AWS credential chain, else ``""``."""
    if not isinstance(cfg, dict):
        return ""
    extra = cfg.get("extra")
    for holder in (cfg, extra if isinstance(extra, dict) else {}):
        value = holder.get("use_default_aws_credentials")
        if value is PLATFORM_DEFAULT_CREDENTIALS or value is None or value is False:
            continue
        if str(value).strip().lower() not in _OFF_VALUES:
            return DEFAULT_CREDENTIALS_MESSAGE
    return ""


def _client_config(service: str, endpoint_url: str, cfg: dict[str, Any]) -> Any:
    """botocore ``Config`` that never takes an endpoint from the process environment.

    ``AWS_ENDPOINT_URL`` / ``AWS_ENDPOINT_URL_<SERVICE>`` and the shared config
    file configure the platform's own AWS clients; a connector with no endpoint
    means real AWS, which is what the egress guard judged.
    """
    from botocore.config import Config

    options: dict[str, Any] = {"ignore_configured_endpoint_urls": True}
    # MinIO and other custom endpoints require path-style addressing.
    # Virtual-hosted style looks up ``bucket.endpoint`` and listing fails
    # while a hand-typed GetObject can still succeed. Real AWS (no custom
    # endpoint) stays on the SDK default.
    if service == "s3" and (endpoint_url or cfg.get("path_style")):
        options["s3"] = {"addressing_style": "path"}
    return Config(**options)


def boto3_client(service: str, cfg: dict[str, Any], *, workspace_id: str = ""):
    import boto3

    refusal = default_credentials_refusal(cfg)
    if refusal:
        raise ValueError(refusal)
    region = resolve_region(cfg)
    endpoint_url = resolve_endpoint_url(cfg)
    kwargs: dict[str, Any] = {
        "region_name": region,
    }
    if cfg.get("use_default_aws_credentials") is not PLATFORM_DEFAULT_CREDENTIALS:
        kwargs["aws_access_key_id"] = (
            (cfg.get("username") or "").strip() or "local"
        )
        kwargs["aws_secret_access_key"] = (
            (cfg.get("password") or "").strip() or "local"
        )
    if endpoint_url:
        from services.egress_guard import assert_connector_dial_allowed

        # The guard skips a region label in ``host`` (``us-east-1``) that this
        # client only signs with: never when ``endpoint_url`` dials that name.
        assert_connector_dial_allowed(
            endpoint_url,
            str(cfg.get("host") or ""),
            workspace_id=str(cfg.get("workspace_id") or ""),
            db_type=service,
            aws_endpoint=endpoint_url,
        )
        kwargs["endpoint_url"] = endpoint_url
    kwargs["config"] = _client_config(service, endpoint_url, cfg)
    client = boto3.client(service, **kwargs)
    if service == "s3":
        from connectors.s3_residency import install_bucket_region_guard

        # The bucket's region decides where a write lands (review round 2, H3).
        install_bucket_region_guard(client, cfg, workspace_id=workspace_id)
    return client
