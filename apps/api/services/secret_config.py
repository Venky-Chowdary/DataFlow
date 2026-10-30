"""Connection config that carries secrets without leaking them into output.

Preflight has to hand a live destination connection down to the collision probe
and the engine, which means the password and connection string travel inside a
general-purpose metadata dict. That dict is logged, stamped into gate details,
and returned through the Studio router, so a plain ``dict`` is one careless
``logger.info("%s", meta)`` away from printing credentials.

``RedactedConfig`` behaves exactly like the mapping consumers already expect
(``dict(cfg)``, ``cfg.get("password")``, iteration) but renders as a redacted
placeholder and refuses JSON serialization, so an accidental log line or
response body fails loudly instead of leaking.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from services.log_redaction import is_sensitive_key


def redact_config(cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Copy of ``cfg`` with every known secret value replaced by ``***``."""
    return {
        k: ("***" if is_sensitive_key(k) and v not in (None, "") else v)
        for k, v in cfg.items()
    }


def redact_url(url: str) -> str:
    """Redact secret-named query parameters and URL user information."""
    parsed = urlsplit(str(url))
    if not parsed.query and "@" not in parsed.netloc:
        return str(url)

    query: list[tuple[str, str]] = []
    for name, value in parse_qsl(parsed.query, keep_blank_values=True):
        if is_sensitive_key(name) and value not in (None, ""):
            value = "***"
        query.append((name, value))

    netloc = parsed.netloc
    if "@" in netloc:
        _, host = netloc.rsplit("@", 1)
        netloc = f"***@{host}"
    return urlunsplit(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            urlencode(query, doseq=True),
            parsed.fragment,
        )
    )


def mask_secrets_in_text(text: str) -> str:
    """Mask URI credentials and secret query values embedded in free text."""
    masked = re.sub(r":([^:@/]+)@", ":****@", str(text))
    url_pattern = re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s<>\"'`]+", re.IGNORECASE)

    def redact(match: re.Match[str]) -> str:
        candidate = match.group(0)
        trailing = ""
        while candidate and candidate[-1] in ".,;:!?)]}":
            trailing = candidate[-1] + trailing
            candidate = candidate[:-1]
        if not candidate:
            return match.group(0)

        parsed = urlsplit(candidate)
        netloc = parsed.netloc
        if "@" in netloc:
            userinfo, host = netloc.rsplit("@", 1)
            if ":" in userinfo:
                username, _, _password = userinfo.partition(":")
                netloc = f"{username}:****@{host}"
            else:
                netloc = f"***@{host}"

        query_url = urlunsplit(
            (parsed.scheme, "mcp-redaction.invalid", parsed.path, parsed.query, parsed.fragment)
        )
        redacted_query = urlsplit(redact_url(query_url)).query
        return (
            urlunsplit((parsed.scheme, netloc, parsed.path, redacted_query, parsed.fragment))
            + trailing
        )

    return url_pattern.sub(redact, masked)


class RedactedConfig(Mapping[str, Any]):
    """Read-only connection config whose repr never shows its secrets."""

    __slots__ = ("_cfg",)

    def __init__(self, cfg: Mapping[str, Any]) -> None:
        self._cfg: dict[str, Any] = dict(cfg)

    def __getitem__(self, key: str) -> Any:
        return self._cfg[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._cfg)

    def __len__(self) -> int:
        return len(self._cfg)

    def __repr__(self) -> str:
        return f"RedactedConfig({redact_config(self._cfg)!r})"

    __str__ = __repr__

    def redacted(self) -> dict[str, Any]:
        """Safe-to-log / safe-to-return view."""
        return redact_config(self._cfg)


def probe_config_from_endpoint(db_type: str, endpoint: Any) -> RedactedConfig:
    """Secret-carrying probe config for an endpoint, safe to place in metadata.

    TLS / driver keywords the endpoint carries under ``extra`` (for example
    SQL Server ``trust_server_certificate``) ride on top level: a probe that
    opened a stricter handshake than the writer failed, and the destination
    FK orphan probe refused every SQL Server run (D112).
    """
    from connectors.generic_sql import connection_options

    extra = getattr(endpoint, "extra", None)
    tls = connection_options(dict(extra) if isinstance(extra, dict) else {})
    return RedactedConfig(
        {
            **tls,
            "type": db_type,
            "host": endpoint.host,
            "port": endpoint.port,
            "database": endpoint.database,
            "schema": endpoint.schema,
            "username": endpoint.username,
            "password": endpoint.password,
            "connection_string": endpoint.connection_string,
            "auth_source": endpoint.auth_source,
        }
    )
