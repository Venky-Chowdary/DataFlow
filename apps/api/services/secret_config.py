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
from typing import Any

SECRET_KEYS: frozenset[str] = frozenset(
    {
        "password",
        "connection_string",
        "api_key",
        "service_account",
        "private_key",
        "private_key_passphrase",
        "token",
        "access_token",
        "refresh_token",
        "secret_key",
        "client_secret",
    }
)


def redact_config(cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Copy of ``cfg`` with every known secret value replaced by ``***``."""
    return {
        k: ("***" if k in SECRET_KEYS and v not in (None, "") else v)
        for k, v in cfg.items()
    }


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
    """Secret-carrying probe config for an endpoint, safe to place in metadata."""
    return RedactedConfig(
        {
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
