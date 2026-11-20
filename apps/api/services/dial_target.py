"""Which host a saved connector actually dials.

Catalog forms for MongoDB prefill ``localhost``. Operators then paste an
Atlas ``mongodb+srv://`` URI. Egress was refusing that leftover loopback
field even though the URI names a public cluster, while the card still
read as a usable Atlas destination. The URI host is the dial target. A
connector that is only loopback stays loopback and is still refused.
"""

from __future__ import annotations

from typing import Any

LOOPBACK_HOSTS = frozenset({
    "localhost",
    "127.0.0.1",
    "::1",
    "0.0.0.0",
    "host.docker.internal",
})


def is_loopback_host(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    return text in LOOPBACK_HOSTS


def uri_host(connection_string: str) -> str:
    """Hostname of a login URL, or ``""`` when the text is not one.

    Uses the last-``@`` parser so a password that contains ``@`` is not
    mistaken for the host.
    """
    raw = (connection_string or "").strip()
    if "://" not in raw:
        return ""
    try:
        from connectors.url_authority import parse_url_authority

        return (parse_url_authority(raw).host or "").strip()
    except Exception:
        return ""


def reconcile_dial_host(cfg: dict[str, Any]) -> str:
    """Return the host that will be dialed and align ``cfg['host']`` with it.

    A non-loopback host inside the connection string or endpoint URL
    replaces a blank or loopback ``host`` field. A loopback-only
    connector is left as loopback. Nothing here invents ``localhost``.
    """
    uri = uri_host(str(cfg.get("connection_string") or "")) or uri_host(
        str(cfg.get("endpoint_url") or "")
    )
    field = str(cfg.get("host") or "").strip()
    if uri and not is_loopback_host(uri) and (not field or is_loopback_host(field)):
        cfg["host"] = uri
        return uri
    return field


def displayed_dial(host: str, connection_string: str = "", endpoint_url: str = "") -> tuple[str, bool]:
    """``(host, is_loopback)`` the operator should see. Secrets stay out."""
    cfg = {
        "host": host or "",
        "connection_string": connection_string or "",
        "endpoint_url": endpoint_url or "",
    }
    dial = reconcile_dial_host(cfg)
    return dial, bool(dial) and is_loopback_host(dial)


def default_dial_host(cfg: dict[str, Any], fmt: str = "") -> str:
    """The host a resolved config dials when the connector saved none.

    With a connection string the string names the host. HTTP SDK sources take
    their endpoint from the manifest; a stamped loopback host would replace it
    and fail as a non-URL base_url. BigQuery with an empty host is the Google
    API: inventing localhost made list and plan refuse a connector that had
    tested green (D387). An explicit emulator host is left as set. SQL engines
    keep the historical localhost default.
    """
    host = str(cfg.get("host") or "")
    if (cfg.get("connection_string") or "").strip():
        return host
    from connectors.sdk import get_sdk_connector
    from src.transfer.connector_capabilities import resolve_driver_type

    kind = str(cfg.get("type") or fmt or "").lower().strip()
    if resolve_driver_type(kind) == "bigquery":
        return host.strip()
    return host or ("" if get_sdk_connector(kind) is not None else "localhost")

