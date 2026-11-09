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
