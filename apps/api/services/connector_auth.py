"""Engine login role vs topology role — never send source/destination/both to a driver."""

from __future__ import annotations

# SavedConnector.role is inventory topology, not a warehouse/login role.
TOPOLOGY_ROLES = frozenset({"source", "destination", "both", "src", "dest", "any"})


def engine_login_role(*candidates: str | None) -> str:
    """Return the first real engine/warehouse role. Topology tokens are ignored.

    Snowflake / Redshift / Databricks login ``role`` must never fall back to
    SavedConnector.role (``both``). That produces ``Role 'BOTH' is not granted``
    which operators read as a failed password.
    """
    for raw in candidates:
        value = str(raw or "").strip()
        if not value:
            continue
        if value.lower() in TOPOLOGY_ROLES:
            continue
        return value
    return ""
