"""Capabilities we do not ship — generated from the modules that make it true.

Google / Microsoft operators ask competitor-adjacent questions first: dbt
Cloud, SSH tunnels, bastion hops. Answering those with a nearby Postgres
or Transfer Studio procedure is a silent lie. Each card below is read
from an enforcing signature or honesty flag so the spoken verdict cannot
drift from the engine.

These are **documented absences**, not invented products. dbt Cloud is
false in ``dbt_export`` honesty. SSH tunnel is absent from the Postgres
connect signature. SFTP is a file connector when the registry says so.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass


@dataclass(frozen=True)
class CapabilityCard:
    """One honest capability answer plus the module that makes it true."""

    title: str
    text: str
    source_module: str
    category: str = "product"


def _sftp_is_a_file_connector() -> bool:
    try:
        import registry

        formats = [
            str(getattr(item, "value", item)).lower()
            for item in getattr(registry, "FILE_FORMATS", ())
        ]
        return "sftp" in formats
    except Exception:
        return False


def postgres_connect_accepts_tunnel() -> bool:
    """Whether the Postgres probe signature takes a tunnel / bastion field.

    ``test_postgresql`` is the operator-facing connect path. A tunnel
    parameter appearing there is the day this card must flip. Today it
    does not — host, port, database, credentials, SSL only.
    """
    try:
        from connectors.postgresql import test_postgresql
    except Exception:
        return False
    names = {name.lower() for name in inspect.signature(test_postgresql).parameters}
    return bool(
        names
        & {
            "ssh_tunnel",
            "tunnel_host",
            "tunnel_port",
            "bastion",
            "ssh_host",
            "jump_host",
        }
    )


def dbt_cloud_shipped() -> bool:
    """Whether the export hook claims dbt Cloud / managed ELT."""
    try:
        from services.dbt_export import dbt_export_honesty
    except Exception:
        return False
    honesty = dbt_export_honesty()
    return bool(honesty.get("is_dbt_cloud") or honesty.get("is_managed_elt"))


def dbt_card() -> CapabilityCard | None:
    """Complement hook, not a dbt Cloud product — from export honesty."""
    try:
        from services.dbt_export import dbt_export_honesty
    except Exception:
        return None
    honesty = dbt_export_honesty()
    if honesty.get("is_dbt_cloud") or honesty.get("is_managed_elt"):
        return None
    return CapabilityCard(
        title="Does Datawrap run dbt Cloud",
        text=(
            "Datawrap does not run dbt Cloud or use dbt as the transfer "
            "engine — transform projects can export a dbt starter pack "
            "(sources and models) as a complement hook after a governed "
            "load, and is_dbt_cloud is false. "
            "Map, Validate, and Execute stay on Datawrap; the export is "
            "not Gate-8 reconcile evidence."
        ),
        source_module="services/dbt_export.py · dbt_export_honesty",
        category="product",
    )


def ssh_tunnel_card() -> CapabilityCard | None:
    """No SSH tunnel on the Postgres connect path; SFTP is a file connector."""
    if postgres_connect_accepts_tunnel():
        return None
    sftp = (
        "SFTP is a file connector, not a bastion or tunnel in front of a warehouse."
        if _sftp_is_a_file_connector()
        else "Database connections are not tunneled through SSH."
    )
    return CapabilityCard(
        title="Does Datawrap open SSH tunnels",
        text=(
            "Datawrap does not open SSH tunnels for database connections — "
            "Postgres and MySQL take host, port, and credentials directly, "
            "with no bastion or jump host on the connect path. "
            f"{sftp}"
        ),
        source_module="connectors/postgresql.py · test_postgresql",
        category="connectors",
    )


def capability_cards() -> tuple[CapabilityCard, ...]:
    """Every honest absence the chatbot is allowed to speak."""
    cards: list[CapabilityCard] = []
    for builder in (dbt_card, ssh_tunnel_card):
        card = builder()
        if card is not None:
            cards.append(card)
    return tuple(cards)
