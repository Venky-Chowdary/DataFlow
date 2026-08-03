"""Production-safe example phrases for Pilot compose / clarify errors.

Never hardcode fixture names like "Local Postgres" when the workspace already
has saved connectors — operators in prod see their real names.
"""

from __future__ import annotations

from typing import Any


def example_connector_name(
    ctx: dict[str, Any] | None = None,
    *,
    fallback: str = "your connector",
) -> str:
    """Prefer live connector names from chat context, then the connector store."""
    for bucket in (
        (ctx or {}).get("connectors"),
        (ctx or {}).get("saved_connectors"),
    ):
        if not isinstance(bucket, list):
            continue
        for c in bucket:
            if isinstance(c, dict):
                name = str(c.get("name") or "").strip()
                if name:
                    return name
            else:
                name = str(getattr(c, "name", "") or "").strip()
                if name:
                    return name

    try:
        from services.connector_store import list_connectors

        for c in list_connectors() or []:
            name = str(getattr(c, "name", None) or (c.get("name") if isinstance(c, dict) else "") or "").strip()
            if name:
                return name
    except Exception:
        pass
    return fallback


def example_dest_connector_name(
    ctx: dict[str, Any] | None = None,
    *,
    source_hint: str = "",
    fallback: str = "your destination",
) -> str:
    """Second saved connector for transfer examples (skip the source when possible)."""
    names: list[str] = []
    for bucket in (
        (ctx or {}).get("connectors"),
        (ctx or {}).get("saved_connectors"),
    ):
        if not isinstance(bucket, list):
            continue
        for c in bucket:
            if isinstance(c, dict):
                name = str(c.get("name") or "").strip()
            else:
                name = str(getattr(c, "name", "") or "").strip()
            if name and name not in names:
                names.append(name)

    if not names:
        try:
            from services.connector_store import list_connectors

            for c in list_connectors() or []:
                name = str(getattr(c, "name", None) or (c.get("name") if isinstance(c, dict) else "") or "").strip()
                if name and name not in names:
                    names.append(name)
        except Exception:
            pass

    hint = (source_hint or "").strip().lower()
    for name in names:
        if hint and name.lower() == hint:
            continue
        if hint and name.lower() == example_connector_name(ctx).lower():
            continue
        return name
    if len(names) >= 2:
        return names[1]
    return fallback
