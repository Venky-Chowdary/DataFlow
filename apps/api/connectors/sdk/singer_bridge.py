"""Singer tap/target bridge entrypoints for the Connector SDK."""

from __future__ import annotations

from connectors.sdk import SingerTapBridge, register_connector

__all__ = ["SingerTapBridge", "register_connector"]
