"""Compatibility shim for the shared platform driver bootstrap entry point.

The runtime server path imports modules through the top-level ``services`` namespace
while the implementation lives under ``src/services``. This wrapper keeps both
execution paths aligned and prevents the health endpoint from breaking during
startup.
"""

from __future__ import annotations

from src.services.driver_bootstrap import ensure_platform_drivers, driver_status

__all__ = ["ensure_platform_drivers", "driver_status"]
