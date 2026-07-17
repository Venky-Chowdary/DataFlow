"""Compatibility shim: canonical implementation now lives in services.driver_bootstrap."""
from __future__ import annotations

from services.driver_bootstrap import (
    _auto_install_enabled,
    _importable,
    _missing_drivers,
    driver_status,
    ensure_platform_drivers,
)

__all__ = ['_importable', '_missing_drivers', '_auto_install_enabled', 'ensure_platform_drivers', 'driver_status']
