"""Loader/shim for the lineage_telemetry implementation in the src.services namespace.

The canonical implementation lives in ``apps/api/services/lineage_telemetry.py``.
This module re-exports it so the backend is safe regardless of whether
``apps/api`` or ``apps/api/src`` is on ``sys.path``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_real_path = Path(__file__).resolve().parents[2] / "services" / "lineage_telemetry.py"

spec = importlib.util.spec_from_file_location("_real_lineage_telemetry", _real_path)
if spec is None or spec.loader is None:
    raise ImportError(
        f"Could not load lineage_telemetry implementation from {_real_path}"
    )

_real = importlib.util.module_from_spec(spec)
sys.modules.setdefault("_real_lineage_telemetry", _real)
spec.loader.exec_module(_real)

__all__ = [name for name in dir(_real) if not name.startswith("_")]
for _name in __all__:
    globals()[_name] = getattr(_real, _name)
