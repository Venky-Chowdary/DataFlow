"""Compatibility shim: canonical implementation now lives in services.sync_cursor."""
from __future__ import annotations

from services.sync_cursor import (
    INCREMENTAL_MODES,
    STORE_PATH,
    SyncContract,
    _load,
    _now,
    _save,
    build_cursor_key,
    compare_cursor_values,
    get_watermark,
    map_source_to_target,
    max_cursor_value,
    requires_incremental,
    requires_upsert,
    resolve_selected_sync_contracts,
    resolve_sync_contract,
    set_watermark,
)

__all__ = [
    "STORE_PATH",
    "INCREMENTAL_MODES",
    "_now",
    "SyncContract",
    "resolve_sync_contract",
    "resolve_selected_sync_contracts",
    "build_cursor_key",
    "_load",
    "_save",
    "get_watermark",
    "set_watermark",
    "max_cursor_value",
    "compare_cursor_values",
    "requires_incremental",
    "requires_upsert",
    "map_source_to_target",
]
