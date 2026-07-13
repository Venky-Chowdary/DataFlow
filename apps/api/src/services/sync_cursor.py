"""Compatibility shim: expose the top-level services.sync_cursor API."""

from services.sync_cursor import *  # noqa: F401,F403
from services.sync_cursor import (  # noqa: F401
    build_cursor_key,
    compare_cursor_values,
    get_watermark,
    map_source_to_target,
    max_cursor_value,
    requires_incremental,
    requires_upsert,
    resolve_sync_contract,
    set_watermark,
)
