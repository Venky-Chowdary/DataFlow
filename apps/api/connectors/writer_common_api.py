"""Stable public surface for shared writer helpers (Phase F8).

Writers and the transfer engine should import from here for quarantine,
LSN guards, and WriteResult. Avoid new deep imports into the 5k-line trunk
except when implementing a helper that belongs in the trunk.
"""

from __future__ import annotations

from connectors.writer_common import (
    WriteResult,
    append_write_quarantine_detail,
    compare_lsn,
    dedupe_rows,
    dedupe_rows_by_pk_and_lsn,
    extract_cdc_lsn,
    filter_stale_lsn_rows,
    gate8_writer_meta,
    lsn_is_newer,
    omit_missing_fields,
    reject_on_strict_policy,
    transform_error_policy,
)

__all__ = [
    "WriteResult",
    "append_write_quarantine_detail",
    "compare_lsn",
    "dedupe_rows",
    "dedupe_rows_by_pk_and_lsn",
    "extract_cdc_lsn",
    "filter_stale_lsn_rows",
    "gate8_writer_meta",
    "lsn_is_newer",
    "omit_missing_fields",
    "reject_on_strict_policy",
    "transform_error_policy",
]
