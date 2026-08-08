"""Kernel Type Engine facade — invent / width / lossy (Phase C2 start).

Call sites that need destination invent or lossiness MUST import from
``services.decision_kernel`` (or this module). Do not fork invent tables in
writers. Implementation still lives in ``type_system`` until the god-module
split completes; this is the stable import surface.
"""

from __future__ import annotations

from services.type_system import (
    create_new_mapping_target_type,
    ddl_invent_never_narrower_than_table,
    ddl_type,
    float_width_carrier,
    integer_width_carrier,
    is_lossy_coercion,
    is_precision_collapse_coercion,
    materialize_dest_ddl,
    normalize_logical_type,
)

__all__ = [
    "create_new_mapping_target_type",
    "ddl_invent_never_narrower_than_table",
    "ddl_type",
    "float_width_carrier",
    "integer_width_carrier",
    "is_lossy_coercion",
    "is_precision_collapse_coercion",
    "materialize_dest_ddl",
    "normalize_logical_type",
]
