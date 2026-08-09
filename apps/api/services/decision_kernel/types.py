"""Kernel Type Engine facade — invent / width (Phase C2).

Call sites that need destination invent MUST import from
``services.decision_kernel`` (or this module). Implementation lives in
``type_invent``; ``type_system`` exposes thin backward-compat shims.
Lossy orchestrators remain on ``type_system`` until ``type_lossy`` extract.
"""

from __future__ import annotations

from services.decision_kernel.type_invent import (
    create_new_mapping_target_type,
    ddl_invent_never_narrower_than_table,
    ddl_type,
    float_width_carrier,
    integer_width_carrier,
    materialize_dest_ddl,
    normalize_logical_type,
)
from services.type_system import (
    is_lossy_coercion,
    is_precision_collapse_coercion,
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
