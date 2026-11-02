"""Kernel DDL identity facade (Phase C2).

Map → materialize → Execute fingerprint lives here as the mandated import path.
Implementation remains in ``services.conversion_contract`` until the god-module
split completes — never fork fingerprint logic in writers or the engine.
"""

from __future__ import annotations

from services.conversion_contract import (
    DdlIdentityError,
    approved_mapping_ddl_fingerprint,
    assert_ddl_identity,
    ddl_identity_columns,
    ddl_identity_divergence,
    ddl_identity_report,
)

__all__ = [
    "DdlIdentityError",
    "approved_mapping_ddl_fingerprint",
    "assert_ddl_identity",
    "ddl_identity_columns",
    "ddl_identity_divergence",
    "ddl_identity_report",
]
