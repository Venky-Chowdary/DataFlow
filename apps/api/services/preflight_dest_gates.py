"""Validate gates about the destination object itself, built together.

``g_dest_requires_primary_key`` (D396 / D377): a keyless write into a server
that forces a primary key needs the approved ``_dw_row_id`` surrogate.

``g_overwrite_governance`` (NF-1): what a Snowflake / BigQuery / Elasticsearch
overwrite keeps of the object's grants, policies, tags, IAM and aliases; a
measured loss blocks until the destination DDL is approved.

Both read the same approval (``ddl_changes_acknowledged``) and the same
destination facts, so Validate builds them in one call.
"""

from __future__ import annotations

from typing import Any


def build_dest_object_gates(
    *,
    connected: bool,
    acknowledged: bool,
    destination_table: str,
    dest_db_type: str | None,
    destination_config: dict[str, Any] | None,
    table_exists: bool | None,
    sync_mode: str | None,
    key_hint: str | None,
    mappings: Any,
    source: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """``(require_pk_gate, overwrite_governance_gate)``; either may be ``None``."""
    from services.dest_require_primary_key import preflight_require_primary_key_gate
    from services.overwrite_governance_gate import build_overwrite_governance_gate

    require_pk_gate = preflight_require_primary_key_gate(
        connected=connected,
        acknowledged=acknowledged,
        destination_table=destination_table,
        dest_db_type=dest_db_type,
        destination_config=destination_config,
        table_exists=table_exists,
        sync_mode=sync_mode,
        key_hint=key_hint,
        mappings=mappings,
        source=source,
    )
    overwrite_governance_gate = build_overwrite_governance_gate(
        sync_mode=sync_mode,
        db_type=dest_db_type,
        table_exists=table_exists,
        destination_config=dict(destination_config or {}),
        destination_table=destination_table,
        acknowledged=acknowledged,
    )
    return require_pk_gate, overwrite_governance_gate
