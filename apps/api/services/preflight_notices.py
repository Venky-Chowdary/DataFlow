"""The plan-time notices every preflight result carries, attached in one place.

Each notice module owns its own rule and wording (Oracle '' as NULL, a
directly addressed dated file, the mirror delete mode, a schemaless source's
absent attributes); this module only calls them in order, so
``preflight_service`` stays one line per run and a new notice is one line here.
"""

from __future__ import annotations

from typing import Any


def attach_plan_notices(
    out: dict[str, Any],
    *,
    dest_kind: str,
    plan_mappings: Any,
    source_object: Any,
    sync_mode: str,
    stream_contracts: Any,
    source_format: str,
    destination_db_type: str,
) -> None:
    from services.file_pickup import attach_direct_dated_file_warning
    from services.mirror_delete_notice import attach_mirror_delete_notice
    from services.oracle_empty_string import attach_oracle_empty_string_warning
    from services.schemaless_absence import attach_absent_attribute_notice

    attach_oracle_empty_string_warning(out, dest_kind, plan_mappings)
    attach_direct_dated_file_warning(out, source_object)
    attach_mirror_delete_notice(out, sync_mode, stream_contracts)
    attach_absent_attribute_notice(out, source_format, destination_db_type, sync_mode)
