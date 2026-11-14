"""Which sync modes each destination can honour (CLM-8).

The sync-mode picker offered every mode to every destination: Kafka was
offered overwrite, upsert and CDC (Validate refused them), file exports CDC,
Redis and the SaaS writers CDC (which failed at the first source DELETE).
``/transfer/capabilities`` now publishes, per destination driver, the modes
this module allows, and the web picker intersects with it.

The rules are the destination-side gates Validate applies, through the same
predicates:

* append-only destinations (``g_dest_append_only``: the capability registry
  says append and nothing else) take only the append modes;
* SCD2 and mirror need a SQL history destination (``g9_sync_contract``,
  ``SQL_HISTORY_SYNC_DESTS``);
* CDC cannot land in a file / object export (``g9_sync_contract``), and needs
  a destination that can apply a DELETE
  (``services.cdc_destination_support.cdc_deletes_supported``).

``tests/test_clm8_destination_sync_modes_parity.py`` runs Validate's gates for
every destination and mode and fails when the two disagree.
"""

from __future__ import annotations

from typing import Any

SYNC_MODE_IDS: tuple[str, ...] = (
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental_append",
    "incremental_deduped",
    "cdc",
    "scd2",
    "mirror",
)

# Modes implemented for a destination but with no recorded test run there.
_UNTESTED: dict[tuple[str, str], str] = {
    ("bigquery", "scd2"): "No recorded test run covers SCD2 into BigQuery yet.",
    ("bigquery", "mirror"): "No recorded test run covers Mirror into BigQuery yet.",
}


def _key(dest: str) -> str:
    return (dest or "").strip().lower()


def destination_sync_mode_refusal(dest: str, mode: str) -> str | None:
    """Why ``mode`` cannot run into ``dest``, or None when it can."""
    from services.cdc_destination_support import cdc_deletes_supported
    from services.connector_capability_registry import is_append_only_destination
    from services.copy_kafka_common import kafka_family_name
    from services.preflight_cursor_gate import SQL_HISTORY_SYNC_DESTS
    from services.sync_cursor import is_append_sync
    from services.type_system import destination_is_file_export

    dest_key = _key(dest)
    if not dest_key:
        return None
    if is_append_only_destination(kafka_family_name(dest_key)) and not is_append_sync(mode):
        return f"{dest} is append only: written rows cannot be replaced, deduplicated or merged."
    if mode in {"scd2", "mirror"} and dest_key not in SQL_HISTORY_SYNC_DESTS:
        return f"{mode.upper()} needs a SQL table destination."
    if mode == "cdc":
        if destination_is_file_export(dest_key):
            return "An export file has no write-ahead log, so change data capture cannot land there."
        if not cdc_deletes_supported(dest_key):
            return (
                f"{dest} cannot apply a source DELETE, so change data capture would stop at "
                "the first delete with the destination half-applied."
            )
    return None


def sync_modes_for_destination(dest: str) -> list[str]:
    return [m for m in SYNC_MODE_IDS if destination_sync_mode_refusal(dest, m) is None]


def sync_mode_notes(dest: str) -> dict[str, str]:
    """Honest captions for modes that run, but not as their name suggests."""
    from services.db_type_utils import overwrite_replaces_rows

    dest_key = _key(dest)
    allowed = set(sync_modes_for_destination(dest_key))
    notes: dict[str, str] = {}
    if "full_refresh_overwrite" in allowed and not overwrite_replaces_rows(dest_key):
        # HubSpot, Salesforce, REST and vector stores: "overwrite" upserts.
        notes["full_refresh_overwrite"] = (
            "This destination has no table to replace: overwrite updates records by key, "
            "and records the source no longer has are left in place."
        )
    for (engine, mode), note in _UNTESTED.items():
        if engine == dest_key and mode in allowed:
            notes[mode] = note
    return notes


def destination_sync_modes(dests: list[str]) -> dict[str, dict[str, Any]]:
    """``{dest: {"sync_modes": [...], "notes": {...}, "refused": {...}}}``."""
    out: dict[str, dict[str, Any]] = {}
    for dest in sorted({_key(d) for d in dests if _key(d)}):
        refused = {
            m: reason for m in SYNC_MODE_IDS if (reason := destination_sync_mode_refusal(dest, m))
        }
        out[dest] = {
            "sync_modes": [m for m in SYNC_MODE_IDS if m not in refused],
            "notes": sync_mode_notes(dest),
            "refused": refused,
        }
    return out


def capabilities_destination_sync_modes() -> dict[str, dict[str, Any]]:
    """The table ``/transfer/capabilities`` publishes, keyed by destination driver type.

    The picker resolves a catalog id to its driver type first (``sql_server``
    -> ``sqlserver``), and Validate receives that same driver type.
    """
    from src.transfer.connector_capabilities import dest_live_driver_types
    from src.transfer.registry import LIVE_DEST_FILE_FORMATS

    dests = set(dest_live_driver_types()) | {f.lower() for f in LIVE_DEST_FILE_FORMATS}
    return destination_sync_modes(sorted(dests))
