"""Deterministic precedence between a saved connector and inline Studio fields.

Transfer Studio keeps editable ``Database`` / ``Schema`` fields even after the
operator picks a saved connector. Those fields used to win unconditionally
(``database or cfg["database"]``), so a stale or defaulted Studio value silently
redirected the write: a SQLite connector pointing at ``exports/smoke.db`` wrote
to ``dataflow_test`` instead and the connector's file stayed empty.

Precedence is now one rule, shared by Validate and Execute:

* No saved connector — the inline value is authoritative (``studio_inline``).
* Saved connector, inline blank or equal — the connector is authoritative.
* Saved connector, inline differs on a **file-backed** engine — the connector
  wins unless the operator explicitly acknowledged the override. ``database``
  there is a filesystem path, so a silent override writes to a different file.
* Saved connector, inline differs on a server engine — the override is honoured
  (picking a database on the same server is legitimate) but is recorded as an
  explicit ``studio_override`` so the Decision Artifact shows both values.

Every outcome is reported, so the effective destination can be surfaced before
Validate and re-checked at Execute instead of being discovered after the write.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from src.transfer.models import EndpointConfig

__all__ = [
    "FILE_BACKED_DB_TYPES",
    "DestinationIdentity",
    "resolve_destination_database",
    "resolve_saved_vs_inline",
]

#: Studio ships these as placeholder defaults, so they are not operator choices.
PLACEHOLDER_DATABASES = frozenset({"test_db", "test"})

#: Engines where ``database`` names a file on disk rather than a server catalog.
FILE_BACKED_DB_TYPES = frozenset({"sqlite", "sqlite3", "duckdb", "access", "msaccess"})

Authority = Literal["saved_connector", "studio_inline", "studio_override"]


@dataclass(frozen=True)
class DestinationIdentity:
    """Resolved destination database/path plus why it won."""

    database: str
    authority: Authority
    #: The value that lost, when the two sides disagreed.
    ignored_value: str = ""
    #: True when saved and inline disagreed, whichever side won.
    conflict: bool = False
    note: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "database": self.database,
            "authority": self.authority,
            "ignored_value": self.ignored_value,
            "conflict": self.conflict,
            "note": self.note,
        }


def _same_target(a: str, b: str, *, file_backed: bool) -> bool:
    if file_backed:
        return os.path.abspath(os.path.expanduser(a)) == os.path.abspath(
            os.path.expanduser(b)
        )
    return a.strip().casefold() == b.strip().casefold()


def resolve_destination_database(
    *,
    saved_database: str | None,
    requested_database: str | None,
    db_type: str = "",
    override_acknowledged: bool = False,
) -> DestinationIdentity:
    """Resolve the effective destination database/path under one precedence rule."""
    requested = (requested_database or "").strip()
    if saved_database is None:
        return DestinationIdentity(database=requested, authority="studio_inline")

    saved = (saved_database or "").strip()
    file_backed = (db_type or "").strip().lower() in FILE_BACKED_DB_TYPES

    if not requested:
        return DestinationIdentity(database=saved, authority="saved_connector")
    if not saved:
        return DestinationIdentity(database=requested, authority="studio_inline")
    if _same_target(saved, requested, file_backed=file_backed):
        return DestinationIdentity(database=saved, authority="saved_connector")

    label = "file" if file_backed else "database"
    if file_backed and not override_acknowledged:
        return DestinationIdentity(
            database=saved,
            authority="saved_connector",
            ignored_value=requested,
            conflict=True,
            note=(
                f"Studio {label} '{requested}' ignored — the saved connector points at "
                f"'{saved}'. Writing elsewhere would leave the connector's {label} empty. "
                "Edit the connector, or acknowledge the override explicitly."
            ),
        )
    return DestinationIdentity(
        database=requested,
        authority="studio_override",
        ignored_value=saved,
        conflict=True,
        note=(
            f"Operator override — writing to {label} '{requested}' instead of the "
            f"connector's '{saved}'."
        ),
    )


def stamp_destination_identity(
    out: dict[str, object], endpoint: "EndpointConfig"
) -> None:
    """Surface the resolved destination on a preflight result.

    Validate must probe the value Execute will write to, so the effective
    database replaces the Studio field in ``_probe_cfg`` and any conflict is
    reported to the operator *before* the run instead of after it.
    """
    from services.secret_config import RedactedConfig

    extra = endpoint.extra or {}
    identity = extra.get("destination_identity")
    if not isinstance(identity, dict):
        return
    out["destination_identity"] = identity
    if identity.get("conflict") and identity.get("note"):
        out["message"] = str(identity["note"])
    cfg = out.get("_probe_cfg")
    if isinstance(cfg, Mapping) and cfg:
        merged = dict(cfg)
        merged["database"] = endpoint.database or merged.get("database")
        out["_probe_cfg"] = RedactedConfig(merged)


def resolve_saved_vs_inline(
    inline_cfg: Mapping[str, object],
    saved_cfg: Mapping[str, object],
    *,
    fmt: str = "",
) -> DestinationIdentity:
    """Apply the precedence rule to a raw inline config and saved connector row."""
    inline_database = str(inline_cfg.get("database") or "")
    if inline_database in PLACEHOLDER_DATABASES:
        inline_database = ""
    saved_database = str(saved_cfg.get("database") or "")
    if fmt == "mongodb" and not saved_database:
        from connectors.mongodb_common import mongodb_database_from_uri

        saved_database = (
            mongodb_database_from_uri(str(saved_cfg.get("connection_string") or "")) or ""
        )
    return resolve_destination_database(
        saved_database=saved_database,
        requested_database=inline_database,
        db_type=str(saved_cfg.get("type") or fmt or ""),
        override_acknowledged=bool(inline_cfg.get("destination_override_acknowledged")),
    )
