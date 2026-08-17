"""Originating offset is stored data, not a SQL-standard token.

PostgreSQL ``timestamp with time zone`` is the worst-named type in SQL:
it stores a UTC instant and **discards** the offset literal. AWS DMS says
so in so many words (``The time zone offset is always normalized to UTC.
The original offset literal is not retained.``). AWS SCT still maps
SQL Server ``DATETIMEOFFSET`` → PostgreSQL ``TIMESTAMP WITH TIME ZONE``
and the load looks green. Npgsql will not round-trip a non-zero
``DateTimeOffset``. Airbyte-class Python pipelines ``astimezone(UTC)``
before bind, so even a dest that *could* store ``+05:30`` receives
``+00:00``.

Instant and offset-label are independent guarantees (see
``timezone_policy``). This module is the offset-label half:

1. Classify whether an engine physically **stores** the originating
   offset. SQL-standard ``WITH TIME ZONE`` is not that question.
2. Extract the label from the cell *before* UTC normalize (minutes east
   of UTC, ``Z`` = 0).
3. Bind: dest that stores the label gets the original offset back on the
   instant; dest that does not stays UTC-normalized and the certificate
   says ``unsupported``, not ``carried``.
4. Certify from the dest engine (``DATEPART(TZOFFSET)``, Oracle
   ``EXTRACT(TIMEZONE_*)``). PostgreSQL ``EXTRACT(TIMEZONE FROM
   timestamptz)`` is the *session* offset — proof the label was not
   stored, never proof that it was.

We do not invent a companion offset column. That would be a schema
change the operator did not approve.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from services.dest_dialect_facts import _normalize_dest_db

Status = Literal["carried", "unsupported", "skipped"]

# Engines whose aware timestamp is UTC-on-disk. The SQL token may say
# "with time zone"; the bytes do not keep the writer's offset.
_INSTANT_ONLY_ENGINES: frozenset[str] = frozenset(
    {
        "postgresql",
        "mysql",
        "mariadb",
        "tidb",
        "redshift",
        "duckdb",
        "bigquery",
        "spanner",
        "databricks",
        "clickhouse",
        "trino",
        "presto",
        "iceberg",
        "mongodb",
        "documentdb",
        "cosmosdb",
        "sqlite",
    }
)

_SQLSERVER_FAMILY: frozenset[str] = frozenset(
    {"sqlserver", "mssql", "azure_sql", "azure_sql_database", "synapse"}
)

_OFFSET_RE = re.compile(r"([+-])(\d{2}):?(\d{2})$")


@dataclass(frozen=True)
class OffsetLabel:
    """Minutes east of UTC as the source cell wrote them.

    ``0`` is UTC (``Z`` / ``+00:00``) — a stored label, not "no label".
    ``None`` from extract means the cell never carried one (naive wall-clock).
    """

    minutes: int

    @property
    def iso_suffix(self) -> str:
        sign = "+" if self.minutes >= 0 else "-"
        mag = abs(self.minutes)
        return f"{sign}{mag // 60:02d}:{mag % 60:02d}"

    def to_tzinfo(self) -> timezone:
        return timezone(timedelta(minutes=self.minutes))

    def to_dict(self) -> dict[str, Any]:
        return {"minutes": self.minutes, "iso_suffix": self.iso_suffix}


@dataclass
class OffsetLabelDecision:
    source_column: str
    dest_column: str
    status: Status
    reason: str
    source_stores: bool
    dest_stores: bool
    source_type: str = ""
    dest_type: str = ""

    def to_item_kwargs(self) -> dict[str, Any]:
        return {
            "aspect": "offset_label",
            "name": self.dest_column or self.source_column,
            "status": self.status,
            "reason": self.reason,
            "source_detail": self.source_type,
            "dest_ddl": self.dest_type if self.status == "carried" else "",
        }


def extract_offset_label(value: Any) -> OffsetLabel | None:
    """Read the originating offset from a cell *before* UTC normalize.

    ``astimezone(UTC)`` destroys this. Callers that UTC-normalize first cannot
    recover ``+05:30`` from a UTC datetime.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return None
        off = value.utcoffset()
        if off is None:
            return None
        return OffsetLabel(minutes=int(off.total_seconds() // 60))
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")) or text.upper().endswith(" UTC"):
        return OffsetLabel(minutes=0)
    match = _OFFSET_RE.search(text)
    if not match:
        return None
    sign = -1 if match.group(1) == "-" else 1
    hours = int(match.group(2))
    minutes = int(match.group(3))
    return OffsetLabel(minutes=sign * (hours * 60 + minutes))


def attach_offset_label(utc_instant: datetime, label: OffsetLabel) -> datetime:
    """Put the originating offset back on a UTC instant for offset-storing dests."""
    if utc_instant.tzinfo is None:
        utc_instant = utc_instant.replace(tzinfo=timezone.utc)
    else:
        utc_instant = utc_instant.astimezone(timezone.utc)
    return utc_instant.astimezone(label.to_tzinfo())


def stores_originating_offset(engine: str, type_name: str) -> bool:
    """True only when this engine physically stores the writer's offset.

    Fail-closed without an engine: only unambiguous spellings
    (``DATETIMEOFFSET``, Snowflake ``TIMESTAMP_TZ``) count. SQL-standard
    ``TIMESTAMP WITH TIME ZONE`` is Oracle (stores) and PostgreSQL (does
    not) — claiming it without an engine is the SCT lie.
    """
    from services.type_system import datetime_timezone_polarity

    raw = (type_name or "").strip()
    if not raw:
        return False
    collapsed = re.sub(r"\s+", " ", raw.upper())
    collapsed = re.sub(r"\s*\(\s*\d+\s*\)", "", collapsed).strip()
    eng = _normalize_dest_db(engine) if engine else ""

    if eng in _INSTANT_ONLY_ENGINES:
        return False

    if eng in _SQLSERVER_FAMILY:
        return "DATETIMEOFFSET" in collapsed

    if eng in {"oracle", "oracledb", "oracle_autonomous"}:
        return (
            "WITH TIME ZONE" in collapsed
            and "LOCAL" not in collapsed
            and not collapsed.startswith("TIME ")
        )

    if eng == "snowflake":
        return "TIMESTAMP TZ" in collapsed or collapsed.startswith("TIMESTAMP_TZ")

    # Unknown / omitted engine: only spellings that cannot mean PG TIMESTAMPTZ.
    if "DATETIMEOFFSET" in collapsed:
        return True
    if "TIMESTAMP TZ" in collapsed or collapsed.startswith("TIMESTAMP_TZ"):
        return True
    return False


def restore_offset_after_utc(
    value: Any,
    utc_instant: datetime,
    *,
    engine: str,
    dest_type: str,
) -> datetime:
    """UTC-normalize for instant-only dests; re-attach the label when dest stores it."""
    if not stores_originating_offset(engine, dest_type):
        if utc_instant.tzinfo is None:
            return utc_instant.replace(tzinfo=timezone.utc)
        return utc_instant.astimezone(timezone.utc)
    label = extract_offset_label(value)
    if label is None:
        if utc_instant.tzinfo is None:
            return utc_instant.replace(tzinfo=timezone.utc)
        return utc_instant.astimezone(timezone.utc)
    return attach_offset_label(utc_instant, label)


def bind_aware_datetime(
    value: datetime,
    *,
    engine: str,
    dest_type: str,
    original: Any = None,
) -> datetime:
    """Writer bind for an aware carrier.

    Instant-only dests receive UTC. Offset-storing dests receive the
    originating offset from ``original`` (the pre-normalize cell) when
    present, else from ``value`` if it still carries one.
    """
    if value.tzinfo is None:
        raise ValueError(
            f"{dest_type} refused naive datetime — provide offset/Z "
            "(refuse silent UTC invent)"
        )
    source = original if original is not None else value
    return restore_offset_after_utc(
        source, value, engine=engine, dest_type=dest_type
    )


def decide_offset_label(
    *,
    source_engine: str,
    source_type: str,
    dest_engine: str,
    dest_type: str,
    source_column: str = "",
    dest_column: str = "",
) -> OffsetLabelDecision | None:
    """Carry / unsupported / skipped for one mapped temporal column.

    ``None`` means the source is not an aware temporal (no offset question).
    """
    from services.type_system import datetime_timezone_polarity

    src_pol = datetime_timezone_polarity(source_type, dest_db=source_engine)
    if src_pol not in {"tz", "ltz"}:
        return None
    src_stores = stores_originating_offset(source_engine, source_type)
    dst_stores = stores_originating_offset(dest_engine, dest_type)
    col = dest_column or source_column
    if not src_stores:
        return OffsetLabelDecision(
            source_column=source_column,
            dest_column=col,
            status="skipped",
            reason=(
                f"Source {source_type or 'aware timestamp'} on {source_engine or 'this engine'} "
                "stores a UTC instant only — there is no originating offset "
                "label to carry (PostgreSQL TIMESTAMPTZ / MySQL TIMESTAMP class)."
            ),
            source_stores=False,
            dest_stores=dst_stores,
            source_type=source_type,
            dest_type=dest_type,
        )
    if dst_stores:
        return OffsetLabelDecision(
            source_column=source_column,
            dest_column=col,
            status="carried",
            reason=(
                f"Destination {dest_type} on {dest_engine} stores the originating "
                "offset; bind keeps the source label rather than UTC-normalizing it."
            ),
            source_stores=True,
            dest_stores=True,
            source_type=source_type,
            dest_type=dest_type,
        )
    return OffsetLabelDecision(
        source_column=source_column,
        dest_column=col,
        status="unsupported",
        reason=(
            f"Source stored originating offset on {source_type}; destination "
            f"{dest_engine} {dest_type or 'aware timestamp'} stores a UTC instant "
            "only. Instant may still land; the offset label does not. We do not "
            "invent a companion offset column."
        ),
        source_stores=True,
        dest_stores=False,
        source_type=source_type,
        dest_type=dest_type,
    )


def plan_offset_label_carry(
    *,
    catalog: Any,
    dest_dialect: str,
    dest_name_for_source: Any,
    dest_type_for_column: Any,
) -> list[OffsetLabelDecision]:
    """One decision per mapped aware-temporal source column."""
    types = dict(getattr(catalog, "column_types", None) or {})
    source_engine = str(getattr(catalog, "dialect", "") or "")
    decisions: list[OffsetLabelDecision] = []
    if not types:
        return decisions
    for src_col, src_type in types.items():
        dest_col = dest_name_for_source(src_col) if dest_name_for_source else src_col
        if not dest_col:
            continue
        dest_type = dest_type_for_column(dest_col) if dest_type_for_column else ""
        decision = decide_offset_label(
            source_engine=source_engine,
            source_type=str(src_type or ""),
            dest_engine=dest_dialect,
            dest_type=str(dest_type or ""),
            source_column=str(src_col),
            dest_column=str(dest_col),
        )
        if decision is not None:
            decisions.append(decision)
    return decisions


def postgres_session_timezone_seconds_sql(column_sql: str) -> str:
    """PG: session offset of a TIMESTAMPTZ, not a stored label.

    Under ``SET TIME ZONE 'UTC'`` this is 0 even when the INSERT used
    ``+05:30``. That is dest-engine proof the originating offset was dropped.
    """
    return f"EXTRACT(TIMEZONE FROM {column_sql})"
