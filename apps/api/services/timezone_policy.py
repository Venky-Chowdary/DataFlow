"""Explicit, auditable timezone policy for cross-engine timestamp transfer.

Timezone fidelity has two independent guarantees and conflating them is how
Airbyte-class pipelines silently shift instants:

``instant``
    The point on the UTC timeline. Preserved whenever the destination carrier
    is UTC-normalized (PostgreSQL ``TIMESTAMPTZ``, MySQL ``TIMESTAMP``) or the
    writer normalizes to UTC under a documented convention.
``offset label``
    The originating wall-clock offset (``+05:30``). Only ``DATETIMEOFFSET`` and
    ``TIMESTAMP WITH TIME ZONE`` carriers store it — PostgreSQL ``TIMESTAMPTZ``
    does **not**, so a PostgreSQL source never had a label to lose.

Every temporal pair therefore resolves to one named policy, and the policy —
not a blanket block — decides whether the route needs an operator contract.
The same resolution runs at Validate and at Execute, so a route cannot green on
one policy and write under another.

MySQL specifics (why this module exists): ``TIMESTAMP`` is stored as UTC and
converted with the session ``time_zone`` on read, so it *is* an instant carrier
— but its range is 1970-01-01 00:00:01 UTC to 2038-01-19 03:14:07 UTC.
``DATETIME`` has the full 1000..9999 range but is wall-clock with no polarity
marker, so it can only carry an instant under an explicit UTC-normalize
contract that a downstream reader has to know about.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from services.keyspace_instant import INSTANT_TEXT_WIRE_ENGINES

# Engines whose bare ``TIMESTAMP`` token stores an instant rather than a
# wall-clock. MySQL/MariaDB/TiDB normalize to UTC on write; BigQuery,
# Spanner and Databricks declare instant semantics natively.
INSTANT_TIMESTAMP_DIALECTS: Final[frozenset[str]] = frozenset(
    {"mysql", "bigquery", "spanner", "databricks"}
)

# MySQL ``TIMESTAMP`` epoch bounds (inclusive), per the MySQL 8 reference.
MYSQL_TIMESTAMP_MIN: Final[datetime] = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
MYSQL_TIMESTAMP_MAX: Final[datetime] = datetime(
    2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc
)

POLICY_NATIVE_INSTANT: Final[str] = "native_instant_carrier"
POLICY_OFFSET_PRESERVED: Final[str] = "offset_preserved_carrier"
POLICY_UTC_NORMALIZED: Final[str] = "utc_normalized_wall_clock"
POLICY_OFFSET_TEXT: Final[str] = "offset_preserving_text"
POLICY_WALL_CLOCK_LOCAL: Final[str] = "wall_clock_local_only"
POLICY_UTC_INVENT: Final[str] = "utc_invented_from_naive"

#: The window a MySQL-family ``TIMESTAMP`` column can hold, in words.
MYSQL_TIMESTAMP_RANGE_TEXT: Final[str] = (
    "1970-01-01 00:00:01 UTC .. 2038-01-19 03:14:07 UTC"
)


@dataclass(frozen=True)
class TimezoneTransferPolicy:
    """What the destination will actually hold, and what it costs."""

    policy: str
    instant_preserved: bool
    offset_label_preserved: bool
    requires_contract: bool
    destination_reads_as: str
    range_limit: str
    note: str
    remediation: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "policy": self.policy,
            "instant_preserved": self.instant_preserved,
            "offset_label_preserved": self.offset_label_preserved,
            "requires_contract": self.requires_contract,
            "destination_reads_as": self.destination_reads_as,
            "range_limit": self.range_limit,
            "note": self.note,
            "remediation": self.remediation,
        }


def _polarity(type_name: str, *, dest_db: str = "") -> str | None:
    """What a carrier holds: an instant, or a wall clock with no zone.

    A document store's bare ``date`` token shares a name with SQL ``DATE`` but
    stores an offset-normalized instant, so it is resolved through the same
    carrier helper the binder and fingerprinter use. Without that this token was
    unclassified, and the timezone question the operator most needs answered on
    a Postgres-to-MongoDB route was never asked.
    """
    from services.document_instant import instant_date_carrier
    from services.type_system import datetime_timezone_polarity

    resolved = instant_date_carrier(dest_db, type_name) if dest_db else type_name
    return datetime_timezone_polarity(resolved or type_name, dest_db=dest_db)


def _is_text_target(target_type: str) -> bool:
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        normalize_logical_type,
    )

    return normalize_logical_type(target_type) in {LOGICAL_STRING, LOGICAL_TEXT}


#: Destinations whose only temporal carrier is an instant. BSON has `date` and
#: nothing zoneless, so telling an operator here to "map to a wall-clock carrier"
#: names an option the destination does not have — a remediation that cannot be
#: followed is the same dead end as no remediation at all.
_INSTANT_ONLY_DESTINATIONS: Final[frozenset[str]] = frozenset({"mongodb", "documentdb", "cosmosdb"})


def _utc_invent_remediation(dest_db: str) -> str:
    """Name only the exits this destination actually has."""
    declare = (
        "Declare the source zone on Map with an assume_timezone transform "
        "(e.g. assume_timezone:UTC) so the instant is asserted rather than guessed"
    )
    if dest_db in _INSTANT_ONLY_DESTINATIONS:
        return (
            f"{declare}. This destination stores instants only, so there is no "
            "wall-clock carrier to map to — the alternative is a string column, "
            "which gives up engine date semantics."
        )
    return f"{declare}, or map to a wall-clock destination carrier."


def effective_source_type(source_type: str, transform: str | None) -> str:
    """The source type a declared zone makes true.

    ``assume_timezone:X`` is the operator supplying the zone the source never
    recorded, so from that point the column really does carry an instant. Every
    decision that asks "is this zoneless?" has to read it the same way, or the
    declaration changes the written value without changing the verdict — the
    worst of both, a transfer still blocked for a problem it no longer has.

    The zone is all the declaration supplies. The column's fractional digits
    are kept — a bare ``TIMESTAMPTZ`` read as "no precision" and passed a
    microsecond column onto a millisecond carrier that truncated it.
    """
    from services.document_instant import source_fractional_digits
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX
    from services.type_system import datetime_timezone_polarity

    token = str(transform or "").strip().lower()
    if not token.startswith(ASSUME_TIMEZONE_PREFIX):
        return source_type
    if not token[len(ASSUME_TIMEZONE_PREFIX):].strip():
        return source_type
    if datetime_timezone_polarity(source_type) != "ntz":
        return source_type
    digits = source_fractional_digits(source_type)
    return f"TIMESTAMPTZ({digits})" if digits is not None else "TIMESTAMPTZ"


def declared_source_column_types(
    column_types: Mapping[str, str],
    mappings: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    """Source column types as the operator's zone declarations make them true.

    Map carries the declaration on the mapping's transform, while every gate
    reasons over the introspected source types. Projecting one onto the other in
    a single place is what keeps the declaration from being a transform that
    changes the written value while the gate still blocks the run for the
    zoneless problem the operator just answered.
    """
    out = dict(column_types)
    for m in mappings:
        col = m.get("source") or m.get("source_column")
        if not isinstance(col, str) or col not in out:
            continue
        transform = m.get("transform")
        out[col] = effective_source_type(
            out[col], transform if isinstance(transform, str) else "",
        )
    return out


def validate_source_timezone(source_timezone: object) -> tuple[str, str]:
    """Return a valid declared IANA zone, or an actionable request error."""
    zone = str(source_timezone or "").strip()
    if not zone:
        return "", ""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    try:
        ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        return "", (
            f"Invalid source_timezone {zone!r}; provide an IANA name such as "
            "UTC or America/New_York."
        )
    return zone, ""


def text_may_carry_wall_clock(source_db: str = "", source_kind: str = "") -> bool:
    """Whether a source's *text* column can be its temporal carrier.

    SQLite has no temporal storage class, so its wall clocks are TEXT; a file
    declares nothing. A catalog engine with real temporal types (PostgreSQL,
    MySQL, SQL Server, ...) declared that column text, and a declared zone must
    not re-type it (D190): ``source_timezone`` answers the zone question for
    columns that are temporal, it does not make text temporal.
    """
    db = (source_db or "").strip().lower()
    if not db or db == "sqlite" or db.startswith("sqlite"):
        return True
    from services.data_profiler import source_types_are_authoritative

    return not source_types_are_authoritative(source_kind or "database", db)


def declare_zone_on_schema(
    source_rows: list[dict[str, Any]],
    zone: str,
    *,
    source_db: str = "",
    source_kind: str = "",
) -> tuple[list[dict[str, Any]], set[str]]:
    """Apply a declared source zone to zoneless temporal schema rows.

    Only columns that are temporal take the zone: a typed zoneless column, an
    instant column whose samples are wall clocks, and - where text is the
    temporal carrier (SQLite, files) - a text column whose every sample is a
    datetime literal. Digits are never a wall clock (D190).
    """
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX
    from services.type_system import datetime_timezone_polarity

    text_carrier = text_may_carry_wall_clock(source_db, source_kind)

    declared: set[str] = set()
    out: list[dict[str, Any]] = []
    transform = f"{ASSUME_TIMEZONE_PREFIX}{zone}"
    for row in source_rows:
        entry = dict(row)
        name = str(entry.get("name") or "")
        source_type = str(entry.get("inferred_type") or "")
        polarity = datetime_timezone_polarity(source_type) if source_type else None
        if polarity == "ntz":
            entry["inferred_type"] = effective_source_type(source_type, transform)
            declared.add(name)
        elif polarity in {"tz", "ltz"} and _samples_are_naive_wall_clock(
            entry.get("samples") or []
        ):
            declared.add(name)
        elif (
            text_carrier
            and _untyped_text_token(source_type)
            and _samples_unanimously_naive_datetimes(entry.get("samples") or [])
        ):
            # SQLite stores a wall-clock column as TEXT. Declaring
            # source_timezone is the operator answering the zone question, so
            # the column is an instant from here on (QA R2-016). Offset-bearing
            # text is left alone: the offset is already in the value.
            entry["inferred_type"] = effective_source_type("TIMESTAMP", transform)
            declared.add(name)
        elif source_type.strip().upper() == "DATE" and all(
            _sample_temporal_polarities(entry.get("samples") or [])
        ):
            entry["inferred_type"] = "TIMESTAMPTZ"
            declared.add(name)
        out.append(entry)
    return out, declared


def declare_transform_zones_on_schema(
    source_rows: list[dict[str, Any]],
    column_transforms: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Project operator ``assume_timezone:`` transforms onto the source schema.

    ``assume_timezone:X`` on a column is the same declaration as
    ``source_timezone=X`` for that one column, so Map has to read it the same
    way: before it grades the pair. Stamped after Map, the mapping kept the
    fidelity Map gave the zoneless type (``lossy_cast`` naive -> instant), and
    G4 held the very remediation the gate had asked for (D148).
    """
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX

    zones = {
        str(k or "").strip().lower(): str(v or "").strip()
        for k, v in (column_transforms or {}).items()
        if str(v or "").strip().lower().startswith(ASSUME_TIMEZONE_PREFIX)
    }
    if not zones:
        return source_rows
    out: list[dict[str, Any]] = []
    for row in source_rows:
        entry = dict(row)
        transform = zones.get(str(entry.get("name") or "").strip().lower())
        if transform and entry.get("inferred_type"):
            entry["inferred_type"] = effective_source_type(
                str(entry["inferred_type"]), transform
            )
        out.append(entry)
    return out


def _sample_temporal_polarities(samples: Any) -> tuple[bool, bool]:
    from connectors.sql_temporal import parse_sql_date, parse_sql_datetime
    from services.type_system import temporal_value_has_timezone

    has_aware = False
    has_naive = False
    for sample in samples or []:
        if sample is None or not str(sample).strip():
            continue
        if (
            parse_sql_datetime(sample) is None
            and parse_sql_date(sample) is None
        ):
            continue
        if temporal_value_has_timezone(sample):
            has_aware = True
        else:
            has_naive = True
    return has_aware, has_naive


def _samples_are_naive_wall_clock(samples: Any) -> bool:
    return _sample_temporal_polarities(samples)[1]


def _untyped_text_token(source_type: str) -> bool:
    token = (source_type or "").strip().upper()
    if not token or "(" in token:
        return False
    return token in {"TEXT", "VARCHAR", "STRING", "CHAR", "CLOB"}


def _samples_unanimously_naive_datetimes(samples: Any) -> bool:
    """True when every non-empty sample is a zoneless datetime."""
    from connectors.sql_temporal import parse_sql_datetime
    from services.type_system import temporal_value_has_timezone

    saw = False
    for sample in samples or []:
        if sample is None or not str(sample).strip():
            continue
        if not is_wall_clock_literal(sample):
            return False
        if parse_sql_datetime(sample) is None or temporal_value_has_timezone(sample):
            return False
        saw = True
    return saw


def is_wall_clock_literal(sample: Any) -> bool:
    """True for a written date/time literal, never for a bare digit run.

    ``parse_sql_datetime`` reads ``'1700000000'`` as epoch seconds and
    ``'20240101'`` as epoch too: right for a bind into a declared TIMESTAMP,
    wrong as evidence that a text column holds wall clocks (D190).
    """
    from connectors.sql_temporal import _integral_digit_token

    if isinstance(sample, bool) or isinstance(sample, (int, float)):
        return False
    return _integral_digit_token(sample) is None


def stamp_zone_transform(
    mappings: list[dict[str, Any]],
    zone: str,
    columns: set[str],
) -> list[dict[str, Any]]:
    """Attach a declared zone without replacing operator-chosen transforms."""
    from services.transform_engine import (
        ASSUME_TIMEZONE_PREFIX,
        normalize_engine_transform,
    )

    out: list[dict[str, Any]] = []
    for mapping in mappings:
        entry = dict(mapping)
        existing = str(entry.get("transform") or "").strip().lower()
        # ``to_timestamp`` / ``to_date`` are the datetime parse under another
        # name. Left in place, the declared zone never applied: every naive
        # value then failed the instant carrier, and Validate held all rows
        # for one DST-gap cell (D52). assume_timezone parses and asserts the zone.
        canon = normalize_engine_transform(existing) if existing else ""
        already_typed = existing not in {
            "", "none", "identity", "datetime", "date", "timestamp"
        } and (canon or "").lower() not in {"none", "identity", "datetime", "date"}
        if str(entry.get("source") or "") in columns and not already_typed:
            entry["transform"] = f"{ASSUME_TIMEZONE_PREFIX}{zone}"
        out.append(entry)
    return out


def columns_with_naive_instants(
    source_rows: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
) -> set[str]:
    """Find mapped temporal columns with at least one zoneless instant sample."""
    from services.schema_inference import classify_value
    from services.type_system import datetime_timezone_polarity

    by_name = {str(row.get("name") or ""): row for row in source_rows}
    found: set[str] = set()
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        source = str(mapping.get("source") or "").strip()
        if not source:
            continue
        source_type = str(
            by_name.get(source, {}).get("inferred_type")
            or mapping.get("source_type")
            or ""
        )
        if datetime_timezone_polarity(source_type) not in {"ntz", "tz", "ltz"}:
            continue
        samples = list(by_name.get(source, {}).get("samples") or [])
        if any(
            classify_value(str(sample), field_name=source) in {"DATE", "TIMESTAMP"}
            for sample in samples
        ):
            found.add(source)
    return found


def apply_declared_source_timezone(
    source_rows: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    zone: str,
    destination_db: str,
) -> list[dict[str, Any]]:
    """Stamp mapping transforms and effective carriers from the shared declaration."""
    from services.decision_kernel.type_invent import (
        create_new_mapping_target_type,
        materialize_dest_ddl,
    )
    from services.transform_engine import ASSUME_TIMEZONE_PREFIX
    from services.type_system import datetime_timezone_polarity

    columns = columns_with_naive_instants(source_rows, mappings)
    if not columns:
        return mappings
    stamped = stamp_zone_transform(mappings, zone, columns)
    transform = f"{ASSUME_TIMEZONE_PREFIX}{zone}"
    out: list[dict[str, Any]] = []
    upgraded: dict[str, str] = {}
    for mapping in stamped:
        entry = dict(mapping)
        source = str(entry.get("source") or "")
        if source not in columns:
            out.append(entry)
            continue
        source_type = str(
            next(
                (
                    row.get("inferred_type")
                    for row in source_rows
                    if str(row.get("name") or "") == source
                ),
                "",
            )
            or entry.get("source_type")
            or ""
        )
        if source_type:
            entry["source_type"] = source_type
        if datetime_timezone_polarity(source_type) == "ntz":
            source_type = effective_source_type(source_type, transform)
            entry["source_type"] = source_type
            if entry.get("create_new") or not str(
                entry.get("target_type") or ""
            ).strip():
                target_type = create_new_mapping_target_type(
                    source_type, destination_db, source_db=""
                )
                if target_type:
                    entry["target_type"] = target_type
                    native = materialize_dest_ddl(destination_db, target_type)
                    if native:
                        entry["dest_native_type"] = native
        upgraded[source] = str(entry.get("source_type") or "")
        out.append(entry)
    for row in source_rows:
        typed = upgraded.get(str(row.get("name") or ""))
        if typed:
            row["inferred_type"] = typed
    return out


def resolve_timezone_policy(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> TimezoneTransferPolicy | None:
    """Name the timezone semantics of one column pair, or ``None`` if not temporal.

    ``None`` means the pair raises no timezone question (non-temporal source, or
    a carrier whose polarity is not knowable from the DDL token).
    """
    from services.type_system import _normalize_dest_db

    db = _normalize_dest_db(dest_db) if dest_db else ""
    src = _polarity(source_type)
    if src is None:
        return None
    tgt = _polarity(target_type, dest_db=db)
    aware = {"tz", "ltz"}

    if src in aware and tgt is None and _is_text_target(target_type):
        # Redis (and any other proven JSON text-wire engine) has no temporal
        # column type. Telling the operator to "map to a temporal carrier"
        # names an exit that does not exist, and the RFC 3339 wire already
        # keeps both the instant and the offset. Same dead-end as telling a
        # Mongo operator to map to a wall-clock date.
        text_wire = db in INSTANT_TEXT_WIRE_ENGINES
        return TimezoneTransferPolicy(
            policy=POLICY_OFFSET_TEXT,
            instant_preserved=True,
            offset_label_preserved=True,
            requires_contract=not text_wire,
            destination_reads_as="ISO-8601 string with offset",
            range_limit="none",
            note=(
                "This engine's JSON text wire writes RFC 3339 with the offset, "
                "so the text is the instant — there is no other temporal "
                "carrier to map to."
                if text_wire
                else (
                    "Instant and offset both survive as text, but the destination "
                    "column is no longer a temporal type — no engine-side date "
                    "arithmetic, ordering is lexical."
                )
            ),
            remediation=(
                ""
                if text_wire
                else "Map to a temporal carrier to keep engine date semantics."
            ),
        )
    if tgt is None:
        return None

    if src in aware and tgt in aware:
        from services.offset_label import stores_originating_offset
        from services.type_system import _SINGLE_AWARE_TIMESTAMP_DIALECTS

        src_label = stores_originating_offset("", source_type)
        dst_label = stores_originating_offset(db, target_type)
        label = src_label and dst_label
        rewrite = {src, tgt} == {"tz", "ltz"}
        spelling_only = rewrite and db in _SINGLE_AWARE_TIMESTAMP_DIALECTS
        return TimezoneTransferPolicy(
            policy=(
                POLICY_OFFSET_PRESERVED if label else POLICY_NATIVE_INSTANT
            ),
            instant_preserved=True,
            offset_label_preserved=label,
            requires_contract=rewrite and not spelling_only,
            destination_reads_as=(
                "offset-pinned instant" if label else "UTC-normalized instant"
            ),
            range_limit=_range_limit(target_type, db),
            note=(
                "Both carriers store an instant. "
                + (
                    "The originating offset label is preserved."
                    if label
                    else "The originating offset label is not stored by the "
                    "destination (PostgreSQL TIMESTAMPTZ / MySQL TIMESTAMP "
                    "keep UTC only — SQL-standard WITH TIME ZONE is not "
                    "DATETIMEOFFSET)."
                )
            ),
            remediation=(
                "Sign a contract to accept offset-label rewrite between "
                "session-relative and offset-pinned carriers."
                if rewrite and not spelling_only
                else ""
            ),
        )

    if src in aware and tgt == "ntz":
        if db == "mysql":
            return TimezoneTransferPolicy(
                policy=POLICY_UTC_NORMALIZED,
                instant_preserved=True,
                offset_label_preserved=False,
                requires_contract=True,
                destination_reads_as="UTC wall clock in a DATETIME column",
                range_limit="1000-01-01 .. 9999-12-31",
                note=(
                    "MySQL DATETIME carries no polarity marker. The instant is "
                    "recoverable only because the writer normalizes to UTC — a "
                    "reader that assumes local time will be wrong."
                ),
                remediation=(
                    "Prefer TIMESTAMP(6) (self-describing instant, 1970..2038), "
                    "or sign a UTC-normalize contract to use DATETIME(6)."
                ),
            )
        return TimezoneTransferPolicy(
            policy=POLICY_WALL_CLOCK_LOCAL,
            instant_preserved=False,
            offset_label_preserved=False,
            requires_contract=True,
            destination_reads_as="wall clock with no zone",
            range_limit=_range_limit(target_type, db),
            note="Dropping the zone loses the instant — the value becomes ambiguous.",
            remediation="Map to a timezone-aware destination carrier.",
        )

    if src == "ntz" and tgt in aware:
        return TimezoneTransferPolicy(
            policy=POLICY_UTC_INVENT,
            instant_preserved=False,
            offset_label_preserved=False,
            requires_contract=True,
            destination_reads_as="instant stamped from a zoneless source",
            range_limit=_range_limit(target_type, db),
            note=(
                "The source never proved a zone; writing an aware carrier invents "
                "one (usually UTC) for every row."
            ),
            remediation=_utc_invent_remediation(db),
        )

    if src == "ntz" and tgt == "ntz":
        return TimezoneTransferPolicy(
            policy=POLICY_WALL_CLOCK_LOCAL,
            instant_preserved=True,
            offset_label_preserved=False,
            requires_contract=False,
            destination_reads_as="wall clock with no zone",
            range_limit=_range_limit(target_type, db),
            note="Both sides are zoneless wall clock — digits round-trip unchanged.",
        )
    return None


def _range_limit(target_type: str, db: str) -> str:
    if db == "mysql" and _polarity(target_type, dest_db="mysql") in {"tz", "ltz"}:
        return MYSQL_TIMESTAMP_RANGE_TEXT
    return "engine default"


def instant_range_would_cap(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
) -> bool:
    """True when an aware source lands in an epoch-bounded instant carrier.

    MySQL ``TIMESTAMP`` is the right carrier for an instant — it is the only
    MySQL column that stores one — but it holds barely 68 years of them. A
    PostgreSQL ``TIMESTAMPTZ`` or Snowflake ``TIMESTAMP_TZ`` column spans
    4713 BC..294276 AD, so create-new picks a carrier whose *domain* is
    narrower than the source's even though its polarity and precision are
    exact. That narrowing is invisible until a row outside the window reaches
    the writer, which rejects it (or, outside STRICT mode, zeroes it).
    """
    from services.type_system import _normalize_dest_db

    db = _normalize_dest_db(dest_db) if dest_db else ""
    if db != "mysql":
        return False
    if not is_mysql_timestamp_carrier(target_type):
        return False
    return _polarity(source_type) in {"tz", "ltz"}


def samples_outside_instant_range(samples: Sequence[Any] | None) -> list[str]:
    """The sampled values a MySQL ``TIMESTAMP`` column could not hold."""
    return [
        str(v)
        for v in (samples or [])
        if v is not None and mysql_timestamp_out_of_range(v)
    ]


def is_mysql_timestamp_carrier(target_type: str) -> bool:
    """True for a MySQL ``TIMESTAMP`` column (instant carrier, epoch-bounded)."""
    from connectors.sql_temporal import sql_base_type

    return sql_base_type(target_type).upper() == "TIMESTAMP"


def mysql_timestamp_out_of_range(value: Any) -> bool:
    """True when a value cannot fit MySQL ``TIMESTAMP`` epoch bounds.

    Out-of-range instants must be quarantined with the DATETIME remediation —
    MySQL would otherwise reject the row or (outside STRICT mode) zero it.
    """
    from connectors.sql_temporal import parse_sql_datetime

    parsed = parse_sql_datetime(value, aware_utc=True)
    if parsed is None:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed < MYSQL_TIMESTAMP_MIN or parsed > MYSQL_TIMESTAMP_MAX


# ---------------------------------------------------------------------------
# Session-independent instant (AWS DMS class)
# ---------------------------------------------------------------------------
# MySQL TIMESTAMP is stored as UTC and converted with session time_zone on
# both read and write. AWS DMS documents ``initstmt=SET time_zone='+00:00'``
# plus ``serverTimezone`` and still gets DST wrong (GMT vs BST, Sydney,
# Calcutta): checksums of copied digits stay green while the instant moved.
# The algorithm here is one pin used by every MySQL connection (source read
# and dest write), plus a wire that carries polarity so dest TIMESTAMPTZ
# does not refuse a naive UTC datetime as wall-clock.
#
# DATETIME is wall-clock and must never go through the instant wire.
# PostgreSQL TIMESTAMP WITHOUT TIME ZONE is also wall-clock — pinning MySQL
# UTC must not be used as an excuse to UTC-shift those digits.

MYSQL_SESSION_UTC: Final[str] = "+00:00"
MYSQL_UTC_PIN_SQL: Final[str] = "SET SESSION time_zone = '+00:00'"


def pin_mysql_session_utc(conn: Any) -> None:
    """Pin a MySQL/MariaDB session so TIMESTAMP conversion is identity.

    After this, a TIMESTAMP cell's civil digits *are* the UTC instant.
    ``UNIX_TIMESTAMP(col)`` is then independent of whatever zone a later
    operator session happens to use — that is the proof, not the display.
    """
    sql = MYSQL_UTC_PIN_SQL
    cursor_factory = getattr(conn, "cursor", None)
    if callable(cursor_factory):
        cur = conn.cursor()
        try:
            cur.execute(sql)
        finally:
            closer = getattr(cur, "close", None)
            if closer is not None:
                closer()
        return
    execute = getattr(conn, "execute", None)
    if callable(execute):
        conn.execute(sql)
        return
    raise TypeError("pin_mysql_session_utc requires a DBAPI connection")


def mysql_timestamp_instant_wire(value: Any) -> Any:
    """Attach UTC to a cell read from a UTC-pinned MySQL ``TIMESTAMP``.

    pymysql returns TIMESTAMP as a naive datetime after session conversion.
    With ``time_zone=+00:00`` those digits are UTC, but ``isoformat()``
    without an offset looks like wall-clock. Destination ``TIMESTAMPTZ``
    then refuses the naive value (or invents UTC). Attaching UTC preserves
    the instant without shifting it.

    Do not call this for ``DATETIME`` — that carrier is zoneless wall-clock.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        from connectors.sql_temporal import input_has_timezone, parse_sql_datetime

        text = value.strip()
        if not text:
            return value
        if input_has_timezone(text):
            return value
        parsed = parse_sql_datetime(text, wall_clock=True)
        if parsed is not None:
            return parsed.replace(tzinfo=timezone.utc)
    return value


def mysql_catalog_instant_sample(value: Any, source_type: str) -> Any:
    """Naive digits of a MySQL TIMESTAMP column, as the reader would emit them.

    The session is pinned to UTC, so those digits are the instant. Validate
    used to refuse them until the operator set ``source_timezone``, and that
    declaration also rewrote every DATETIME column. Only a catalog instant
    is wired. A wall-clock DATETIME is returned unchanged.
    """
    from services.dest_dialect_facts import _normalize_dest_db
    from services.source_engine_scope import active_source_engine
    from services.type_system import datetime_timezone_polarity

    if _normalize_dest_db(active_source_engine()) != "mysql":
        return value
    if datetime_timezone_polarity(source_type) not in {"tz", "ltz"}:
        return value
    wired = mysql_timestamp_instant_wire(value)
    if not isinstance(wired, datetime):
        return value
    return wired.isoformat()


def is_mysql_timestamp_data_type(data_type: str) -> bool:
    """True for information_schema ``DATA_TYPE = timestamp`` (not datetime)."""
    return (data_type or "").strip().lower() == "timestamp"
