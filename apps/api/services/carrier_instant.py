"""Fractional-second granularity a destination instant carrier actually keeps.

Gate-8 compares a fingerprint of the source cell against a fingerprint of the
cell the destination returns. A carrier that keeps fewer fractional digits than
the source value carries makes those two differ on *every* row of the column:
``2024-01-01 00:00:00.654321`` bound into MySQL ``DATETIME`` comes back as
``00:00:01`` because the engine rounds to whole seconds. The transfer did
exactly what the operator approved (the narrowing is declared at Validate by
``temporal_precision_would_narrow``), yet reconcile reported two unequal hashes
and no column — the strict checksum failure on a correct load.

So the source side is fingerprinted at the granularity the carrier keeps. This
cannot mask a real difference: rounding is applied identically to both sides and
only ever removes digits the destination provably cannot store. Where the
carrier's granularity is not knowable (unknown engine, non-temporal DDL), the
value passes through unrounded and any difference still fails Gate-8.

Rounding, not truncation, is the measured behaviour of MySQL and PostgreSQL
(see ``docs/CARRIER_PRECISION_EVIDENCE.md``); SQL Server ``datetime`` rounds to
1/300 s and ``smalldatetime`` to the minute, both modelled here. An engine that
truncated instead would fail Gate-8 rather than pass it — the honest direction.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

from connectors.sql_temporal import round_to_smalldatetime, sql_base_type
from services.document_instant import INSTANT_DATE_TOKEN_ENGINES
from services.type_system import destination_temporal_fractional_digits

# SQL Server ``datetime`` stores 1/300-second ticks (.000/.003/.007 endings).
MSSQL_LEGACY_DATETIME_TICK_US = 10_000 / 3
# Any date works to carry a TIME rounding into the next second; only the
# roll-over past midnight matters.
_EPOCH_DAY = date(2000, 1, 1)
_MSSQL_ENGINES = frozenset({"sqlserver", "mssql", "azuresql", "azure_sql", "synapse"})


def carrier_instant_digits(ddl_type: str | None, *, engine: str = "") -> int | None:
    """Fractional-second digits the carrier keeps, or ``None`` when unknown.

    ``engine`` is required for bare spellings: ``DATETIME`` keeps whole seconds
    on MySQL and microseconds on BigQuery, so guessing without the engine would
    quantize away digits a destination can hold.
    """
    if not ddl_type or not (engine or "").strip():
        return None
    return destination_temporal_fractional_digits(ddl_type, dest_db=engine)


def temporal_carrier_display_label(ddl_type: str, engine: str = "") -> str:
    """Operator-facing label for a declared temporal carrier.

    Document stores stamp their instant carrier ``date`` — the real BSON type
    name — but a bare "date" in a verdict reads as a calendar DATE column, so
    the same carrier flips between ``date`` and ``TIMESTAMPTZ(6)`` across
    surfaces (X-138). On instant-only engines the token is rendered as the
    instant it is; every other declared spelling stays verbatim.
    """
    token = str(ddl_type or "").strip()
    if not token:
        return token
    from services.type_system import temporal_carrier_holds_time

    if token.lower() == "date" and temporal_carrier_holds_time(engine):
        digits = carrier_instant_digits(token, engine=engine)
        suffix = f", {digits} fractional digits" if digits else ""
        from services.type_system import _normalize_dest_db

        kind = (
            "BSON instant"
            if _normalize_dest_db(engine or "") in {"mongodb", "documentdb", "cosmosdb"}
            else "instant"
        )
        return f"date ({kind}{suffix})"
    return token


def carrier_rounded_columns(
    mappings: list[dict] | None,
    *,
    source_schema: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
    dest_engine: str = "",
) -> list[dict[str, object]]:
    """Columns whose instants are fingerprinted at a coarser granularity.

    Gate-8 passing on these columns proves every cell matches *what the
    destination can hold* — it cannot prove the fractional seconds the carrier
    dropped, so the report has to say which columns those were instead of
    claiming full cell fidelity.
    """
    from services.type_system import temporal_precision_would_narrow

    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for mapping in mappings or []:
        target = str(mapping.get("target") or "").strip()
        if not target or target in seen:
            continue
        source = str(mapping.get("source") or "").strip()
        target_type = str(
            (dest_types or {}).get(target)
            or mapping.get("target_type")
            or mapping.get("inferredType")
            or ""
        ).strip()
        source_type = str(
            mapping.get("source_type")
            or (source_schema or {}).get(source)
            or ""
        ).strip()
        if not target_type or not source_type:
            continue
        if not temporal_precision_would_narrow(
            source_type, target_type, dest_db=dest_engine
        ) and not _tick_dropped_at_cap(source_type, target_type, dest_engine):
            continue
        seen.add(target)
        out.append({
            "column": target,
            "source_type": temporal_carrier_display_label(
                source_type, dest_engine
            ),
            "target_type": temporal_carrier_display_label(
                target_type, dest_engine
            ),
            "kept_fractional_digits": carrier_instant_digits(
                target_type, engine=dest_engine
            ),
        })
    return out


def _tick_dropped_at_cap(source_type: str, target_type: str, dest_engine: str) -> bool:
    """A declared source fraction (DATETIME2(7) / TIMESTAMP_NTZ(7)) into a
    carrier that keeps fewer digits.

    Validate lets this pair through when the destination is already at its
    own maximum (PostgreSQL / MySQL microseconds cannot be widened), so it is
    no G3 narrowing. The seventh digit is still not stored, and Gate-8 compares
    at the carrier's six digits: the report names the column rather than
    claiming the 100 ns digit was proven.
    """
    from services.type_system import parse_temporal_fractional_precision

    src_digits = parse_temporal_fractional_precision(source_type)
    if src_digits is None and (source_type or "").strip().upper() == "DATETIME2":
        src_digits = 7  # SQL Server's default precision
    kept = carrier_instant_digits(target_type, engine=dest_engine)
    return src_digits is not None and kept is not None and kept < src_digits


def _round_microseconds(value: datetime | time, digits: int) -> datetime | time:
    """Round the fractional second to ``digits``, carrying into the second."""
    if digits >= 6:
        return value
    step = 10 ** (6 - digits)
    micro = value.microsecond
    rounded = ((micro + step // 2) // step) * step
    if rounded < 1_000_000:
        return value.replace(microsecond=rounded)
    if isinstance(value, datetime):
        return value.replace(microsecond=0) + timedelta(seconds=1)
    carried = (
        datetime.combine(_EPOCH_DAY, value.replace(microsecond=0)) + timedelta(seconds=1)
    )
    if carried.date() != _EPOCH_DAY:
        # 23:59:59.9 has no next second inside a TIME column; the engine cannot
        # wrap the clock either, so keep the second it can store.
        return value.replace(microsecond=0)
    return carried.timetz() if value.tzinfo else carried.time()


def _round_mssql_legacy(value: datetime) -> datetime:
    """Round to the nearest 1/300 second (SQL Server ``datetime``)."""
    ticks = round(value.microsecond / MSSQL_LEGACY_DATETIME_TICK_US)
    micro = int(round(ticks * MSSQL_LEGACY_DATETIME_TICK_US))
    if micro >= 1_000_000:
        return value.replace(microsecond=0) + timedelta(seconds=1)
    return value.replace(microsecond=micro)


def _quantize_document_instant(value: object, *, ddl_type: str, engine: str) -> object:
    """A BSON / Elasticsearch date as the store keeps it: milliseconds, truncated.

    The encoder floors to the millisecond (``.123999`` is stored ``.123``), so
    rounding here would fail Gate-8 on a contracted narrowing (D156). Bind
    leaves the cell as ISO text on this carrier — a zoneless reader cell is
    refused by the SQL binder and an aware one is rendered — so text is parsed
    too; text that is not ISO-8601 passes through and still compares as-is.
    """
    digits = carrier_instant_digits(ddl_type, engine=engine)
    if digits is None:
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError:
            return value
    if not isinstance(value, datetime) or digits >= 6:
        return value
    step = 10 ** (6 - digits)
    return value.replace(microsecond=(value.microsecond // step) * step)


def quantize_instant_for_carrier(
    value: object,
    *,
    ddl_type: str = "",
    engine: str = "",
) -> object:
    """Return ``value`` as the destination carrier would store it.

    Non-temporal values, unknown carriers and unknown engines pass through
    unchanged — an unmodelled carrier must not be assumed lossless *or* lossy.
    """
    eng = (engine or "").strip().lower()
    if eng in INSTANT_DATE_TOKEN_ENGINES and ddl_type:
        return _quantize_document_instant(value, ddl_type=ddl_type, engine=eng)
    if not isinstance(value, (datetime, time, timedelta)):
        return value
    if not eng or not ddl_type:
        return value
    base = sql_base_type(ddl_type)
    if isinstance(value, datetime):
        if base == "SMALLDATETIME":
            return round_to_smalldatetime(value)
        if base == "DATETIME" and eng in _MSSQL_ENGINES:
            return _round_mssql_legacy(value)
    digits = carrier_instant_digits(ddl_type, engine=eng)
    if digits is None:
        return value
    if digits <= 6:
        # A DATETIME2(7) seventh digit: a microsecond carrier is bound the
        # six-digit datetime (sql_temporal attaches the digit only for a
        # DATETIME2(7) destination), so it never stores it.
        from connectors.sqlserver_tick import drop_tick

        value = drop_tick(value)
    if isinstance(value, timedelta):
        # MySQL TIME comes back from the driver as a timedelta.
        step_us = 10 ** (6 - digits) if digits < 6 else 1
        total = value // timedelta(microseconds=1)
        sign = -1 if total < 0 else 1
        total = abs(total)
        rounded = ((total + step_us // 2) // step_us) * step_us
        return timedelta(microseconds=sign * rounded)
    return _round_microseconds(value, digits)
