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
        ):
            continue
        seen.add(target)
        out.append({
            "column": target,
            "source_type": source_type,
            "target_type": target_type,
            "kept_fractional_digits": carrier_instant_digits(
                target_type, engine=dest_engine
            ),
        })
    return out


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
    if not isinstance(value, (datetime, time, timedelta)):
        return value
    eng = (engine or "").strip().lower()
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
    if isinstance(value, timedelta):
        # MySQL TIME comes back from the driver as a timedelta.
        step_us = 10 ** (6 - digits) if digits < 6 else 1
        total = value // timedelta(microseconds=1)
        sign = -1 if total < 0 else 1
        total = abs(total)
        rounded = ((total + step_us // 2) // step_us) * step_us
        return timedelta(microseconds=sign * rounded)
    return _round_microseconds(value, digits)
