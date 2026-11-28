"""Minimal, dependency-free 5-field cron parser with IANA timezone support.

No maintained cron library is present in ``requirements.txt`` (no ``croniter`` /
``APScheduler``), so this is a small, well-tested implementation covering the
standard Vixie-cron 5-field grammar:

    ┌───────── minute        (0-59)
    │ ┌─────── hour          (0-23)
    │ │ ┌───── day of month  (1-31)
    │ │ │ ┌─── month         (1-12 or JAN-DEC)
    │ │ │ │ ┌─ day of week   (0-6, Sun=0; 7 also = Sun, or SUN-SAT)
    * * * * *

Supported per field: ``*``, ``*/step``, ``a-b``, ``a-b/step``, comma lists, and
named months/weekdays. Day-of-month and day-of-week follow Vixie semantics: when
BOTH are restricted (neither is ``*``) a timestamp matches if EITHER matches.

``next_run`` returns the next matching instant strictly after ``after`` as a
timezone-aware UTC ``datetime``. Wall-clock field matching is evaluated in the
schedule's IANA timezone so DST transitions are handled correctly.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from services import schedule_clock

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}
_DOW = {d: i for i, d in enumerate(
    ["sun", "mon", "tue", "wed", "thu", "fri", "sat"], start=0)}

# (min, max) inclusive bounds for each of the five fields.
_BOUNDS = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]
# Search horizon: 4 years covers leap-year-only rules such as "Feb 29".
FIRE_HORIZON_DAYS = 4 * 366
_MAX_MINUTES = FIRE_HORIZON_DAYS * 24 * 60


class CronError(ValueError):
    """Raised for a malformed cron expression or timezone."""


class CronNeverFiresError(CronError):
    """A well-formed cron with no fire time within the search horizon (``0 0 30 2 *``)."""


def _alias(token: str, field_index: int) -> str:
    low = token.lower()
    if field_index == 3 and low in _MONTHS:
        return str(_MONTHS[low])
    if field_index == 4 and low in _DOW:
        return str(_DOW[low])
    return token


def _parse_field(field: str, field_index: int) -> set[int]:
    lo, hi = _BOUNDS[field_index]
    values: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        if not part:
            raise CronError(f"Empty term in cron field '{field}'")
        step = 1
        if "/" in part:
            base, _, step_s = part.partition("/")
            if not step_s.isdigit() or int(step_s) < 1:
                raise CronError(f"Invalid step '{part}'")
            step = int(step_s)
            part = base or "*"
        if part == "*":
            start, end = lo, hi
        elif "-" in part:
            a, _, b = part.partition("-")
            a, b = _alias(a, field_index), _alias(b, field_index)
            if not a.lstrip("-").isdigit() or not b.lstrip("-").isdigit():
                raise CronError(f"Invalid range '{part}'")
            start, end = int(a), int(b)
        else:
            tok = _alias(part, field_index)
            if not tok.lstrip("-").isdigit():
                raise CronError(f"Invalid value '{part}' in cron field")
            start = end = int(tok)
        # Day-of-week: normalize 7 -> 0 (Sunday).
        if field_index == 4:
            start = 0 if start == 7 else start
            end = 0 if end == 7 else end
        if start > end:
            raise CronError(f"Range start after end in '{part}'")
        if start < lo or end > hi:
            raise CronError(f"Cron field out of bounds [{lo},{hi}]: '{part}'")
        values.update(range(start, end + 1, step))
    return values


class CronExpression:
    """A parsed, immutable 5-field cron expression."""

    __slots__ = ("raw", "minutes", "hours", "days", "months", "dow",
                 "_dom_restricted", "_dow_restricted", "_fields")

    def __init__(self, expr: str):
        self.raw = expr.strip()
        fields = self.raw.split()
        if len(fields) != 5:
            raise CronError(
                f"Cron expression must have exactly 5 fields, got {len(fields)}: '{expr}'"
            )
        self.minutes = _parse_field(fields[0], 0)
        self.hours = _parse_field(fields[1], 1)
        self.days = _parse_field(fields[2], 2)
        self.months = _parse_field(fields[3], 3)
        self.dow = _parse_field(fields[4], 4)
        self._fields = fields
        self._dom_restricted = fields[2].strip() != "*"
        self._dow_restricted = fields[4].strip() != "*"

    def matches_day(self, day: date | datetime) -> bool:
        """Whether the calendar day satisfies the month / day-of-month / day-of-week fields."""
        if day.month not in self.months:
            return False
        # Vixie semantics: if both DOM and DOW are restricted, match on EITHER.
        dom_ok = day.day in self.days
        dow_ok = (day.weekday() + 1) % 7 in self.dow  # Python Mon=0 -> cron Sun=0
        if self._dom_restricted and self._dow_restricted:
            return dom_ok or dow_ok
        if self._dom_restricted:
            return dom_ok
        if self._dow_restricted:
            return dow_ok
        return True

    def matches(self, dt: datetime) -> bool:
        return dt.minute in self.minutes and dt.hour in self.hours and self.matches_day(dt)

    def assert_can_fire(self, today: date | None = None) -> None:
        """Raise :class:`CronNeverFiresError` when no calendar day in the horizon matches.

        Minutes and hours are never empty once parsed, so only the day-level
        fields can make an expression unsatisfiable. One pass over the horizon's
        days, independent of the timezone (a zone only shifts the wall clock).
        """
        start = today or schedule_clock.utc_now().date()
        for offset in range(FIRE_HORIZON_DAYS):
            if self.matches_day(start + timedelta(days=offset)):
                return
        dom, month, dow = self._fields[2], self._fields[3], self._fields[4]
        raise CronNeverFiresError(
            f"cron '{self.raw}' can never fire: no calendar day in the next "
            f"{FIRE_HORIZON_DAYS // 366} years matches day-of-month '{dom}' in "
            f"month '{month}' (day-of-week '{dow}')"
        )


def validate_cron(expr: str) -> None:
    """Raise :class:`CronError` if ``expr`` is malformed or can never fire."""
    CronExpression(expr).assert_can_fire()


def resolve_timezone(tz: str | None) -> ZoneInfo:
    name = (tz or "UTC").strip() or "UTC"
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, KeyError, ValueError) as exc:
        raise CronError(f"Unknown timezone: {tz}") from exc


def next_run(expr: str, after: datetime, tz: str | ZoneInfo = "UTC") -> datetime:
    """Return the next instant matching ``expr`` strictly after ``after`` (UTC).

    Field matching is performed against the wall-clock time in ``tz`` so DST is
    handled correctly (each candidate is a real UTC instant whose local time is
    tested). ``after`` may be naive (assumed UTC) or timezone-aware.
    """
    cron = CronExpression(expr)
    # Fail in one calendar pass, not after walking every minute of the horizon.
    cron.assert_can_fire(after.date())
    zone = tz if isinstance(tz, ZoneInfo) else resolve_timezone(tz)

    if after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    # Advance to the start of the next minute (cron has minute resolution).
    start = (after.astimezone(timezone.utc) + timedelta(minutes=1)).replace(
        second=0, microsecond=0
    )
    found = _first_match_from(cron, zone, start)
    if found is None:
        raise CronError(f"No matching time found for cron '{expr}' within horizon")
    return found


_MINUTE = timedelta(minutes=1)


def _ceil_minute(t: datetime) -> datetime:
    floor = t.replace(second=0, microsecond=0)
    return floor if floor == t else floor + _MINUTE


def _minutes_of_day(cron: CronExpression) -> list[int]:
    """Every matching wall-clock minute of a day (hour * 60 + minute), ascending."""
    return [h * 60 + m for h in sorted(cron.hours) for m in sorted(cron.minutes)]


def _local_days(zone: ZoneInfo, start: datetime, end: datetime):
    """Split the UTC minutes [start, end) at local midnights, in time order.

    Yields ``(lo, hi, regular, local)``: ``regular`` when the zone's offset is
    the same whole number of minutes at both ends, so every minute in between
    has the local date and wall clock ``local + (t - lo)``. A day with a DST
    change (or an offset with seconds) is not regular and is walked minute by
    minute, exactly as the definition reads.
    """
    t = start
    while t < end:
        local = t.astimezone(zone)
        d = local.date() + timedelta(days=1)
        midnight = _ceil_minute(datetime(d.year, d.month, d.day, tzinfo=zone).astimezone(timezone.utc))
        hi = min(midnight if midnight > t else t + _MINUTE, end)
        off = local.utcoffset()
        regular = (
            off is not None
            and off.total_seconds() % 60 == 0
            and (hi - _MINUTE).astimezone(zone).utcoffset() == off
        )
        yield t, hi, regular, local
        t = hi


def _may_match(cron: CronExpression, zone: ZoneInfo, lo: datetime, hi: datetime, local: datetime) -> bool:
    """Whether any local date the UTC minutes [lo, hi) fall on satisfies the day fields."""
    return cron.matches_day(local) or cron.matches_day((hi - _MINUTE).astimezone(zone))


def _first_match_from(cron: CronExpression, zone: ZoneInfo, start: datetime) -> datetime | None:
    """The earliest instant >= ``start`` whose wall clock in ``zone`` matches ``cron``.

    Walks forward in time one local day at a time. On a day without an
    offset change the first matching minute is a binary search over the
    day's matching wall-clock minutes, so a dense expression ("* * * * *")
    answers in microseconds and a sparse one ("0 0 29 2 *") skips whole days
    on the day fields. A day with a DST change is walked minute by minute
    (the definition itself). Same horizon as before: an instant counts only
    within _MAX_MINUTES of ``start``.
    """
    from bisect import bisect_left

    limit = start + timedelta(minutes=_MAX_MINUTES)
    day_minutes = _minutes_of_day(cron)
    for lo, hi, regular, local in _local_days(zone, start, limit):
        if not _may_match(cron, zone, lo, hi, local):
            continue
        if regular:
            wall = local.hour * 60 + local.minute
            idx = bisect_left(day_minutes, wall)
            if idx < len(day_minutes):
                candidate = lo + timedelta(minutes=day_minutes[idx] - wall)
                if candidate < hi:
                    return candidate
            continue
        t = lo
        while t < hi:
            if cron.matches(t.astimezone(zone)):
                return t
            t += _MINUTE
    return None


def count_matches(expr: str, after: datetime, until: datetime, tz: str | ZoneInfo = "UTC", *, limit: int) -> int:
    """How many instants ``next_run`` would step through in (``after``, ``until``], at most ``limit``.

    The same instants as calling ``next_run`` repeatedly from ``after`` while
    the result is <= ``until``, counted a local day at a time (a binary search
    per day) instead of one call per instant: a schedule overdue three days on
    "* * * * *" is 4,320 instants, which the scheduler counted inside its claim
    lock one ``next_run`` at a time.
    """
    from bisect import bisect_left, bisect_right

    cron = CronExpression(expr)
    zone = tz if isinstance(tz, ZoneInfo) else resolve_timezone(tz)
    if after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    start = (after.astimezone(timezone.utc) + _MINUTE).replace(second=0, microsecond=0)
    end = until.astimezone(timezone.utc).replace(second=0, microsecond=0) + _MINUTE
    day_minutes = _minutes_of_day(cron)
    total = 0
    for lo, hi, regular, local in _local_days(zone, start, end):
        if total >= limit:
            break
        if not _may_match(cron, zone, lo, hi, local):
            continue
        if regular:
            first = local.hour * 60 + local.minute
            last = first + int((hi - lo).total_seconds() // 60) - 1
            total += bisect_right(day_minutes, last) - bisect_left(day_minutes, first)
            continue
        t = lo
        while t < hi:
            if cron.matches(t.astimezone(zone)):
                total += 1
            t += _MINUTE
    return min(total, limit)
