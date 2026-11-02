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
        start = today or datetime.now(timezone.utc).date()
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
    candidate = (after.astimezone(timezone.utc) + timedelta(minutes=1)).replace(
        second=0, microsecond=0
    )
    for _ in range(_MAX_MINUTES):
        if cron.matches(candidate.astimezone(zone)):
            return candidate
        candidate += timedelta(minutes=1)
    raise CronError(f"No matching time found for cron '{expr}' within horizon")
