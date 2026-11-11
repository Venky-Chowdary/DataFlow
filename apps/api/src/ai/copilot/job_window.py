"""Time windows for job questions, in the workspace's time zone.

"How many jobs failed today" used to be answered for the whole history: the
ledger read took no dates, so the word *today* was dropped and the answer was
still stated as fact. A window is resolved here into exact ``[start, end)``
instants and handed to the job store's own created-at filter
(``services.job_list_filters``), so the count is the store's count.

Two rules keep it honest. The window is named back to the operator with its
time zone and the instant the ledger was read. And a time phrase that is not
understood is refused by name — never silently ignored.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SUPPORTED_WINDOWS_HINT = (
    "today, yesterday, this week, last week, this month, last month, "
    "the last N hours / days / weeks, or a date range (YYYY-MM-DD to YYYY-MM-DD)"
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def workspace_zone_name() -> str:
    """The IANA zone saved in Settings → General ("UTC" until one is set)."""
    try:
        from services.workspace_store import get_workspace_settings

        return str(get_workspace_settings().get("timezone") or "UTC")
    except Exception:  # noqa: BLE001 - settings store down: UTC is stated in the answer
        return "UTC"


def _zone(name: str | None) -> tuple[ZoneInfo, str]:
    wanted = (name or workspace_zone_name() or "UTC").strip() or "UTC"
    try:
        return ZoneInfo(wanted), wanted
    except (ZoneInfoNotFoundError, ValueError, KeyError, OSError):
        return ZoneInfo("UTC"), "UTC"


@dataclass(frozen=True)
class JobWindow:
    """A resolved window: ``[start, end)`` in UTC, labelled in the workspace zone."""

    key: str
    label: str
    start: datetime
    end: datetime
    zone: str

    def to_dict(self) -> dict[str, str]:
        return {
            "key": self.key,
            "label": self.label,
            "from": self.start.isoformat(),
            "to": self.end.isoformat(),
            "timezone": self.zone,
        }


_ROLLING_KEY = re.compile(r"^last_(\d{1,4})_(hours?|days?|weeks?)$")


def _day(d: date) -> str:
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b %Y')}"


def named_window(key: str, *, now: datetime | None = None, zone: str | None = None) -> JobWindow | None:
    """Resolve a window key (``today``, ``last_7_days``…) or return None."""
    token = re.sub(r"[\s-]+", "_", (key or "").strip().lower())
    tz, zone_name = _zone(zone)
    current = (now or utc_now()).astimezone(tz)

    def build(name: str, detail: str, start: datetime, end: datetime) -> JobWindow:
        return JobWindow(
            key=token,
            label=f"{name} ({detail}, {zone_name})",
            start=start.astimezone(timezone.utc),
            end=end.astimezone(timezone.utc),
            zone=zone_name,
        )

    def local_day(d: date) -> datetime:
        return datetime.combine(d, time.min, tzinfo=tz)

    today = current.date()
    if token == "today":
        return build("today", _day(today), local_day(today), local_day(today + timedelta(days=1)))
    if token == "yesterday":
        y = today - timedelta(days=1)
        return build("yesterday", _day(y), local_day(y), local_day(today))
    if token == "since_yesterday":
        y = today - timedelta(days=1)
        return build("since yesterday", f"{_day(y)} to now", local_day(y), current)
    if token in ("this_week", "last_week"):
        monday = today - timedelta(days=today.weekday())
        if token == "this_week":
            return build("this week", f"since {_day(monday)}", local_day(monday),
                         local_day(monday + timedelta(days=7)))
        prev = monday - timedelta(days=7)
        return build("last week", f"{_day(prev)} to {_day(monday - timedelta(days=1))}",
                     local_day(prev), local_day(monday))
    if token in ("this_month", "last_month"):
        first = today.replace(day=1)
        if token == "this_month":
            nxt = (first + timedelta(days=32)).replace(day=1)
            return build("this month", first.strftime("%B %Y"), local_day(first), local_day(nxt))
        prev_first = (first - timedelta(days=1)).replace(day=1)
        return build("last month", prev_first.strftime("%B %Y"), local_day(prev_first), local_day(first))
    rolling = _ROLLING_KEY.match(token)
    if rolling:
        n = int(rolling.group(1))
        unit = rolling.group(2).rstrip("s")
        if n < 1:
            return None
        span = {"hour": timedelta(hours=n), "day": timedelta(days=n), "week": timedelta(weeks=n)}[unit]
        plural = unit if n == 1 else unit + "s"
        return build(f"the last {n} {plural}", f"to {current.strftime('%H:%M')}", current - span, current)
    return None


def _parse_bound(text: str, tz: ZoneInfo, *, end: bool) -> datetime | None:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            day = date.fromisoformat(raw)
            # A bare date is the whole local day, inclusive at both ends.
            return datetime.combine(day + timedelta(days=1 if end else 0), time.min, tzinfo=tz)
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)


def resolve_window(
    window: str = "",
    date_from: str = "",
    date_to: str = "",
    *,
    now: datetime | None = None,
    zone: str | None = None,
) -> tuple[JobWindow | None, str]:
    """``(window, "")``, ``(None, "")`` when none was asked, or ``(None, refusal)``."""
    if (window or "").strip():
        resolved = named_window(window, now=now, zone=zone)
        if resolved is None:
            return None, (
                f"I can't apply the time window “{str(window).strip()[:60]}” to the job ledger. "
                f"I can filter jobs by {SUPPORTED_WINDOWS_HINT}."
            )
        return resolved, ""
    if not (str(date_from or "").strip() or str(date_to or "").strip()):
        return None, ""
    tz, zone_name = _zone(zone)
    current = now or utc_now()
    start = _parse_bound(str(date_from or ""), tz, end=False)
    end = _parse_bound(str(date_to or ""), tz, end=True)
    if (str(date_from or "").strip() and start is None) or (str(date_to or "").strip() and end is None):
        return None, "date_from and date_to must be ISO dates (YYYY-MM-DD) or ISO date-times."
    start = start or datetime(1970, 1, 1, tzinfo=timezone.utc)
    end = end or current
    if start >= end:
        return None, "date_from must be before date_to."
    label = f"{str(date_from).strip() or 'the beginning'} to {str(date_to).strip() or 'now'} ({zone_name})"
    return (
        JobWindow("range", label, start.astimezone(timezone.utc), end.astimezone(timezone.utc), zone_name),
        "",
    )


# ---- reading a window out of a question -------------------------------------

_WORD_NUMBERS = {"a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
                 "six": 6, "seven": 7, "twelve": 12, "couple of": 2}
_ISO = r"\d{4}-\d{2}-\d{2}"
_RANGE_RE = re.compile(
    rf"\b(?:from|between)\s+({_ISO})\s+(?:to|and|until|through|-)\s+({_ISO})\b", re.I
)
_ON_RE = re.compile(rf"\bon\s+({_ISO})\b", re.I)
_SINCE_ISO_RE = re.compile(rf"\b(?:since|after|from)\s+({_ISO})\b", re.I)
_ROLLING_RE = re.compile(
    r"\b(?:in\s+|over\s+|during\s+|for\s+)?(?:the\s+)?(?:last|past|previous)\s+"
    r"(?:(\d{1,4}|a|an|one|two|three|four|five|six|seven|twelve|couple\s+of)\s+)?"
    r"(hours?|days?|weeks?)\b",
    re.I,
)
_NAMED_RES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("since_yesterday", re.compile(r"\bsince\s+yesterday\b", re.I)),
    ("today", re.compile(r"\b(?:today|so\s+far\s+today)\b", re.I)),
    ("yesterday", re.compile(r"\byesterday\b", re.I)),
    ("this_week", re.compile(r"\bthis\s+week\b", re.I)),
    ("last_week", re.compile(r"\b(?:last|previous)\s+week\b(?!s)", re.I)),
    ("this_month", re.compile(r"\bthis\s+month\b", re.I)),
    ("last_month", re.compile(r"\b(?:last|previous)\s+month\b(?!s)", re.I)),
)
# A time phrase Pilot recognises as one but cannot turn into a ledger filter.
_UNSUPPORTED_RE = re.compile(
    r"\b(?:this|last|past|previous|next)\s+(?:\d+\s+)?"
    r"(?:quarters?|years?|months?|mornings?|afternoons?|evenings?|nights?|weekends?|"
    r"fortnights?|minutes?|mondays?|tuesdays?|wednesdays?|thursdays?|fridays?|saturdays?|sundays?)\b"
    r"|\b(?:tonight|overnight|this\s+morning|recently|lately|earlier\s+today)\b"
    r"|\b(?:since|before|after|until)\s+(?:last\s+)?(?:monday|tuesday|wednesday|thursday|friday|"
    r"saturday|sunday|january|february|march|april|may|june|july|august|september|october|"
    r"november|december|noon|midnight|\d{1,2}\s*(?:am|pm))\b"
    r"|\b(?:in|during)\s+(?:january|february|march|april|june|july|august|september|october|"
    r"november|december|q[1-4])\b"
    r"|\b\d+\s+(?:minutes?|hours?|days?|weeks?|months?|years?)\s+ago\b",
    re.I,
)


def window_args_from_message(message: str) -> dict[str, str]:
    """``list_jobs`` window arguments named in a question, or ``{}``.

    An unsupported time phrase is returned verbatim as ``window`` so the tool
    refuses it by name instead of answering for the whole history.
    """
    text = message or ""
    rng = _RANGE_RE.search(text)
    if rng:
        return {"date_from": rng.group(1), "date_to": rng.group(2)}
    on = _ON_RE.search(text)
    if on:
        return {"date_from": on.group(1), "date_to": on.group(1)}
    since = _SINCE_ISO_RE.search(text)
    if since:
        return {"date_from": since.group(1)}
    unsupported = _UNSUPPORTED_RE.search(text)
    for key, pattern in _NAMED_RES:
        if pattern.search(text):
            return {"window": key}
    rolling = _ROLLING_RE.search(text)
    if rolling and not unsupported:
        raw = re.sub(r"\s+", " ", (rolling.group(1) or "1").lower())
        n = int(raw) if raw.isdigit() else _WORD_NUMBERS.get(raw, 0)
        unit = rolling.group(2).lower().rstrip("s")
        if n >= 1:
            return {"window": f"last_{n}_{unit}s"}
    if unsupported:
        return {"window": unsupported.group(0).strip()}
    return {}
