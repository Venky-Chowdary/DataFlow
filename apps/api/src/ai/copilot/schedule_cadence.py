"""Turn an operator's cadence words into a schedule the runner can evaluate.

The product stores a cadence as either a preset ``interval`` (hourly / daily /
weekly, measured from the previous run) or a 5-field ``cron`` expression
evaluated in an IANA timezone. Chat has to land on exactly one of those, so this
module resolves the wording and — where the wording does not determine a
schedule — returns the question instead of a guess.

What is deliberately *not* guessed:

* A timezone. "nightly at 2" is a different instant in every zone, so an
  unstated zone is resolved to UTC and said out loud in the preview; an
  abbreviation with more than one meaning (IST, CST) is asked about.
* A day of month for "monthly", because there is no preset for it and a cron
  needs the day.
* "every N days", because cron day-of-month steps restart every month — a
  "every 10 days" cron fires on the 1st, 11th, 21st and then 8 days later.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

#: Zones an operator can name without ambiguity. Abbreviations that denote more
#: than one offset are refused rather than picked — see ``_AMBIGUOUS_ZONES``.
_ZONE_ALIASES: dict[str, str] = {
    "utc": "UTC",
    "gmt": "UTC",
    "bst": "Europe/London",
    "cet": "Europe/Paris",
    "cest": "Europe/Paris",
    "eet": "Europe/Helsinki",
    "est": "America/New_York",
    "edt": "America/New_York",
    "cdt": "America/Chicago",
    "mst": "America/Denver",
    "pst": "America/Los_Angeles",
    "pdt": "America/Los_Angeles",
    "jst": "Asia/Tokyo",
    "aest": "Australia/Sydney",
}
#: "IST" is Indian, Irish and Israeli Standard Time; "CST" is US Central, China
#: Standard and Cuba Standard. Only the unambiguous readings above are mapped.
_AMBIGUOUS_ZONES = {"ist", "cst", "wst", "amt", "act"}

_WEEKDAYS: dict[str, int] = {
    "sunday": 0, "sun": 0,
    "monday": 1, "mon": 1,
    "tuesday": 2, "tue": 2, "tues": 2,
    "wednesday": 3, "wed": 3,
    "thursday": 4, "thu": 4, "thurs": 4,
    "friday": 5, "fri": 5,
    "saturday": 6, "sat": 6,
}

_TIME_RE = re.compile(
    r"\b(?:at|@)?\s*(?:(?P<h>[01]?\d|2[0-3])(?::(?P<m>[0-5]\d))?\s*(?P<ap>am|pm)"
    r"|(?P<h24>[01]?\d|2[0-3]):(?P<m24>[0-5]\d)"
    r"|(?P<word>midnight|noon|midday))\b",
    re.IGNORECASE,
)
_IANA_RE = re.compile(r"\b([A-Za-z]+(?:_[A-Za-z]+)*/[A-Za-z]+(?:_[A-Za-z]+)*)\b")
_ZONE_WORD_RE = re.compile(r"\b(?:in|timezone|time\s*zone|tz)\s+([A-Za-z_/]+)\b", re.I)
_BARE_ZONE_RE = re.compile(
    r"\b(" + "|".join(sorted(set(_ZONE_ALIASES) | _AMBIGUOUS_ZONES, key=len, reverse=True)) + r")\b",
    re.I,
)
#: An explicit cron, with the five fields read positionally so a trailing zone
#: clause ("cron 0 3 * * 1-5 in Europe/Paris") does not swallow the expression.
_CRON_RE = re.compile(r"\bcron\b\s*[:=]?\s*((?:[-\d*/,]+\s+){4}[-\d*/,]+)", re.IGNORECASE)
_EVERY_N_RE = re.compile(
    r"\bevery\s+(\d{1,3})\s*(minutes?|mins?|hours?|hrs?|days?|weeks?)\b", re.I
)
_DAY_OF_MONTH_RE = re.compile(
    r"\b(?:on\s+)?the\s+(\d{1,2})(?:st|nd|rd|th)?\b|\bday\s+(\d{1,2})\b", re.I
)
_WEEKDAY_RE = re.compile(
    r"\b(" + "|".join(sorted(_WEEKDAYS, key=len, reverse=True)) + r")s?\b", re.I
)


#: A time-of-day qualifier only counts with an explicit "at"/"@", so a value
#: inside a row filter ("where updated_at > 12:00") is never read as a cadence.
_AT_TIME_RE = re.compile(
    r"\b(?:at|@)\s+(?:(?:[01]?\d|2[0-3])(?::[0-5]\d)?\s*(?:am|pm)"
    r"|(?:[01]?\d|2[0-3]):[0-5]\d|midnight|noon|midday)\b",
    re.IGNORECASE,
)
_ON_WEEKDAY_RE = re.compile(
    r"\b(?:on\s+)?(" + "|".join(sorted(_WEEKDAYS, key=len, reverse=True)) + r")s?\b", re.I
)


def cadence_qualifiers(text: str) -> list[tuple[int, int]]:
    """Spans of the words that qualify a cadence: time of day, weekday, timezone.

    They are cut from the route text so "…to Warehouse nightly at 2am IST" resolves
    to the connector **Warehouse** — and carried into the cadence so the schedule
    fires when the operator said, not 24 hours after they typed it.
    """
    spans: list[tuple[int, int]] = []
    for pattern in (_AT_TIME_RE, _ON_WEEKDAY_RE, _ZONE_WORD_RE, _IANA_RE, _BARE_ZONE_RE):
        m = pattern.search(text or "")
        if m:
            spans.append((m.start(), m.end()))
    return spans


@dataclass(frozen=True)
class CadenceSpec:
    """A resolved cadence, or the question that has to be answered first."""

    interval: str = ""
    cron: str = ""
    timezone: str = "UTC"
    #: What the schedule will actually do, in the operator's words.
    description: str = ""
    #: Set when the wording does not determine a schedule. Non-empty means the
    #: caller must refuse: there is no cadence to create.
    question: str = ""
    #: True when the zone was not stated and UTC was used. The preview says so —
    #: a silently-assumed zone moves a nightly run by up to a day.
    timezone_assumed: bool = False

    @property
    def resolved(self) -> bool:
        return not self.question and bool(self.interval)


def _ask(question: str) -> CadenceSpec:
    return CadenceSpec(question=question)


def parse_timezone(text: str) -> tuple[str, str]:
    """Read an IANA zone or unambiguous abbreviation. Returns ``(zone, question)``."""
    named = _IANA_RE.search(text or "")
    if named:
        zone = named.group(1)
        try:
            ZoneInfo(zone)
        except (ZoneInfoNotFoundError, ValueError, KeyError):
            return "", f"I don't recognise the timezone “{zone}”. Give me an IANA name such as Asia/Kolkata or UTC."
        return zone, ""
    for pattern in (_ZONE_WORD_RE, _BARE_ZONE_RE):
        m = pattern.search(text or "")
        if not m:
            continue
        token = m.group(1).strip().lower()
        if token in _AMBIGUOUS_ZONES:
            return "", (
                f"“{m.group(1).upper()}” names more than one timezone, and the run "
                "instant depends on which. Give me an IANA name such as "
                "America/Chicago or Asia/Shanghai."
            )
        if token in _ZONE_ALIASES:
            return _ZONE_ALIASES[token], ""
    return "", ""


def parse_time_of_day(text: str) -> tuple[int, int] | None:
    """Read "at 2am" / "at 02:30" / "at 14:00" / "at midnight" as ``(hour, minute)``."""
    m = _TIME_RE.search(text or "")
    if not m:
        return None
    word = (m.group("word") or "").lower()
    if word:
        return (0, 0) if word == "midnight" else (12, 0)
    if m.group("h24") is not None:
        return int(m.group("h24")), int(m.group("m24"))
    hour = int(m.group("h") or 0)
    minute = int(m.group("m") or 0)
    meridiem = (m.group("ap") or "").lower()
    if meridiem == "pm" and hour < 12:
        hour += 12
    elif meridiem == "am" and hour == 12:
        hour = 0
    return hour, minute


def _clock(hour: int, minute: int) -> str:
    return f"{hour:02d}:{minute:02d}"


def _daily(hour: int | None, minute: int, tz: str, assumed: bool) -> CadenceSpec:
    if hour is None:
        return CadenceSpec(
            interval="daily",
            timezone=tz,
            description="every 24 hours, starting at the first run",
            timezone_assumed=assumed,
        )
    return CadenceSpec(
        interval="daily",
        cron=f"{minute} {hour} * * *",
        timezone=tz,
        description=f"every day at {_clock(hour, minute)} {tz}",
        timezone_assumed=assumed,
    )


def _weekly(
    dow: int | None, hour: int | None, minute: int, tz: str, assumed: bool
) -> CadenceSpec:
    if dow is None or hour is None:
        return CadenceSpec(
            interval="weekly",
            timezone=tz,
            description="every 7 days, starting at the first run",
            timezone_assumed=assumed,
        )
    day = next(name for name, i in _WEEKDAYS.items() if i == dow and len(name) > 3)
    return CadenceSpec(
        interval="weekly",
        cron=f"{minute} {hour} * * {dow}",
        timezone=tz,
        description=f"every {day} at {_clock(hour, minute)} {tz}",
        timezone_assumed=assumed,
    )


def _every_n(unit: str, count: int, tz: str, assumed: bool) -> CadenceSpec:
    if unit.startswith("min"):
        if not 1 <= count <= 59:
            return _ask(
                "A minute cadence has to divide an hour, so I can take every 1–59 "
                "minutes. For anything longer, say hourly, daily or weekly."
            )
        return CadenceSpec(
            interval="hourly",
            cron=f"*/{count} * * * *",
            timezone=tz,
            description=f"every {count} minute(s)",
            timezone_assumed=assumed,
        )
    if unit.startswith("h"):
        if not 1 <= count <= 23:
            return _ask(
                "An hourly step has to divide a day, so I can take every 1–23 hours. "
                "For a daily run, say daily and the time."
            )
        return CadenceSpec(
            interval="hourly",
            cron=f"0 */{count} * * *",
            timezone=tz,
            description=f"every {count} hour(s) on the hour",
            timezone_assumed=assumed,
        )
    if unit.startswith("w"):
        if count == 1:
            return _weekly(None, None, 0, tz, assumed)
        return _ask(
            "The runner has weekly, not every-N-weeks — a multi-week cadence has no "
            "cron form that stays even across month ends. Weekly, or a monthly day, "
            "I can do."
        )
    # days
    if count == 1:
        return _daily(None, 0, tz, assumed)
    if count == 7:
        return _weekly(None, None, 0, tz, assumed)
    return _ask(
        f"“every {count} days” has no honest cron form: a day-of-month step restarts "
        "each month, so it would fire unevenly. Tell me a weekday (weekly) or a day "
        "of the month (monthly) instead."
    )


def parse_cadence(text: str) -> CadenceSpec:
    """Resolve cadence wording into a preset interval or a cron expression.

    ``text`` is the operator's own phrasing — the cadence clause plus anything
    that qualifies it (time of day, weekday, timezone). An unresolvable phrase
    comes back as a :attr:`CadenceSpec.question`, never as a nearby cadence.
    """
    raw = (text or "").strip()
    if not raw:
        return _ask(
            "How often should this run? I can do hourly, daily, weekly, a weekday, "
            "a day of the month, or an explicit cron."
        )
    lower = raw.lower()

    tz, tz_question = parse_timezone(raw)
    if tz_question:
        return _ask(tz_question)
    assumed = not tz
    tz = tz or "UTC"

    if "cron" in lower:
        from services.cron_schedule import CronError, validate_cron

        cron_literal = _CRON_RE.search(raw)
        expr = " ".join(cron_literal.group(1).split()) if cron_literal else ""
        if not expr:
            return _ask(
                "That is not a 5-field cron (minute hour day-of-month month "
                "day-of-week). Send the five fields, or say daily/hourly/weekly."
            )
        try:
            # The runner's own parser decides: chat must not accept an expression
            # the scheduler would then reject or evaluate differently.
            validate_cron(expr)
        except CronError as exc:
            return _ask(f"I cannot schedule cron “{expr}”: {exc}")
        return CadenceSpec(
            interval="daily",
            cron=expr,
            timezone=tz,
            description=f"cron “{expr}” ({tz})",
            timezone_assumed=assumed,
        )

    clock = parse_time_of_day(raw)
    hour, minute = clock if clock else (None, 0)

    every = _EVERY_N_RE.search(lower)
    if every:
        return _every_n(every.group(2).lower(), int(every.group(1)), tz, assumed)

    weekday = _WEEKDAY_RE.search(lower)
    if "month" in lower:
        dom_match = _DAY_OF_MONTH_RE.search(lower)
        dom = int(next((g for g in (dom_match.groups() if dom_match else ()) if g), 0) or 0)
        if not dom or hour is None:
            return _ask(
                "A monthly run needs the day of the month and the time — the runner "
                "has no monthly preset, so I have to write it as a cron. "
                "Say e.g. “monthly on the 1st at 02:00 UTC”."
            )
        if dom > 28:
            return _ask(
                f"Day {dom} does not exist in every month, so that cadence would skip "
                "months. Pick day 1–28, or say “last day” is not supported yet."
            )
        return CadenceSpec(
            interval="daily",
            cron=f"{minute} {hour} {dom} * *",
            timezone=tz,
            description=f"on day {dom} of every month at {_clock(hour, minute)} {tz}",
            timezone_assumed=assumed,
        )

    if any(w in lower for w in ("hourly", "every hour", "each hour")):
        if hour is not None and clock:
            # "hourly at :15" — the hour field is meaningless, the minute is not.
            return CadenceSpec(
                interval="hourly",
                cron=f"{minute} * * * *",
                timezone=tz,
                description=f"every hour at :{minute:02d}",
                timezone_assumed=assumed,
            )
        return CadenceSpec(
            interval="hourly",
            timezone=tz,
            description="every hour, starting at the first run",
            timezone_assumed=assumed,
        )

    if "week" in lower or weekday:
        dow = _WEEKDAYS[weekday.group(1).lower()] if weekday else None
        return _weekly(dow, hour, minute, tz, assumed)

    if any(
        w in lower
        for w in ("nightly", "every night", "each night", "daily", "every day", "each day")
    ):
        if hour is None and "night" in lower:
            # A preset daily run starts whenever it was created, which for
            # "nightly" is almost never night. Ask rather than run at noon.
            return _ask(
                "What time should the nightly run start, and in which timezone? "
                "Without a time it would run at whatever hour it was created — "
                "say e.g. “nightly at 02:00 Asia/Kolkata”."
            )
        return _daily(hour, minute, tz, assumed)

    if clock:
        # A bare time with no cadence word is a daily run at that time.
        return _daily(hour, minute, tz, assumed)

    return _ask(
        f"I could not turn “{raw.strip()}” into a schedule. I can do hourly, daily "
        "at a time, weekly on a weekday, a day of the month, every N minutes/hours, "
        "or an explicit 5-field cron."
    )
