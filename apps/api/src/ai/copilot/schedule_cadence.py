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
        description=f"every day at {_clock(hour, minute)}{_zone_txt(tz, assumed)}",
        timezone_assumed=assumed,
    )


def _weekly(
    dow: int | None, hour: int | None, minute: int, tz: str, assumed: bool
) -> CadenceSpec:
    if dow is None:
        return CadenceSpec(
            interval="weekly",
            timezone=tz,
            description="every 7 days, starting at the first run",
            timezone_assumed=assumed,
        )
    # "weekly on Monday" with no time anchors to the start of that weekday —
    # dropping the named day onto a rolling 7-day preset ran it on whatever
    # day it was created (QA S10).
    anchored = hour if hour is not None else 0
    day = next(name for name, i in _WEEKDAYS.items() if i == dow and len(name) > 3)
    return CadenceSpec(
        interval="weekly",
        cron=f"{minute} {anchored} * * {dow}",
        timezone=tz,
        description=f"every {day} at {_clock(anchored, minute)}{_zone_txt(tz, assumed)}",
        timezone_assumed=assumed,
    )


def _every_n(
    unit: str,
    count: int,
    tz: str,
    assumed: bool,
    *,
    text: str = "",
    times: list[tuple[int, int]] | None = None,
    dows: list[int] | None = None,
) -> CadenceSpec:
    if unit.startswith("min"):
        if count < 1:
            return _ask(
                f"Every {count} minutes is not a cadence. "
                "A minute step has to be from 1 to 59."
            )
        if count > 59:
            return _ask(
                "A minute cadence has to divide an hour, so I can take every 1–59 "
                "minutes. For anything longer, say hourly, daily or weekly."
            )
        # ``*/7`` fires at :00,:07,...,:56 and then waits four minutes for the
        # next hour. That is not every 7 minutes (R12-002). A step that divides
        # 60 stays a clock cron. Any other step is measured from the previous run.
        if 60 % count == 0:
            return CadenceSpec(
                interval="cron",
                cron=f"*/{count} * * * *",
                timezone=tz,
                description=f"every {count} minute(s)",
                timezone_assumed=assumed,
            )
        return CadenceSpec(
            interval=f"every_{count}m",
            cron="",
            timezone=tz,
            description=(
                f"every {count} minutes, measured from the previous run"
            ),
            timezone_assumed=assumed,
        )
    if unit.startswith("h"):
        if not 1 <= count <= 23:
            return _ask(
                "An hourly step has to divide a day, so I can take every 1–23 hours. "
                "For a daily run, say daily and the time."
            )
        times = times or []
        dows = dows or []
        dow_f = _dow_field(dows)
        if _BETWEEN_RE.search(text) and len(times) >= 2:
            # "every 2 hours on weekdays between 9am and 5pm" — the window and
            # the day set are part of the cadence, not decoration (D29).
            lo, hi = sorted(times)[:2]
            cron = f"{lo[1]} {lo[0]}-{hi[0]}/{count} * * {dow_f}"
            day_txt = f" ({_dow_field(dows)})" if dows else ""
            return CadenceSpec(
                interval="cron",
                cron=cron,
                timezone=tz,
                description=(
                    f"every {count} hours between {_clock(lo[0], lo[1])} and "
                    f"{_clock(hi[0], hi[1])}{day_txt}{_zone_txt(tz, assumed)}"
                ),
                timezone_assumed=assumed,
            )
        if _ANCHOR_RE.search(text) and times:
            # "every 3 hours starting at 1am" — the anchor is part of the
            # cadence: 1,4,7,… not 0,3,6,… (D44).
            ah, am = times[0]
            cron = f"{am} {ah}-23/{count} * * {dow_f}"
            return CadenceSpec(
                interval="cron",
                cron=cron,
                timezone=tz,
                description=(
                    f"every {count} hours from {_clock(ah, am)}"
                    f"{_zone_txt(tz, assumed)}"
                ),
                timezone_assumed=assumed,
            )
        if dows:
            cron = f"0 */{count} * * {dow_f}"
            return CadenceSpec(
                interval="cron",
                cron=cron,
                timezone=tz,
                description=(
                    f"every {count} hours on {_dow_field(dows)}"
                    f"{_zone_txt(tz, assumed)}"
                ),
                timezone_assumed=assumed,
            )
        return CadenceSpec(
            interval="cron",
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


_BARE_CRON_RE = re.compile(
    r"^(?:[^\s]+\s+){4}[^\s]+$"
)

#: "every other Friday" / "every 2nd week" / "biweekly" — a multi-week cadence
#: has no honest 5-field cron form and the schedule model stores a single cron,
#: so it is refused by name instead of silently becoming weekly (D44).
_BIWEEKLY_RE = re.compile(
    r"\b(?:every\s+(?:other|second|2nd|alternate)\s+|bi-?week|fortnight)", re.I
)
#: "between 9am and 5pm" — the two times bound the firing window (D29).
_BETWEEN_RE = re.compile(r"\bbetween\b", re.I)
#: "starting at 1am" / "from 3pm" — an every-N anchor (D44).
_ANCHOR_RE = re.compile(r"\b(?:starting\s+(?:at|from)|from|beginning\s+at)\b", re.I)
#: ":15 past the hour" / "at :15" — an hourly minute anchor (D44).
_PAST_HOUR_RE = re.compile(r"(?<!\d):([0-5]?\d)\b")
#: "weekend" — Saturday and Sunday (D41).
_WEEKEND_RE = re.compile(r"\bweekends?\b", re.I)
#: "twice/three times a day" — a multi-time daily cadence (D41).
_MULTI_A_DAY_RE = re.compile(r"\b(?:twice|three\s+times|\d+\s+times)\s+a\s+(?:day|week|month)\b", re.I)


def _all_times(raw: str) -> list[tuple[int, int]]:
    """Every time-of-day named in the text, in order, deduplicated."""
    out: list[tuple[int, int]] = []
    for m in _TIME_RE.finditer(raw or ""):
        word = (m.group("word") or "").lower()
        if word == "midnight":
            t = (0, 0)
        elif word in {"noon", "midday"}:
            t = (12, 0)
        elif m.group("h24") is not None:
            t = (int(m.group("h24")), int(m.group("m24")))
        else:
            h = int(m.group("h"))
            minute = int(m.group("m") or 0)
            if (m.group("ap") or "").lower() == "pm" and h != 12:
                h += 12
            if (m.group("ap") or "").lower() == "am" and h == 12:
                h = 0
            t = (h, minute)
        if t not in out:
            out.append(t)
    return out


def _dow_list(lower: str) -> list[int]:
    """Ordered day-of-week set from the phrasing: named days, weekend, weekdays."""
    dows: list[int] = []
    for m in _WEEKDAY_RE.finditer(lower):
        dow = _WEEKDAYS[m.group(1).lower()]
        if dow not in dows:
            dows.append(dow)
    if _WEEKEND_RE.search(lower):
        for dow in (6, 0):
            if dow not in dows:
                dows.append(dow)
    if re.search(r"\bweekdays?\b", lower):
        for dow in (1, 2, 3, 4, 5):
            if dow not in dows:
                dows.append(dow)
    return dows


def _times_cron(times: list[tuple[int, int]]) -> str | None:
    """Cron hour/minute fields for a set of times of day.

    Cron fires the cartesian product of the two fields, so times that share a
    minute (``8am and 8pm``) or an hour fold exactly; a pair like ``8:15 and
    20:30`` would silently double the fires and is refused instead (D41).
    """
    if len(times) == 1:
        h, m = times[0]
        return f"{m} {h}"
    minutes = {m for _, m in times}
    hours = {h for h, _ in times}
    if len(minutes) == 1:
        return f"{minutes.pop()} {','.join(str(h) for h in sorted(hours))}"
    if len(hours) == 1:
        return f"{','.join(str(m) for m in sorted(minutes))} {hours.pop()}"
    return None


def _dow_names(dows: list[int]) -> str:
    """Human day names for a dow set: ``1-5`` -> ``weekdays``."""
    ordered = sorted(dows)
    if ordered == [1, 2, 3, 4, 5]:
        return "weekdays"
    if ordered == [0, 6]:
        return "weekend"
    by_num: dict[int, str] = {}
    for n, i in _WEEKDAYS.items():
        if len(n) > 3:
            by_num[i] = n
    return ", ".join(by_num[d].capitalize() for d in ordered)


def _dow_field(dows: list[int]) -> str:
    if not dows:
        return "*"
    ordered = sorted(dows)
    if ordered == [0, 1, 2, 3, 4, 5, 6]:
        return "*"
    if ordered == [1, 2, 3, 4, 5]:
        return "1-5"
    return ",".join(str(d) for d in ordered)


def _zone_txt(tz: str, assumed: bool) -> str:
    """The zone the cadence text may claim. An assumed zone is not the
    operator's — the preview names the kept zone separately instead of the
    description claiming "(UTC)" over a New York schedule (D22)."""
    return "" if assumed else f" {tz}"


def _bare_cron(raw: str, tz: str, assumed: bool) -> CadenceSpec | None:
    """Accept a 5-field cron that was not prefixed with the word cron.

    ``*/5 * * * *`` is a schedule. Requiring the word "cron" made
    create_schedule answer with the failure question instead of the
    expression the runner already knows how to evaluate. A trailing zone
    (``30 2 * * 0 America/New_York`` or ``… in Asia/Kolkata``) is stripped
    before matching — the zone was already read by ``parse_timezone`` (D22).
    """
    text = " ".join((raw or "").split())
    # Cut any zone clause out of the token stream before the 5-field check.
    for pattern in (_ZONE_WORD_RE, _IANA_RE):
        m = pattern.search(text)
        if m:
            text = (text[: m.start()] + " " + text[m.end() :]).strip()
            text = " ".join(text.split())
            break
    if not _BARE_CRON_RE.match(text):
        return None
    from services.cron_schedule import CronError, CronNeverFiresError, validate_cron

    try:
        validate_cron(text)
    except CronNeverFiresError as exc:
        return _ask(f"{exc}.")
    except CronError:
        return None
    return CadenceSpec(
        interval="cron",
        cron=text,
        timezone=tz,
        description=f"cron “{text}”{_zone_txt(tz, assumed) or ' (UTC)'}",
        timezone_assumed=assumed,
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
        from services.cron_schedule import CronError, CronNeverFiresError, validate_cron

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
        except CronNeverFiresError as exc:
            return _ask(f"{exc}.")
        except CronError as exc:
            return _ask(f"I cannot schedule cron “{expr}”: {exc}")
        return CadenceSpec(
            interval="cron",
            cron=expr,
            timezone=tz,
            description=f"cron “{expr}”{_zone_txt(tz, assumed) or ' (UTC)'}",
            timezone_assumed=assumed,
        )

    clock = parse_time_of_day(raw)
    hour, minute = clock if clock else (None, 0)
    times = _all_times(raw)
    dows = _dow_list(lower)

    # A multi-week cadence has no single-cron form — refuse by name rather
    # than keep "every Friday" for "every other Friday" (D44).
    if _BIWEEKLY_RE.search(lower):
        frag = _BIWEEKLY_RE.search(lower).group(0).strip()
        return _ask(
            f"“{frag}” is a multi-week cadence, which a 5-field cron cannot "
            "express evenly — I will not quietly turn it into a weekly run. "
            "Pick a weekday for weekly, or a day of the month for monthly."
        )

    every = _EVERY_N_RE.search(lower)
    if every:
        return _every_n(
            every.group(2).lower(),
            int(every.group(1)),
            tz,
            assumed,
            text=lower,
            times=times,
            dows=dows,
        )

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
            interval="cron",
            cron=f"{minute} {hour} {dom} * *",
            timezone=tz,
            description=f"on day {dom} of every month at {_clock(hour, minute)}{_zone_txt(tz, assumed)}",
            timezone_assumed=assumed,
        )

    if any(w in lower for w in ("hourly", "every hour", "each hour")):
        minute_at = re.search(r"\bminute\s+(\d{1,2})\b", lower)
        past = _PAST_HOUR_RE.search(lower)
        if past and not clock and not minute_at:
            # "hourly at :15 (past the hour)" — the minute is the anchor, the
            # hour field stays * (D44).
            anchored = int(past.group(1))
            return CadenceSpec(
                interval="hourly",
                cron=f"{anchored} * * * *",
                timezone=tz,
                description=f"every hour at :{anchored:02d}{_zone_txt(tz, assumed)}",
                timezone_assumed=assumed,
            )
        if minute_at and not clock:
            anchored = int(minute_at.group(1))
            if not 0 <= anchored <= 59:
                return _ask(
                    "An hourly run needs a minute from 0 to 59. "
                    "Say e.g. “hourly at minute 7”."
                )
            return CadenceSpec(
                interval="hourly",
                cron=f"{anchored} * * * *",
                timezone=tz,
                description=f"every hour at :{anchored:02d}{_zone_txt(tz, assumed)}",
                timezone_assumed=assumed,
            )
        if hour is not None and clock:
            # "hourly at :15" — the hour field is meaningless, the minute is not.
            return CadenceSpec(
                interval="hourly",
                cron=f"{minute} * * * *",
                timezone=tz,
                description=f"every hour at :{minute:02d}{_zone_txt(tz, assumed)}",
                timezone_assumed=assumed,
            )
        return CadenceSpec(
            interval="hourly",
            timezone=tz,
            description="every hour, starting at the first run",
            timezone_assumed=assumed,
        )

    # Multi-day and multi-time cadences: every named day and every named time
    # belongs in the cron — never just the first match (D41).
    if dows or "week" in lower or _MULTI_A_DAY_RE.search(lower):
        if times and len(times) > 1:
            mm = _times_cron(times)
            if mm is None:
                frag = " and ".join(_clock(h, m) for h, m in times)
                return _ask(
                    f"“{frag}” is not one cron — the fields would cross and fire "
                    f"{len(times) * 2 - len(times)} extra times. Give me one daily "
                    "time, or make these separate schedules."
                )
            cron = f"{mm} * * {_dow_field(dows)}"
            day_names = _dow_names(dows)
            return CadenceSpec(
                interval="cron",
                cron=cron,
                timezone=tz,
                description=(
                    f"{'every ' + day_names if day_names else 'every day'} at "
                    + ", ".join(_clock(h, m) for h, m in times)
                    + _zone_txt(tz, assumed)
                ),
                timezone_assumed=assumed,
            )
        if dows and (hour is not None or (times and len(times) == 1)):
            h, m = times[0] if times else (hour, minute)
            if len(dows) == 1:
                # A single named weekday stays the weekly preset the schedule
                # model already carries — same cron, honest interval class.
                return _weekly(dows[0], h, m, tz, assumed)
            cron = f"{m} {h} * * {_dow_field(dows)}"
            return CadenceSpec(
                interval="cron",
                cron=cron,
                timezone=tz,
                description=(
                    f"every {_dow_names(dows)} at {_clock(h, m)}"
                    f"{_zone_txt(tz, assumed)}"
                ),
                timezone_assumed=assumed,
            )
        if len(dows) == 1:
            # One named day with no time anchors to the start of that day —
            # "weekly on Monday" means Monday 00:00, not whenever it was typed.
            return _weekly(dows[0], hour, minute, tz, assumed)
        if dows:
            return _ask(
                "That needs a time of day. Say e.g. “Tuesdays and Thursdays "
                "at 16:15 UTC” — without a time it would run whenever it was created."
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

    bare = _bare_cron(raw, tz, assumed)
    if bare is not None:
        return bare

    return _ask(
        f"I could not turn “{raw.strip()}” into a schedule. I can do hourly, daily "
        "at a time, weekly on a weekday, a day of the month, every N minutes/hours, "
        "or an explicit 5-field cron."
    )


def describe_stored_cadence(interval: str, cron: str = "", timezone: str = "UTC") -> str:
    """Human label for a saved schedule. Cron wins over the interval preset.

    ``hourly`` plus ``7 * * * *`` is hourly at minute 7, not a rolling hour.
    ``daily`` plus ``40 21 * * 1-5`` is weekdays at 21:40, not every day.
    """
    expr = " ".join(str(cron or "").split())
    tz = (timezone or "UTC").strip() or "UTC"
    if expr:
        parts = expr.split(" ")
        if len(parts) == 5:
            minute, hour, dom, month, dow = parts
            step = re.fullmatch(r"\*/(\d+)", minute)
            if (
                hour == "*"
                and dom == "*"
                and month == "*"
                and dow == "*"
                and step
                and 1 <= int(step.group(1)) <= 59
            ):
                count = int(step.group(1))
                if 60 % count != 0:
                    return (
                        f"Cron {expr} ({tz}) — not every {count} minutes; "
                        "the step does not divide 60"
                    )
                return f"Every {count} minutes {tz}"
            if (
                hour == "*"
                and dom == "*"
                and month == "*"
                and dow == "*"
                and minute.isdigit()
            ):
                return f"Hourly at :{int(minute):02d} {tz}"
            if (
                dom == "*"
                and month == "*"
                and dow == "1-5"
                and minute.isdigit()
                and hour.isdigit()
            ):
                return f"Weekdays at {int(hour):02d}:{int(minute):02d} {tz}"
            if (
                dom == "*"
                and month == "*"
                and dow == "*"
                and minute.isdigit()
                and hour.isdigit()
            ):
                return f"Daily at {int(hour):02d}:{int(minute):02d} {tz}"
        return f"Cron {expr} ({tz})"
    token = (interval or "").strip().lower()
    rolling = re.fullmatch(r"every_(\d+)m", token)
    if rolling:
        return f"Every {int(rolling.group(1))} minutes, measured from the previous run"
    labels = {"hourly": "Every hour", "daily": "Daily", "weekly": "Weekly"}
    return labels.get(token, token or "Unscheduled")
