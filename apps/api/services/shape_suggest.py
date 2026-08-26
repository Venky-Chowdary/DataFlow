"""Profile the sampled rows, then suggest the step that fixes what it found.

Trifacta's lesson, and DataKitchen's: preparation is profile-driven, not a blank
canvas. Their TestGen only proposes an Email Format test when the profile found
emails; here a `trim` step is only offered when a value actually carries stray
whitespace, and the offer states how many rows it would touch.

Every suggestion is a ready-to-apply step payload plus the evidence for it, so
the operator accepts a decision rather than authors one — and the decision that
would have prevented the `NUMBER(11,8)` write failure ("27 of these values need
scale 9; round to 8") is one of them, offered at design time.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, DecimalException, InvalidOperation
from typing import Any, Mapping, Sequence

from services.shape_expr import is_blank

__all__ = ["ColumnProfile", "profile_columns", "suggest_steps"]

# Values a spreadsheet uses to mean "nothing" that arrive as text and would
# otherwise be loaded as the literal string.
_SENTINELS = ("n/a", "na", "null", "none", "nil", "-", "--", "?", "unknown", "#n/a")

_BOOLEAN_WORDS = {"y", "n", "yes", "no", "true", "false", "t", "f"}

# Formats offered for a date-looking column. Ambiguous day/month orders are
# never chosen automatically: when both fit, the operator is asked.
_DATE_FORMATS: tuple[tuple[str, str], ...] = (
    ("%Y-%m-%d", "ISO date"),
    ("%Y-%m-%d %H:%M:%S", "ISO timestamp"),
    ("%Y/%m/%d", "year/month/day"),
    ("%d/%m/%Y", "day/month/year"),
    ("%m/%d/%Y", "month/day/year"),
    ("%d-%m-%Y", "day-month-year"),
    ("%m-%d-%Y", "month-day-year"),
    ("%d.%m.%Y", "day.month.year"),
    ("%d %b %Y", "day month-name year"),
    ("%b %d, %Y", "month-name day, year"),
)

_MAX_DISTINCT = 50
_MAX_SAMPLES = 5


@dataclass
class ColumnProfile:
    """What the sampled values of one column look like."""

    name: str
    rows: int = 0
    blanks: int = 0
    distinct: int = 0
    distinct_capped: bool = False
    samples: list[str] = field(default_factory=list)
    logical_type: str = "empty"
    numeric_like: int = 0
    integer_like: int = 0
    max_scale: int = 0
    max_integer_digits: int = 0
    min_value: str = ""
    max_value: str = ""
    needs_parse_number: int = 0
    untrimmed: int = 0
    inner_whitespace: int = 0
    sentinels: dict[str, int] = field(default_factory=dict)
    non_printable: int = 0
    unnormalized_unicode: int = 0
    boolean_like: int = 0
    # How many sampled values carry each decimal scale, so "how many rows does
    # NUMBER(11,8) reject" is answered by counting, not by estimating.
    scale_counts: dict[int, int] = field(default_factory=dict)
    date_formats: list[str] = field(default_factory=list)
    ambiguous_date_order: bool = False
    max_length: int = 0

    @property
    def non_blank(self) -> int:
        return self.rows - self.blanks

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "rows": self.rows,
            "blanks": self.blanks,
            "non_blank": self.non_blank,
            "distinct": self.distinct,
            "distinct_capped": self.distinct_capped,
            "samples": list(self.samples),
            "logical_type": self.logical_type,
            "numeric_like": self.numeric_like,
            "integer_like": self.integer_like,
            "max_scale": self.max_scale,
            "max_integer_digits": self.max_integer_digits,
            "min": self.min_value,
            "max": self.max_value,
            "untrimmed": self.untrimmed,
            "inner_whitespace": self.inner_whitespace,
            "sentinels": dict(self.sentinels),
            "non_printable": self.non_printable,
            "unnormalized_unicode": self.unnormalized_unicode,
            "boolean_like": self.boolean_like,
            "scale_counts": {str(k): v for k, v in sorted(self.scale_counts.items())},
            "date_formats": list(self.date_formats),
            "ambiguous_date_order": self.ambiguous_date_order,
            "max_length": self.max_length,
        }


def profile_columns(
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: Sequence[str] | None = None,
) -> list[ColumnProfile]:
    """Profile each column of the sampled rows.

    This is a *sample* profile, and the suggestions it produces say so: the
    population-level decision still belongs to Validate, which scans every row.
    """
    names: list[str] = list(columns) if columns else []
    if not names:
        seen: dict[str, None] = {}
        for row in rows:
            for key in row:
                seen.setdefault(str(key), None)
        names = list(seen)

    profiles: list[ColumnProfile] = []
    for name in names:
        profile = ColumnProfile(name=name, rows=len(rows))
        distinct: set[str] = set()
        date_candidates: set[str] | None = None
        numbers: list[Decimal] = []
        texts: list[str] = []

        for row in rows:
            value = row.get(name)
            if is_blank(value):
                profile.blanks += 1
                continue
            text = _as_text(value)
            if len(distinct) < _MAX_DISTINCT:
                distinct.add(text)
            else:
                profile.distinct_capped = True
            if len(profile.samples) < _MAX_SAMPLES and text not in profile.samples:
                profile.samples.append(text)

            profile.max_length = max(profile.max_length, len(text))
            if text != text.strip():
                profile.untrimmed += 1
            stripped = text.strip()
            if "  " in stripped or "\t" in stripped or "\n" in stripped:
                profile.inner_whitespace += 1
            if any(not c.isprintable() and c not in "\t\n" for c in text):
                profile.non_printable += 1
            if not unicodedata.is_normalized("NFC", text):
                profile.unnormalized_unicode += 1
            folded = stripped.casefold()
            if folded in _SENTINELS:
                profile.sentinels[stripped] = profile.sentinels.get(stripped, 0) + 1
            if folded in _BOOLEAN_WORDS or folded in ("0", "1"):
                profile.boolean_like += 1

            number = _plain_decimal(value)
            if number is None:
                human = _human_decimal(stripped)
                if human is not None:
                    profile.numeric_like += 1
                    profile.needs_parse_number += 1
                    number = human
                elif "," in stripped or (stripped.count(".") > 1):
                    # Grouped but Auto refused — still needs parse_number + locale.
                    profile.needs_parse_number += 1
            else:
                profile.numeric_like += 1
            if number is not None:
                numbers.append(number)
                scale = max(0, -int(number.as_tuple().exponent))
                profile.max_scale = max(profile.max_scale, scale)
                profile.scale_counts[scale] = profile.scale_counts.get(scale, 0) + 1
                digits = len(number.as_tuple().digits) - scale
                profile.max_integer_digits = max(profile.max_integer_digits, max(digits, 1))
                if scale == 0:
                    profile.integer_like += 1
            else:
                texts.append(stripped)
                fits = _date_formats_for(stripped, value)
                date_candidates = fits if date_candidates is None else (date_candidates & fits)

        profile.distinct = len(distinct)
        if numbers:
            profile.min_value = format(min(numbers), "f")
            profile.max_value = format(max(numbers), "f")
        elif texts:
            profile.min_value = min(texts)
            profile.max_value = max(texts)

        if date_candidates and not numbers:
            ordered = [f for f, _ in _DATE_FORMATS if f in date_candidates]
            profile.date_formats = ordered
            profile.ambiguous_date_order = _is_ambiguous(ordered)

        profile.logical_type = _logical_type(profile)
        profiles.append(profile)
    return profiles


def suggest_steps(
    profiles: Sequence[ColumnProfile],
    *,
    target_schema: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Steps worth offering, each with the evidence that justifies it.

    ``target_schema`` is the destination's declared carriers. When present, the
    narrowing cases — the ones the writer would otherwise reject mid-load — are
    offered first, because they are the ones that fail a run.
    """
    schema = {str(k): str(v) for k, v in (target_schema or {}).items()}
    out: list[dict[str, Any]] = []

    for profile in profiles:
        name = profile.name

        fit = _narrowing_scale(profile, schema.get(name, ""))
        if fit is not None:
            scale, offenders = fit
            out.append(
                _suggestion(
                    op="round_number",
                    column=name,
                    options={"places": scale},
                    title=f"Round '{name}' to {scale} decimal place(s)",
                    reason=(
                        f"{offenders} sampled value(s) carry more than {scale} decimal "
                        f"place(s), which {schema.get(name, 'the destination carrier')} "
                        "cannot hold — the writer would reject those rows mid-load"
                    ),
                    rows_affected=offenders,
                    severity="blocking",
                )
            )

        if profile.untrimmed:
            out.append(
                _suggestion(
                    op="trim",
                    column=name,
                    options={},
                    title=f"Trim whitespace around '{name}'",
                    reason=(
                        f"{profile.untrimmed} sampled value(s) have leading or trailing "
                        "whitespace, which changes joins, keys and uniqueness downstream"
                    ),
                    rows_affected=profile.untrimmed,
                )
            )

        if profile.inner_whitespace:
            out.append(
                _suggestion(
                    op="collapse_whitespace",
                    column=name,
                    options={},
                    title=f"Collapse repeated whitespace in '{name}'",
                    reason=f"{profile.inner_whitespace} sampled value(s) contain runs of whitespace",
                    rows_affected=profile.inner_whitespace,
                )
            )

        if profile.sentinels:
            listed = sorted(profile.sentinels)
            out.append(
                _suggestion(
                    op="null_if",
                    column=name,
                    options={"values": listed},
                    title=f"Treat {', '.join(repr(v) for v in listed)} in '{name}' as null",
                    reason=(
                        "these are placeholders for missing data; loaded as text they "
                        "become real values the destination cannot tell from data"
                    ),
                    rows_affected=sum(profile.sentinels.values()),
                )
            )

        if profile.non_printable:
            out.append(
                _suggestion(
                    op="strip_characters",
                    column=name,
                    options={"characters": "non_printable"},
                    title=f"Strip control characters from '{name}'",
                    reason=(
                        f"{profile.non_printable} sampled value(s) contain non-printable "
                        "characters, which break delimited exports and comparisons"
                    ),
                    rows_affected=profile.non_printable,
                )
            )

        if profile.unnormalized_unicode:
            out.append(
                _suggestion(
                    op="normalize_unicode",
                    column=name,
                    options={"form": "NFC"},
                    title=f"Normalise '{name}' to Unicode NFC",
                    reason=(
                        f"{profile.unnormalized_unicode} sampled value(s) are not in NFC, "
                        "so two visually identical values compare as different"
                    ),
                    rows_affected=profile.unnormalized_unicode,
                )
            )

        if profile.needs_parse_number and profile.numeric_like == profile.non_blank and profile.non_blank:
            out.append(
                _suggestion(
                    op="parse_number",
                    column=name,
                    options={},
                    title=f"Parse '{name}' as a number",
                    reason=(
                        f"{profile.needs_parse_number} sampled value(s) are numbers written "
                        "for people (grouping separators, currency, parenthesised negatives) "
                        "and would land as text"
                    ),
                    rows_affected=profile.needs_parse_number,
                )
            )

        if profile.date_formats and profile.non_blank:
            if profile.ambiguous_date_order:
                out.append(
                    _suggestion(
                        op="parse_date",
                        column=name,
                        options={"format": profile.date_formats[0]},
                        title=f"Declare the date format of '{name}'",
                        reason=(
                            "the sampled values fit both day/month and month/day order — "
                            f"{', '.join(profile.date_formats)} — so the order must be "
                            "stated, never guessed"
                        ),
                        rows_affected=profile.non_blank,
                        severity="decision",
                    )
                )
            else:
                out.append(
                    _suggestion(
                        op="parse_date",
                        column=name,
                        options={"format": profile.date_formats[0]},
                        title=f"Parse '{name}' as a date ({profile.date_formats[0]})",
                        reason="every sampled value matches this format and only this one",
                        rows_affected=profile.non_blank,
                    )
                )

    order = {"blocking": 0, "decision": 1, "hygiene": 2}
    out.sort(key=lambda s: (order.get(s["severity"], 3), -int(s["rows_affected"])))
    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _suggestion(
    *,
    op: str,
    column: str,
    options: Mapping[str, Any],
    title: str,
    reason: str,
    rows_affected: int,
    severity: str = "hygiene",
) -> dict[str, Any]:
    return {
        "id": f"{op}:{column}",
        "title": title,
        "reason": reason,
        "rows_affected": rows_affected,
        "severity": severity,
        # Ready to POST straight back as a recipe step — no client-side
        # translation, so the UI cannot invent an option the engine rejects.
        "step": {"op": op, "column": column, "options": dict(options)},
    }


def _narrowing_scale(profile: ColumnProfile, declared: str) -> tuple[int, int] | None:
    """Scale the destination allows, and how many sampled values exceed it."""
    if not declared or not profile.numeric_like:
        return None
    from services.type_system import parse_numeric_precision_scale

    _, scale = parse_numeric_precision_scale(declared)
    if scale is None or profile.max_scale <= scale:
        return None
    offenders = sum(
        count for observed, count in profile.scale_counts.items() if observed > scale
    )
    return scale, offenders


def _plain_decimal(value: Any) -> Decimal | None:
    """The finite number this cell holds, or ``None``.

    ``NaN`` / ``Infinity`` parse as Decimal but have no digits or exponent to
    measure — ``as_tuple().exponent`` is ``'n'`` / ``'F'``, so profiling them
    raised and the whole Transform preview answered 500. They are
    non-numbers here and profile as text.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    if isinstance(value, str):
        try:
            parsed = Decimal(value.strip())
        except (InvalidOperation, DecimalException, ValueError):
            return None
        return parsed if parsed.is_finite() else None
    return None


def _human_decimal(text: str) -> Decimal | None:
    """A number written for a person — 1,234.50, $99, (9.99) — or ``None``.

    Only reached for values a plain ``Decimal`` refused, so anything this returns
    is by definition a value that needs a `parse_number` step to land as a
    number rather than as text. Uses the write-path parser — Auto fails closed
    on a lone ``1,234`` / ``1.234``.
    """
    from services.transform_engine import decimal_wire_value

    return decimal_wire_value(text)


def _date_formats_for(text: str, original: Any) -> set[str]:
    if isinstance(original, (datetime, date)):
        return {"%Y-%m-%d"}
    fits = set()
    for fmt, _label in _DATE_FORMATS:
        try:
            datetime.strptime(text, fmt)
        except ValueError:
            continue
        fits.add(fmt)
    return fits


def _is_ambiguous(formats: Sequence[str]) -> bool:
    """Whether day-first and month-first both fit — the classic silent corruption."""
    day_first = {"%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"}
    month_first = {"%m/%d/%Y", "%m-%d-%Y"}
    present = set(formats)
    return bool(present & day_first) and bool(present & month_first)


def _logical_type(profile: ColumnProfile) -> str:
    if profile.non_blank == 0:
        return "empty"
    if profile.numeric_like == profile.non_blank:
        return "integer" if profile.integer_like == profile.non_blank else "decimal"
    if profile.boolean_like == profile.non_blank:
        return "boolean"
    if profile.date_formats:
        return "date"
    return "text"


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)
