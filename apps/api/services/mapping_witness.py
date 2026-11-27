"""Mapping Witness: deterministic evidence that each mapped pair means what it says.

Gate-8 proves the destination holds exactly what the mapping wrote. It cannot
see a mapping that is *wrong*: ``first_name → last_name`` copies perfectly and
verifies perfectly. The witness looks at the data instead of the names:

* **Keyed** — the destination already holds rows for keys in the source sample
  (an existing table, a previous load by another system). For each mapped
  pair it counts how often the source value equals the destination value on the
  same key, and it does the same for every other destination column. A pair
  that disagrees while another column agrees is a likely swap or wrong target;
  a pair that agrees only after ×100 is a unit mismatch; dates that agree only
  when read the other way round from the run's declared order are a day/month
  mismatch.
* **Profile** — destination rows that do not join on a key: value-set overlap
  stands in for the key join, and only a swap whose two halves point at each
  other is flagged (one side alone proves nothing: the rows differ).
* **Source only** — before load: the values are checked against what the
  destination column's name and type promise (an ``email`` column gets email
  addresses, a ``*_cents`` column gets whole numbers, ``first_name`` gets given
  names) and against the run's declared date order.

Prefer silence to a wrong flag. A flag needs corroboration: on keys the mapped
column must disagree (≤ 50 %) *and* an alternative must agree (≥ 90 %, κ ≥ 0.8
on a column with more than one distinct value). "Witnessed" needs the same κ
and no other column that matches strictly more often; otherwise the pair is
"consistent". A pair whose mapping transforms the value (mask, hash, redact, an
expression) is "not comparable", never "differs". A pair the witness cannot
judge says "no evidence", not "passed".

Every result is evidence with a reason; the witness never changes a mapping.
Nothing here returns a cell value: reasons and evidence carry column names and
counts only. Values are bounded before parsing (length and exponent), so a
hostile cell cannot stall or crash the check.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import unicodedata
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, DecimalException
from typing import Any, Iterable

WITNESS_VERSION = "mapping_witness.v2"

#: Rows read from each side; the witness is a sample check, never a full scan.
MAX_ROWS = 500
#: Mapped pairs judged per request.
MAX_PAIRS = 400
#: Matching keys a keyed verdict needs.
MIN_KEYED = 5
#: Non-empty values a profile verdict needs.
MIN_PROFILE = 5
#: Share of matching keys that must agree for "witnessed".
WITNESSED_AT = 0.9
#: The mapped pair must agree on at most this share before another column can contradict it.
CONTRADICTED_AT_MOST = 0.5
#: An alternative (another column, a unit factor, the other date order) must agree this often.
ALTERNATIVE_AT = 0.9
#: …and beat chance by this much (Cohen's κ) on a column with more than one value.
ALTERNATIVE_KAPPA = 0.8
#: Cells are compared on at most this many characters; numbers parse only when this short.
MAX_CELL_CHARS = 512
MAX_NUMBER_CHARS = 64
#: Decimal exponents beyond this are text, not numbers (no 1 MB expansions, no Overflow).
MAX_EXPONENT = 60

STATUS_WITNESSED = "witnessed"
STATUS_CONSISTENT = "consistent"
STATUS_FLAGGED = "flagged"
STATUS_DIFFERS = "differs"
STATUS_NOT_COMPARABLE = "not_comparable"
STATUS_NO_EVIDENCE = "no_evidence"
#: The destination column was not read (not in the catalog yet, a name the sampler cannot address, or past
#: the read bound): no evidence either way, never "differs".
STATUS_NOT_READ = "not_read"

KIND_SWAP = "swap"
KIND_WRONG_TARGET = "wrong_target"
KIND_UNIT = "unit_mismatch"
KIND_FORMAT = "format_mismatch"

MODE_KEYED = "keyed"
MODE_PROFILE = "profile"
MODE_SOURCE_ONLY = "source_only"

#: Transforms whose output is still comparable to the source value as written
#: (casts, case, trimming); anything else makes the pair "not comparable".
COMPARABLE_TRANSFORMS = frozenset({
    "", "none", "identity", "trim", "strip", "upper", "lower", "uppercase", "lowercase", "string", "text",
    "varchar", "integer", "int", "bigint", "smallint", "decimal", "numeric", "number", "float", "double",
    "boolean", "bool", "date", "datetime", "timestamp", "timestamptz", "time", "iso", "date_iso",
})

_HONESTY = (
    "Evidence only: the witness never changes a mapping. It reads a bounded sample, "
    "compares values on matching keys when the destination already holds them, and "
    "otherwise checks value profiles. Reasons carry column names and counts, never values."
)

_NUM = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_ISO_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO = re.compile(
    r"^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?)?\s*(Z|[+-]\d{2}:?\d{2})?$"
)
_SLASH_DATE = re.compile(r"^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4}|\d{2})$")
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_HANDLE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,63}$")
_PREFIXED_ID = re.compile(r"^[a-z]{2,6}_[a-z0-9]{2,}$")
_TRUE = {"true", "t", "yes", "y"}
_FALSE = {"false", "f", "no", "n"}
_UNIT_FACTORS = (Decimal(100), Decimal("0.01"), Decimal(1000), Decimal("0.001"), Decimal(10), Decimal("0.1"))


# ── value normalization ──────────────────────────────────────────────────────


def _bounded_decimal(text: str) -> Decimal | None:
    """A Decimal for a short numeric spelling with a sane exponent, else None."""
    if len(text) > MAX_NUMBER_CHARS or not _NUM.match(text):
        return None
    try:
        value = Decimal(text)
    except (DecimalException, ValueError):
        return None
    if not value.is_finite() or abs(value.adjusted()) > MAX_EXPONENT:
        return None
    return value


def _canon_number(value: Decimal) -> str | None:
    try:
        if not value.is_finite() or abs(value.adjusted()) > MAX_EXPONENT:
            return None
        if value == 0:
            return "0"
        text = format(value.normalize(), "f")
    except (DecimalException, ValueError, OverflowError):
        return None
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _canon_iso(text: str, match: re.Match[str]) -> str:
    day, hh, _mm, _ss, _frac, tz = match.groups()
    if hh is None:
        return day
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return text.casefold()
    if parsed.tzinfo is not None:
        # The same instant in another zone is the same value: compare in UTC.
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    if parsed.time() == datetime.min.time():
        # A DATE carried as midnight on a TIMESTAMP is the same calendar day.
        return parsed.date().isoformat()
    return parsed.isoformat()


def norm(value: Any) -> str | None:
    """One comparable spelling of a cell: numbers, booleans, instants and case folded.

    ``None``/blank is ``None`` (missing, never a match). Bounded: long cells are
    cut to :data:`MAX_CELL_CHARS`, and huge exponents stay text.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if value.bit_length() > 200:
            return str(value)[:MAX_CELL_CHARS]
        return _canon_number(Decimal(value))
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return _canon_number(value) or str(value).casefold()[:MAX_CELL_CHARS]
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        # Beyond the exponent bound a float is compared as written, never dropped as missing.
        return _canon_number(Decimal(repr(value))) or repr(value).casefold()
    if isinstance(value, datetime):
        text = value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    else:
        text = unicodedata.normalize("NFC", str(value)[:MAX_CELL_CHARS * 2]).strip()[:MAX_CELL_CHARS]
    if not text:
        return None
    low = text.casefold()
    if low in _TRUE:
        return "true"
    if low in _FALSE:
        return "false"
    number = _bounded_decimal(text)
    if number is not None:
        return _canon_number(number) or low
    iso = _ISO.match(text)
    if iso:
        return _canon_iso(text, iso)
    return low


def _decimal(value: str | None) -> Decimal | None:
    return None if value is None else _bounded_decimal(value)


def _slash_parts(text: Any) -> tuple[int, int, int] | None:
    if text is None:
        return None
    m = _SLASH_DATE.match(str(text).strip()[:16])
    if not m:
        return None
    a, b, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if len(m.group(3)) == 2:
        year += 2000 if year < 70 else 1900
    return a, b, year


def _iso_day(day: int, month: int, year: int) -> str | None:
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _read_slash_date(parts: tuple[int, int, int], order: str) -> str | None:
    a, b, year = parts
    return _iso_day(a, b, year) if order == "DMY" else _iso_day(b, a, year)


def _orders_valid(parts: tuple[int, int, int]) -> tuple[bool, bool]:
    """(valid read day-first, valid read month-first)."""
    return _read_slash_date(parts, "DMY") is not None, _read_slash_date(parts, "MDY") is not None


def _loader_order(parts: tuple[int, int, int], date_locale: str) -> str | None:
    """The day/month order the loader reads this value in (transform_engine rules)."""
    if date_locale in ("DMY", "MDY"):
        return date_locale
    a, b, _ = parts
    if a > 12:
        return "DMY"
    if b > 12 or a == b:
        return "MDY"
    return None  # ambiguous: the loader refuses it rather than guess


def _is_date_only(value: str | None) -> bool:
    return bool(value) and bool(_ISO_DAY.match(value or ""))


def _date_part(value: str | None) -> str | None:
    if value is None:
        return None
    return value[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", value) else None


def _transform_name(mapping: dict[str, Any]) -> str:
    raw = mapping.get("transform") if "transform" in mapping else mapping.get("transformation")
    if isinstance(raw, dict):
        raw = raw.get("type") or raw.get("name") or "expression"
    return str(raw or "").strip().lower()


def comparable_transform(name: str) -> bool:
    return name.replace("-", "_") in COMPARABLE_TRANSFORMS


# ── names and types → what a destination column promises ─────────────────────


def _tokens(name: str) -> list[str]:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(name or ""))
    return [t for t in re.split(r"[^a-z0-9]+", spaced.lower()) if t]


def _is_email_name(name: str) -> bool:
    """The column holds an address: ``email``, ``work_email``, ``e_mail``, ``email_address``.

    ``email_domain``, ``email_status`` or ``email_notifications`` name *about*
    an email, not an address.
    """
    toks = _tokens(name)
    if not toks:
        return False
    if toks[-1] in ("email", "emailaddress", "emailaddr"):
        return True
    if toks[-2:] == ["e", "mail"]:
        return True
    return toks[-2:] in (["email", "address"], ["email", "addr"], ["mail", "address"])


def _is_minor_units_name(name: str) -> bool:
    toks = _tokens(name)
    return "cents" in toks or "pennies" in toks or ("minor" in toks and ("units" in toks or "unit" in toks)) or (
        bool(toks) and toks[-1] == "minor"
    )


def _is_money_name(name: str) -> bool:
    money = {"amount", "price", "total", "cost", "fee", "balance", "revenue", "salary", "subtotal", "tax",
             "paid", "cents", "dollars", "usd", "eur"}
    return bool(set(_tokens(name)) & money)


def _fractional_money_type(declared: str) -> bool:
    """A declared type that stores currency units with a fraction (DECIMAL(p,s>0), MONEY)."""
    typ = str(declared or "").upper().replace(" ", "")
    if typ.startswith(("MONEY", "SMALLMONEY")):
        return True
    m = re.match(r"^(DECIMAL|NUMERIC|NUMBER)\((\d+),(\d+)\)$", typ)
    return bool(m and int(m.group(3)) > 0)


def _is_date_target(name: str, declared: str) -> bool:
    typ = str(declared or "").upper()
    if typ.startswith(("DATE", "TIMESTAMP", "DATETIME", "TIME")):
        return True
    toks = set(_tokens(name))
    return bool(toks & {"date", "dt", "day", "dob", "birthday", "timestamp", "on", "at"})


def _name_role(name: str) -> str | None:
    toks = _tokens(name)
    joined = "".join(toks)
    if {"first", "given", "forename", "fname", "firstname", "givenname"} & set(toks) or joined in (
        "firstname", "givenname", "forename", "fname",
    ):
        return "given"
    if {"last", "surname", "family", "lname", "lastname", "familyname"} & set(toks) or joined in (
        "lastname", "surname", "familyname", "lname",
    ):
        return "surname"
    return None


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text[:MAX_CELL_CHARS])
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).casefold().strip()


# ── statistics ───────────────────────────────────────────────────────────────


def _agreement(pairs: Iterable[tuple[str | None, str | None]]) -> tuple[int, int, float | None]:
    """(agreed, compared, κ) over pairs whose left value is present.

    κ is ``None`` when agreement cannot beat chance: a column with one distinct
    value on either side "agrees" with any other constant column.
    """
    left: Counter[str] = Counter()
    right: Counter[str] = Counter()
    agreed = compared = 0
    for a, b in pairs:
        if a is None:
            continue
        compared += 1
        agreed += int(a == b)
        left[a] += 1
        right[b if b is not None else "∅"] += 1
    if not compared:
        return 0, 0, None
    if len(left) < 2 or len(right) < 2:
        return agreed, compared, None
    po = agreed / compared
    pe = sum(left[v] * right.get(v, 0) for v in left) / (compared * compared)
    if pe >= 1.0:
        return agreed, compared, None
    return agreed, compared, (po - pe) / (1.0 - pe)


def _scaled_agreement(pairs: list[tuple[str | None, str | None]]) -> tuple[Decimal, int, int] | None:
    """The unit factor under which numeric source values equal the destination's, if any."""
    numeric = [(_decimal(a), _decimal(b)) for a, b in pairs if a is not None]
    numeric = [(a, b) for a, b in numeric if a is not None and b is not None]
    if len(numeric) < MIN_KEYED or len({a for a, _ in numeric}) < 2:
        return None
    for factor in _UNIT_FACTORS:
        try:
            agreed = sum(1 for a, b in numeric
                         if abs(a * factor - b) <= max(abs(b), Decimal(1)) * Decimal("1e-9"))
        except DecimalException:
            return None
        if agreed / len(numeric) >= ALTERNATIVE_AT:
            return factor, agreed, len(numeric)
    return None


def _ratio(agreed: int, compared: int) -> float:
    return round(agreed / compared, 4) if compared else 0.0


def _unit_reason(factor: Decimal, money: bool) -> str:
    if money and factor == 100:
        return "Destination values are 100 times the source values: dollars going into cents."
    if money and factor == Decimal("0.01"):
        return "Source values are 100 times the destination values: cents going into dollars."
    if factor > 1:
        return f"Destination values are {_canon_number(factor)} times the source values: a unit or scale mismatch."
    inverse = _canon_number(Decimal(1) / factor)
    return f"Source values are {inverse} times the destination values: a unit or scale mismatch."


# ── the report ───────────────────────────────────────────────────────────────


def _omitted(m: dict[str, Any]) -> bool:
    return bool(m.get("intentional_omit") or m.get("intentionalOmit")) or _transform_name(m) == "omit"


def mapped_pairs(mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The pairs a mapping writes: source and target present, not omitted, one row per source."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        source = str(m.get("source") or "").strip()
        target = str(m.get("target") or "").strip()
        if not source or not target or _omitted(m):
            continue
        if source.casefold() in seen:
            continue
        seen.add(source.casefold())
        out.append({
            "source": source,
            "target": target,
            "create_new": bool(m.get("create_new")),
            "target_type": str(m.get("target_type") or m.get("dest_type") or ""),
            "source_type": str(m.get("source_type") or m.get("inferred_type") or ""),
            "transform": _transform_name(m),
        })
        if len(out) >= MAX_PAIRS:
            break
    return out


def _pair_result(pair: dict[str, Any], status: str, *, flag: dict[str, Any] | None = None,
                 evidence: dict[str, Any] | None = None, reason: str = "") -> dict[str, Any]:
    return {
        "source": pair["source"],
        "target": pair["target"],
        "status": status,
        "reason": reason,
        "flag": flag,
        "evidence": evidence or {},
    }


def _flag(kind: str, reason: str, *, suggested_target: str | None = None, basis: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "reason": reason,
        "suggested_target": suggested_target,
        "basis": basis,
        "action": "review",
    }


def _get(row: dict[str, Any], column: str) -> Any:
    """A cell by column name, case-insensitively (Snowflake returns NAME for name)."""
    if column in row:
        return row[column]
    folded = column.casefold()
    for key, value in row.items():
        if str(key).casefold() == folded:
            return value
    return None


def _dest_shape(values: list[str | None]) -> tuple[bool, bool]:
    """(holds dates or instants, holds only booleans) for a destination column's values."""
    present = [v for v in values if v is not None]
    if not present:
        return False, False
    dated = sum(1 for v in present if _date_part(v) is not None)
    return dated >= 0.8 * len(present), all(v in ("true", "false") for v in present)


def _loaded(raw: Any, date_locale: str, *, dest_dated: bool, dest_bool: bool) -> str | None:
    """The source value the way the run writes it into a column of this shape."""
    if dest_dated:
        parts = _slash_parts(raw)
        if parts is not None:
            order = _loader_order(parts, date_locale)
            return _read_slash_date(parts, order) if order else None
    value = norm(raw)
    if dest_bool and value in ("1", "0"):
        return "true" if value == "1" else "false"
    return value


def _comparable(loaded: list[str | None], dest: list[str | None]) -> list[str | None]:
    """Destination values in the source's granularity: a date compared to an instant uses its day."""
    present = [v for v in loaded if v is not None]
    if present and sum(1 for v in present if _is_date_only(v)) >= 0.8 * len(present):
        return [_date_part(v) if v is not None else None for v in dest]
    return dest


def _keyed(pairs: list[dict[str, Any]], source_rows: list[dict], dest_rows: list[dict], key: tuple[str, str],
           date_locale: str, unread: dict[str, str]) -> tuple[list[dict[str, Any]], int]:
    skey, tkey = key
    index: dict[str, dict] = {}
    for row in dest_rows:
        k = norm(_get(row, tkey)) if isinstance(row, dict) else None
        if k is not None and k not in index:
            index[k] = row
    joined: list[tuple[dict, dict]] = []
    for row in source_rows:
        k = norm(_get(row, skey)) if isinstance(row, dict) else None
        if k is not None and k in index:
            joined.append((row, index[k]))
    # Destination columns by folded name; reported with the destination's own spelling.
    spelled: dict[str, str] = {}
    for _, d in joined:
        for c in d:
            spelled.setdefault(str(c).casefold(), str(c))
    dest_vals = {f: [norm(_get(d, f)) for _, d in joined] for f in spelled}
    results: dict[str, dict[str, Any]] = {}
    for pair in pairs:
        s, t = pair["source"], pair["target"]
        tf = t.casefold()
        if not comparable_transform(pair["transform"]):
            results[s] = _pair_result(pair, STATUS_NOT_COMPARABLE,
                                      evidence={"mode": MODE_KEYED, "transform": pair["transform"]},
                                      reason=f"Not comparable: the mapping transforms the value ({pair['transform']}).")
            continue
        if tf in unread:
            results[s] = _pair_result(pair, STATUS_NOT_READ, evidence={"mode": MODE_KEYED},
                                      reason=f"Not read: {unread[tf]}")
            continue
        if pair["create_new"] and tf not in dest_vals:
            continue  # a column this run will create holds nothing yet
        raw = [_get(r, s) for r, _ in joined]
        # A destination column the rows do not carry is empty on every matching key.
        target_vals = dest_vals.get(tf) or [None] * len(joined)
        dated, boolean = _dest_shape(target_vals)
        loaded = [_loaded(v, date_locale, dest_dated=dated, dest_bool=boolean) for v in raw]
        agreed, compared, kappa = _agreement(zip(loaded, _comparable(loaded, target_vals)))
        if compared < MIN_KEYED:
            continue
        evidence: dict[str, Any] = {
            "mode": MODE_KEYED, "matching_keys": len(joined), "compared": compared, "agreed": agreed,
            "agreement": _ratio(agreed, compared), "kappa": None if kappa is None else round(kappa, 4),
        }
        if s.casefold() == skey.casefold() and tf == tkey.casefold():
            evidence["join_key"] = True
        ratio = agreed / compared
        best: tuple[str, int, int, float | None] | None = None
        for f, values in dest_vals.items():
            if f == tf:
                continue
            d2, b2 = _dest_shape(values)
            alt_loaded = loaded if (d2, b2) == (dated, boolean) else [
                _loaded(v, date_locale, dest_dated=d2, dest_bool=b2) for v in raw]
            a2, n2, k2 = _agreement(zip(alt_loaded, _comparable(alt_loaded, values)))
            if n2 >= MIN_KEYED and (best is None or (a2 / n2) > (best[1] / best[2])):
                best = (spelled[f], a2, n2, k2)
        strong_alt = best if best and best[1] / best[2] >= ALTERNATIVE_AT and best[3] is not None \
            and best[3] >= ALTERNATIVE_KAPPA else None
        if best and best[1] / best[2] > ratio:
            evidence["best_other_column"] = {"target": best[0], "agreed": best[1], "compared": best[2],
                                             "agreement": _ratio(best[1], best[2])}
        if ratio >= WITNESSED_AT:
            if evidence.get("join_key"):
                results[s] = _pair_result(pair, STATUS_WITNESSED, evidence=evidence,
                                          reason=f"Join key: {agreed} of {compared} keys match.")
            elif kappa is None:
                results[s] = _pair_result(pair, STATUS_CONSISTENT, evidence=evidence,
                                          reason="Values agree, but the column has too few distinct values to "
                                                 "prove the pair.")
            elif best and best[1] / best[2] > ratio:
                results[s] = _pair_result(pair, STATUS_CONSISTENT, evidence=evidence,
                                          reason=f"Values agree with {t}, but {best[0]} matches more often "
                                                 f"({best[1]} of {best[2]}).")
            elif kappa >= ALTERNATIVE_KAPPA:
                results[s] = _pair_result(pair, STATUS_WITNESSED, evidence=evidence,
                                          reason=f"{agreed} of {compared} matching keys hold the same value in {t}.")
            else:
                results[s] = _pair_result(pair, STATUS_CONSISTENT, evidence=evidence,
                                          reason="Values mostly agree, but not clearly beyond chance.")
            continue
        # 1. Day/month order: the values agree only when read in the order the run does not use.
        if dated and date_locale in ("DMY", "MDY") and ratio <= CONTRADICTED_AT_MOST:
            other = "MDY" if date_locale == "DMY" else "DMY"
            parts = [_slash_parts(v) for v in raw]
            read = [_read_slash_date(p, other) if p else None for p in parts]
            a3, n3, _ = _agreement(zip(read, _comparable(read, target_vals)))
            if n3 >= MIN_KEYED and a3 / n3 >= ALTERNATIVE_AT:
                wanted = "day first (dd/mm)" if other == "DMY" else "month first (mm/dd)"
                run = "day first" if date_locale == "DMY" else "month first"
                evidence["date_order_agreement"] = {"order": other, "agreed": a3, "compared": n3}
                results[s] = _pair_result(
                    pair, STATUS_FLAGGED, evidence=evidence,
                    flag=_flag(KIND_FORMAT, f"Dates match {t} only when read {wanted}; this run reads {run}.",
                               basis=MODE_KEYED),
                    reason=f"{a3} of {n3} dates agree when read {wanted}.",
                )
                continue
        # 2. Units: the values agree after a power-of-ten factor.
        if ratio <= CONTRADICTED_AT_MOST:
            scaled = _scaled_agreement(list(zip(loaded, target_vals)))
            if scaled:
                factor, a4, n4 = scaled
                money = _is_money_name(s) or _is_money_name(t) or _is_minor_units_name(t)
                evidence["unit_factor"] = {"factor": _canon_number(factor), "agreed": a4, "compared": n4}
                results[s] = _pair_result(
                    pair, STATUS_FLAGGED, evidence=evidence,
                    flag=_flag(KIND_UNIT, _unit_reason(factor, money), basis=MODE_KEYED),
                    reason=f"{a4} of {n4} values agree after multiplying by {_canon_number(factor)}.",
                )
                continue
        # 3. Another column holds these values, beyond chance: wrong target, or half of a swap.
        if strong_alt and ratio <= CONTRADICTED_AT_MOST:
            results[s] = _pair_result(
                pair, STATUS_FLAGGED, evidence=evidence,
                flag=_flag(KIND_WRONG_TARGET,
                           f"Values match {strong_alt[0]} on {strong_alt[1]} of {strong_alt[2]} keys, "
                           f"not {t} ({agreed} of {compared}).",
                           suggested_target=strong_alt[0], basis=MODE_KEYED),
                reason=f"{strong_alt[1]} of {strong_alt[2]} matching keys hold this value in {strong_alt[0]}.",
            )
            continue
        results[s] = _pair_result(
            pair, STATUS_DIFFERS, evidence=evidence,
            reason=(f"{compared - agreed} of {compared} matching keys differ and no other column explains it; "
                    "the destination may hold older values."),
        )
    _mark_swaps(results, pairs)
    return list(results.values()), len(joined)


def _mark_swaps(results: dict[str, dict[str, Any]], pairs: list[dict[str, Any]]) -> None:
    """Two wrong targets that point at each other's columns are one swap."""
    by_target = {p["target"].casefold(): p["source"] for p in pairs}
    for res in results.values():
        flag = res.get("flag") or {}
        if flag.get("kind") != KIND_WRONG_TARGET:
            continue
        other = results.get(by_target.get(str(flag.get("suggested_target") or "").casefold(), ""))
        other_flag = (other or {}).get("flag") or {}
        if other_flag.get("kind") in (KIND_WRONG_TARGET, KIND_SWAP) and \
                str(other_flag.get("suggested_target") or "").casefold() == res["target"].casefold():
            for this, that in ((res, other), (other, res)):
                this["flag"] = {
                    **this["flag"], "kind": KIND_SWAP,
                    "reason": (f"{this['target']} and {that['target']} look swapped: values headed for "
                               f"{this['target']} match {that['target']} on the destination keys."),
                    "swapped_with": that["source"],
                }


def _profile_checks(pair: dict[str, Any], values: list[Any], date_locale: str) -> tuple[dict | None, dict, bool]:
    """Source-only checks against what the destination column promises. (flag, evidence, checked)."""
    s, t = pair["source"], pair["target"]
    present = [v for v in values if norm(v) is not None]
    n = len(present)
    evidence: dict[str, Any] = {"mode": MODE_SOURCE_ONLY, "values": n}
    if n < MIN_PROFILE:
        return None, evidence, False
    checked = False
    if _is_email_name(t):
        checked = True
        emails = sum(1 for v in present if _EMAIL.match(str(v).strip()[:MAX_CELL_CHARS]))
        evidence["email_values"] = emails
        if emails / n < 0.2:
            others = [str(v).strip().lower()[:MAX_CELL_CHARS] for v in present
                      if not _EMAIL.match(str(v).strip()[:MAX_CELL_CHARS])]
            if sum(1 for o in others if _PREFIXED_ID.match(o)) >= 0.6 * len(others):
                looks = "identifiers"
            elif sum(1 for o in others if _HANDLE.match(o)) >= 0.6 * len(others):
                looks = "user names"
            else:
                looks = "other text"
            return _flag(KIND_FORMAT, f"{t} holds email addresses, but {n - emails} of {n} values are not ({looks}).",
                         basis=MODE_SOURCE_ONLY), evidence, True
    numbers = [_decimal(norm(v)) for v in present]
    numeric = [d for d in numbers if d is not None]
    if _is_minor_units_name(t) and len(numeric) >= MIN_PROFILE:
        checked = True
        fractional = sum(1 for d in numeric if d != d.to_integral_value())
        evidence["fractional_values"] = fractional
        if fractional / len(numeric) >= 0.2:
            return _flag(KIND_UNIT, f"{t} stores cents, but {fractional} of {len(numeric)} values have fractions "
                                    "(they look like dollars).", basis=MODE_SOURCE_ONLY), evidence, True
    if (_is_minor_units_name(s) and not _is_minor_units_name(t) and _fractional_money_type(pair["target_type"])
            and len(numeric) >= MIN_PROFILE):
        checked = True
        whole = sum(1 for d in numeric if d == d.to_integral_value())
        evidence["whole_values"] = whole
        if whole == len(numeric):
            return _flag(KIND_UNIT, f"{s} holds cents and {t} holds currency units: values would load 100 times "
                                    "too large.", basis=MODE_SOURCE_ONLY), evidence, True
    if _is_date_target(t, pair["target_type"]):
        parts = [_slash_parts(v) for v in present]
        valid = [p for p in parts if p is not None and any(_orders_valid(p))]
        if len(valid) >= MIN_PROFILE and len(valid) >= 0.8 * n:
            checked = True
            only_dmy = sum(1 for p in valid if _orders_valid(p) == (True, False))
            only_mdy = sum(1 for p in valid if _orders_valid(p) == (False, True))
            evidence["day_first_values"] = only_dmy
            evidence["month_first_values"] = only_mdy
            if only_dmy and only_mdy:
                return _flag(KIND_FORMAT, f"Day/month order is mixed: {only_dmy} values are day first and "
                                          f"{only_mdy} month first.", basis=MODE_SOURCE_ONLY), evidence, True
            if date_locale == "MDY" and only_dmy:
                return _flag(KIND_FORMAT, f"Dates are written day first ({only_dmy} of {len(valid)} start with a "
                                          "day above 12), but this run reads dates month first.",
                             basis=MODE_SOURCE_ONLY), evidence, True
            if date_locale == "DMY" and only_mdy:
                return _flag(KIND_FORMAT, f"Dates are written month first ({only_mdy} of {len(valid)} have a day "
                                          "above 12 second), but this run reads dates day first.",
                             basis=MODE_SOURCE_ONLY), evidence, True
    if _name_role(t):
        folded = [_fold(str(v)) for v in present]
        from services.mapping_witness_lexicon import GIVEN_ONLY, SURNAME_ONLY

        evidence["given_names"] = sum(1 for v in folded if v in GIVEN_ONLY)
        evidence["surnames"] = sum(1 for v in folded if v in SURNAME_ONLY)
        checked = True
    return None, evidence, checked


def _name_swaps(results: dict[str, dict[str, Any]], pairs: list[dict[str, Any]]) -> None:
    """``first_name`` fed surnames while ``last_name`` is fed given names: flag both, or neither."""
    by_role: dict[str, list[dict[str, Any]]] = {"given": [], "surname": []}
    for p in pairs:
        role = _name_role(p["target"])
        if role:
            by_role[role].append(p)
    for g in by_role["given"]:
        for sname in by_role["surname"]:
            rg, rs = results.get(g["source"]), results.get(sname["source"])
            if not rg or not rs or rg.get("flag") or rs.get("flag"):
                continue
            if rg["status"] in (STATUS_WITNESSED, STATUS_NOT_COMPARABLE) or \
                    rs["status"] in (STATUS_WITNESSED, STATUS_NOT_COMPARABLE):
                continue  # values on the keys outrank a name list
            eg, es = rg["evidence"], rs["evidence"]
            ng, ns = int(eg.get("values") or 0), int(es.get("values") or 0)
            if ng < MIN_PROFILE or ns < MIN_PROFILE:
                continue
            given_side_is_surnames = eg.get("surnames", 0) >= 0.6 * ng and eg.get("given_names", 0) <= 0.2 * ng
            surname_side_is_given = es.get("given_names", 0) >= 0.6 * ns and es.get("surnames", 0) <= 0.2 * ns
            if given_side_is_surnames and surname_side_is_given:
                for this, that in ((rg, rs), (rs, rg)):
                    this["status"] = STATUS_FLAGGED
                    this["flag"] = _flag(
                        KIND_SWAP,
                        f"{this['target']} and {that['target']} look swapped: the values headed for "
                        f"{this['target']} are mostly {'surnames' if this is rg else 'given names'}.",
                        suggested_target=that["target"], basis=MODE_SOURCE_ONLY,
                    )
                    this["flag"]["swapped_with"] = that["source"]


def _profile_candidates(pairs: list[dict[str, Any]], source_rows: list[dict],
                        dest_cols: dict[str, set[str]], spelled: dict[str, str]) -> dict[str, tuple]:
    """Per source: (best other destination column, its overlap, shared, own overlap, own shared)."""
    out: dict[str, tuple] = {}
    for pair in pairs:
        vals = {v for v in (norm(_get(r, pair["source"])) for r in source_rows) if v is not None}
        tf = pair["target"].casefold()
        if len(vals) < MIN_PROFILE or tf not in dest_cols:
            continue
        own_shared = len(vals & dest_cols[tf])
        best: tuple[str, float, int] | None = None
        for f, other in dest_cols.items():
            if f == tf:
                continue
            shared = len(vals & other)
            if shared >= 3 and shared / len(vals) >= 0.5 and (best is None or shared / len(vals) > best[1]):
                best = (spelled[f], shared / len(vals), shared)
        out[pair["source"]] = (best, own_shared / len(vals), own_shared, len(vals))
    return out


def _fingerprint(pairs: list[dict[str, Any]], *, mode: str, key: tuple[str, str] | None, date_locale: str,
                 source_rows: list[dict], dest_rows: list[dict] | None, source_digest: str = "") -> str:
    """What the witness looked at, without a single value: names, counts, settings and the caller's
    keyed digest of the source sample (an edited file with the same name and row count is other inputs)."""
    def counts(rows: list[dict] | None, cols: list[str]) -> dict[str, int]:
        return {c: sum(1 for r in rows or [] if isinstance(r, dict) and norm(_get(r, c)) is not None) for c in cols}

    body = {
        "version": WITNESS_VERSION,
        "mode": mode,
        "key": list(key) if key else None,
        "date_locale": date_locale,
        "pairs": [[p["source"], p["target"], p["transform"]] for p in pairs],
        "source_rows": len(source_rows),
        "destination_rows": len(dest_rows or []),
        "source_non_null": counts(source_rows, [p["source"] for p in pairs]),
        "destination_non_null": counts(dest_rows, [p["target"] for p in pairs]),
        "source_content": str(source_digest or "") or None,
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def witness_mappings(
    mappings: list[dict[str, Any]],
    source_rows: list[dict[str, Any]] | None,
    dest_rows: list[dict[str, Any]] | None = None,
    *,
    key: tuple[str, str] | None = None,
    date_locale: str = "",
    generated_at: str | None = None,
    unread: dict[str, str] | None = None,
    source_digest: str = "",
) -> dict[str, Any]:
    """Judge every mapped pair against the data. Pure: same inputs, same report.

    ``unread`` names destination columns (any case) the sample did not read, with
    why; a pair onto one is "not read", never judged as an empty column.
    ``source_digest`` is the caller's keyed digest of the source sample (never a
    value); it goes into ``inputs_sha256`` and so into every flag id.
    """
    unread = {str(k).casefold(): str(v) for k, v in (unread or {}).items()}
    date_locale = str(date_locale or "").strip().upper()
    date_locale = date_locale if date_locale in ("DMY", "MDY") else ""
    pairs = mapped_pairs(mappings)
    source_rows = [r for r in (source_rows or []) if isinstance(r, dict)][:MAX_ROWS]
    dest_rows = [r for r in (dest_rows or []) if isinstance(r, dict)][:MAX_ROWS] if dest_rows else None
    results: dict[str, dict[str, Any]] = {}
    joined = 0
    mode = MODE_SOURCE_ONLY
    if dest_rows and key and key[0] and key[1]:
        keyed, joined = _keyed(pairs, source_rows, dest_rows, (key[0], key[1]), date_locale, unread)
        for res in keyed:
            results[res["source"]] = res
        if joined >= MIN_KEYED:
            mode = MODE_KEYED
    profile: dict[str, tuple] = {}
    if dest_rows and mode != MODE_KEYED:
        mode = MODE_PROFILE
        spelled: dict[str, str] = {}
        dest_cols: dict[str, set[str]] = {}
        for row in dest_rows:
            for c, v in row.items():
                f = str(c).casefold()
                spelled.setdefault(f, str(c))
                nv = norm(v)
                if nv is not None:
                    dest_cols.setdefault(f, set()).add(nv)
        profile = _profile_candidates(pairs, source_rows, dest_cols, spelled)
    for pair in pairs:
        prior = results.get(pair["source"])
        if prior and prior["status"] not in (STATUS_DIFFERS, STATUS_NOT_READ):
            continue
        if not comparable_transform(pair["transform"]):
            results[pair["source"]] = _pair_result(
                pair, STATUS_NOT_COMPARABLE, evidence={"mode": mode, "transform": pair["transform"]},
                reason=f"Not comparable: the mapping transforms the value ({pair['transform']}).")
            continue
        values = [_get(r, pair["source"]) for r in source_rows]
        flag, evidence, checked = _profile_checks(pair, values, date_locale)
        if prior:
            # Keys disagreed with no other column to explain it; a profile flag still counts.
            if flag:
                results[pair["source"]] = _pair_result(pair, STATUS_FLAGGED, flag=flag,
                                                       evidence={**prior["evidence"], **evidence},
                                                       reason=flag["reason"])
            continue
        if not flag and pair["source"] in profile:
            best, own, own_shared, distinct = profile[pair["source"]]
            evidence = {**evidence, "mode": MODE_PROFILE, "distinct_values": distinct,
                        "overlap_with_target": round(own, 4)}
            if best and own <= 0.1:
                evidence["best_other_column"] = {"target": best[0], "overlap": round(best[1], 4), "shared": best[2]}
            checked = True
        if flag:
            results[pair["source"]] = _pair_result(pair, STATUS_FLAGGED, flag=flag, evidence=evidence,
                                                   reason=flag["reason"])
        elif checked and evidence.get("mode") == MODE_PROFILE and evidence.get("overlap_with_target", 1) <= 0.1:
            results[pair["source"]] = _pair_result(
                pair, STATUS_NO_EVIDENCE, evidence=evidence,
                reason="The destination rows are different records; overlap alone proves nothing.")
        elif checked:
            results[pair["source"]] = _pair_result(pair, STATUS_CONSISTENT, evidence=evidence,
                                                   reason="Values fit what the destination column holds.")
        else:
            results[pair["source"]] = _pair_result(
                pair, STATUS_NO_EVIDENCE, evidence=evidence,
                reason=("Not enough values to judge." if int(evidence.get("values") or 0) < MIN_PROFILE
                        else "Nothing in the destination to compare with before load."),
            )
    _profile_swaps(results, pairs, profile)
    _name_swaps(results, pairs)
    _mark_swaps(results, pairs)
    ordered = [results[p["source"]] for p in pairs if p["source"] in results]
    summary = Counter(r["status"] for r in ordered)
    inputs_sha256 = _fingerprint(pairs, mode=mode, key=key, date_locale=date_locale,
                                 source_rows=source_rows, dest_rows=dest_rows, source_digest=source_digest)
    for res in ordered:
        if res.get("flag"):
            res["flag"]["id"] = flag_id(inputs_sha256, res)
    return {
        "version": WITNESS_VERSION,
        "mode": mode,
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "key": {"source": key[0], "target": key[1]} if key and key[0] and key[1] else None,
        "date_locale": date_locale or None,
        "sample": {"source_rows": len(source_rows), "destination_rows": len(dest_rows or []),
                   "matching_keys": joined},
        "pairs": ordered,
        "summary": {
            "pairs": len(ordered),
            "witnessed": summary.get(STATUS_WITNESSED, 0),
            "consistent": summary.get(STATUS_CONSISTENT, 0),
            "flagged": summary.get(STATUS_FLAGGED, 0),
            "differs": summary.get(STATUS_DIFFERS, 0),
            "not_comparable": summary.get(STATUS_NOT_COMPARABLE, 0),
            "not_read": summary.get(STATUS_NOT_READ, 0),
            "no_evidence": summary.get(STATUS_NO_EVIDENCE, 0),
        },
        "inputs_sha256": inputs_sha256,
        "honesty": _HONESTY,
    }


def _profile_swaps(results: dict[str, dict[str, Any]], pairs: list[dict[str, Any]],
                   profile: dict[str, tuple]) -> None:
    """Profile mode flags only a swap whose two halves point at each other's columns."""
    by_target = {p["target"].casefold(): p for p in pairs}
    for pair in pairs:
        info = profile.get(pair["source"])
        if not info or not info[0] or info[1] > 0.1:
            continue
        other = by_target.get(info[0][0].casefold())
        other_info = profile.get(other["source"]) if other else None
        if not other or not other_info or not other_info[0] or other_info[1] > 0.1:
            continue
        if other_info[0][0].casefold() != pair["target"].casefold():
            continue
        for this, that in ((pair, other), (other, pair)):
            res = results.get(this["source"])
            if not res or res.get("flag"):
                continue
            res["status"] = STATUS_FLAGGED
            res["flag"] = _flag(
                KIND_SWAP,
                f"{this['target']} and {that['target']} look swapped: the values headed for {this['target']} "
                f"overlap {that['target']}'s and hardly its own.",
                suggested_target=that["target"], basis=MODE_PROFILE,
            )
            res["flag"]["swapped_with"] = that["source"]
            res["reason"] = res["flag"]["reason"]


def source_sample_digest(mappings: list[dict[str, Any]], source_rows: list[dict[str, Any]] | None,
                         key: tuple[str, str] | None, secret: bytes) -> str:
    """A keyed digest of the source sample's mapped columns: an edited file is other inputs.

    HMAC under a server-held secret, so the digest cannot be matched against guessed
    values; nothing here is a value. Row order does not matter.
    """
    cols = sorted({p["source"] for p in mapped_pairs(mappings)} | ({key[0]} if key and key[0] else set()))
    rows = sorted(json.dumps([norm(_get(r, c)) for c in cols], separators=(",", ":"))
                  for r in (source_rows or [])[:MAX_ROWS] if isinstance(r, dict))
    body = json.dumps({"columns": cols, "rows": rows}, separators=(",", ":"))
    return hmac.new(secret, body.encode("utf-8"), hashlib.sha256).hexdigest()


def flag_id(inputs_sha256: str, res: dict[str, Any]) -> str:
    """A flag's id: what a person reviews ("Keep this mapping") names exactly this flag.

    It covers the check's inputs (names, counts, settings, the keyed source digest), the
    pair, and everything the flag says: kind, suggested column, reason, swap partner and
    the evidence counts. Reasons and evidence carry names and counts, never a value. A
    flag that says anything else is another flag, and a Keep given to this one does not
    reach it.
    """
    flag = {k: v for k, v in (res.get("flag") or {}).items() if k != "id"}
    body = {"inputs": inputs_sha256, "source": res.get("source"), "target": res.get("target"), "flag": flag,
            "evidence": res.get("evidence") or {}}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()[:20]


def flagged_pairs(report: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [p for p in (report or {}).get("pairs") or [] if p.get("status") == STATUS_FLAGGED]


def report_sha256(report: dict[str, Any] | None) -> str | None:
    """Content digest of a whole report (what a job snapshots and the ledger cites)."""
    if not report:
        return None
    return hashlib.sha256(
        json.dumps(report, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
