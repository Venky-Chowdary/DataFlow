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
  when read the other way round are a day/month mismatch.
* **Profile** — the destination holds rows but none join on a key: value-set
  overlap and value shapes per column stand in for the key join.
* **Source only** — before load, with nothing in the destination: the values
  are checked against what the destination column's name promises (an
  ``email`` column gets email addresses, a ``*_cents`` column gets whole
  numbers, ``first_name`` gets given names), and against the run's date order.

Every result is **evidence with a reason**; the witness never changes a
mapping, and a pair it cannot judge says so (``no_evidence``) rather than
passing. Nothing here returns a cell value: reasons and evidence carry column
names and counts only, so the report can travel into the signed Trust Report.

Thresholds are fixed constants (no tuning on held-out data). A flag needs
corroboration: on keys, the mapped column must disagree (≤ 50 %) *and* the
alternative must agree (≥ 90 %, chance-corrected κ ≥ 0.8); by profile, both
halves of a swap must point at each other.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

WITNESS_VERSION = "mapping_witness.v1"

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
#: …and beat chance by this much (Cohen's κ), so two boolean columns never "agree" by luck.
ALTERNATIVE_KAPPA = 0.8

STATUS_WITNESSED = "witnessed"
STATUS_CONSISTENT = "consistent"
STATUS_FLAGGED = "flagged"
STATUS_DIFFERS = "differs"
STATUS_NO_EVIDENCE = "no_evidence"

KIND_SWAP = "swap"
KIND_WRONG_TARGET = "wrong_target"
KIND_UNIT = "unit_mismatch"
KIND_FORMAT = "format_mismatch"

MODE_KEYED = "keyed"
MODE_PROFILE = "profile"
MODE_SOURCE_ONLY = "source_only"

_HONESTY = (
    "Evidence only: the witness never changes a mapping. It reads a bounded sample, "
    "compares values on matching keys when the destination already holds them, and "
    "otherwise checks value profiles. Reasons carry column names and counts, never values."
)

_NUM = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
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


def _canon_number(value: Decimal) -> str | None:
    if not value.is_finite():
        return None
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _canon_iso(match: re.Match[str]) -> str:
    day, hh, mm, ss, frac, tz = match.groups()
    if hh is None:
        return day
    frac = (frac or "").rstrip("0").rstrip(".")
    clock = f"{hh}:{mm}:{ss or '00'}{frac}"
    zone = "" if tz in (None, "Z", "+00:00", "+0000", "-00:00") else tz
    if clock == "00:00:00" and not zone:
        # A DATE carried as midnight on a TIMESTAMP is the same calendar day.
        return day
    return f"{day}T{clock}{zone}"


def norm(value: Any) -> str | None:
    """One comparable spelling of a cell: numbers, booleans, dates and case folded.

    ``None``/blank is ``None`` (missing, never a match).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, Decimal)):
        return _canon_number(Decimal(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return _canon_number(Decimal(repr(value)))
    if isinstance(value, datetime):
        text = value.isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    else:
        text = unicodedata.normalize("NFC", str(value)).strip()
    if not text:
        return None
    low = text.casefold()
    if low in _TRUE:
        return "true"
    if low in _FALSE:
        return "false"
    if _NUM.match(text):
        try:
            return _canon_number(Decimal(text))
        except InvalidOperation:
            return low
    iso = _ISO.match(text)
    if iso:
        return _canon_iso(iso)
    return low


def _decimal(value: str | None) -> Decimal | None:
    if value is None or not _NUM.match(value):
        return None
    try:
        out = Decimal(value)
    except InvalidOperation:
        return None
    return out if out.is_finite() else None


def _slash_parts(text: Any) -> tuple[int, int, int] | None:
    if text is None:
        return None
    m = _SLASH_DATE.match(str(text).strip())
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


def _as_loaded(value: Any, date_locale: str) -> str | None:
    """The source value the way the run will write it (slash dates parsed, the rest normalized)."""
    parts = _slash_parts(value)
    if parts is not None:
        order = _loader_order(parts, date_locale)
        return _read_slash_date(parts, order) if order else None
    return norm(value)


def _date_part(value: str | None) -> str | None:
    if value is None:
        return None
    return value[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", value) else None


# ── names → what a destination column promises ───────────────────────────────


def _tokens(name: str) -> list[str]:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(name or ""))
    return [t for t in re.split(r"[^a-z0-9]+", spaced.lower()) if t]


def _is_email_name(name: str) -> bool:
    low = str(name or "").lower()
    return bool(re.search(r"e[_-]?mail", low)) and not re.search(r"(opt|optout|verified|flag|bounce|count|sent)", low)


def _is_minor_units_name(name: str) -> bool:
    toks = _tokens(name)
    return "cents" in toks or "pennies" in toks or ("minor" in toks and ("units" in toks or "unit" in toks)) or (
        bool(toks) and toks[-1] == "minor"
    )


def _is_money_name(name: str, declared_type: str = "") -> bool:
    toks = set(_tokens(name))
    money = {"amount", "price", "total", "cost", "fee", "balance", "revenue", "salary", "subtotal", "tax", "paid"}
    typ = str(declared_type or "").upper()
    return bool(toks & money) or typ.startswith(("DECIMAL", "NUMERIC", "MONEY", "NUMBER("))


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
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).casefold().strip()


# ── statistics ───────────────────────────────────────────────────────────────


def _agreement(pairs: Iterable[tuple[str | None, str | None]]) -> tuple[int, int, float]:
    """(agreed, compared, κ) over pairs whose left value is present."""
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
        return 0, 0, 0.0
    po = agreed / compared
    pe = sum(left[v] * right.get(v, 0) for v in left) / (compared * compared)
    if pe >= 1.0:
        return agreed, compared, 1.0 if po >= 1.0 else 0.0
    return agreed, compared, (po - pe) / (1.0 - pe)


def _scaled_agreement(pairs: list[tuple[str | None, str | None]]) -> tuple[Decimal, int, int] | None:
    """The unit factor under which numeric source values equal the destination's, if any."""
    numeric = [(_decimal(a), _decimal(b)) for a, b in pairs if a is not None]
    numeric = [(a, b) for a, b in numeric if a is not None and b is not None]
    if len(numeric) < MIN_KEYED:
        return None
    for factor in _UNIT_FACTORS:
        agreed = sum(1 for a, b in numeric if abs(a * factor - b) <= max(abs(b), Decimal(1)) * Decimal("1e-9"))
        if agreed / len(numeric) >= ALTERNATIVE_AT:
            return factor, agreed, len(numeric)
    return None


def _ratio(agreed: int, compared: int) -> float:
    return round(agreed / compared, 4) if compared else 0.0


def _unit_reason(factor: Decimal) -> str:
    if factor == 100:
        return "Destination values are 100 times the source values: dollars going into cents."
    if factor == Decimal("0.01"):
        return "Source values are 100 times the destination values: cents going into dollars."
    if factor > 1:
        return f"Destination values are {_canon_number(factor)} times the source values: a unit mismatch."
    inverse = _canon_number(Decimal(1) / factor)
    return f"Source values are {inverse} times the destination values: a unit mismatch."


# ── the report ───────────────────────────────────────────────────────────────


def _mapped_pairs(mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        source = str(m.get("source") or "").strip()
        target = str(m.get("target") or "").strip()
        if not source or not target or m.get("intentional_omit") or m.get("intentionalOmit"):
            continue
        if source in seen:
            continue
        seen.add(source)
        out.append({
            "source": source,
            "target": target,
            "create_new": bool(m.get("create_new")),
            "target_type": str(m.get("target_type") or m.get("dest_type") or ""),
            "source_type": str(m.get("source_type") or m.get("inferred_type") or ""),
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


def _keyed(pairs: list[dict[str, Any]], source_rows: list[dict], dest_rows: list[dict], key: tuple[str, str],
           date_locale: str) -> tuple[list[dict[str, Any]], int]:
    skey, tkey = key
    index: dict[str, dict] = {}
    for row in dest_rows:
        k = norm(row.get(tkey)) if isinstance(row, dict) else None
        if k is not None and k not in index:
            index[k] = row
    joined: list[tuple[dict, dict]] = []
    for row in source_rows:
        k = norm(row.get(skey)) if isinstance(row, dict) else None
        if k is not None and k in index:
            joined.append((row, index[k]))
    dest_columns = sorted({c for _, d in joined for c in d})
    dest_vals = {c: [norm(d.get(c)) for _, d in joined] for c in dest_columns}
    results: dict[str, dict[str, Any]] = {}
    best_alt: dict[str, tuple[str, int, int, float]] = {}
    for pair in pairs:
        s, t = pair["source"], pair["target"]
        raw = [r.get(s) for r, _ in joined]
        loaded = [_as_loaded(v, date_locale) for v in raw]
        if pair["create_new"] and t not in dest_vals:
            continue  # a column this run will create holds nothing yet
        # A destination column the rows do not carry is empty on every matching key.
        target_vals = dest_vals.get(t) or [None] * len(joined)
        agreed, compared, kappa = _agreement(zip(loaded, target_vals))
        if compared < MIN_KEYED:
            continue
        evidence: dict[str, Any] = {
            "mode": MODE_KEYED, "matching_keys": len(joined), "compared": compared, "agreed": agreed,
            "agreement": _ratio(agreed, compared),
        }
        if s == skey and t == tkey:
            evidence["join_key"] = True
        ratio = agreed / compared
        alt: tuple[str, int, int, float] | None = None
        for c in dest_columns:
            if c == t:
                continue
            a2, n2, k2 = _agreement(zip(loaded, dest_vals[c]))
            if n2 >= MIN_KEYED and a2 / n2 >= ALTERNATIVE_AT and k2 >= ALTERNATIVE_KAPPA:
                if alt is None or (a2 / n2, k2) > (alt[1] / alt[2], alt[3]):
                    alt = (c, a2, n2, k2)
        if alt:
            best_alt[s] = alt
            evidence["best_other_column"] = {"target": alt[0], "agreed": alt[1], "compared": alt[2],
                                             "agreement": _ratio(alt[1], alt[2])}
        if ratio >= WITNESSED_AT:
            results[s] = _pair_result(
                pair, STATUS_WITNESSED, evidence=evidence,
                reason=f"{agreed} of {compared} matching keys hold the same value in {t}.",
            )
            continue
        # 1. Day/month order: the values agree when read the other way round.
        parts = [_slash_parts(v) for v in raw]
        if sum(p is not None for p in parts) >= MIN_KEYED:
            orders = []
            for order in ("DMY", "MDY"):
                read = [_read_slash_date(p, order) if p else None for p in parts]
                a3, n3, _ = _agreement(zip(read, (_date_part(v) for v in target_vals)))
                orders.append((order, a3, n3))
            for order, a3, n3 in orders:
                loader_ok = ratio > CONTRADICTED_AT_MOST
                if n3 >= MIN_KEYED and a3 / n3 >= ALTERNATIVE_AT and not loader_ok:
                    wanted = "day first (dd/mm)" if order == "DMY" else "month first (mm/dd)"
                    run = {"DMY": "day first", "MDY": "month first"}.get(date_locale, "each value's own order")
                    evidence["date_order_agreement"] = {"order": order, "agreed": a3, "compared": n3}
                    results[s] = _pair_result(
                        pair, STATUS_FLAGGED, evidence=evidence,
                        flag=_flag(KIND_FORMAT, f"Dates match {t} only when read {wanted}; this run reads {run}.",
                                   basis=MODE_KEYED),
                        reason=f"{a3} of {n3} dates agree when read {wanted}.",
                    )
                    break
            if s in results:
                continue
        # 2. Units: the values agree after a power-of-ten factor.
        if ratio <= CONTRADICTED_AT_MOST:
            scaled = _scaled_agreement(list(zip(loaded, target_vals)))
            if scaled:
                factor, a4, n4 = scaled
                evidence["unit_factor"] = {"factor": _canon_number(factor), "agreed": a4, "compared": n4}
                results[s] = _pair_result(
                    pair, STATUS_FLAGGED, evidence=evidence,
                    flag=_flag(KIND_UNIT, _unit_reason(factor), basis=MODE_KEYED),
                    reason=f"{a4} of {n4} values agree after multiplying by {_canon_number(factor)}.",
                )
                continue
        # 3. Another column holds these values: wrong target, or half of a swap.
        if alt and ratio <= CONTRADICTED_AT_MOST:
            results[s] = _pair_result(
                pair, STATUS_FLAGGED, evidence=evidence,
                flag=_flag(KIND_WRONG_TARGET,
                           f"Values match {alt[0]} on {alt[1]} of {alt[2]} keys, not {t} ({agreed} of {compared}).",
                           suggested_target=alt[0], basis=MODE_KEYED),
                reason=f"{alt[1]} of {alt[2]} matching keys hold this value in {alt[0]}.",
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
    by_target = {p["target"]: p["source"] for p in pairs}
    for s, res in results.items():
        flag = res.get("flag") or {}
        if flag.get("kind") != KIND_WRONG_TARGET:
            continue
        other_source = by_target.get(flag.get("suggested_target") or "")
        other = results.get(other_source or "")
        other_flag = (other or {}).get("flag") or {}
        if other_flag.get("kind") in (KIND_WRONG_TARGET, KIND_SWAP) and other_flag.get("suggested_target") == res["target"]:
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
    # Email column fed with something else (user names, ids).
    if _is_email_name(t):
        checked = True
        emails = sum(1 for v in present if _EMAIL.match(str(v).strip()))
        evidence["email_values"] = emails
        if emails / n < 0.2:
            others = [str(v).strip().lower() for v in present if not _EMAIL.match(str(v).strip())]
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
    # Cents column fed with fractional amounts, or cents fed into a currency column.
    if _is_minor_units_name(t) and len(numeric) >= MIN_PROFILE:
        checked = True
        fractional = sum(1 for d in numeric if d != d.to_integral_value())
        evidence["fractional_values"] = fractional
        if fractional / len(numeric) >= 0.2:
            return _flag(KIND_UNIT, f"{t} stores cents, but {fractional} of {len(numeric)} values have fractions "
                                    "(they look like dollars).", basis=MODE_SOURCE_ONLY), evidence, True
    if (_is_minor_units_name(s) and not _is_minor_units_name(t) and _is_money_name(t, pair.get("target_type", ""))
            and len(numeric) >= MIN_PROFILE):
        checked = True
        whole = sum(1 for d in numeric if d == d.to_integral_value())
        evidence["whole_values"] = whole
        if whole == len(numeric):
            return _flag(KIND_UNIT, f"{s} holds cents and {t} holds currency units: values would load 100 times "
                                    "too large.", basis=MODE_SOURCE_ONLY), evidence, True
    # Day/month order against the run's date order.
    parts = [_slash_parts(v) for v in present]
    dated = [p for p in parts if p is not None]
    if len(dated) >= MIN_PROFILE and len(dated) >= 0.8 * n:
        checked = True
        day_first = sum(1 for a, b, _ in dated if a > 12)
        month_first = sum(1 for a, b, _ in dated if b > 12)
        evidence["day_first_values"] = day_first
        evidence["month_first_values"] = month_first
        if day_first and month_first:
            return _flag(KIND_FORMAT, f"Day/month order is mixed: {day_first} values are day first and "
                                      f"{month_first} month first.", basis=MODE_SOURCE_ONLY), evidence, True
        if date_locale == "MDY" and day_first:
            return _flag(KIND_FORMAT, f"Dates are written day first ({day_first} of {len(dated)} start with a day "
                                      "above 12), but this run reads dates month first.",
                         basis=MODE_SOURCE_ONLY), evidence, True
        if date_locale == "DMY" and month_first:
            return _flag(KIND_FORMAT, f"Dates are written month first ({month_first} of {len(dated)} have a day "
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


def _profile_vs_destination(pair: dict[str, Any], values: list[Any],
                            dest_cols: dict[str, list[str]]) -> tuple[dict | None, dict]:
    """No key join: value-set overlap per destination column stands in for it."""
    t = pair["target"]
    vals = {v for v in (norm(x) for x in values) if v is not None}
    evidence: dict[str, Any] = {"mode": MODE_PROFILE, "distinct_values": len(vals)}
    if len(vals) < MIN_PROFILE or t not in dest_cols:
        return None, evidence

    def containment(col: str) -> tuple[float, int]:
        other = set(dest_cols.get(col) or [])
        shared = len(vals & other)
        return shared / len(vals), shared

    own, own_shared = containment(t)
    evidence["overlap_with_target"] = round(own, 4)
    best: tuple[str, float, int] | None = None
    for col in dest_cols:
        if col == t:
            continue
        c, shared = containment(col)
        if shared >= 3 and c >= 0.5 and (best is None or c > best[1]):
            best = (col, c, shared)
    if best and own <= 0.1:
        evidence["best_other_column"] = {"target": best[0], "overlap": round(best[1], 4)}
        return _flag(KIND_WRONG_TARGET,
                     f"Values overlap {best[0]} ({best[2]} shared) but hardly {t} ({own_shared} shared).",
                     suggested_target=best[0], basis=MODE_PROFILE), evidence
    return None, evidence


def _fingerprint(pairs: list[dict[str, Any]], *, mode: str, key: tuple[str, str] | None, date_locale: str,
                 source_rows: list[dict], dest_rows: list[dict] | None) -> str:
    """What the witness looked at, without a single value: names, counts and settings."""
    def counts(rows: list[dict] | None, cols: list[str]) -> dict[str, int]:
        return {c: sum(1 for r in rows or [] if isinstance(r, dict) and norm(r.get(c)) is not None) for c in cols}

    body = {
        "version": WITNESS_VERSION,
        "mode": mode,
        "key": list(key) if key else None,
        "date_locale": date_locale,
        "pairs": [[p["source"], p["target"]] for p in pairs],
        "source_rows": len(source_rows),
        "destination_rows": len(dest_rows or []),
        "source_non_null": counts(source_rows, [p["source"] for p in pairs]),
        "destination_non_null": counts(dest_rows, [p["target"] for p in pairs]),
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
) -> dict[str, Any]:
    """Judge every mapped pair against the data. Pure: same inputs, same report."""
    date_locale = str(date_locale or "").strip().upper()
    date_locale = date_locale if date_locale in ("DMY", "MDY") else ""
    pairs = _mapped_pairs(mappings)
    source_rows = [r for r in (source_rows or []) if isinstance(r, dict)][:MAX_ROWS]
    dest_rows = [r for r in (dest_rows or []) if isinstance(r, dict)][:MAX_ROWS] if dest_rows else None
    results: dict[str, dict[str, Any]] = {}
    joined = 0
    mode = MODE_SOURCE_ONLY
    if dest_rows and key and key[0] and key[1]:
        keyed, joined = _keyed(pairs, source_rows, dest_rows, (key[0], key[1]), date_locale)
        for res in keyed:
            results[res["source"]] = res
        if joined >= MIN_KEYED:
            mode = MODE_KEYED
    dest_cols: dict[str, list[str]] = {}
    if dest_rows and mode != MODE_KEYED:
        mode = MODE_PROFILE
        for row in dest_rows:
            for c, v in row.items():
                nv = norm(v)
                if nv is not None:
                    dest_cols.setdefault(c, []).append(nv)
    for pair in pairs:
        prior = results.get(pair["source"])
        if prior and prior["status"] != STATUS_DIFFERS:
            continue
        values = [r.get(pair["source"]) for r in source_rows]
        flag, evidence, checked = _profile_checks(pair, values, date_locale)
        if prior:
            # Keys disagreed with no other column to explain it; a profile flag still counts.
            if flag:
                results[pair["source"]] = _pair_result(pair, STATUS_FLAGGED, flag=flag,
                                                       evidence={**prior["evidence"], **evidence},
                                                       reason=flag["reason"])
            continue
        if not flag and dest_cols:
            flag, more = _profile_vs_destination(pair, values, dest_cols)
            evidence = {**evidence, **more}
            checked = checked or "overlap_with_target" in more
        if flag:
            results[pair["source"]] = _pair_result(pair, STATUS_FLAGGED, flag=flag, evidence=evidence,
                                                   reason=flag["reason"])
        elif checked:
            results[pair["source"]] = _pair_result(pair, STATUS_CONSISTENT, evidence=evidence,
                                                   reason="Values fit what the destination column holds.")
        else:
            results[pair["source"]] = _pair_result(
                pair, STATUS_NO_EVIDENCE, evidence=evidence,
                reason=("Not enough values to judge." if int(evidence.get("values") or 0) < MIN_PROFILE
                        else "Nothing in the destination to compare with before load."),
            )
    _name_swaps(results, pairs)
    _mark_swaps(results, pairs)
    ordered = [results[p["source"]] for p in pairs if p["source"] in results]
    summary = Counter(r["status"] for r in ordered)
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
            "no_evidence": summary.get(STATUS_NO_EVIDENCE, 0),
        },
        "inputs_sha256": _fingerprint(pairs, mode=mode, key=key, date_locale=date_locale,
                                      source_rows=source_rows, dest_rows=dest_rows),
        "honesty": _HONESTY,
    }


def flagged_pairs(report: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [p for p in (report or {}).get("pairs") or [] if p.get("status") == STATUS_FLAGGED]
