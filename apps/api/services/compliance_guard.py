"""Deterministic PII and compliance-risk detection for data mappings.

This is a rule-based safety layer: it flags sensitive fields by name and by
sample-value pattern, then returns an auditable score that can gate transfer
execution or require human review.
"""

from __future__ import annotations

import ipaddress
import logging
import re
from decimal import Decimal
from typing import Any, Iterable

from services.pii_guard import (
    EMAIL_VALUE_RE,
    PII_PATTERNS,
    SSN_VALUE_RE,
    _is_decimal_literal,
    luhn_valid,
)

_logger = logging.getLogger(__name__)

# The value patterns live in ``services.pii_guard`` (the log masker uses the
# same ones unanchored). This module is the one column classifier: the
# preflight gate, the mapper's column profile and create_schedule all read
# PII through :func:`pii_value_category` (D94) — no second detector.
EMAIL_RE = EMAIL_VALUE_RE
# Digits plus the separators printed numbers use; a leading ``(`` is an area
# code, ``(555) 123-4567`` (D94). The digit count is checked separately
# against E.164's 7..15 so the shape alone never decides.
PHONE_RE = re.compile(r"^\+?[0-9(][0-9\s().-]{5,22}[0-9]$")
# Four dot-separated 1-3 digit groups: an IPv4 (validated) or a version /
# malformed address — never a phone, though it has phone-like digit count.
_DOTTED_QUAD_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
# ICAO 9303 TD3 (passport) machine-readable zone, 44 characters per line.
_MRZ_LINE1_RE = re.compile(r"^P[A-Z<][A-Z][A-Z<]{2}[A-Z<]{39}$")
_MRZ_LINE2_RE = re.compile(r"^[A-Z0-9<]{9}\d[A-Z][A-Z<]{2}\d{7}[MFX<]\d{7}[A-Z0-9<]{14}[0-9<]\d$")
SSN_RE = SSN_VALUE_RE
# Calendar shapes (ISO / slash) are not PII by themselves. DOB is name-gated
#: Words after ``pan_`` that are not card data: identifiers, regions and the
#: camera / chart sense of "pan". Everything else around a ``pan`` token is a
#: card column (QA D7 review 3).
_PAN_NON_CARD_WORDS = (
    "id", "ids", "african", "africa", "american", "america", "asian", "asia",
    "european", "europe", "india", "indian", "arab", "arabic", "region", "regional",
    "tilt", "zoom", "x", "y", "offset", "left", "right", "level", "speed",
)

# in ``_NAME_PATTERN_GROUPS`` — Fivetran / Airbyte / Collibra classifiers treat
# date-of-birth as a semantic class, not "any 01/02/2024". Value-only matching
# invented a HIPAA hold on ``event_date`` / ``created_at``.
# PCI PAN is its own class: a bare 8-19 digit run is NOT an account number
# (every order id matched). Cards are Luhn-valid 13-19 digits, tolerant of
# space/dash separators — same classifier as services.pii_guard (C28).
ACCOUNT_RE = re.compile(r"^[A-Z]{2}\d{12,30}$")

_ACCOUNT_SURROGATE_NAME_RE = re.compile(r"account_?id\b")

_TEMPORAL_VALUE_TYPES = frozenset({"DATE", "TIMESTAMP", "TIMESTAMPTZ"})

_NAME_PATTERN_GROUPS: dict[str, tuple[tuple[str, ...], float]] = {
    "email": ((r"email", r"e_mail", r"\bmail\b"), 0.22),
    "phone": ((r"phone", r"mobile", r"\btel\b", r"contact_?(?:phone|number|no)"), 0.22),
    "name": ((
        r"\bname\b",
        r"first_?name",
        r"last_?name",
        r"full_?name",
        r"contact_?name",
        r"customer_?name",
        r"user_?name",
        r"display_?name",
    ), 0.18),
    "address": ((
        r"address",
        r"street",
        r"\bcity\b",
        r"\bzip\b",
        r"postal",
        r"country(?:_?code)?\b",
    ), 0.18),
    # ``birth`` must start a word: ``rebirth_count`` / ``childbirth_ward`` are
    # not a date of birth (QA Snowflake D2). ``birth_date``, ``birthday``,
    # ``BirthDate`` and ``date_of_birth`` still are.
    "dob": ((r"\bdob\b", r"(?:^|_|\b)birth", r"date_of_birth"), 0.42),
    # Online identifier under GDPR. ``ip`` must be a whole snake_case token:
    # ``zip`` / ``tip`` / ``skip_count`` are not addresses.
    "ip_address": ((
        r"(?:^|_)ip(?:_|$)",
        r"ip_?addr",
        r"\bipv[46]\b",
        r"remote_?addr",
    ), 0.18),
    # Government travel-document number: as regulated as an SSN (D94).
    "passport": ((r"passport", r"travel_?doc"), 0.5),
    "ssn": ((r"\bssn\b", r"social_security", r"tax_id", r"\btin\b"), 0.5),
    # PCI/financial account — NOT boolean flags like accountLocked / accountActive.
    "account": ((
        r"account_?(?:number|no|num|id)\b",
        r"bank_?account",
        r"routing_?(?:number|no|num)?\b",
        r"\biban\b",
        r"\bswift\b",
    ), 0.4),
    # PCI card data — own class (C28). Card number / PAN / CVV columns are
    # PCI-DSS scope regardless of sampled values.
    "pci_pan": ((
        r"card_?(?:number|no|num)\b",
        r"credit_?card",
        r"debit_?card",
        r"payment_?card",
        # ``pan`` as a whole ``_``-separated token, any prefix or suffix:
        # masked_pan, encrypted_pan, pan_encrypted, tokenized_pan, pan_values…
        # Masked / encrypted / tokenized PANs are not Luhn-valid, so this name
        # rule is their only guard. Only known non-card words are excluded
        # (_PAN_NON_CARD_WORDS: pan_id, pan_african, pan-India, a camera's
        # pan_tilt, a chart's pan_x …). ``_`` is a word character, so
        # ``\bpan\b`` would miss card_pan.
        r"(?:^|_)pan(?:_(?!(?:" + "|".join(_PAN_NON_CARD_WORDS) + r")(?:_|$))|$)",
        r"card_?pan",
        r"(?:^|_)cc_?(?:number|no|num)\b",
        r"\bcvv\b",
        r"card_?holder",
        r"expir(?:y|ation)_?(?:date|month|year)?\b",
    ), 0.45),
    # High-signal regulated IDs only — bare ``id`` / ``*Id`` matches every CRM field.
    # Technical UUID/GUID surrogate keys are not PCI/HIPAA identifiers.
    "identifier": ((
        r"policy_?(?:no|number|num)\b",
        r"member_?id\b",
        r"\bmrn\b",
        r"record_?id\b",
        r"patient_?id\b",
    ), 0.32),
    # Compensation is regulated the same way a date of birth is: a load of
    # full_name + salary must not skip the acknowledgement a birth_date load
    # already requires (QA RT5-06 / R2-036).
    "salary": ((
        r"\bsalary\b",
        r"compensation",
        r"\bwage\b",
        r"pay_?rate",
    ), 0.4),
}

# Categories that alone force compliance review.
_HIGH_RISK_CATEGORIES = frozenset({"ssn", "passport", "dob", "account", "salary", "pci_pan"})

# Personal names, not technical surrogates. ``username`` matches the name
# category via user_name and must stay a medium signal — forcing review on
# every login column is a different policy.
_HIGH_RISK_NAME_RE = re.compile(
    r"(?:^|_)(?:full_?name|first_?name|last_?name|contact_?name|customer_?name)(?:_|$)"
)

# QA Snowflake D2 — value-pattern checks where a column name alone is weak.
# Each rule clears a column only on what its *values* show, and only for the
# ambiguous names; an explicit person/date name keeps flagging by name.

#: A column the schema names a quantity or measure. A phone number is never
#: stored under these words, and their bare digit runs (``qty = 1234567``) are
#: counts, so they are not phone evidence. Formatted phones (``+1 555 …``)
#: still are, whatever the column is called.
_QUANTITY_NAME_RE = re.compile(
    r"(?:^|_)(?:qty|qtys|quantity|quantities|count|counts|cnt|units?|amount|amt|"
    r"total|totals|sum|stock|inventory|balance|price|cost|score|weight|volume|"
    r"size|rank|seq|sequence|num|nbr)(?:_|$)|^n_"
)

#: Names that are ``name`` only by word, not by person: the name of a table,
#: file, product... ``display_name`` is here too — it is as often an app's or
#: a device's label as a person's — so it is judged by its values instead of
#: being a high-risk personal name by spelling alone.
_AMBIGUOUS_NAME_RE = re.compile(r"^(?:name|display_?name)$")

#: A person's name as it is written: one to five words of letters with the
#: punctuation names carry (O'Brien, Jean-Luc, J. R. R.), no digits, no
#: underscores/slashes/@ — those are identifiers, paths and handles.
_PERSON_NAME_VALUE_RE = re.compile(r"^[^\W\d_]+(?:[ '\-.,]+[^\W\d_]+){0,4}\.?$")


def _snake(name: str) -> str:
    """``orderQty`` → ``order_qty``; ``TABLE NAME`` → ``table_name``."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(name or ""))
    return re.sub(r"[\s\-.]+", "_", spaced).lower()


def _quantity_column(col: str) -> bool:
    return bool(_QUANTITY_NAME_RE.search(_snake(col)))


def _non_person_name(col: str, values: list[str]) -> bool:
    """An ambiguous ``name`` column whose values are not people's names.

    Only ``name`` / ``display_name`` are judged this way, and only on a sample:
    with nothing sampled the name rule stands (fail closed). The column is
    cleared when most values carry digits, underscores, slashes or other
    identifier marks — ``orders_2024``, ``widget-3000``, ``s3://bucket``.
    """
    if not _AMBIGUOUS_NAME_RE.search(_snake(col)):
        return False
    non_empty = [v for v in values if v]
    if not non_empty:
        return False
    person_like = sum(1 for v in non_empty if _PERSON_NAME_VALUE_RE.match(v))
    return person_like * 2 < len(non_empty)


def _not_a_birth_date(col: str, values: list[str], raw_values: list[Any]) -> bool:
    """A ``birth…`` column whose sampled values are plain numbers, not dates.

    ``births = 12``, ``birth_rate = 11.4`` and ``birth_year = 1990`` hold no
    date of birth: HIPAA Safe Harbor's date elements exclude the year alone,
    and a count or rate is not a date at all. A date, a timestamp or a
    date-shaped digit run (``19900520`` under a date name) keeps the flag, as
    does an unsampled column.
    """
    non_empty = [v for v in values if v]
    if not non_empty or _temporal_column(col, values):
        return False
    numeric = _numeric_column(raw_values) or all(
        re.fullmatch(r"[+-]?\d+(?:\.\d+)?", v) for v in non_empty
    )
    if not numeric:
        return False
    # yyyymmdd / epoch digit runs are dates written as numbers.
    return all(len(v.lstrip("+-").split(".")[0]) <= 4 for v in non_empty)


def _decode_cell(value: Any) -> Any:
    """A sampled cell as its value: wire NULL/missing sentinels are ``None``.

    Preflight samples arrive as wire text, where SQL NULL is the
    ``__DF_SQL_NULL__`` sentinel. Read as a string it made every nullable
    numeric column look non-numeric, so the value rules never cleared it
    (QA Snowflake D2: ``births`` with a NULL row stayed a date of birth).
    """
    from services.value_serializer import (
        SQL_NULL_SENTINEL,
        WireLiteral,
        is_missing_sentinel,
        unescape_wire_text,
    )

    if isinstance(value, WireLiteral):
        return str(value)
    if isinstance(value, str):
        if value == SQL_NULL_SENTINEL or is_missing_sentinel(value):
            return None
        decoded = unescape_wire_text(value)
        return str(decoded) if isinstance(decoded, str) else decoded
    if is_missing_sentinel(value):
        return None
    return value


def _field_hits(name: str) -> list[str]:
    lowered = name.lower()
    # camelCase word starts (``patientBirthDate``) for the word-anchored
    # ``birth`` rule; every other rule keeps reading the plain lowercase name.
    snaked = _snake(name)
    hits: list[str] = []
    for field, (patterns, _) in _NAME_PATTERN_GROUPS.items():
        forms = (lowered, snaked) if field == "dob" else (lowered,)
        if any(re.search(pat, form) for pat in patterns for form in forms):
            hits.append(field)
    return hits


def _is_ip_address(text: str) -> bool:
    """Canonical IPv4 (octets 0-255, no leading zeros) or IPv6 literal."""
    if "." not in text and ":" not in text:
        return False
    try:
        ipaddress.ip_address(text)
    except ValueError:
        return False
    return True


#: ``2024-03-10`` / ``2024/3/9``: year-first dates. No phone format leads with
#: a 4-digit group followed by two 1-2 digit groups (D241).
_YEAR_FIRST_DATE_RE = re.compile(r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}$")


def _is_phone(text: str) -> bool:
    if (
        not PHONE_RE.match(text)
        or _DOTTED_QUAD_RE.match(text)
        or _YEAR_FIRST_DATE_RE.match(text)
    ):
        return False
    # ``12345.67`` is an amount, not a phone (X-26 for the log masker).
    if _is_decimal_literal(text):
        return False
    return 7 <= sum(ch.isdigit() for ch in text) <= 15


def _is_bare_digits(text: str) -> bool:
    """``5551234567`` — a phone, an order number and a count all look like this."""
    return text.isascii() and text.isdigit()


#: Share of non-empty sampled values that must be phone-shaped before a column
#: of bare digit runs counts as phones without a name hint (QA round 11).
_BARE_PHONE_SHARE = 0.8

#: Share of a numeric column's non-empty sampled values that must be card
#: numbers (13-19 digits, Luhn-valid, a card network's IIN) before the column
#: reads as PANs without a name hint. One in ten integers passes Luhn by chance
#: (QA MySQL D7's ids measured 0.1005); a real card column passes ~100%.
NUMERIC_PAN_SHARE = 0.8

#: The share rule needs evidence, not one lucky value: below this many card
#: hits a numeric column is judged by its name alone. A 1-row sample (or one
#: Kafka message, or ``[hit, None, None]``) is 100% by arithmetic and proves
#: nothing about a column of ids.
NUMERIC_PAN_MIN_HITS = 5

_INTEGRAL_DECIMAL = re.compile(r"^(\d+)\.0+$")


#: Issuer prefixes of the card networks, as ``(low, high)`` on a prefix of
#: ``len(str(low))`` digits. A PAN under a neutral name in a numeric column is
#: recognised only through these, so every network that issues 13-19 digit
#: Luhn PANs is listed.
_CARD_IIN_RANGES: tuple[tuple[int, int], ...] = (
    (4, 4),               # Visa
    (51, 55), (2221, 2720),  # Mastercard
    (34, 34), (37, 37),   # American Express
    (6011, 6011), (644, 649), (65, 65),  # Discover (65 includes RuPay 6521)
    (35, 35),             # JCB
    (62, 62), (81, 81),   # UnionPay
    (36, 36), (300, 305), (38, 39),  # Diners Club
    (50, 50), (56, 58), (63, 63), (67, 67),  # Maestro (67 includes 6759; 50 RuPay 508)
    (2200, 2204),         # Mir
    (60, 60),             # RuPay
)


def _card_iin(digits: str) -> bool:
    """True when ``digits`` starts with a card network's issuer prefix."""
    for low, high in _CARD_IIN_RANGES:
        width = len(str(low))
        if len(digits) >= width and low <= int(digits[:width]) <= high:
            return True
    return False


def _numeric_column_holds_pans(values: list[str]) -> bool:
    """A numeric column of card numbers, judged on the column, never per value."""
    digits = []
    for value in values:
        if not value:
            continue
        match = _INTEGRAL_DECIMAL.match(value)
        digits.append(match.group(1) if match else value)
    if not digits:
        return False
    cards = sum(
        1 for d in digits
        if _is_bare_digits(d) and 13 <= len(d) <= 19 and luhn_valid(d) and _card_iin(d)
    )
    return cards >= NUMERIC_PAN_MIN_HITS and cards / len(digits) >= NUMERIC_PAN_SHARE

_NUMERIC_TYPE_RE = re.compile(
    r"^(?:big|small|tiny|medium)?int(?:eger)?\d*\b|^(?:numeric|decimal|number|"
    r"float\d*|double|real|money|serial|bigserial|smallserial)\b|^u?int\d+$",
    re.I,
)


def _numeric_column(raw_values: list[Any], declared_type: str = "") -> bool:
    """The column holds numbers: a declared numeric type, or native numeric cells."""
    if declared_type and _NUMERIC_TYPE_RE.match(declared_type.strip()):
        return True
    present = [v for v in raw_values if v is not None and not (isinstance(v, str) and not v.strip())]
    return bool(present) and all(
        isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) for v in present
    )


def _bare_digits_may_be_phone(
    name_hits: list[str],
    values: list[str],
    raw_values: list[Any],
    declared_type: str,
    col: str = "",
) -> bool:
    """Whether a bare digit run in this column is evidence of a phone number.

    A single 7-15 digit value used to flag the column, so counts
    (``num_sequences = 12345678``) and order numbers read as phones and forced
    a PII acknowledgment (QA round 11, RNAcentral). Bare digits now count only
    under a phone-hinting name, or when most non-empty values look like a phone
    and the column does not hold numbers. Formatted values (``+1 555 …``,
    ``(555) 123-4567``) are phones on their own.

    A column named as a quantity (``qty``, ``units``, ``total``…) never reads
    its bare digits as phones, even when the sample carries them as text — a
    CSV/text source, or a ``qty`` cast to text (QA Snowflake D2).
    """
    if "phone" in name_hits:
        return True
    if _numeric_column(raw_values, declared_type):
        return False
    if _quantity_column(col):
        return False
    non_empty = [v for v in values if v]
    if not non_empty:
        return False
    phone_like = sum(1 for v in non_empty if _is_bare_digits(v) and _is_phone(v))
    return phone_like / len(non_empty) >= _BARE_PHONE_SHARE


def _mrz_check_digit(field: str) -> int:
    """ICAO 9303 check digit: weights 7-3-1, A-Z = 10..35, ``<`` = 0."""
    total = 0
    for i, ch in enumerate(field):
        if ch.isdigit():
            value = int(ch)
        elif "A" <= ch <= "Z":
            value = ord(ch) - 55
        else:
            value = 0
        total += value * (7, 3, 1)[i % 3]
    return total % 10


def _is_passport_mrz(text: str) -> bool:
    """A TD3 MRZ line. Line 2 must pass every check digit, so a 44-character
    code that merely has the shape is not a passport. A bare passport number
    (``K1234567``) is indistinguishable from a SKU and is name-gated."""
    if _MRZ_LINE1_RE.match(text):
        return "<<" in text[5:]
    if not _MRZ_LINE2_RE.match(text):
        return False
    digit = _mrz_check_digit
    if text[42] == "<":
        optional_ok = set(text[28:42]) == {"<"}
    else:
        optional_ok = digit(text[28:42]) == int(text[42])
    return (
        optional_ok
        and digit(text[0:9]) == int(text[9])
        and digit(text[13:19]) == int(text[19])
        and digit(text[21:27]) == int(text[27])
        and digit(text[0:10] + text[13:20] + text[21:43]) == int(text[43])
    )


def pii_value_category(
    value: Any, *, bare_digit_phones: bool = True, bare_digit_pans: bool = True
) -> str:
    """The PII category one whole value is, or ``""``.

    Precedence: email, SSN, IP, passport MRZ, then a Luhn-valid 13-19 digit
    PAN (the phone shape matches card numbers, so PCI wins), phone, account.
    ``bare_digit_phones=False`` keeps an unformatted digit run from reading as
    a phone (counts and order numbers look the same); ``bare_digit_pans=False``
    does the same for a card number (one in ten integers passes Luhn).
    """
    text = str(value if value is not None else "").strip()
    if not text:
        return ""
    if EMAIL_RE.fullmatch(text):
        return "email"
    if SSN_RE.fullmatch(text):
        return "ssn"
    if _is_ip_address(text):
        return "ip_address"
    if _is_passport_mrz(text):
        return "passport"
    if luhn_valid(text) and (bare_digit_pans or not _is_bare_digits(text)):
        return "pci_pan"
    if _is_phone(text) and (bare_digit_phones or not _is_bare_digits(text)):
        return "phone"
    if ACCOUNT_RE.match(text):
        return "account"
    return ""


def is_email_value(value: Any) -> bool:
    return pii_value_category(value) == "email"


def is_phone_value(value: Any) -> bool:
    return pii_value_category(value) == "phone"


#: Any value that can carry an embedded identifier has an ``@`` or a digit.
_EMBED_CHARS = frozenset("@0123456789")


def embedded_pii_categories(value: Any) -> list[str]:
    """Identifiers inside free text (``"contact me at x@gmail.com or
    212-555-0001"``), validated the way whole values are: an IP must parse, a
    card must pass Luhn, a phone must be formatted (a bare digit run inside
    prose is an order number). A ``notes`` column full of such text used to
    pass the gate untouched (D94).
    """
    text = str(value if value is not None else "")
    if not text or _EMBED_CHARS.isdisjoint(text):
        return []
    found: list[str] = []
    for label, pattern in PII_PATTERNS.items():
        for match in pattern.finditer(text):
            token = match.group(0)
            if label == "ip_address" and not _is_ip_address(token):
                continue
            if label == "pci_pan" and not luhn_valid(token):
                continue
            if label == "phone" and (_is_bare_digits(token) or not _is_phone(token)):
                continue
            found.append(label)
            break
    return list(dict.fromkeys(found))


def _value_hits(
    values: list[str], *, bare_digit_phones: bool = True, bare_digit_pans: bool = True
) -> list[str]:
    hits: list[str] = []
    for value in values:
        category = pii_value_category(
            value, bare_digit_phones=bare_digit_phones, bare_digit_pans=bare_digit_pans
        )
        if category:
            hits.append(category)
    return list(dict.fromkeys(hits))


def _phone_shape(values: list[str]) -> str:
    """``formatted`` when at least one sampled phone has a phone shape
    (separators, parentheses, a country code); ``bare_digits`` when every
    phone-shaped value is a plain digit run (an order id, an epoch, a key).
    A bare digit run is reported, never counted as a direct identifier."""
    for value in values:
        text = str(value).strip()
        if text and not _is_bare_digits(text) and _is_phone(text):
            return "formatted"
    return "bare_digits"


def _embedded_hits(values: list[str]) -> list[str]:
    """Categories found inside values that are not themselves an identifier."""
    hits: list[str] = []
    for value in values:
        if pii_value_category(value):
            continue
        hits.extend(embedded_pii_categories(value))
    return list(dict.fromkeys(hits))


def _temporal_column(col: str, values: list[str]) -> bool:
    """Every sampled value is a date / instant, so its digits are not a phone.

    ``1990-05-20``, ``20240115`` and epoch-ms ``1705312200000`` all match the
    phone/account digit shapes. Formatted dates count as temporal on their
    own; bare digits only under a temporal column name, because a 10-digit
    phone number is epoch-shaped too. Name rules (``birth_date`` → dob) still
    apply to the column.
    """
    from services.schema_inference import _is_date_field_name, classify_value

    non_empty = [v for v in values if v]
    if not non_empty:
        return False
    temporal_name = _is_date_field_name(col)
    for value in non_empty:
        if value.lstrip("+-").isdigit() and not temporal_name:
            return False
        if classify_value(value, field_name=col) not in _TEMPORAL_VALUE_TYPES:
            return False
    return True


def detect_pii_fields(
    columns: list[str],
    rows: list[dict[str, Any]] | None = None,
    *,
    sample_limit: int = 100,
    column_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Identify columns that are likely sensitive or regulated.

    ``column_types`` (source column → declared type) lets a numeric column's
    digits be read as numbers even when the sample carries them as text.

    Returns a structured report with the sensitive fields, their detected risk
    categories, and a coarse risk level suitable for preflight gating.
    """
    sample_rows = (rows or [])[:sample_limit]
    sensitive_fields: list[str] = []
    field_risk: dict[str, list[str]] = {}
    high_risk_fields: list[str] = []
    sampled_fields: list[str] = []

    field_evidence: dict[str, list[dict[str, str]]] = {}

    for col in columns:
        name_hits = _field_hits(col)
        raw_values = [_decode_cell(row.get(col)) for row in sample_rows if col in row]
        sample_values = [
            "" if value is None else str(value).strip() for value in raw_values
        ]
        declared = str((column_types or {}).get(col) or "")
        numeric = _numeric_column(raw_values, declared)
        value_hits = _value_hits(
            sample_values,
            bare_digit_phones=_bare_digits_may_be_phone(
                name_hits, sample_values, raw_values, declared, col,
            ),
            # A numeric column (declared BIGINT/DECIMAL, or int cells) is judged
            # on its share of card numbers, not value by value: one in ten
            # integer keys passes Luhn (QA MySQL D7: multiples of 100000000007),
            # a card column passes nearly all. A card-hinting name still flags.
            bare_digit_pans="pci_pan" in name_hits or not numeric,
        )
        if numeric and "pci_pan" not in value_hits and _numeric_column_holds_pans(sample_values):
            value_hits.append("pci_pan")
        # Plain digit strings (order ids, epochs, Kafka keys) read as phones
        # only by the share rule above; that is a warning, not an identifier
        # (D94 follow-up: a second Kafka run sampled one message and held).
        phone_shape = _phone_shape(sample_values) if "phone" in value_hits else ""
        if phone_shape == "bare_digits" and "phone" in name_hits:
            phone_shape = "name_hinted"
        if "name" in name_hits and _non_person_name(col, sample_values):
            _logger.info(
                "Column %r: 'name' by spelling, but its sampled values are not "
                "people's names — not flagged as a personal name",
                col,
            )
            name_hits = [h for h in name_hits if h != "name"]
        if "dob" in name_hits and _not_a_birth_date(col, sample_values, raw_values):
            _logger.info(
                "Column %r: 'birth' by spelling, but every sampled value is a "
                "plain number (count, rate or year) — not a date of birth",
                col,
            )
            name_hits = [h for h in name_hits if h != "dob"]
        embedded_hits: list[str] = []
        if value_hits and _temporal_column(col, sample_values):
            _logger.info(
                "PII value pattern(s) %s on column %r ignored: every sampled value "
                "is a date/timestamp",
                value_hits,
                col,
            )
            value_hits = []
        elif not _numeric_column(raw_values, str((column_types or {}).get(col) or "")):
            embedded_hits = [h for h in _embedded_hits(sample_values) if h not in value_hits]
        non_empty = [v for v in sample_values if v]
        if non_empty:
            sampled_fields.append(col)
        if (
            "account" in name_hits
            and "account" not in value_hits
            and non_empty
            and _ACCOUNT_SURROGATE_NAME_RE.search(col.lower())
            and not any(ACCOUNT_RE.match(v) for v in non_empty)
        ):
            # ``account_id`` is usually a surrogate key. Sampled values that do
            # not look like a bank/card number keep it a regulated identifier
            # (PCI tag, visible) instead of a high-risk account number that
            # forces review on a synthetic numeric table.
            name_hits = ["identifier" if h == "account" else h for h in name_hits]
        categories = list(dict.fromkeys(name_hits + value_hits + embedded_hits))
        if not categories:
            continue
        sensitive_fields.append(col)
        field_risk[col] = categories
        field_evidence[col] = [
            # ``sampled`` False: the sample had no value for this column, so
            # the name is all the evidence there is (and the hold says so).
            {"category": cat, "rule": "column_name", **({} if non_empty else {"sampled": False})}
            for cat in dict.fromkeys(name_hits)
        ] + [
            # Content is evidence in its own right, also where the name agrees:
            # "phone by column name" alone read as a name-only detector (D94).
            {"category": cat, "rule": "sample_value", **({"shape": phone_shape} if cat == "phone" else {})}
            for cat in value_hits
        ] + [
            {"category": cat, "rule": "embedded_value"} for cat in embedded_hits
        ]
        personal_name = "name" in categories and bool(
            _HIGH_RISK_NAME_RE.search(col.lower())
        )
        if personal_name or any(
            cat in _HIGH_RISK_CATEGORIES
            for cat in categories
        ):
            high_risk_fields.append(col)

    risk_level = "low"
    if high_risk_fields:
        risk_level = "high"
    elif sensitive_fields:
        risk_level = "medium"

    return {
        "sensitive_fields": sorted(sensitive_fields),
        "field_risk": field_risk,
        "high_risk_fields": sorted(high_risk_fields),
        "risk_level": risk_level,
        "sensitive_count": len(sensitive_fields),
        "field_evidence": field_evidence,
        "sampled_fields": sorted(sampled_fields),
    }


#: Direct identifiers under GDPR art. 4: medium weight alone, but a table
#: made of them is a review hold, not a warning (D94).
_DIRECT_IDENTIFIER_CATEGORIES = frozenset({"email", "phone", "ip_address"})
_CONTENT_RULES = frozenset({"sample_value", "embedded_value"})


def direct_identifier_columns(pii_report: dict[str, Any]) -> list[str]:
    """Columns whose *content* is an email, phone or IP address.

    Content evidence only: ``email_opt_in`` holds booleans, not addresses,
    and an empty or sparse table has shown nothing yet. A name-only finding
    is still reported (marked "no sample" when the column had no value) but
    never forces the review hold on its own. A phone needs a phone shape
    (separators, a country code) or a phone-hinting name: a plain digit run
    is an order id, an epoch or a key until something says otherwise.
    """
    evidence = pii_report.get("field_evidence") or {}
    out: list[str] = []
    for col, items in evidence.items():
        if any(
            str(item.get("category")) in _DIRECT_IDENTIFIER_CATEGORIES
            and str(item.get("rule")) in _CONTENT_RULES
            and item.get("shape") != "bare_digits"
            for item in items
        ):
            out.append(col)
    return sorted(out)


def direct_identifier_review_reason(direct: list[str], column_count: int) -> str:
    """Why a set of direct identifiers forces review, or ``""``.

    Three or more identifier columns, or identifiers in at least half of a
    table's columns (two minimum), is a table *of* personal data. A single
    customer ``email`` beside twenty business columns stays a warning.
    """
    n = len(direct)
    if n >= 3 or (n >= 2 and 2 * n >= max(column_count, 1)):
        return (
            f"direct identifiers (email/phone/IP) in {n} of {column_count} column(s): "
            + ", ".join(direct)
        )
    return ""


def score_compliance_risk(
    columns: list[str],
    rows: list[dict[str, Any]] | None = None,
    *,
    sample_limit: int = 100,
    column_types: dict[str, str] | None = None,
    protected_columns: Iterable[str] = (),
) -> dict[str, Any]:
    """Score the compliance and privacy risk of a dataset.

    The score is deterministic and bounded to [0.0, 1.0]. The result is meant
    for preflight review, not for claiming legal compliance.
    ``protected_columns`` (masked / hashed / redacted by their mapping) stay in
    the findings but do not count toward the direct-identifier hold: masking
    is the remediation the hold asks for.
    """
    pii_report = detect_pii_fields(
        columns, rows, sample_limit=sample_limit, column_types=column_types
    )
    sensitive_fields = pii_report["sensitive_fields"]
    field_risk = pii_report["field_risk"]

    weight_by_category = {
        "email": 0.18,
        "phone": 0.18,
        "name": 0.14,
        "address": 0.16,
        "dob": 0.42,
        "ssn": 0.5,
        "passport": 0.5,
        "ip_address": 0.18,
        "account": 0.4,
        "pci_pan": 0.45,
        "identifier": 0.28,
        "salary": 0.4,
    }

    total_weight = 0.0
    high_risk_weight = 0.0
    for col in sensitive_fields:
        for category in field_risk.get(col, []):
            weight = weight_by_category.get(category, 0.12)
            total_weight += weight
            if category in _HIGH_RISK_CATEGORIES:
                high_risk_weight += weight

    if not sensitive_fields:
        risk_score = 0.0
    else:
        base = total_weight / max(len(sensitive_fields), 1)
        uplift = min(0.18, high_risk_weight / max(len(sensitive_fields), 1) * 0.4)
        risk_score = min(1.0, round(base + uplift, 3))

    risk_level = "low"
    if risk_score >= 0.45 or pii_report["high_risk_fields"]:
        risk_level = "high" if risk_score >= 0.6 or pii_report["high_risk_fields"] else "medium"

    compliance_tags: list[str] = []
    if any("ssn" in field_risk.get(col, []) or "dob" in field_risk.get(col, []) for col in sensitive_fields):
        compliance_tags.append("HIPAA")
    if any(
        {"account", "identifier", "pci_pan"} & set(field_risk.get(col, []))
        for col in sensitive_fields
    ):
        compliance_tags.append("PCI-DSS")
    if sensitive_fields:
        compliance_tags.append("PII")

    direct = direct_identifier_columns(pii_report)
    protected = {str(c) for c in protected_columns}
    review_reason = direct_identifier_review_reason(
        [c for c in direct if c not in protected], len(columns)
    )
    if review_reason:
        risk_level = "high"
    requires_review = (
        risk_score >= 0.45 or bool(pii_report["high_risk_fields"]) or bool(review_reason)
    )
    evidence = pii_report.get("field_evidence") or {}
    findings = [
        {
            "column": col,
            "categories": list(field_risk.get(col, [])),
            "high_risk": col in pii_report["high_risk_fields"],
            "evidence": list(evidence.get(col, [])),
        }
        for col in sensitive_fields
    ]

    return {
        "risk_score": risk_score,
        "risk_level": risk_level,
        "requires_review": requires_review,
        "sensitive_fields": sensitive_fields,
        "high_risk_fields": pii_report["high_risk_fields"],
        "field_risk": field_risk,
        "findings": findings,
        "compliance_tags": sorted(set(compliance_tags)),
        "direct_identifier_columns": direct,
        "review_reason": review_reason,
    }
