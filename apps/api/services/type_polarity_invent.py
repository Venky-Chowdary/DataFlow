"""Polarity/overflow invent checks between a source and destination type.

Split out of :mod:`services.type_system` unchanged (Phase F8 size freeze). Each
predicate answers one question: would writing this source type into this
destination type *invent* a semantic the source never had — a case/accent/width/
kana fold, a timezone on a bare date, or a signed carrier that cannot hold an
unsigned range. Type-system imports are deferred inside the functions:
``type_system`` re-exports these names, so a module-level import would be
circular.
"""

from __future__ import annotations

import re

def specialty_polarity_mismatch(source_type: str, target_type: str) -> bool:
    """True when two distinct specialty carriers would rewrite domain polarity.

    INET→CIDR invents network masking; MACADDR→MACADDR8 changes wire width;
    HSTORE→LTREE is not identity. IP/INET/IPv4/IPv6 are host-address twins.
    """
    from services.type_system import (
        _IP_HOST_ADDRESS_TWINS,
        _XML_DOCUMENT_TWINS,
        specialty_carrier_base,
    )

    src = specialty_carrier_base(source_type)
    tgt = specialty_carrier_base(target_type)
    if src is None or tgt is None:
        return False
    if src in _IP_HOST_ADDRESS_TWINS and tgt in _IP_HOST_ADDRESS_TWINS:
        return False
    if src in _XML_DOCUMENT_TWINS and tgt in _XML_DOCUMENT_TWINS:
        return False
    return src != tgt


def case_fold_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents or drops case-fold equality polarity.

    Covers TEXT→CITEXT / CS→CI invent and CITEXT→TEXT / explicit CS drop.
    MySQL/SQL Server CI collation metadata → bare create-new TEXT/VARCHAR is
    dialect normalization (values round-trip); uniqueness follows the destination
    platform default and must not block every string column on Validate.
    """
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        is_case_insensitive_collation,
        normalize_logical_type,
        parse_collation,
        specialty_carrier_base,
    )

    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    tgt_ci = is_case_insensitive_collation(target_type) or (
        specialty_carrier_base(target_type) == "CITEXT"
    )
    src_ci = is_case_insensitive_collation(source_type) or (
        specialty_carrier_base(source_type) == "CITEXT"
    )
    # Only when at least one side declares CI/CS polarity (or CITEXT).
    src_declares = bool(parse_collation(source_type)) or (
        specialty_carrier_base(source_type) == "CITEXT"
    )
    tgt_declares = bool(parse_collation(target_type)) or (
        specialty_carrier_base(target_type) == "CITEXT"
    )
    if not src_declares and not tgt_declares:
        return False
    # CITEXT invent from open text is always polarity invent.
    if specialty_carrier_base(target_type) == "CITEXT" and not src_ci:
        return True
    # Inventing CI on the target from an *explicitly* CS-collated source.
    # Uncollated source (Mongo/NoSQL VARCHAR) → dest platform CI is dialect
    # default wire — not invent (mirrors CI→bare normalize above).
    if tgt_ci and not src_ci and src_declares:
        return True
    # Dropping CITEXT specialty into bare CS text — real polarity loss.
    if specialty_carrier_base(source_type) == "CITEXT" and not tgt_ci:
        return True
    # Explicit CS (or non-CI) collation on target vs CI source.
    if src_ci and tgt_declares and not tgt_ci:
        return True
    # Source CI collation metadata + bare destination TEXT/VARCHAR: normalize.
    return False

def accent_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents or explicitly drops accent-insensitive equality.

    Source AI collation → bare create-new TEXT is dialect strip, not invent.
    Uncollated source → dest AI collation is platform default wire, not invent.
    """
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        is_accent_insensitive_collation,
        normalize_logical_type,
        parse_collation,
    )

    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    if not parse_collation(source_type) and not parse_collation(target_type):
        return False
    src_ai = is_accent_insensitive_collation(source_type)
    tgt_ai = is_accent_insensitive_collation(target_type)
    # Invent AI only when the source declared a non-AI (accent-sensitive) collation.
    if tgt_ai and not src_ai:
        if not parse_collation(source_type):
            return False
        return True
    # Drop AI only when the target declares an accent-sensitive collation.
    if src_ai and not tgt_ai and parse_collation(target_type):
        return True
    return False

def width_fold_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents/drops width-insensitive equality (WS omit).

    SQL Server: ``_WS`` is width-sensitive; omitting ``_WS`` folds fullwidth/
    halfwidth — unique keys can collide without Accept risk. The token is
    Windows-collation-only; a MySQL ``*_ai_ci`` name has no WS polarity, so
    comparing it to SQL Server's default (omit ``_WS``) invented a fold the
    source never declared.
    """
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        _is_windows_style_collation,
        is_width_insensitive_collation,
        normalize_logical_type,
        parse_collation,
    )

    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    if not parse_collation(source_type) or not parse_collation(target_type):
        return False
    src_name = (parse_collation(source_type) or "").upper()
    tgt_name = (parse_collation(target_type) or "").upper()
    if not _is_windows_style_collation(src_name) or not _is_windows_style_collation(
        tgt_name
    ):
        return False
    return is_width_insensitive_collation(source_type) != is_width_insensitive_collation(
        target_type
    )


def kana_fold_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when mapping invents/drops kana-insensitive equality (KS omit).

    ``_KS`` is a SQL Server Windows-collation token. Cross-engine names have
    none — do not grade MySQL/PG equality as a kana fold against CI_AS.
    """
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        _is_windows_style_collation,
        is_kana_insensitive_collation,
        normalize_logical_type,
        parse_collation,
    )

    src_l = normalize_logical_type(source_type)
    tgt_l = normalize_logical_type(target_type)
    if src_l not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt_l not in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
    }:
        return False
    if not parse_collation(source_type) or not parse_collation(target_type):
        return False
    src_name = (parse_collation(source_type) or "").upper()
    tgt_name = (parse_collation(target_type) or "").upper()
    if not _is_windows_style_collation(src_name) or not _is_windows_style_collation(
        tgt_name
    ):
        return False
    return is_kana_insensitive_collation(source_type) != is_kana_insensitive_collation(
        target_type
    )


def date_to_tz_aware_invent(source_type: str, target_type: str) -> bool:
    """True when DATE widens into TZ-aware datetime (midnight instant invent)."""
    from services.type_system import (
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        datetime_timezone_polarity,
        normalize_logical_type,
    )

    if normalize_logical_type(source_type) != LOGICAL_DATE:
        return False
    if normalize_logical_type(target_type) != LOGICAL_DATETIME:
        return False
    return datetime_timezone_polarity(target_type) in {"tz", "ltz"}


def unsigned_integer_would_overflow(source_type: str, target_type: str) -> bool:
    """True when UNSIGNED source max can exceed a signed (or narrower) integer dest.

    Fivetran/Airbyte class: BIGINT UNSIGNED → signed BIGINT is silent overflow
    unless widened to DECIMAL/NUMBER. INT UNSIGNED → INT is the same class.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        LOGICAL_JSON,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        integer_bit_width,
        normalize_logical_type,
        strip_identity_qualifier,
    )

    src_raw = (source_type or "").lower()
    if "unsigned" not in src_raw and not re.search(r"\buint\d*\b", src_raw):
        # ClickHouse UInt8/UInt32 — case-sensitive leading U.
        raw = strip_identity_qualifier(source_type) or ""
        if not re.match(r"^UInt(8|16|32|64)\b", raw):
            return False
    tgt_l = normalize_logical_type(target_type)
    # Lossless sinks for unsigned range.
    if tgt_l in {LOGICAL_DECIMAL, LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON}:
        return False
    if tgt_l == LOGICAL_FLOAT:
        # IEEE cannot represent full UINT64 / large UINT32 exactly.
        src_w = integer_bit_width(source_type)
        return src_w is not None and src_w > 24
    if tgt_l != LOGICAL_INTEGER:
        return False
    src_w = integer_bit_width(source_type)
    tgt_w = integer_bit_width(target_type)
    if src_w is None:
        from services.numeric_fit import unsigned_bare_int_fits_signed_target

        if unsigned_bare_int_fits_signed_target(source_type, target_type):
            # Bare INT UNSIGNED (MySQL 32-bit) into a signed 64-bit sink.
            return False
        # Unknown unsigned width into signed integer — fail closed.
        return "unsigned" not in (target_type or "").lower() and not re.match(
            r"^UInt", strip_identity_qualifier(target_type) or ""
        )
    if tgt_w is None:
        tgt_w = 32  # bare INTEGER / INT
    return src_w > tgt_w


def _is_unsigned_integer_carrier(inferred: str | None) -> bool:
    """True for MySQL UNSIGNED / ClickHouse UInt* / UINT* integer carriers."""
    from services.type_system import (
        strip_identity_qualifier,
    )

    raw = strip_identity_qualifier(inferred) or ""
    if re.match(r"^UInt(8|16|32|64)\b", raw):
        return True
    lower = raw.lower()
    return "unsigned" in lower or bool(re.search(r"\buint\d*\b", lower))


def unsigned_signed_polarity_invent(source_type: str, target_type: str) -> bool:
    """True when UNSIGNED/UInt invents a signed integer sink (or reverse).

    Value-safe *clear* widens (``INT UNSIGNED → BIGINT``) do not invent
    Accept-risk theater — the signed 64-bit sink holds the full unsigned 32-bit
    range. Tighter signed sinks (``UInt8 → SMALLINT``) still drop unsigned
    polarity and require Accept risk even when samples fit. Overflow stays
    blocked by :func:`unsigned_integer_would_overflow`.
    """
    from services.type_system import (
        LOGICAL_INTEGER,
        integer_bit_width,
        normalize_logical_type,
    )

    if normalize_logical_type(source_type) != LOGICAL_INTEGER:
        return False
    if normalize_logical_type(target_type) != LOGICAL_INTEGER:
        return False
    src_u = _is_unsigned_integer_carrier(source_type)
    tgt_u = _is_unsigned_integer_carrier(target_type)
    if src_u == tgt_u:
        return False
    # INT / INT UNSIGNED → BIGINT class: full range fits in signed 64-bit.
    if src_u and not tgt_u and not unsigned_integer_would_overflow(source_type, target_type):
        src_w = integer_bit_width(source_type)
        tgt_w = integer_bit_width(target_type)
        if src_w is not None and tgt_w is not None and tgt_w >= 64 and src_w <= 33:
            return False
        if src_w is None:
            # ``INT UNSIGNED`` itself: the width-unknown keyword the sibling
            # overflow check already resolves. Without this, the widest, most
            # obviously safe unsigned widen in the product asked the operator
            # to Accept risk on a MySQL→Postgres qty column.
            from services.numeric_fit import unsigned_bare_int_fits_signed_target

            if unsigned_bare_int_fits_signed_target(source_type, target_type):
                return False
    return True
