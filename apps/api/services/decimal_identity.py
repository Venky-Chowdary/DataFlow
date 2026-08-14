"""Exact decimal identity is an unscaled integer and a scale, not a float.

Airbyte's intermediate JSON ``number`` and many Python pipelines ``float()``
a DECIMAL cell, so ``1.2300`` (money scale 4) and ``1.23`` collapse, and
integers past ``2**53`` round through binary64. PostgreSQL ``NUMERIC(p,s)``
silently **rounds** excess fractional digits (ties away from zero). MySQL
``DECIMAL`` under non-strict SQL stores a truncated/rounded value; checksums
of the *accepted* digits stay green. SQLite ``DECIMAL`` affinity is IEEE
``REAL``. Fivetran converts unspecified BIGDECIMAL to FLOAT.

Bind compatibility (``fits_decimal`` / ``coerce_decimal_wire``) may still
match what the destination engine will do at INSERT — PG rounds, MySQL
STRICT refuses. That is not the same as claiming the source identity landed.
This module is the identity half:

1. Extract ``(sign, unscaled digits, scale)`` from the cell **before**
   ``float()``. Trailing zeros are the money contract (``1.2300`` ≠ ``1.23``
   as stored scale).
2. Classify dest storage: exact DECIMAL/NUMERIC, approximate FLOAT/REAL,
   SQLite NUMERIC affinity (IEEE), or digit-text (SQLite TEXT invent).
3. Fidelity aspect ``decimal``: source FLOAT → ``skipped`` (never had an
   exact identity); source DECIMAL → dest FLOAT / narrower scale /
   SQLite affinity → ``unsupported``; dest that can store the source scale
   → ``carried``. We do not invent a companion integer-cents column.
4. Certify from the dest engine (``amt::text`` / ``CAST(amt AS CHAR)``),
   not from Python ``Decimal`` after a second parse.

IEEE-754 binary64 (``2**53``) is the same mantissa bound as JSON polarity.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, Overflow
from typing import Any, Literal

from services.dest_dialect_facts import _normalize_dest_db
from services.json_polarity import IEEE754_SAFE_INT

Status = Literal["carried", "unsupported", "skipped"]
Storage = Literal["exact", "approximate", "sqlite_affinity", "text_digits", "unknown"]

_FLOAT_RE = re.compile(
    r"^(?:FLOAT(?:4|8|16|32|64)?|REAL|DOUBLE(?:\s+PRECISION)?|"
    r"BINARY_FLOAT|BINARY_DOUBLE|HALF(?:FLOAT)?|IEEE754)\b",
    re.I,
)
_EXACT_RE = re.compile(
    r"^(?:DECIMAL(?:32|64|128|256)?|NUMERIC|NUMBER|NUM|DEC|"
    r"MONEY|SMALLMONEY|CURRENCY|BIGNUMERIC|DECFLOAT|BIGDECIMAL)\b",
    re.I,
)
_TEXT_RE = re.compile(
    r"^(?:TEXT|CLOB|STRING|VARCHAR|CHARACTER(?:\s+VARYING)?|CHAR)\b",
    re.I,
)


@dataclass(frozen=True)
class DecimalIdentity:
    """Unscaled integer × 10^(−scale). Trailing zeros are data."""

    sign: int
    digits: str
    scale: int
    approximate: bool = False

    @property
    def unscaled(self) -> int:
        mag = int(self.digits or "0")
        return mag if self.sign == 0 else -mag

    @property
    def beyond_ieee(self) -> bool:
        return abs(self.unscaled) > IEEE754_SAFE_INT

    def to_canonical_text(self) -> str:
        """Exact decimal text without scientific notation."""
        if not self.digits or self.digits == "0":
            body = "0"
            if self.scale:
                body = "0." + ("0" * self.scale)
        elif self.scale <= 0:
            body = self.digits + ("0" * (-self.scale))
        elif self.scale >= len(self.digits):
            body = "0." + ("0" * (self.scale - len(self.digits))) + self.digits
        else:
            split = len(self.digits) - self.scale
            body = self.digits[:split] + "." + self.digits[split:]
        return ("-" if self.sign else "") + body

    def to_dict(self) -> dict[str, object]:
        return {
            "sign": self.sign,
            "digits": self.digits,
            "scale": self.scale,
            "unscaled": self.unscaled,
            "approximate": self.approximate,
            "beyond_ieee": self.beyond_ieee,
        }


@dataclass(frozen=True)
class DecimalStorage:
    """What an engine can physically store for one numeric column."""

    kind: Storage
    precision: int | None = None
    scale: int | None = None
    unconstrained: bool = False
    name: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "precision": self.precision,
            "scale": self.scale,
            "unconstrained": self.unconstrained,
            "name": self.name,
        }


@dataclass
class DecimalDecision:
    source_column: str
    dest_column: str
    status: Status
    reason: str
    source_storage: DecimalStorage | None = None
    dest_storage: DecimalStorage | None = None
    source_type: str = ""
    dest_type: str = ""

    def to_item_kwargs(self) -> dict[str, Any]:
        src = self.source_storage
        dst = self.dest_storage
        return {
            "aspect": "decimal",
            "name": self.dest_column or self.source_column,
            "status": self.status,
            "reason": self.reason,
            "source_detail": (
                f"{self.source_type} {src.kind}" if src else self.source_type
            ),
            "dest_ddl": (
                (dst.name or self.dest_type) if dst and self.status == "carried" else ""
            ),
        }


def is_float_catalog_type(data_type: str) -> bool:
    collapsed = " ".join((data_type or "").split())
    return bool(collapsed and _FLOAT_RE.match(collapsed))


def is_exact_decimal_catalog_type(data_type: str) -> bool:
    collapsed = " ".join((data_type or "").split())
    if not collapsed or is_float_catalog_type(collapsed):
        return False
    return bool(_EXACT_RE.match(collapsed))


def is_numeric_catalog_type(data_type: str) -> bool:
    """True when the column participates in the decimal-identity question."""
    collapsed = " ".join((data_type or "").split())
    if not collapsed:
        return False
    if is_float_catalog_type(collapsed) or is_exact_decimal_catalog_type(collapsed):
        return True
    if _TEXT_RE.match(collapsed):
        return False
    logical = collapsed.strip().lower()
    return logical in {"decimal", "numeric", "float", "money", "number"}


def extract_decimal_identity(value: Any) -> DecimalIdentity | None:
    """Unscaled digits and scale from a cell, never via binary64.

    ``float`` input is marked approximate — the original unscaled integer is
    unrecoverable. Trailing zeros from ``Decimal('1.2300')`` / ``'1.2300'``
    stay in ``digits``.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return None
    if isinstance(value, bool):
        raise ValueError("decimal identity refuses bool — refuse invent 0/1 money")
    approximate = False
    try:
        if isinstance(value, Decimal):
            d = value
        elif isinstance(value, int):
            d = Decimal(value)
        elif isinstance(value, float):
            if value != value or value in (float("inf"), float("-inf")):  # noqa: PLR0124
                raise ValueError("decimal identity refuses NaN/Inf")
            d = Decimal(str(value))
            approximate = True
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            d = Decimal(text)
        else:
            d = Decimal(str(value).strip())
        if not d.is_finite():
            raise ValueError("decimal identity refuses non-finite")
    except (InvalidOperation, Overflow, ValueError) as exc:
        raise ValueError(
            "decimal identity parse failed — refuse float invent"
        ) from exc
    sign, digits, exp = d.as_tuple()
    digit_str = "".join(str(dgt) for dgt in digits) or "0"
    scale = -int(exp) if int(exp) < 0 else 0
    if int(exp) > 0:
        digit_str = digit_str + ("0" * int(exp))
        scale = 0
    return DecimalIdentity(
        sign=int(sign),
        digits=digit_str,
        scale=scale,
        approximate=approximate,
    )


def classify_storage(
    engine: str,
    type_str: str = "",
    *,
    create_new: bool = False,
) -> DecimalStorage:
    """Physical numeric storage. SQLite DECIMAL affinity is not DECIMAL."""
    from services.type_system import parse_numeric_precision_scale

    eng = _normalize_dest_db(engine)
    collapsed = " ".join((type_str or "").split())
    name = collapsed or "numeric"

    if eng == "sqlite":
        upper = collapsed.upper()
        if create_new and (
            is_exact_decimal_catalog_type(collapsed)
            or "DECIMAL" in upper
            or "NUMERIC" in upper
            or "NUMBER" in upper
            or "MONEY" in upper
            or not collapsed
        ):
            return DecimalStorage(kind="text_digits", unconstrained=True, name="TEXT")
        if upper.startswith("TEXT") or upper.startswith("CLOB") or upper.startswith("VARCHAR"):
            return DecimalStorage(kind="text_digits", unconstrained=True, name=name)
        if "INT" in upper and "POINT" not in upper and "INTERVAL" not in upper:
            return DecimalStorage(kind="exact", precision=18, scale=0, name=name)
        if not collapsed or "DECIMAL" in upper or "NUMERIC" in upper or "NUMBER" in upper:
            return DecimalStorage(kind="sqlite_affinity", name=name)
        if is_float_catalog_type(collapsed) or "REAL" in upper or "DOUBLE" in upper:
            return DecimalStorage(kind="approximate", name=name)
        if _TEXT_RE.match(collapsed):
            return DecimalStorage(kind="text_digits", unconstrained=True, name=name)

    if is_float_catalog_type(collapsed):
        return DecimalStorage(kind="approximate", name=name)

    if _TEXT_RE.match(collapsed) and eng == "sqlite":
        return DecimalStorage(kind="text_digits", unconstrained=True, name=name)

    if not collapsed:
        return DecimalStorage(kind="unknown", name="")

    if not is_exact_decimal_catalog_type(collapsed) and collapsed.lower() not in {
        "decimal",
        "numeric",
        "money",
        "number",
    }:
        return DecimalStorage(kind="unknown", name=name)

    precision, scale = parse_numeric_precision_scale(collapsed)
    if precision is None and scale is None:
        return DecimalStorage(
            kind="exact",
            unconstrained=True,
            name=name,
        )
    return DecimalStorage(
        kind="exact",
        precision=precision,
        scale=0 if scale is None else scale,
        unconstrained=False,
        name=name,
    )


def dest_can_store_source_scale(source: DecimalStorage, dest: DecimalStorage) -> bool:
    """True when dest scale is unconstrained or ≥ source *declared* scale.

    Unconstrained source (PostgreSQL ``numeric``) has no declared ceiling;
    dest exactness is the storage-class question, not a scale comparison.
    """
    if dest.kind == "text_digits":
        return True
    if dest.kind != "exact":
        return False
    if dest.unconstrained or source.unconstrained:
        return True
    src_scale = 0 if source.scale is None else source.scale
    dst_scale = 0 if dest.scale is None else dest.scale
    return dst_scale >= src_scale


def decide_decimal_identity(
    *,
    source_engine: str,
    source_type: str,
    dest_engine: str,
    dest_type: str,
    source_column: str = "",
    dest_column: str = "",
    create_new: bool = False,
) -> DecimalDecision | None:
    """Carry / unsupported / skipped for one mapped numeric column.

    ``None`` means the source is not a decimal/float column.
    """
    if not is_numeric_catalog_type(source_type) and not is_exact_decimal_catalog_type(
        source_type
    ):
        if not is_float_catalog_type(source_type):
            return None
    src = classify_storage(source_engine, source_type)
    dst = classify_storage(dest_engine, dest_type, create_new=create_new)
    col = dest_column or source_column

    if src.kind == "unknown" and not is_numeric_catalog_type(source_type):
        return None

    if src.kind == "approximate":
        return DecimalDecision(
            source_column=source_column,
            dest_column=col,
            status="skipped",
            reason=(
                f"Source {source_type or 'FLOAT'} on {source_engine or 'this engine'} "
                "is IEEE binary floating-point — there is no exact unscaled "
                "integer to carry (binary64 / REAL class)."
            ),
            source_storage=src,
            dest_storage=dst,
            source_type=source_type,
            dest_type=dest_type,
        )

    if src.kind in {"unknown"} and not is_exact_decimal_catalog_type(source_type):
        return None

    if dst.kind == "approximate":
        return DecimalDecision(
            source_column=source_column,
            dest_column=col,
            status="unsupported",
            reason=(
                f"Source stored exact decimal on {source_type}; destination "
                f"{dest_engine} {dest_type or 'FLOAT'} is IEEE binary. Digits "
                f"past the {IEEE754_SAFE_INT} mantissa round off. We do not "
                "invent a companion TEXT column."
            ),
            source_storage=src,
            dest_storage=dst,
            source_type=source_type,
            dest_type=dest_type,
        )

    if dst.kind == "sqlite_affinity":
        return DecimalDecision(
            source_column=source_column,
            dest_column=col,
            status="unsupported",
            reason=(
                "SQLite DECIMAL/NUMERIC affinity stores IEEE REAL. Create-new "
                "emits TEXT so digits survive; an existing affinity column "
                "cannot claim exact decimal identity."
            ),
            source_storage=src,
            dest_storage=dst,
            source_type=source_type,
            dest_type=dest_type,
        )

    if dst.kind == "unknown":
        return DecimalDecision(
            source_column=source_column,
            dest_column=col,
            status="unsupported",
            reason=(
                f"Destination {dest_engine or 'engine'} {dest_type or 'numeric'} "
                "storage was not measured; refuse to claim unscaled digits land."
            ),
            source_storage=src,
            dest_storage=dst,
            source_type=source_type,
            dest_type=dest_type,
        )

    if dst.kind == "text_digits":
        return DecimalDecision(
            source_column=source_column,
            dest_column=col,
            status="carried",
            reason=(
                "Destination stores decimal as digit text (SQLite TEXT class) "
                "so the unscaled integer is not pushed through IEEE REAL."
            ),
            source_storage=src,
            dest_storage=dst,
            source_type=source_type,
            dest_type=dest_type,
        )

    if dest_can_store_source_scale(src, dst):
        return DecimalDecision(
            source_column=source_column,
            dest_column=col,
            status="carried",
            reason=(
                f"Destination {dst.name or dest_type} is an exact decimal "
                "carrier whose scale can hold the source unscaled integer."
            ),
            source_storage=src,
            dest_storage=dst,
            source_type=source_type,
            dest_type=dest_type,
        )

    return DecimalDecision(
        source_column=source_column,
        dest_column=col,
        status="unsupported",
        reason=(
            f"Source {src.name or source_type} scale "
            f"{src.scale if src.scale is not None else 'unconstrained'} "
            f"exceeds destination {dest_engine} "
            f"{dst.name or dest_type} scale {dst.scale}. "
            "The destination engine will round or reject extra fractional "
            "digits. Magnitude may still land; the unscaled integer does not."
        ),
        source_storage=src,
        dest_storage=dst,
        source_type=source_type,
        dest_type=dest_type,
    )


def plan_decimal_identity_carry(
    *,
    catalog: Any,
    dest_dialect: str,
    dest_name_for_source: Any,
    dest_type_for_column: Any,
) -> list[DecimalDecision]:
    """One decision per mapped decimal/float source column."""
    types = dict(getattr(catalog, "column_types", None) or {})
    source_engine = str(getattr(catalog, "dialect", "") or "")
    decisions: list[DecimalDecision] = []
    if not types:
        return decisions
    for src_col, src_type in types.items():
        dest_col = dest_name_for_source(src_col) if dest_name_for_source else src_col
        if not dest_col:
            continue
        dest_type = dest_type_for_column(dest_col) if dest_type_for_column else ""
        decision = decide_decimal_identity(
            source_engine=source_engine,
            source_type=str(src_type or ""),
            dest_engine=dest_dialect,
            dest_type=str(dest_type or ""),
            source_column=str(src_col),
            dest_column=str(dest_col),
            create_new=True,
        )
        if decision is not None:
            decisions.append(decision)
    return decisions


def identities_match(source: DecimalIdentity, dest: DecimalIdentity) -> bool:
    """True when dest stored the same unscaled integer and scale."""
    return (
        source.sign == dest.sign
        and source.digits.lstrip("0") == dest.digits.lstrip("0")
        and source.scale == dest.scale
        and not dest.approximate
    )


def identities_same_magnitude(source: DecimalIdentity, dest: DecimalIdentity) -> bool:
    """True when dest stored the same rational, possibly with a different scale."""
    try:
        return Decimal(source.to_canonical_text()) == Decimal(dest.to_canonical_text())
    except (InvalidOperation, Overflow):
        return False


def dest_numeric_text_sql(engine: str, column_sql: str) -> str:
    """Dest-engine decimal spelling. Certify digits from this, not Python Decimal."""
    eng = _normalize_dest_db(engine)
    if eng in {"mysql", "mariadb", "tidb"}:
        return f"CAST({column_sql} AS CHAR)"
    if eng in {"sqlserver", "mssql", "azure_sql"}:
        return f"CONVERT(VARCHAR(64), {column_sql})"
    return f"({column_sql})::text"
