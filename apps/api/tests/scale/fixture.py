"""Wide typed fixture for the 100K relational scale matrix.

One declaration of the fixture serves every engine: the column list carries the
per-engine DDL carrier, the value generator, and the normalizer used to compare
source against destination. Comparing normalized *values* rather than an
engine-side ``MD5(...)`` expression is what makes the checksum cross-engine: a
``DECIMAL(20,9)`` rendered by MySQL and by Oracle are different strings and the
same number, and a checksum that disagreed on the rendering would report a
fidelity defect that does not exist.

A type an engine genuinely lacks is a **skip with a reason**, never a silently
dropped column: MySQL and SQLite have no timezone-aware timestamp, and Oracle
cannot store an empty string distinct from NULL. Those columns leave the mapped
projection for that pair and the reason is recorded in the cell.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping, Sequence

ENGINES = ("postgresql", "mysql", "sqlserver", "sqlite", "oracle")

#: Capabilities an engine must have for a fixture column to be in scope.
#: Absence is a documented skip, not a silent omission.
CAPABILITY_GAPS: dict[str, dict[str, str]] = {
    "mysql": {"tz_timestamp": "engine has no timezone-aware timestamp type"},
    "sqlite": {
        "tz_timestamp": "engine has no timezone-aware timestamp type",
        "json_type": "engine has no JSON type (TEXT carrier only)",
        # Measured: a DECIMAL(20,9) column has NUMERIC affinity, so SQLite
        # rewrote -12345678901.123456789 to the REAL -12345678901.123457 and
        # 99999999999.999999999 to the INTEGER 100000000000. TEXT keeps the
        # digits but is a genuine type change against a DECIMAL destination,
        # which the product blocks. Either way SQLite cannot carry an exact
        # 20-digit decimal, so the column is a skip on SQLite routes.
        "exact_decimal": "engine has no exact decimal type (NUMERIC affinity is IEEE-754 REAL)",
    },
    "sqlserver": {
        "json_type": "SQL Server 2022 has no JSON data type (NVARCHAR carrier only)"
    },
    "oracle": {"empty_string": "engine stores '' as NULL (no empty-string domain)"},
}

#: Gaps that only bite when the engine is the *destination*, and that are
#: product limitations rather than engine limitations. Recorded here so the rest
#: of the route is still measured, and reported as a defect in the evidence doc
#: rather than passed over in silence.
DEST_CAPABILITY_GAPS: dict[str, dict[str, str]] = {
    "oracle": {
        "json_type": (
            "product defect: create-new stamps JSON->CLOB for Oracle "
            "destinations (no version-aware native JSON for 21c+), so a JSON "
            "source column fail-closed blocks the whole route"
        )
    },
}

_UNICODE_SAMPLES = (
    "plain-ascii-row",
    "日本語テストデータ",  # CJK
    "emoji 🚀🙂🧪",
    "مرحبا بالعالم",  # RTL Arabic
    "Ωμέγα ß ünïcödé",
)

#: DECIMAL(20,9) shapes: negative, zero, leading zero, trailing zeros, max scale.
_DECIMALS = (
    Decimal("-12345678901.123456789"),
    Decimal("0.000000000"),
    Decimal("0.000000001"),
    Decimal("123.450000000"),
    Decimal("0.100000000"),
    Decimal("-0.000000001"),
    Decimal("99999999999.999999999"),
)

#: Exactly representable IEEE-754 doubles plus two extreme exponents, so a
#: mismatch means the carrier changed rather than a decimal-string round trip.
_FLOATS = (0.0, -0.25, 1.5, -1048576.5, 1.5e300, 2.5e-300, 0.0625)


@dataclass(frozen=True)
class Column:
    name: str
    ddl: dict[str, str]
    value: Callable[[int], Any]
    normalize: Callable[[Any], str]
    capability: str = ""
    nullable: bool = False
    primary_key: bool = False


def _n_int(v: Any) -> str:
    if v is None:
        return "\x00"
    if isinstance(v, Decimal):
        return str(int(v))
    return str(int(v))


def _n_decimal(v: Any) -> str:
    if v is None:
        return "\x00"
    d = v if isinstance(v, Decimal) else Decimal(str(v))
    # Scale-independent numeric identity: 123.450000000 == 123.45.
    return format(d.normalize(), "f")


def _n_float(v: Any) -> str:
    if v is None:
        return "\x00"
    return repr(float(v))


def _n_text(v: Any) -> str:
    if v is None:
        return "\x00"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8")
    return str(v)


def _n_date(v: Any) -> str:
    if v is None:
        return "\x00"
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)[:10]


def _parse_dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v
    text = str(v).strip().replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    return datetime.fromisoformat(text)


def _n_ts_naive(v: Any) -> str:
    if v is None:
        return "\x00"
    dt = _parse_dt(v)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat(timespec="microseconds")


def _n_ts_tz(v: Any) -> str:
    if v is None:
        return "\x00"
    dt = _parse_dt(v)
    if dt.tzinfo is None:
        # A tz-aware source landing naive is a real collapse, not a rendering
        # difference — keep it distinguishable in the checksum.
        return "naive:" + dt.isoformat(timespec="microseconds")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _n_bool(v: Any) -> str:
    if v is None:
        return "\x00"
    if isinstance(v, str):
        return "1" if v.strip().lower() in {"1", "true", "t", "yes", "y"} else "0"
    if isinstance(v, (bytes, bytearray)):
        return "1" if bytes(v) not in (b"\x00", b"0", b"") else "0"
    if isinstance(v, Decimal):
        return "1" if v != 0 else "0"
    return "1" if bool(v) else "0"


def _n_uuid(v: Any) -> str:
    if v is None:
        return "\x00"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return str(uuid.UUID(bytes=bytes(v)))
    return str(uuid.UUID(str(v).strip()))


def _n_json(v: Any) -> str:
    if v is None:
        return "\x00"
    if isinstance(v, (bytes, bytearray, memoryview)):
        v = bytes(v).decode("utf-8")
    obj = json.loads(v) if isinstance(v, str) else v
    return json.dumps(
        _json_canonical(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _json_canonical(obj: Any) -> Any:
    """Native-JSON engines hand back numbers as Decimal — compare by value."""
    if isinstance(obj, dict):
        return {k: _json_canonical(x) for k, x in obj.items()}
    if isinstance(obj, list):
        return [_json_canonical(x) for x in obj]
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    return obj


def _n_binary(v: Any) -> str:
    if v is None:
        return "\x00"
    if isinstance(v, str):
        # Hex carriers (Oracle RAW over some drivers) come back as hex text.
        return v.strip().lower()
    return bytes(v).hex()


_EPOCH_DATE = date(2020, 1, 1)
_EPOCH_TS = datetime(2021, 3, 14, 1, 59, 26)
_TZ_OFFSETS = (0, 5 * 60 + 30, -8 * 60, 60, -3 * 60 - 30)
_JSON_NS = uuid.UUID("6f4d1f0a-6a0e-4c0f-9a6b-2b1c9c0f0000")

COLUMNS: tuple[Column, ...] = (
    Column(
        name="id",
        # 64-bit on every engine: SQLite's INTEGER *is* 64-bit and is
        # introspected as BIGINT, so a 32-bit INT elsewhere would make every
        # SQLite-source route a real (correctly blocked) narrowing.
        ddl={
            "postgresql": "BIGINT",
            "mysql": "BIGINT",
            "sqlserver": "BIGINT",
            "sqlite": "BIGINT",
            "oracle": "NUMBER(19)",
        },
        value=lambda i: i + 1,
        normalize=_n_int,
        primary_key=True,
    ),
    Column(
        name="big_id",
        ddl={
            "postgresql": "BIGINT",
            "mysql": "BIGINT",
            "sqlserver": "BIGINT",
            "sqlite": "BIGINT",
            "oracle": "NUMBER(19)",
        },
        value=lambda i: 9_007_199_254_740_993 + i,
        normalize=_n_int,
    ),
    Column(
        name="amt_dec",
        ddl={
            "postgresql": "NUMERIC(20,9)",
            "mysql": "DECIMAL(20,9)",
            "sqlserver": "DECIMAL(20,9)",
            "sqlite": "TEXT",
            "oracle": "NUMBER(20,9)",
        },
        value=lambda i: _DECIMALS[i % len(_DECIMALS)],
        normalize=_n_decimal,
        capability="exact_decimal",
    ),
    Column(
        name="amt_float",
        ddl={
            "postgresql": "DOUBLE PRECISION",
            "mysql": "DOUBLE",
            "sqlserver": "FLOAT(53)",
            "sqlite": "DOUBLE",
            "oracle": "BINARY_DOUBLE",
        },
        value=lambda i: _FLOATS[i % len(_FLOATS)],
        normalize=_n_float,
    ),
    Column(
        name="name_txt",
        # MySQL and SQL Server default to case/accent-INsensitive collations
        # while PostgreSQL / Oracle / SQLite compare case-sensitively, so the
        # fixture pins a case- and accent-sensitive collation on both. Left
        # default, the equality class itself changes on the route and
        # services.collation_carry rightly refuses to call that "carried".
        ddl={
            "postgresql": "VARCHAR(64)",
            "mysql": "VARCHAR(64) COLLATE utf8mb4_0900_bin",
            "sqlserver": "NVARCHAR(64) COLLATE Latin1_General_100_BIN2",
            "sqlite": "VARCHAR(64)",
            "oracle": "NVARCHAR2(64)",
        },
        value=lambda i: f"{_UNICODE_SAMPLES[i % len(_UNICODE_SAMPLES)]} #{i}",
        normalize=_n_text,
    ),
    Column(
        name="note_null",
        ddl={
            # Bounded on every engine: an unbounded TEXT source against a
            # bounded destination is a genuine narrowing the product blocks,
            # which belongs to the narrowing shape rather than to the
            # "destination exists compatible" shape this column carries.
            "postgresql": "VARCHAR(400)",
            "mysql": "VARCHAR(400) COLLATE utf8mb4_0900_bin",
            "sqlserver": "NVARCHAR(400) COLLATE Latin1_General_100_BIN2",
            "sqlite": "VARCHAR(400)",
            "oracle": "NVARCHAR2(400)",
        },
        value=lambda i: None if i % 3 == 0 else f"note {i}",
        normalize=_n_text,
        nullable=True,
    ),
    Column(
        name="note_empty",
        ddl={
            "postgresql": "VARCHAR(400)",
            "mysql": "VARCHAR(400) COLLATE utf8mb4_0900_bin",
            "sqlserver": "NVARCHAR(400) COLLATE Latin1_General_100_BIN2",
            "sqlite": "VARCHAR(400)",
            "oracle": "NVARCHAR2(400)",
        },
        value=lambda i: "" if i % 3 == 1 else f"filled {i}",
        normalize=_n_text,
        capability="empty_string",
    ),
    Column(
        name="d_date",
        ddl={
            "postgresql": "DATE",
            "mysql": "DATE",
            "sqlserver": "DATE",
            "sqlite": "DATE",
            "oracle": "DATE",
        },
        value=lambda i: _EPOCH_DATE + timedelta(days=i % 3650),
        normalize=_n_date,
    ),
    Column(
        name="ts_naive",
        ddl={
            "postgresql": "TIMESTAMP(6)",
            "mysql": "DATETIME(6)",
            "sqlserver": "DATETIME2(6)",
            "sqlite": "TIMESTAMP",
            "oracle": "TIMESTAMP(6)",
        },
        value=lambda i: _EPOCH_TS + timedelta(seconds=i, microseconds=(i * 7) % 1_000_000),
        normalize=_n_ts_naive,
    ),
    Column(
        name="ts_tz",
        ddl={
            "postgresql": "TIMESTAMPTZ",
            "sqlserver": "DATETIMEOFFSET(6)",
            "oracle": "TIMESTAMP(6) WITH TIME ZONE",
        },
        value=lambda i: (_EPOCH_TS + timedelta(seconds=i * 3)).replace(
            tzinfo=timezone(timedelta(minutes=_TZ_OFFSETS[i % len(_TZ_OFFSETS)]))
        ),
        normalize=_n_ts_tz,
        capability="tz_timestamp",
    ),
    Column(
        name="flag",
        ddl={
            "postgresql": "BOOLEAN",
            "mysql": "TINYINT(1)",
            "sqlserver": "BIT",
            "sqlite": "BOOLEAN",
            "oracle": "NUMBER(1)",
        },
        value=lambda i: bool(i % 2 == 0),
        normalize=_n_bool,
    ),
    Column(
        name="uid",
        ddl={
            "postgresql": "UUID",
            "mysql": "CHAR(36) COLLATE utf8mb4_0900_bin",
            "sqlserver": "UNIQUEIDENTIFIER",
            # Bounded ≥36 is the certified UUID wire; bare TEXT loses UUID
            # polarity and the product blocks it without a risk contract.
            "sqlite": "CHAR(36)",
            "oracle": "CHAR(36 CHAR)",
        },
        value=lambda i: str(uuid.uuid5(_JSON_NS, str(i))),
        normalize=_n_uuid,
    ),
    Column(
        name="payload_json",
        ddl={
            "postgresql": "JSONB",
            "mysql": "JSON",
            "sqlserver": "NVARCHAR(MAX)",
            "sqlite": "TEXT",
            "oracle": "JSON",
        },
        capability="json_type",
        value=lambda i: json.dumps(
            {
                "i": i,
                "u": "日本 🚀 مرحبا",
                "nested": {"a": [1, 2, 3], "b": None},
                "dec_as_text": "-0.000000001",
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ),
        normalize=_n_json,
    ),
    Column(
        name="blob_bin",
        ddl={
            # Unbounded on every engine: PostgreSQL BYTEA has no length, so a
            # bounded VARBINARY(64) destination is a real narrowing and the
            # write is blocked (measured) rather than truncated.
            "postgresql": "BYTEA",
            "mysql": "BLOB",
            "sqlserver": "VARBINARY(MAX)",
            "sqlite": "BLOB",
            "oracle": "BLOB",
        },
        value=lambda i: hashlib.md5(str(i).encode(), usedforsecurity=False).digest(),
        normalize=_n_binary,
    ),
)

COLUMNS_BY_NAME = {c.name: c for c in COLUMNS}


def engine_gap(engine: str, capability: str) -> str:
    """Why ``engine`` cannot carry a column needing ``capability`` ('' if it can)."""
    if not capability:
        return ""
    return CAPABILITY_GAPS.get(engine, {}).get(capability, "")


def dest_gap(engine: str, capability: str) -> str:
    """Why ``engine`` cannot *receive* a column needing ``capability``."""
    if not capability:
        return ""
    return DEST_CAPABILITY_GAPS.get(engine, {}).get(capability, "")


#: Columns whose *domain* the destination engine cannot declare, where the
#: product's documented remedy is a signed Migration Risk Contract rather than a
#: refusal for all time (``services.type_coercion_validator``: create-new
#: UUID -> bare TEXT is a polarity collapse, and
#: ``services.migration_risk_contract`` is the authority that clears it). The
#: route is measured twice: unsigned it must block, signed it must round-trip
#: every value byte-exact.
DOMAIN_CONTRACT_COLUMNS: dict[str, dict[str, str]] = {
    "sqlite": {
        "uid": (
            "SQLite has no UUID domain: create-new stamps TEXT and preflight "
            "blocks the UUID->TEXT polarity collapse until a Migration Risk "
            "Contract is signed"
        )
    },
}


def domain_contract_columns(destination: str, columns: Sequence[str]) -> dict[str, str]:
    """Mapped columns of this route that need a signed contract to be written."""
    gaps = DOMAIN_CONTRACT_COLUMNS.get(destination, {})
    return {name: gaps[name] for name in columns if name in gaps}


def projection(source: str, destination: str) -> tuple[list[str], dict[str, str]]:
    """Mapped columns for a route plus the skip reason for each excluded one."""
    cols: list[str] = []
    skips: dict[str, str] = {}
    for col in COLUMNS:
        src_gap = engine_gap(source, col.capability)
        dst_gap = engine_gap(destination, col.capability) or dest_gap(
            destination, col.capability
        )
        if src_gap or dst_gap:
            side = source if src_gap else destination
            skips[col.name] = f"skip ({side}: {src_gap or dst_gap})"
            continue
        cols.append(col.name)
    return cols, skips


def engine_columns(engine: str) -> list[str]:
    return [c.name for c in COLUMNS if not engine_gap(engine, c.capability)]


def rows(count: int, columns: Sequence[str], *, offset: int = 0) -> Iterable[dict[str, Any]]:
    """Deterministic fixture rows — same index always yields the same row."""
    specs = [COLUMNS_BY_NAME[name] for name in columns]
    for i in range(offset, offset + count):
        yield {spec.name: spec.value(i) for spec in specs}


def row_digest(row: dict[str, Any], columns: Sequence[str]) -> int:
    payload = "\x1f".join(COLUMNS_BY_NAME[c].normalize(row.get(c)) for c in columns)
    return int.from_bytes(
        hashlib.md5(payload.encode("utf-8"), usedforsecurity=False).digest()[:8], "big"
    )


@dataclass
class Checksum:
    """Order-independent aggregate over a mapped projection."""

    columns: list[str]
    count: int = 0
    total: int = 0
    _seen: set[int] = field(default_factory=set, repr=False)

    def add(self, row: dict[str, Any]) -> None:
        self.count += 1
        self.total = (self.total + row_digest(row, self.columns)) % (1 << 64)

    @property
    def hex(self) -> str:
        return f"{self.total:016x}"

    def matches(self, other: "Checksum") -> bool:
        return self.count == other.count and self.total == other.total


def expected_checksum(count: int, columns: Sequence[str], *, offset: int = 0) -> Checksum:
    chk = Checksum(columns=list(columns))
    for row in rows(count, columns, offset=offset):
        chk.add(row)
    return chk


def ddl_for(
    engine: str,
    columns: Sequence[str],
    *,
    narrow: str = "",
    quote: Callable[[str], str] | None = None,
    keyless: bool = False,
) -> str:
    """Column DDL body for a table on ``engine``.

    ``narrow`` names a column to declare deliberately too small (the
    dest-exists-narrower shape) — the engine must block rather than truncate.
    ``keyless`` drops the primary key, which is the shape an append sink needs:
    the same fixture landing twice is duplicate keys by definition.
    """
    qt = quote or (lambda ident: f'"{ident}"')
    parts = []
    for name in columns:
        col = COLUMNS_BY_NAME[name]
        type_sql = col.ddl[engine]
        if name == narrow:
            type_sql = _NARROW_DDL[name][engine]
        null_sql = "NULL" if col.nullable else "NOT NULL"
        pk = " PRIMARY KEY" if col.primary_key and not keyless else ""
        parts.append(f"{qt(name)} {type_sql} {null_sql}{pk}")
    return ", ".join(parts)


def invented_ddl_for(
    engine: str,
    columns: Sequence[str],
    source_types: Mapping[str, str],
    *,
    quote: Callable[[str], str] | None = None,
    keyless: bool = False,
    source_engine: str = "",
) -> str:
    """Column DDL body the *product* would create for this route.

    The dest-exists-compatible shape has to be the table create-new would have
    stamped, not the destination engine's own idiomatic fixture DDL: an Oracle
    ``NUMBER(19,0)`` source against a hand-written PostgreSQL ``BIGINT`` is a
    genuine narrowing (``NUMBER(19,0)`` holds 10**19-1) and the product is right
    to block it. Asking the canonical inventor
    (``decision_kernel.type_invent.create_new_mapping_target_type``) keeps one
    algorithm owner: the harness declares no second type map. ``source_engine``
    is what lets the inventor keep encoding capacity — a PostgreSQL
    ``VARCHAR(64)`` holding CJK must land on SQL Server ``NVARCHAR(64)``, since
    a code-page ``VARCHAR`` cannot spell U+8A9E.
    """
    from services.decision_kernel.type_invent import create_new_mapping_target_type

    qt = quote or (lambda ident: f'"{ident}"')
    parts = []
    for name in columns:
        col = COLUMNS_BY_NAME[name]
        source_type = source_types.get(name) or col.ddl[engine]
        type_sql = create_new_mapping_target_type(
            source_type, engine, source_db=source_engine or engine
        )
        null_sql = "NULL" if col.nullable else "NOT NULL"
        pk = " PRIMARY KEY" if col.primary_key and not keyless else ""
        parts.append(f"{qt(name)} {type_sql} {null_sql}{pk}")
    return ", ".join(parts)


#: Narrower carrier per attackable column — a destination that must block.
#: ``amt_dec``: DECIMAL(20,9) into an integer carrier (rounding).
#: ``name_txt``: VARCHAR(64) Unicode into VARCHAR(8) (truncation) — used on
#: routes where the decimal is skipped for lack of an exact decimal type.
_NARROW_DDL = {
    "amt_dec": {
        "postgresql": "INTEGER",
        "mysql": "INT",
        "sqlserver": "INT",
        "sqlite": "INTEGER",
        "oracle": "NUMBER(10)",
    },
    "name_txt": {
        "postgresql": "VARCHAR(8)",
        "mysql": "VARCHAR(8) COLLATE utf8mb4_0900_bin",
        "sqlserver": "NVARCHAR(8) COLLATE Latin1_General_100_BIN2",
        "sqlite": "VARCHAR(8)",
        "oracle": "NVARCHAR2(8)",
    },
}
