"""Encoding capacity is physical storage of Unicode scalar values, not a charset name.

MySQL ``utf8`` is three-byte (BMP). Oracle ``UTF8`` is CESU-8 (UTR #26).
PostgreSQL ``UTF8`` is Unicode. AWS DMS's answer is character substitution;
Airbyte travels through JSON UTF-8 and hopes. A checksum of Python ``str``
after the driver decoded can match while the destination stored ``?``,
rejected a 4-byte sequence, or ingested ``ED A0 BD`` as invalid UTF-8.

Charset DDL (``CHARACTER SET utf8mb4``) and collation equality are independent
guarantees — see ``collation_carry``. This module is the *value* half:

1. Classify dest (and source) **form** and **capacity**. SQL-standard names
   lie: MySQL ``latin1`` is cp1252; Oracle ``UTF8`` is not UTF-8.
2. Decode cells to Unicode scalars. CESU-8 six-byte supplementary sequences
   and UTF-16 surrogate pairs leaked into Python ``str`` are recomposed.
   Unpaired surrogates and ill-formed UTF-8 raise — we do not invent U+FFFD.
3. Bind: dest that cannot encode a scalar quarantines the cell. Never latin-1
   fallback, never replacement-character invent, never a companion BYTEA
   column the operator did not approve.
4. Fidelity aspect ``encoding``: source supplementary → dest utf8mb4 is
   ``carried``; source supplementary → dest utf8mb3 is ``unsupported``.
   U+FFFD already in the source is prior loss, still a character — we carry
   it when dest can store it and do not strip it (DMS substitution).

Certification is dest-engine (``OCTET_LENGTH`` / ``encode(..., 'hex')``),
not Python ``len(s.encode('utf-8'))`` after a second decode.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from services.dest_dialect_facts import _normalize_dest_db
from services.value_serializer import (
    DF_MISSING_SENTINEL,
    cell_to_string,
    is_missing_sentinel,
)

Status = Literal["carried", "unsupported", "skipped"]
Form = Literal[
    "utf8",
    "utf8mb3",
    "cesu8",
    "utf16",
    "gb18030",
    "cp1252",
    "latin1",
    "ascii",
    "binary",
    "unknown",
]

REPLACEMENT = "\ufffd"
BMP_MAX = 0xFFFF
# UTF-16 surrogate code units leaked into a Python str (CESU-8 / JDBC drivers).
# Absence is decided in one C-level scan so the recompose loop stays off the
# per-cell write path.
_SURROGATE_RE = re.compile("[\ud800-\udfff]")
UNICODE_MAX = 0x10FFFF

_STRING_RE = re.compile(
    r"^(?:N?(?:VAR)?CHAR|CHARACTER(?:\s+VARYING)?|TEXT|CLOB|NCLOB|STRING|"
    r"CITEXT|NVARCHAR2|VARCHAR2|LONGTEXT|TINYTEXT|MEDIUMTEXT|NTEXT|SYSNAME|"
    r"LONG|BPCHAR)\b",
    re.I,
)
_CHARSET_RE = re.compile(
    r"(?:CHARACTER\s+SET|CHARSET)\s+['\"]?([A-Za-z0-9_]+)",
    re.I,
)
_COLLATE_RE = re.compile(r"COLLATE\s+['\"]?([A-Za-z0-9_]+)", re.I)

_MYSQL_FAMILY = frozenset({"mysql", "mariadb", "tidb", "aurora_mysql", "singlestore"})
_PG_FAMILY = frozenset(
    {
        "postgresql",
        "redshift",
        "greenplum",
        "cockroachdb",
        "yugabytedb",
        "timescaledb",
        "alloydb",
        "citus",
        "duckdb",
    }
)
_SQLSERVER_FAMILY = frozenset(
    {"sqlserver", "mssql", "azure_sql", "azure_sql_database", "synapse"}
)
_ORACLE_FAMILY = frozenset({"oracle", "oracledb", "oracle_autonomous"})
_UTF8_DESTS = frozenset(
    {
        "snowflake",
        "bigquery",
        "spanner",
        "sqlite",
        "mongodb",
        "documentdb",
        "cosmosdb",
        "databricks",
        "clickhouse",
        "trino",
        "presto",
        "iceberg",
        "hive",
        "spark",
    }
)


@dataclass(frozen=True)
class EncodingCapacity:
    """What an engine can physically store for one string column."""

    form: Form
    name: str = ""
    codec: str = ""
    max_code_point: int = UNICODE_MAX

    @property
    def rank(self) -> int:
        """Partial order for type-level carry. Higher can store lower."""
        if self.form == "binary":
            return -1
        if self.form == "unknown":
            return 0
        if self.form == "ascii":
            return 10
        if self.form in {"latin1", "cp1252"}:
            return 20
        if self.form == "utf8mb3":
            return 30
        return 40

    def to_dict(self) -> dict[str, object]:
        return {
            "form": self.form,
            "name": self.name,
            "codec": self.codec,
            "max_code_point": self.max_code_point,
        }


@dataclass(frozen=True)
class CellEncoding:
    """Unicode scalars in one cell, after CESU-8 / surrogate recompose."""

    text: str
    max_code_point: int
    prior_loss: bool
    supplementary: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "max_code_point": self.max_code_point,
            "prior_loss": self.prior_loss,
            "supplementary": self.supplementary,
        }


@dataclass
class EncodingDecision:
    source_column: str
    dest_column: str
    status: Status
    reason: str
    source_capacity: EncodingCapacity | None = None
    dest_capacity: EncodingCapacity | None = None
    source_type: str = ""
    dest_type: str = ""

    def to_item_kwargs(self) -> dict[str, Any]:
        src = self.source_capacity
        dst = self.dest_capacity
        return {
            "aspect": "encoding",
            "name": self.dest_column or self.source_column,
            "status": self.status,
            "reason": self.reason,
            "source_detail": (
                f"{self.source_type} {src.name or src.form}" if src else self.source_type
            ),
            "dest_ddl": (
                (dst.name or dst.form) if dst and self.status == "carried" else ""
            ),
        }


def is_string_catalog_type(data_type: str, udt_name: str = "") -> bool:
    """True for a character column, not JSON / BINARY / numeric."""
    from services.json_polarity import is_json_catalog_type

    if is_json_catalog_type(data_type, udt_name):
        return False
    collapsed = " ".join((data_type or "").split())
    if not collapsed:
        return False
    upper = collapsed.upper()
    if upper.startswith(("BINARY", "VARBINARY", "BYTEA", "BLOB", "RAW", "IMAGE")):
        return False
    return bool(_STRING_RE.match(collapsed))


def parse_declared_charset(type_str: str, charset: str = "") -> str:
    """Charset from an explicit argument, CHARACTER SET, or COLLATE prefix."""
    if (charset or "").strip():
        return charset.strip()
    text = type_str or ""
    m = _CHARSET_RE.search(text)
    if m:
        return m.group(1)
    c = _COLLATE_RE.search(text)
    if c:
        name = c.group(1).lower()
        # SQL Server 2019+ ``*_UTF8`` collations make CHAR/VARCHAR UTF-8. The
        # code-page prefix of the collation name (``Latin1_General_...``) is the
        # sort rule, not the storage encoding, so it must not win here.
        if name.endswith("_utf8"):
            return "utf8"
        if name.startswith("utf8mb4"):
            return "utf8mb4"
        if name.startswith("utf8mb3"):
            return "utf8mb3"
        if name.startswith("utf8_"):
            return "utf8"
        if name.startswith("latin1"):
            return "latin1"
        if name.startswith("ascii"):
            return "ascii"
        if name == "binary":
            return "binary"
    return ""


def classify_capacity(
    engine: str,
    type_str: str = "",
    charset: str = "",
) -> EncodingCapacity:
    """Physical encoding capacity. Name-copy is not capacity."""
    eng = _normalize_dest_db(engine)
    declared = parse_declared_charset(type_str, charset).strip()
    token = declared.lower().replace("-", "")
    upper_type = (type_str or "").upper()

    if token in {"binary"}:
        return EncodingCapacity(form="binary", name=declared or "binary")

    # SQL Server N-types are UTF-16 whatever the collation says. A column
    # declared ``NVARCHAR(64) COLLATE Latin1_General_100_BIN2`` holds CJK and
    # emoji fine; reading the collation prefix as the encoding classified it
    # latin1 and quarantined every non-Latin cell the engine writes happily.
    if eng in _SQLSERVER_FAMILY and any(
        tok in upper_type for tok in ("NVARCHAR", "NCHAR", "NTEXT")
    ):
        return EncodingCapacity(form="utf16", name="national")

    if token in {"utf8mb3"} or (token == "utf8" and eng in _MYSQL_FAMILY):
        return EncodingCapacity(
            form="utf8mb3",
            name=declared or "utf8mb3",
            max_code_point=BMP_MAX,
        )
    if token in {"utf8mb4", "utf8mb4bin"}:
        return EncodingCapacity(form="utf8", name=declared or "utf8mb4")
    if token in {"al32utf8"}:
        return EncodingCapacity(form="utf8", name="AL32UTF8")
    if token == "utf8" and eng in _ORACLE_FAMILY:
        return EncodingCapacity(form="cesu8", name="UTF8")
    if token in {"cesu8", "cesu-8"}:
        return EncodingCapacity(form="cesu8", name=declared or "CESU-8")
    if token in {"gb18030"}:
        return EncodingCapacity(form="gb18030", name="GB18030")
    if token in {"latin1", "iso88591", "iso8859p1", "we8iso8859p1"}:
        # MySQL latin1 is cp1252; ISO-8859-1 engines are latin-1.
        if eng in _MYSQL_FAMILY:
            return EncodingCapacity(
                form="cp1252",
                name="latin1",
                codec="cp1252",
                max_code_point=0xFF,
            )
        return EncodingCapacity(
            form="latin1",
            name=declared or "latin1",
            codec="latin-1",
            max_code_point=0xFF,
        )
    if token in {"cp1252", "windows1252", "we8mswin1252", "win1252"}:
        return EncodingCapacity(
            form="cp1252",
            name=declared or "cp1252",
            codec="cp1252",
            max_code_point=0xFF,
        )
    if token in {"ascii", "us7ascii", "usascii"}:
        return EncodingCapacity(
            form="ascii",
            name=declared or "ascii",
            codec="ascii",
            max_code_point=0x7F,
        )
    if token in {"utf16", "al16utf16", "utf16le", "utf16be"}:
        return EncodingCapacity(form="utf16", name=declared or "UTF-16")

    if any(tok in upper_type for tok in ("NVARCHAR", "NCHAR", "NTEXT", "NVARCHAR2", "NCLOB")):
        return EncodingCapacity(form="utf16", name="national")

    if token in {"utf8"} or token.startswith("utf8"):
        return EncodingCapacity(form="utf8", name=declared or "utf8")

    if declared:
        return EncodingCapacity(form="unknown", name=declared, max_code_point=0)

    # Engine defaults when the column did not declare a charset.
    if eng in _MYSQL_FAMILY:
        return EncodingCapacity(form="utf8", name="utf8mb4")
    if eng in _PG_FAMILY or eng in _UTF8_DESTS:
        return EncodingCapacity(form="utf8", name="utf8")
    if eng in _SQLSERVER_FAMILY:
        # VARCHAR without a collation is a code-page type, not Unicode.
        return EncodingCapacity(
            form="cp1252",
            name="varchar",
            codec="cp1252",
            max_code_point=0xFF,
        )
    if eng in _ORACLE_FAMILY:
        # VARCHAR2 charset is the database character set — unmeasured.
        return EncodingCapacity(form="unknown", name="VARCHAR2", max_code_point=0)
    return EncodingCapacity(form="unknown", name="", max_code_point=0)


def looks_like_cesu8(data: bytes) -> bool:
    """True when a 6-byte CESU-8 supplementary sequence is present (UTR #26)."""
    n = len(data)
    i = 0
    while i + 5 < n:
        if (
            data[i] == 0xED
            and 0xA0 <= data[i + 1] <= 0xAF
            and 0x80 <= data[i + 2] <= 0xBF
            and data[i + 3] == 0xED
            and 0xB0 <= data[i + 4] <= 0xBF
            and 0x80 <= data[i + 5] <= 0xBF
        ):
            return True
        i += 1
    return False


def decode_cesu8(data: bytes) -> str:
    """Decode CESU-8, recomposing supplementary surrogate pairs to scalars."""
    out: list[str] = []
    i = 0
    n = len(data)
    while i < n:
        b0 = data[i]
        if b0 < 0x80:
            out.append(chr(b0))
            i += 1
            continue
        if (
            b0 == 0xED
            and i + 5 < n
            and 0xA0 <= data[i + 1] <= 0xAF
            and 0x80 <= data[i + 2] <= 0xBF
            and data[i + 3] == 0xED
            and 0xB0 <= data[i + 4] <= 0xBF
            and 0x80 <= data[i + 5] <= 0xBF
        ):
            hs = _utf8_three_byte_cp(data[i], data[i + 1], data[i + 2])
            ls = _utf8_three_byte_cp(data[i + 3], data[i + 4], data[i + 5])
            out.append(chr(0x10000 + ((hs - 0xD800) << 10) + (ls - 0xDC00)))
            i += 6
            continue
        ch, size = _one_utf8_char(data, i)
        out.append(ch)
        i += size
    return "".join(out)


def decode_source_bytes(data: bytes) -> str:
    """UTF-8, or CESU-8 when the 6-byte supplementary form is present.

    Never latin-1. Ill-formed bytes raise — replacement invent is silent loss.
    """
    if looks_like_cesu8(data):
        return decode_cesu8(data)
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "bytes are not well-formed UTF-8 — refuse latin-1 / U+FFFD invent"
        ) from exc


def compose_unicode_scalars(text: str) -> str:
    """Recompose UTF-16 surrogate pairs leaked into a Python str (CESU-8 / JDBC).

    Unpaired surrogates raise. U+FFFD invent would hide the defect.
    """
    # A str carrying no surrogate code unit is returned unchanged by the scan
    # below, so the scan itself is skipped. The per-character loop ran on every
    # cell of every row and dominated the write path; the surrogate range is
    # decided here in one C-level scan.
    if not _SURROGATE_RE.search(text):
        return text
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        cp = ord(text[i])
        if 0xD800 <= cp <= 0xDBFF:
            if i + 1 < n:
                low = ord(text[i + 1])
                if 0xDC00 <= low <= 0xDFFF:
                    out.append(chr(0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00)))
                    i += 2
                    continue
            raise ValueError(
                f"unpaired high surrogate U+{cp:04X} — refuse U+FFFD invent"
            )
        if 0xDC00 <= cp <= 0xDFFF:
            raise ValueError(
                f"unpaired low surrogate U+{cp:04X} — refuse U+FFFD invent"
            )
        out.append(text[i])
        i += 1
    return "".join(out)


def cell_encoding(value: Any) -> CellEncoding | None:
    """Unicode scalars in a cell, or None for SQL NULL / missing."""
    if value is None:
        return None
    if is_missing_sentinel(value):
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        text = decode_source_bytes(bytes(value))
    elif isinstance(value, str):
        text = compose_unicode_scalars(value)
    else:
        text = compose_unicode_scalars(cell_to_string(value))
    # ``max`` over the str compares by code point already — the generator form
    # allocated one int per character of every cell.
    max_cp = ord(max(text)) if text else 0
    return CellEncoding(
        text=text,
        max_code_point=max_cp,
        prior_loss=REPLACEMENT in text,
        supplementary=max_cp > BMP_MAX,
    )


def cell_fits_capacity(text: str, cap: EncodingCapacity) -> bool:
    """True when every scalar in ``text`` is encodable on dest."""
    if cap.form == "binary":
        return True
    if cap.form == "unknown":
        return False
    # Every destination encoding this product models is an ASCII superset, so an
    # ASCII cell needs no per-scalar scan (Latin-1/UTF-8/UTF-16/utf8mb3 alike).
    ascii_only = text.isascii()
    if cap.form in {"utf8mb3"}:
        return ascii_only or all(
            ord(c) <= BMP_MAX and not (0xD800 <= ord(c) <= 0xDFFF) for c in text
        )
    if ascii_only and cap.max_code_point >= 0x7F and not cap.codec:
        return True
    if cap.codec:
        try:
            text.encode(cap.codec)
            return True
        except UnicodeEncodeError:
            return False
    return all(
        ord(c) <= cap.max_code_point and not (0xD800 <= ord(c) <= 0xDFFF)
        for c in text
    )


def bind_unicode_text(
    value: Any,
    *,
    engine: str,
    dest_type: str,
    dest_charset: str = "",
) -> Any:
    """Writer bind for a character carrier.

    Recompose CESU-8 / surrogates first. Dest that cannot encode a scalar
    raises — the write matrix quarantines; we never substitute ``?``.
    """
    if value is None:
        return None
    if is_missing_sentinel(value):
        return value
    if not isinstance(value, (str, bytes, bytearray, memoryview)):
        return value
    cell = cell_encoding(value)
    if cell is None:
        return None
    cap = classify_capacity(engine, dest_type, dest_charset)
    if cap.form == "binary":
        return value
    if cap.form != "unknown" and not cell_fits_capacity(cell.text, cap):
        shown = f"U+{cell.max_code_point:04X}"
        raise ValueError(
            f"{shown} exceeds destination {cap.name or cap.form} capacity "
            "— refuse replacement-character invent"
        )
    return cell.text


def decide_encoding(
    *,
    source_engine: str,
    source_type: str,
    dest_engine: str,
    dest_type: str,
    source_charset: str = "",
    dest_charset: str = "",
    source_column: str = "",
    dest_column: str = "",
) -> EncodingDecision | None:
    """Carry / unsupported / skipped for one mapped character column.

    ``None`` means the source is not a character column (no encoding question).
    """
    if not is_string_catalog_type(source_type) and not is_string_catalog_type(dest_type):
        if not source_type:
            return None
        if not is_string_catalog_type(source_type):
            return None
    if not is_string_catalog_type(source_type):
        return None
    src = classify_capacity(source_engine, source_type, source_charset)
    dst = classify_capacity(dest_engine, dest_type, dest_charset)
    col = dest_column or source_column
    if src.form == "binary" or dst.form == "binary":
        return EncodingDecision(
            source_column=source_column,
            dest_column=col,
            status="skipped",
            reason="Column is a binary carrier — encoding of Unicode scalars does not apply.",
            source_capacity=src,
            dest_capacity=dst,
            source_type=source_type,
            dest_type=dest_type,
        )
    if dst.form == "unknown":
        return EncodingDecision(
            source_column=source_column,
            dest_column=col,
            status="unsupported",
            reason=(
                f"Destination {dest_engine or 'engine'} {dest_type or 'string'} "
                "character set was not measured; refuse to claim Unicode scalars "
                "will land."
            ),
            source_capacity=src,
            dest_capacity=dst,
            source_type=source_type,
            dest_type=dest_type,
        )
    if dst.rank >= src.rank and src.form != "unknown":
        convert = ""
        if src.form == "cesu8" and dst.form == "utf8":
            convert = (
                " CESU-8 supplementary pairs are recomposed to Unicode scalars "
                "before UTF-8 bind."
            )
        return EncodingDecision(
            source_column=source_column,
            dest_column=col,
            status="carried",
            reason=(
                f"Destination {dst.name or dst.form} can store every Unicode "
                f"scalar the source {src.name or src.form} can."
                + convert
            ),
            source_capacity=src,
            dest_capacity=dst,
            source_type=source_type,
            dest_type=dest_type,
        )
    if src.form == "unknown" and dst.rank >= 40:
        return EncodingDecision(
            source_column=source_column,
            dest_column=col,
            status="carried",
            reason=(
                f"Source character set was unmeasured; destination "
                f"{dst.name or dst.form} is a full-Unicode carrier so scalars "
                "that exist will land. Unencodable source bytes still quarantine."
            ),
            source_capacity=src,
            dest_capacity=dst,
            source_type=source_type,
            dest_type=dest_type,
        )
    return EncodingDecision(
        source_column=source_column,
        dest_column=col,
        status="unsupported",
        reason=(
            f"Source {src.name or src.form} can hold code points the destination "
            f"{dest_engine} {dst.name or dest_type or dst.form} cannot. "
            "Those cells quarantine; we do not substitute '?'."
        ),
        source_capacity=src,
        dest_capacity=dst,
        source_type=source_type,
        dest_type=dest_type,
    )


def plan_encoding_carry(
    *,
    catalog: Any,
    dest_dialect: str,
    dest_name_for_source: Any,
    dest_type_for_column: Any,
    dest_charset_for_column: Any = None,
) -> list[EncodingDecision]:
    """One decision per mapped character source column."""
    types = dict(getattr(catalog, "column_types", None) or {})
    charsets = dict(getattr(catalog, "charsets", None) or {})
    source_engine = str(getattr(catalog, "dialect", "") or "")
    decisions: list[EncodingDecision] = []
    if not types:
        return decisions
    for src_col, src_type in types.items():
        dest_col = dest_name_for_source(src_col) if dest_name_for_source else src_col
        if not dest_col:
            continue
        dest_type = dest_type_for_column(dest_col) if dest_type_for_column else ""
        dest_cs = ""
        if dest_charset_for_column:
            dest_cs = dest_charset_for_column(dest_col) or ""
        decision = decide_encoding(
            source_engine=source_engine,
            source_type=str(src_type or ""),
            dest_engine=dest_dialect,
            dest_type=str(dest_type or ""),
            source_charset=str(charsets.get(src_col) or ""),
            dest_charset=str(dest_cs or ""),
            source_column=str(src_col),
            dest_column=str(dest_col),
        )
        if decision is not None:
            decisions.append(decision)
    return decisions


def quarantine_unfit_encoding(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
    *,
    dest_db: str = "",
) -> list[tuple]:
    """Hold out cells whose scalars dest cannot encode. Compose CESU-8 leaks.

    Width (VARCHAR n) is a different axis — ``quarantine_unfit_strings``.
    Unlimited TEXT still cannot store supplementary characters on utf8mb3.
    """
    from connectors.writer_common import append_write_quarantine_detail

    caps: list[tuple[int, EncodingCapacity]] = []
    for i, typ in enumerate(target_types):
        if not is_string_catalog_type(str(typ or "")):
            # Bare logical "string" / "text" from Map still needs the check.
            logical = str(typ or "").strip().lower()
            if logical not in {"string", "text", "varchar", "nvarchar", "clob"}:
                continue
        cap = classify_capacity(dest_db, str(typ or ""))
        if cap.form in {"binary", "unknown"} and cap.form == "binary":
            continue
        if cap.form == "unknown":
            continue
        if cap.rank >= 40 and cap.form in {"utf8", "utf16", "cesu8", "gb18030"}:
            # Full Unicode dest: still compose surrogates; nothing to hold out
            # for capacity. We still rewrite composed text below.
            caps.append((i, cap))
            continue
        caps.append((i, cap))
    if not caps:
        return mapped_rows

    out: list[tuple] = []
    for row_idx, row in enumerate(mapped_rows):
        cells = list(row)
        hold_out = False
        for col_idx, cap in caps:
            if col_idx >= len(cells) or cells[col_idx] is None:
                continue
            cell_value = cells[col_idx]
            # ASCII text has no surrogate pair to compose and no scalar above
            # U+007F, so every capacity above ASCII carries it unchanged. This
            # is the common case for identifier/code columns and skips building
            # a CellEncoding per cell.
            if (
                type(cell_value) is str
                and cap.max_code_point >= 0x7F
                and cell_value.isascii()
            ):
                continue
            if is_missing_sentinel(cells[col_idx]):
                continue
            try:
                cell = cell_encoding(cells[col_idx])
            except ValueError as exc:
                sample = cell_to_string(cells[col_idx])[:120]
                append_write_quarantine_detail(
                    rejected_details,
                    {
                        "row": row_idx + 1,
                        "column": target_cols[col_idx],
                        "target": target_cols[col_idx],
                        "value": sample,
                        "reason": (
                            f"{exc} — quarantined (would invent U+FFFD or "
                            "reject on write)"
                        ),
                        "policy": "write_quarantine",
                        "chars": [],
                    },
                    mapped_row=cells,
                    target_cols=target_cols,
                )
                if policy == "coerce_null":
                    cells[col_idx] = DF_MISSING_SENTINEL
                else:
                    hold_out = True
                    break
                continue
            if cell is None:
                continue
            if not cell_fits_capacity(cell.text, cap):
                sample = cell.text[:120]
                append_write_quarantine_detail(
                    rejected_details,
                    {
                        "row": row_idx + 1,
                        "column": target_cols[col_idx],
                        "target": target_cols[col_idx],
                        "value": sample,
                        "reason": (
                            f"U+{cell.max_code_point:04X} exceeds destination "
                            f"{cap.name or cap.form} capacity — quarantined "
                            "(refuse replacement-character invent)"
                        ),
                        "policy": "write_quarantine",
                        "chars": [],
                    },
                    mapped_row=cells,
                    target_cols=target_cols,
                )
                if policy == "coerce_null":
                    cells[col_idx] = DF_MISSING_SENTINEL
                else:
                    hold_out = True
                    break
                continue
            cells[col_idx] = cell.text
        if hold_out:
            continue
        out.append(tuple(cells))
    return out


def dest_utf8_octet_length_sql(engine: str, column_sql: str) -> str:
    """Dest-engine UTF-8 byte length. Supplementary plane is 4, CESU-8 would be 6."""
    eng = _normalize_dest_db(engine)
    if eng in _MYSQL_FAMILY:
        return f"OCTET_LENGTH({column_sql})"
    if eng in _SQLSERVER_FAMILY:
        return f"DATALENGTH(CONVERT(VARCHAR(MAX), {column_sql}))"
    return f"octet_length({column_sql})"


def dest_utf8_hex_sql(engine: str, column_sql: str) -> str:
    """Dest-engine hex of the stored bytes (UTF-8 on PG/MySQL utf8mb4)."""
    eng = _normalize_dest_db(engine)
    if eng in _MYSQL_FAMILY:
        return f"HEX({column_sql})"
    if eng in _SQLSERVER_FAMILY:
        return f"CONVERT(VARCHAR(MAX), CONVERT(VARBINARY(MAX), {column_sql}), 2)"
    return f"encode(convert_to({column_sql}, 'UTF8'), 'hex')"


def _utf8_three_byte_cp(b0: int, b1: int, b2: int) -> int:
    return ((b0 & 0x0F) << 12) | ((b1 & 0x3F) << 6) | (b2 & 0x3F)


def _one_utf8_char(data: bytes, i: int) -> tuple[str, int]:
    try:
        # Decode the shortest well-formed UTF-8 sequence at i.
        for size in (1, 2, 3, 4):
            if i + size > len(data):
                break
            try:
                ch = data[i : i + size].decode("utf-8")
            except UnicodeDecodeError:
                continue
            if len(ch) == 1:
                return ch, size
        raise UnicodeDecodeError("utf-8", data, i, i + 1, "invalid")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "bytes are not well-formed UTF-8 — refuse latin-1 / U+FFFD invent"
        ) from exc
