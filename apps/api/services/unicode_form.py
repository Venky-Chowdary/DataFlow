"""Unicode form identity is NFC vs NFD (UAX #15) plus UCA version, not CS/CI.

Collation equality (``collation_carry``) is case / accent polarity. That is
not canonical class. AWS DMS copies bytes into the destination default
collation; a checksum of *accepted* rows stays green while UNIQUE silently
refuses the second of a canonically-equivalent pair (DMS ``MISSING_TARGET``).
MySQL ``utf8mb4_general_ci`` treats NFC ``café`` and NFD ``café`` as
distinct keys. MariaDB ``utf8mb4_unicode_ci`` (UCA 4.0) equates them — and
equates ``ß`` with ``ss``. MySQL's own docs say ``unicode_ci`` combining-mark
support is incomplete, so NFC may stay distinct there. Protocol ``mysql`` is
not an engine: this host's MariaDB speaks it. We never claim MySQL
``unicode_ci`` equals MariaDB ``unicode_ci`` without a dest-engine
measurement.

UCA 4.0 (``unicode_ci``), 5.2 (``unicode_520``), 9.0 (``0900``), and 14.0
(``uca1400``) are different weight tables. Competitors paste the source
collation *name* when the dest happens to know it. Name-copy is not
canonical class — ``utf8mb4_unicode_ci`` is not ``utf8mb4_0900_ai_ci``.

This module is the form / UCA half (bind still does not NFC):

1. ``classify_form`` — NFC / NFD / already-composed-equals-decomposed /
   mixed, via ``unicodedata.normalize``. The cell is not rewritten.
2. ``classify_uca`` — weight table (codepoint / general / UCA / unknown),
   UCA version, expansions (``ß=ss``), canonical equivalence (NFC=NFD).
   ``unicode_ci`` canonical equivalence without a MariaDB engine is unknown.
3. Fidelity aspect ``unicode_form``: source codepoint/general → dest UCA is
   ``unsupported`` (UNIQUE / JOIN would collapse forms the source
   distinguished). Same UCA version on the same engine family is ``carried``.
   4.0 MySQL vs 4.0 MariaDB is ``unsupported``. 0900 vs 1400 is
   ``unsupported``. We do not silently NFC, and we do not invent a companion
   composed-form column.
4. Certify from the dest engine: UTF-8 HEX of stored NFC vs NFD
   (``C3A9`` vs ``CC81``), PostgreSQL ``normalize(col, NFC)``, UNIQUE second
   insert BOTH_LAND vs SECOND_REJECT.

Encoding capacity (utf8mb4 vs utf8mb3) and collation CS/CI are independent
axes. ``normalize_unicode`` on Map is an explicit NFKC transform — not this
path.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Literal

from services.encoding_capacity import dest_utf8_hex_sql, is_string_catalog_type
from services.type_system import parse_collation

Status = Literal["carried", "unsupported", "skipped"]
FormKind = Literal["nfc", "nfd", "identity", "mixed"]
WeightTable = Literal["codepoint", "general", "uca", "unknown"]
FormUniqueness = Literal["preserved", "collapsed", "split", "n/a"]
UniqueSecond = Literal["BOTH_LAND", "SECOND_REJECT", "FIRST_REJECT"]

# UAX #15 probes. HEX is dest-engine proof, not Python encode after a rewrite.
NFC_CAFE = "caf\u00e9"
NFD_CAFE = "cafe\u0301"
NFC_CAFE_UTF8_HEX = "636166C3A9"
NFD_CAFE_UTF8_HEX = "63616665CC81"
SHARP_S = "\u00df"
SS_EXPANSION = "ss"

_BIN_RE = re.compile(r"(?:^|_)BIN(?:ARY)?(?:_|$)|^(?:C|POSIX|BINARY)$", re.I)
_UCA_1400_RE = re.compile(r"(?:uca)?1400", re.I)
_UCA_0900_RE = re.compile(r"(?:^|_)0900(?:_|$)", re.I)
_UCA_520_RE = re.compile(r"unicode_520|(?:^|_)520(?:_|$)", re.I)
_GENERAL_RE = re.compile(r"general_(?:ci|cs)\b", re.I)
_UNICODE_CI_RE = re.compile(r"unicode_(?:ci|cs)\b", re.I)

__all__ = [
    "NFC_CAFE",
    "NFD_CAFE",
    "NFC_CAFE_UTF8_HEX",
    "NFD_CAFE_UTF8_HEX",
    "SHARP_S",
    "SS_EXPANSION",
    "UcaProfile",
    "UnicodeFormDecision",
    "classify_form",
    "classify_uca",
    "decide_unicode_form",
    "plan_unicode_form_carry",
    "utf8_form_hex",
    "dest_is_nfc_sql",
    "dest_utf8_hex_sql",
    "unique_second_outcome",
]


@dataclass(frozen=True)
class UcaProfile:
    """How a collation compares canonically equivalent strings and expansions."""

    table: WeightTable
    version: str | None = None
    expansions: bool | None = None
    canonical_equivalence: bool | None = None
    engine: str = ""
    collation: str = ""

    @property
    def folds_forms(self) -> bool | None:
        """True when UNIQUE/JOIN may equate NFC with NFD or ß with ss."""
        if self.table in {"codepoint", "general"}:
            return False
        if self.table == "uca":
            if self.expansions is True or self.canonical_equivalence is True:
                return True
            if self.expansions is False and self.canonical_equivalence is False:
                return False
            return True
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "version": self.version,
            "expansions": self.expansions,
            "canonical_equivalence": self.canonical_equivalence,
            "engine": self.engine,
            "collation": self.collation,
            "folds_forms": self.folds_forms,
        }


@dataclass
class UnicodeFormDecision:
    source_column: str
    dest_column: str
    status: Status
    reason: str
    source_profile: UcaProfile | None = None
    dest_profile: UcaProfile | None = None
    uniqueness: FormUniqueness = "n/a"

    def to_item_kwargs(self) -> dict[str, Any]:
        src = self.source_profile
        dst = self.dest_profile
        return {
            "aspect": "unicode_form",
            "name": self.dest_column or self.source_column,
            "status": self.status,
            "reason": self.reason,
            "source_detail": (
                f"{src.table}:{src.version or '-'} {src.collation or src.engine}"
                if src
                else self.source_column
            ),
            "dest_ddl": (
                (dst.collation or dst.table) if dst and self.status == "carried" else ""
            ),
        }


def utf8_form_hex(text: str) -> str:
    """UTF-8 HEX of the cell as stored / bound, never after NFC rewrite."""
    return (text or "").encode("utf-8").hex().upper()


def classify_form(text: str) -> FormKind:
    """UAX #15 form of a cell. Does not mutate the cell.

    ``identity`` means NFC and NFD are the same spelling (ASCII, or no
    combining involvement). ``mixed`` is a string that is neither fully
    composed nor fully decomposed. Compatibility (NFKC) is out of scope —
    that is the explicit Map transform ``normalize_unicode``.
    """
    if text is None:
        raise TypeError("classify_form refuses None — SQL NULL is not a form")
    nfc = unicodedata.normalize("NFC", text)
    nfd = unicodedata.normalize("NFD", text)
    if text == nfc == nfd:
        return "identity"
    if text == nfc and text != nfd:
        return "nfc"
    if text == nfd and text != nfc:
        return "nfd"
    return "mixed"


def _norm_engine(engine: str | None) -> str:
    """Keep mysql vs mariadb distinct — UCA 4.0 NFC fold is not the same."""
    d = (engine or "").strip().lower()
    if d in {
        "postgres",
        "pg",
        "postgresql",
        "cockroachdb",
        "cockroach",
        "timescaledb",
        "timescale",
        "alloydb",
        "yugabyte",
        "citus",
        "greenplum",
        "neon",
        "aurora_postgres",
        "aurora-postgresql",
    }:
        return "postgresql"
    if d == "redshift":
        return "redshift"
    if d in {"mssql", "azure_sql", "azure_sql_database", "sqlserver", "synapse"}:
        return "sqlserver"
    if d in {"mariadb", "maria"}:
        return "mariadb"
    if d in {"tidb", "tidb_cloud", "aurora_mysql", "aurora-mysql", "percona", "mysql2"}:
        return "mysql"
    if d == "mysql":
        return "mysql"
    if d in {"sqlite", "libsql", "turso"}:
        return "sqlite"
    if d in {"oracle", "oracledb"}:
        return "oracle"
    if d in {"duckdb", "motherduck"}:
        return "duckdb"
    return d


def _clean_collation(name: str) -> str:
    text = (name or "").strip().strip('"').strip("`").strip("'")
    if "." in text and not text.lower().startswith("utf"):
        text = text.rsplit(".", 1)[-1]
    return text


def _engine_default(engine: str) -> UcaProfile:
    """Default collation class when the catalog has no name.

    MySQL 8 default is often ``utf8mb4_0900_ai_ci``; 5.7 / MariaDB 10.x is
    often ``utf8mb4_general_ci``. Protocol ``mysql`` is not enough to pick.
    PostgreSQL / SQLite / Oracle BINARY default is code-point identity.
    """
    if engine in {"postgresql", "sqlite", "oracle", "duckdb", "redshift"}:
        return UcaProfile(
            table="codepoint",
            expansions=False,
            canonical_equivalence=False,
            engine=engine,
        )
    if engine == "mariadb":
        return UcaProfile(
            table="general",
            expansions=False,
            canonical_equivalence=False,
            engine=engine,
            collation="",
        )
    return UcaProfile(table="unknown", engine=engine)


def _uca_flags(engine: str, version: str | None) -> tuple[bool | None, bool | None]:
    """(expansions, canonical_equivalence) for a UCA weight table.

    Expansions (ß=ss) are true for unicode_ci / 520 / 0900 / 1400 on both
    MySQL and MariaDB. Canonical equivalence of combining marks is not:
    MariaDB 10.11 ``unicode_ci`` equated NFC/NFD here; MySQL docs say
    ``unicode_ci`` combining-mark support is incomplete. Dialect ``mysql``
    against MariaDB would lie if we stamped False. Leave 4.0 unknown unless
    the engine is actually MariaDB.
    """
    if version in {"5.2", "9.0", "14.0"}:
        return True, True
    if version == "4.0":
        if engine == "mariadb":
            return True, True
        return True, None
    return True, None


def classify_uca(engine: str, collation: str = "") -> UcaProfile:
    """Weight table + UCA version from a catalog collation name.

    Binary / ``C`` / ``POSIX`` / PG default: code-point identity (NFC ≠ NFD
    as keys). ``general_ci`` is MySQL's own latin-centric table — not UCA —
    and does not fold NFC/NFD or ß/ss (measured MariaDB 10.11). UCA names
    are classified by version token, never by hoping the dest knows the
    source spelling.
    """
    eng = _norm_engine(engine)
    name = _clean_collation(collation)
    if not name:
        return _engine_default(eng)

    if _BIN_RE.search(name) and "unicode" not in name.lower():
        return UcaProfile(
            table="codepoint",
            expansions=False,
            canonical_equivalence=False,
            engine=eng,
            collation=name,
        )
    if _GENERAL_RE.search(name):
        return UcaProfile(
            table="general",
            expansions=False,
            canonical_equivalence=False,
            engine=eng,
            collation=name,
        )
    if _UCA_1400_RE.search(name):
        exp, canon = _uca_flags(eng, "14.0")
        return UcaProfile(
            table="uca",
            version="14.0",
            expansions=exp,
            canonical_equivalence=canon,
            engine=eng,
            collation=name,
        )
    if _UCA_0900_RE.search(name):
        exp, canon = _uca_flags(eng, "9.0")
        return UcaProfile(
            table="uca",
            version="9.0",
            expansions=exp,
            canonical_equivalence=canon,
            engine=eng,
            collation=name,
        )
    if _UCA_520_RE.search(name):
        exp, canon = _uca_flags(eng, "5.2")
        return UcaProfile(
            table="uca",
            version="5.2",
            expansions=exp,
            canonical_equivalence=canon,
            engine=eng,
            collation=name,
        )
    if _UNICODE_CI_RE.search(name):
        exp, canon = _uca_flags(eng, "4.0")
        return UcaProfile(
            table="uca",
            version="4.0",
            expansions=exp,
            canonical_equivalence=canon,
            engine=eng,
            collation=name,
        )
    if "icu" in name.lower():
        return UcaProfile(
            table="uca",
            version=None,
            expansions=None,
            canonical_equivalence=None,
            engine=eng,
            collation=name,
        )
    return UcaProfile(
        table="unknown",
        engine=eng,
        collation=name,
        expansions=None,
        canonical_equivalence=None,
    )


def _form_uniqueness(src: UcaProfile, dst: UcaProfile) -> FormUniqueness:
    src_fold = src.folds_forms
    dst_fold = dst.folds_forms
    if src_fold is None or dst_fold is None:
        return "n/a"
    if src_fold == dst_fold:
        return "preserved"
    if (not src_fold) and dst_fold:
        return "collapsed"
    return "split"


def decide_unicode_form(
    *,
    source_engine: str,
    source_collation: str,
    dest_engine: str,
    dest_collation: str,
    source_column: str,
    dest_column: str,
    source_type: str = "",
    dest_type: str = "",
) -> UnicodeFormDecision | None:
    """One column: did dest canonical class preserve source form uniqueness?"""
    logical = (source_type or dest_type or "").strip().lower()
    is_string = (
        is_string_catalog_type(source_type)
        or is_string_catalog_type(dest_type)
        or logical in {"string", "text", "varchar", "nvarchar", "clob", "nclob"}
        or bool(source_collation)
        or bool(dest_collation)
    )
    if not is_string:
        return None
    src = classify_uca(source_engine, source_collation)
    dst = classify_uca(dest_engine, dest_collation)
    polarity = _form_uniqueness(src, dst)

    if src.table == "unknown" or dst.table == "unknown":
        return UnicodeFormDecision(
            source_column=source_column,
            dest_column=dest_column,
            status="unsupported",
            reason=(
                "Canonical class is unmeasured for "
                f"source {src.engine or source_engine} {src.collation or '(default)'} "
                f"→ dest {dst.engine or dest_engine} {dst.collation or '(default)'}. "
                "Refusing to claim NFC/NFD uniqueness was carried."
            ),
            source_profile=src,
            dest_profile=dst,
            uniqueness=polarity,
        )

    if src.table == "uca" and dst.table == "uca":
        if src.version and dst.version and src.version != dst.version:
            return UnicodeFormDecision(
                source_column=source_column,
                dest_column=dest_column,
                status="unsupported",
                reason=(
                    f"UCA {src.version} ({src.collation or 'source'}) is not "
                    f"UCA {dst.version} ({dst.collation or 'dest'}). Weight "
                    "tables differ; utf8mb4_unicode_ci is not utf8mb4_0900_ai_ci."
                ),
                source_profile=src,
                dest_profile=dst,
                uniqueness=polarity,
            )
        if src.version is None or dst.version is None:
            return UnicodeFormDecision(
                source_column=source_column,
                dest_column=dest_column,
                status="unsupported",
                reason=(
                    "UCA version is unknown on one side (ICU name without a "
                    "version token). Refusing to equate weight tables."
                ),
                source_profile=src,
                dest_profile=dst,
                uniqueness=polarity,
            )
        # Same version: MySQL 4.0 combining marks ≠ MariaDB 4.0 (measured).
        if src.engine != dst.engine and src.version == "4.0":
            return UnicodeFormDecision(
                source_column=source_column,
                dest_column=dest_column,
                status="unsupported",
                reason=(
                    "UCA 4.0 unicode_ci canonical equivalence is engine-specific: "
                    "MariaDB 10.11 equated NFC/NFD here; MySQL documents incomplete "
                    "combining-mark support. Protocol mysql is not dest-engine proof."
                ),
                source_profile=src,
                dest_profile=dst,
                uniqueness=polarity,
            )
        if (
            src.canonical_equivalence is None or dst.canonical_equivalence is None
        ) and src.engine != dst.engine:
            return UnicodeFormDecision(
                source_column=source_column,
                dest_column=dest_column,
                status="unsupported",
                reason=(
                    "Canonical equivalence of UCA "
                    f"{src.version} is unmeasured across {src.engine} → {dst.engine}."
                ),
                source_profile=src,
                dest_profile=dst,
                uniqueness=polarity,
            )
        return UnicodeFormDecision(
            source_column=source_column,
            dest_column=dest_column,
            status="carried",
            reason=(
                f"UCA {src.version} form uniqueness preserved as "
                f"{dst.collation or dst.table} on {dst.engine}."
            ),
            source_profile=src,
            dest_profile=dst,
            uniqueness="preserved",
        )

    if polarity == "collapsed":
        return UnicodeFormDecision(
            source_column=source_column,
            dest_column=dest_column,
            status="unsupported",
            reason=(
                f"Source {src.table}:{src.version or '-'} treats NFC café and "
                f"NFD café (and ß vs ss) as distinct keys; dest "
                f"{dst.table}:{dst.version or '-'} {dst.collation or dst.engine} "
                "UCA-folds them (UNIQUE second insert SECOND_REJECT / JOIN "
                "collapse). That is the DMS MISSING_TARGET class for canonical "
                "equivalents. Bind does not NFC to hide it."
            ),
            source_profile=src,
            dest_profile=dst,
            uniqueness=polarity,
        )

    if polarity == "split":
        return UnicodeFormDecision(
            source_column=source_column,
            dest_column=dest_column,
            status="unsupported",
            reason=(
                f"Source {src.table}:{src.version or '-'} UCA-folded canonical "
                f"equivalents; dest {dst.table} is code-point identity. UNIQUE "
                "meaning widened — not the same form-uniqueness rule."
            ),
            source_profile=src,
            dest_profile=dst,
            uniqueness=polarity,
        )

    # Both codepoint, both general, or codepoint↔general (neither UCA-folds).
    return UnicodeFormDecision(
        source_column=source_column,
        dest_column=dest_column,
        status="carried",
        reason=(
            f"Canonical class {src.table} → {dst.table} "
            f"({dst.collation or 'destination default'}) preserves NFC ≠ NFD "
            "as distinct stored keys."
        ),
        source_profile=src,
        dest_profile=dst,
        uniqueness="preserved",
    )


def plan_unicode_form_carry(
    *,
    catalog: Any,
    dest_dialect: str,
    dest_name_for_source: Any,
    dest_type_for_column: Any,
    dest_collation_for_column: Any = None,
    unique_or_pk: set[str] | None = None,
) -> list[UnicodeFormDecision]:
    """One decision per mapped character source column."""
    types = dict(getattr(catalog, "column_types", None) or {})
    collations = dict(getattr(catalog, "collations", None) or {})
    source_engine = str(getattr(catalog, "dialect", "") or "")
    decisions: list[UnicodeFormDecision] = []
    columns = list(getattr(catalog, "columns", None) or [])
    if not columns:
        return decisions
    keyed = {str(c) for c in (unique_or_pk or [])}
    for src_col in columns:
        src_type = str(types.get(src_col) or "")
        dest_col = dest_name_for_source(src_col) if dest_name_for_source else src_col
        dest_type = ""
        if dest_col and dest_type_for_column:
            dest_type = str(dest_type_for_column(dest_col) or "")
        dest_collation = ""
        if dest_col and dest_collation_for_column:
            dest_collation = str(dest_collation_for_column(dest_col) or "")
        if not dest_collation:
            dest_collation = (parse_collation(dest_type) or "").strip().strip('"').strip("`")
        src_collation = str(collations.get(src_col) or "")
        if not src_collation:
            src_collation = (parse_collation(src_type) or "") or ""
        decision = decide_unicode_form(
            source_engine=source_engine,
            source_collation=src_collation,
            dest_engine=dest_dialect,
            dest_collation=dest_collation,
            source_column=str(src_col),
            dest_column=str(dest_col or ""),
            source_type=src_type,
            dest_type=dest_type,
        )
        if decision is None:
            continue
        if dest_col in keyed or src_col in keyed:
            if decision.uniqueness == "n/a" and decision.source_profile and decision.dest_profile:
                decision.uniqueness = _form_uniqueness(
                    decision.source_profile, decision.dest_profile
                )
        decisions.append(decision)
    return decisions


def dest_is_nfc_sql(engine: str, column_sql: str) -> str | None:
    """PostgreSQL dest-engine predicate: stored text is NFC.

    ``NFC`` is a keyword (PG 13+ ``normalize`` form enum). Quoting it as an
    identifier folds to column ``nfc`` and fails. MySQL/MariaDB have no
    ``normalize()`` — certify those with HEX of known NFC vs NFD probes.
    """
    eng = _norm_engine(engine)
    if eng in {"postgresql", "redshift"}:
        return f"normalize({column_sql}, NFC) IS NOT DISTINCT FROM {column_sql}"
    return None


def unique_second_outcome(*, first_ok: bool, second_ok: bool) -> UniqueSecond:
    """Classify a UNIQUE probe of two canonically-related spellings."""
    if not first_ok:
        return "FIRST_REJECT"
    if second_ok:
        return "BOTH_LAND"
    return "SECOND_REJECT"
