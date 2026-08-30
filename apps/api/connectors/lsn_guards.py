"""CDC log-sequence-number (LSN) guard SSOT for destination writers.

Split out of ``connectors.writer_common`` (a god module over its size budget).
Everything here answers one question: *is this change newer than what the
destination already holds?* — the monotonic-apply guard that keeps an
at-least-once CDC stream idempotent (replayed or out-of-order events must never
resurrect an older row version).

Covers the stamp families DataFlow ingests (Postgres WAL, MySQL binlog file:pos
and GTID sets, Oracle SCN, Mongo resume tokens, SQL Server LSN, numeric
versions) plus the per-dialect SQL predicates that push the same comparison
into the destination MERGE/UPDATE.

``writer_common`` re-exports these names, so imports of it are deferred inside
the functions to avoid a cycle.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable

#: How one dialect spells "this expression matches this regex".
LsnMatcher = Callable[[str, str], str]

# Destination metadata column for CDC monotonic apply (PK + LSN guard).
DF_LSN_COL = "_df_lsn"

def lsn_family(lsn: Any) -> str:
    """Return CDC stamp family for LSN-guard compares (Debezium-class).

    Families: ``empty``, ``pg_wal``, ``mysql_binlog``, ``mysql_gtid``,
    ``oracle_scn``, ``mongo_resume``, ``mssql_lsn``, ``numeric_version``,
    ``opaque``. Cross-family compares are incomparable — never invent
    ``newer`` across dialects (silent regression). Oracle SCN must not share
    ``numeric_version`` with SQL Server CT (bare integers collide).
    """
    if lsn is None:
        return "empty"
    text = str(lsn).strip()
    if not text:
        return "empty"
    lower = text.lower()
    if lower.startswith("gtid:"):
        return "mysql_gtid"
    if lower.startswith("scn:"):
        return "oracle_scn"
    if lower.startswith("mongo:"):
        return "mongo_resume"
    # Postgres WAL LSN: hex/hex
    if "/" in text:
        hi, _, lo = text.partition("/")
        if hi and lo and all(c in "0123456789abcdefABCDEF" for c in hi + lo):
            return "pg_wal"
    # MySQL binlog file:pos
    if ":" in text:
        file_name, _, pos = text.rpartition(":")
        if file_name and pos.isdigit():
            return "mysql_binlog"
    # SQL Server binary LSN hex (0x… or long hex)
    if lower.startswith("0x") and all(c in "0123456789abcdef" for c in lower[2:]):
        return "mssql_lsn"
    if len(text) >= 10 and all(c in "0123456789abcdefABCDEF" for c in text) and not text.isdigit():
        return "mssql_lsn"
    if text.isdigit():
        return "numeric_version"
    return "opaque"


def lsn_sort_key(lsn: Any) -> tuple:
    """Return a sortable key for PG ``hi/lo``, MySQL ``file:pos``, versions, or opaque tokens.

    Kind order is only valid **within** the same ``lsn_family``. Use
    ``compare_lsn`` for guards — it refuses cross-family invent.
    """
    if lsn is None:
        return (0, -1, -1, "")
    text = str(lsn).strip()
    if not text:
        return (0, -1, -1, "")
    lower = text.lower()
    if lower.startswith("scn:"):
        body = text.split(":", 1)[1].strip()
        try:
            return (1, int(body), 0, "")
        except (TypeError, ValueError):
            return (0, 0, 0, body)
    if lower.startswith("mongo:"):
        return (0, 0, 0, text.split(":", 1)[1])
    # Postgres WAL LSN: hex/hex (reject paths that look like URLs).
    if "/" in text and not lower.startswith("gtid:"):
        hi, _, lo = text.partition("/")
        if hi and lo and all(c in "0123456789abcdefABCDEF" for c in hi + lo):
            try:
                return (3, int(hi, 16), int(lo, 16), "")
            except ValueError:
                pass
    # MySQL binlog file:pos (pos may already be zero-padded from extract_cdc_lsn).
    if ":" in text and not lower.startswith("gtid:"):
        file_name, _, pos = text.rpartition(":")
        if file_name and pos.isdigit():
            return (2, file_name, int(pos), "")
    # Zero-padded / numeric versions (SQL Server CT, etc.).
    if text.isdigit():
        return (1, int(text), 0, "")
    return (0, 0, 0, text)


def compare_lsn(left: Any, right: Any) -> int:
    """Compare two LSN-like values. Returns -1, 0, or 1.

    Same-family stamps compare numerically/lexically. Cross-family pairs are
    **incomparable** and return ``0`` so LSN guards refuse invent overwrite
    (Debezium at-least-once + PK high-water-mark class). Empty is older than
    any concrete stamp.
    """
    left_empty = left is None or str(left).strip() == ""
    right_empty = right is None or str(right).strip() == ""
    if left_empty and right_empty:
        return 0
    if left_empty:
        return -1
    if right_empty:
        return 1
    fa, fb = lsn_family(left), lsn_family(right)
    if fa != fb:
        return 0
    a, b = lsn_sort_key(left), lsn_sort_key(right)
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def lsn_is_newer(incoming: Any, existing: Any) -> bool:
    """True when ``incoming`` should replace ``existing`` under at-least-once CDC.

    Requires a strictly greater same-family stamp. Equal and cross-family are
    not newer (idempotent redelivery / refuse invent).
    """
    if existing is None or str(existing).strip() == "":
        return True
    if incoming is None or str(incoming).strip() == "":
        return False
    return compare_lsn(incoming, existing) > 0


def parse_mysql_gtid_set(gtid_set: Any) -> dict[str, list[tuple[int, int]]]:
    """Parse MySQL ``gtid_executed`` into ``{uuid: [(start, end), ...]}``.

    Research: Debezium read-only incremental snapshots use executed GTID sets
    as low/high watermarks (DBZ-3577). Intervals are inclusive.
    """
    text = str(gtid_set or "").strip()
    if text.lower().startswith("gtid:"):
        text = text[5:].strip()
    out: dict[str, list[tuple[int, int]]] = {}
    if not text:
        return out
    for chunk in text.replace("\n", ",").split(","):
        part = chunk.strip()
        if not part or ":" not in part:
            continue
        uuid, _, ranges = part.partition(":")
        uuid = uuid.strip()
        if not uuid:
            continue
        intervals: list[tuple[int, int]] = []
        for rng in ranges.split(":"):
            rng = rng.strip()
            if not rng:
                continue
            if "-" in rng:
                a, _, b = rng.partition("-")
                try:
                    start, end = int(a), int(b)
                except ValueError:
                    continue
                if end < start:
                    start, end = end, start
                intervals.append((start, end))
            else:
                try:
                    n = int(rng)
                except ValueError:
                    continue
                intervals.append((n, n))
        if intervals:
            out.setdefault(uuid, []).extend(intervals)
    return out


def gtid_set_contains(haystack: Any, needle: Any) -> bool:
    """True when every GTID interval in ``needle`` is covered by ``haystack``.

    Used for Debezium-class watermark checks: high watermark contains a
    streamed event's GTID ⇒ window can close. Cross-empty: empty needle is
    contained; empty haystack contains nothing non-empty.
    """
    needle_map = parse_mysql_gtid_set(needle)
    if not needle_map:
        return True
    hay = parse_mysql_gtid_set(haystack)
    if not hay:
        return False
    for uuid, intervals in needle_map.items():
        covers = hay.get(uuid) or []
        if not covers:
            return False
        for start, end in intervals:
            for n in range(start, end + 1):
                if not any(a <= n <= b for a, b in covers):
                    return False
    return True


def gtid_watermark_window_closed(
    *,
    low: Any,
    high: Any,
    event_gtid: Any = None,
) -> bool:
    """True when read-only incremental snapshot GTID window can close.

    Research: Debezium DBZ-3577 — executed GTID set as low/high watermarks.
    Window closes when ``high`` contains ``low`` (and optional event GTID).
    Never invent closed from lexicographic string order.
    """
    if not gtid_set_contains(high, low):
        return False
    if event_gtid is None or str(event_gtid).strip() == "":
        return True
    return gtid_set_contains(high, event_gtid)


def dedupe_rows_by_pk_and_lsn(
    rows: list[tuple],
    conflict_columns: list[str],
    target_cols: list[str],
    *,
    lsn_column: str = DF_LSN_COL,
) -> list[tuple]:
    """Keep the highest-LSN row per PK; fall back to last-wins when LSN absent."""
    kept, _numbers = dedupe_rows_by_pk_and_lsn_keeping_numbers(
        rows, conflict_columns, target_cols, lsn_column=lsn_column
    )
    return kept


def dedupe_rows_by_pk_and_lsn_keeping_numbers(
    rows: list[tuple],
    conflict_columns: list[str],
    target_cols: list[str],
    row_numbers: list[int] | None = None,
    *,
    lsn_column: str = DF_LSN_COL,
) -> tuple[list[tuple], list[int] | None]:
    """Dedupe by PK/LSN, and report which source row each survivor came from.

    The winner is the highest LSN rather than the last arrival, so the surviving
    row's number is not simply the last one seen for that key.
    """
    from connectors.writer_common import (
        _conflict_key_identity,
        dedupe_rows_keeping_numbers,
        resolve_conflict_targets,
        resolve_row_number,
    )

    if not conflict_columns or not rows:
        return rows, row_numbers
    if lsn_column not in target_cols:
        return dedupe_rows_keeping_numbers(
            rows, conflict_columns, target_cols, row_numbers
        )
    conflict = resolve_conflict_targets(conflict_columns, target_cols, strict=True)
    if not conflict:
        return rows, row_numbers
    indices = [target_cols.index(c) for c in conflict]
    lsn_idx = target_cols.index(lsn_column)
    best: dict[tuple, tuple] = {}
    best_numbers: dict[tuple, int] = {}
    for position, row in enumerate(rows):
        key = tuple(_conflict_key_identity(row[i]) for i in indices)
        prev = best.get(key)
        if prev is None or compare_lsn(row[lsn_idx], prev[lsn_idx]) >= 0:
            best[key] = row
            best_numbers[key] = resolve_row_number(row_numbers, position)
    if row_numbers is None:
        return list(best.values()), None
    return list(best.values()), [best_numbers[k] for k in best]


def _format_file_pos_lsn(file_name: str, pos: Any) -> str:
    """Format file:pos LSN for downstream SQL guards.

    MySQL binlog files use zero-padded numeric suffixes (e.g. ``mysql-bin.000003``);
    for those we zero-pad the position so lexicographic text ordering stays
    monotonic.  For unpadded file names we emit the plain integer position so
    unit-test fixtures like ``bin.1:9`` stay readable.
    """
    try:
        int_pos = int(pos)
    except (TypeError, ValueError):
        return f"{file_name}:{pos}"
    # Detect zero-padded numeric token in the file name (MySQL binlog style).
    if re.search(r"(?<!\d)0\d+(?!\d)", file_name):
        return f"{file_name}:{int_pos:020d}"
    return f"{file_name}:{int_pos}"


def extract_cdc_lsn(resume_token: Any) -> str | None:
    """Pull a sortable LSN/position string from a CDC resume token.

    Supports PG ``lsn=``, MySQL ``file:pos`` / ``gtid``, Mongo ``_data``,
    SQL Server LSN hex, and Oracle SCN. Used to stamp ``_df_lsn`` for
    at-least-once upsert guards (not exactly-once).
    """
    if resume_token is None:
        return None
    from services.cdc_resume_tokens import unwrap_resume_token

    resume_token = unwrap_resume_token(resume_token)
    if resume_token is None:
        return None
    if isinstance(resume_token, dict):
        # Nested PG hold / incremental wrappers
        nested = resume_token.get("token")
        if isinstance(nested, (dict, str)) and nested:
            nested_lsn = extract_cdc_lsn(nested)
            if nested_lsn:
                return nested_lsn
        file_name = resume_token.get("file") or resume_token.get("filename")
        pos = resume_token.get("pos")
        if file_name is not None and pos is not None:
            return _format_file_pos_lsn(file_name, pos)
        gtid = resume_token.get("gtid") or resume_token.get("gtid_set")
        if gtid is not None and str(gtid).strip():
            return f"gtid:{str(gtid).strip()}"
        for key in ("lsn", "scn", "version", "position", "resume_lsn", "pos", "_data"):
            value = resume_token.get(key)
            if value is None or not str(value).strip():
                continue
            if key == "scn":
                body = str(value).strip()
                return body if body.lower().startswith("scn:") else f"scn:{body}"
            if key == "_data":
                body = str(value).strip()
                return body if body.lower().startswith("mongo:") else f"mongo:{body}"
            if key == "version":
                try:
                    return f"{int(value):020d}"
                except (TypeError, ValueError):
                    return str(value).strip()
            return str(value).strip()
        return None
    text = str(resume_token).strip()
    if not text or text in {"None", "null"}:
        return None
    # Bare MySQL file:pos strings — pad pos for lexicographic guards.
    if ":" in text and not text.lower().startswith("gtid:") and "/" not in text and not text.startswith("{"):
        file_name, _, pos = text.rpartition(":")
        if file_name and pos.isdigit():
            return _format_file_pos_lsn(file_name, pos)
    # JSON CDC tokens (SQL Server native / CT, Oracle LogMiner, etc.)
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except Exception:
            data = None
        if isinstance(data, dict):
            kind = str(data.get("kind") or "")
            if kind == "mssql-cdc":
                lsn = data.get("lsn")
                if lsn is not None and str(lsn).strip():
                    return str(lsn).strip()
            if kind in {"mssql-ct", "sqlserver-ct"}:
                ver = data.get("version")
                if ver is not None and str(ver).strip():
                    # Zero-pad so lexicographic compare stays monotonic for versions.
                    try:
                        return f"{int(ver):020d}"
                    except (TypeError, ValueError):
                        return str(ver).strip()
            nested = extract_cdc_lsn(data)
            if nested:
                return nested
    if "lsn=" in text:
        for part in text.split("|"):
            if part.startswith("lsn=") and part[4:].strip():
                return part[4:].strip()
    return text


#: Families that carry a ``prefix:`` yet are **not** MySQL binlog ``file:pos``.
#: Mirrors the prefix branches at the top of :func:`lsn_family`, which are
#: tested before the ``file:pos`` shape. The destination predicates below must
#: apply the same precedence: a Mongo resume token is hex, and pushing it into
#: the integer position compare either errors (``::bigint``) or silently
#: compares garbage.
NON_BINLOG_LSN_PREFIXES = ("gtid:", "mongo:", "scn:")

_PREFIX_ALTERNATION = "|".join(p.rstrip(":") for p in NON_BINLOG_LSN_PREFIXES)

# Regex, not LIKE: a literal ``%`` in generated SQL collides with the ``%s``
# paramstyle of psycopg2/pymysql, which format the statement against the bound
# row (``TypeError: not enough arguments for format string``) — so the guard
# would take down the whole executemany batch it is meant to protect.


def _re_prefixed(full: bool) -> str:
    """Pattern: the stamp carries a non-binlog family prefix."""
    return f"({_PREFIX_ALTERNATION}):.*" if full else f"^({_PREFIX_ALTERNATION}):"


def _re_one_prefix(prefix: str, full: bool) -> str:
    return f"{prefix}.*" if full else f"^{prefix}"


def _re_filepos_tail(full: bool) -> str:
    """Pattern: ``file:pos`` — ends in the integer position.

    :func:`lsn_family` requires ``pos.isdigit()`` after ``rpartition(':')``, so
    a hex Mongo resume token or an SCN never enters the position compare.
    """
    return ".*:[0-9]+" if full else ":[0-9]+$"


def _lsn_filepos_family_sql(expr: str, matcher: LsnMatcher, *, full: bool = False) -> str:
    """SQL: ``expr`` is a MySQL binlog ``file:pos`` stamp and nothing else."""
    return (
        f"({matcher(expr, _re_filepos_tail(full))} "
        f"AND NOT {matcher(expr, _re_prefixed(full))})"
    )


def _lsn_scn_family_sql(expr: str, matcher: LsnMatcher, *, full: bool = False) -> str:
    """SQL: ``expr`` is an Oracle ``scn:<digits>`` stamp.

    :func:`lsn_sort_key` orders SCNs by ``int(body)``, so the destination guard
    must compare them numerically too — as text ``scn:9`` outranks ``scn:100``
    and an older redelivery wins.
    """
    return matcher(expr, "scn:[0-9]+" if full else "^scn:[0-9]+$")


def _lsn_same_prefix_sql(
    inc: str, dest: str, matcher: LsnMatcher, *, full: bool = False
) -> str:
    """SQL: both sides carry the same family prefix, or neither carries one.

    Keeps the opaque-text branch from inventing ``newer`` across families —
    ``mongo:…`` vs ``gtid:…`` is incomparable in :func:`compare_lsn` and must
    stay incomparable in the destination guard.
    """
    return " AND ".join(
        f"({matcher(inc, _re_one_prefix(prefix, full))} "
        f"= {matcher(dest, _re_one_prefix(prefix, full))})"
        for prefix in NON_BINLOG_LSN_PREFIXES
    )


def postgres_lsn_update_guard_sql(table_name: str, lsn_column: str = DF_LSN_COL) -> str:
    """WHERE fragment for ON CONFLICT when ``_df_lsn`` is present.

    Real PG ``hi/lo`` LSNs use ``::pg_lsn``. Mixed CDC stamps use family-aware
    compare for ``file:pos`` / numeric versions / opaque tokens — never invent
    cross-family ``newer`` via bare text ``>`` (mirrors :func:`compare_lsn`).
    """
    pg_pat = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_pat = r"^[0-9]+$"
    excl = f'EXCLUDED."{lsn_column}"'
    dest = f'"{table_name}"."{lsn_column}"'
    dest_c = f"COALESCE({dest}, '')"

    def match(expr: str, pat: str) -> str:
        return f"LOWER({expr}) ~ '{pat}'"

    excl_filepos = _lsn_filepos_family_sql(excl, match)
    dest_filepos = _lsn_filepos_family_sql(dest_c, match)
    both_filepos = f"({excl_filepos} AND {dest_filepos})"
    # split_part is Postgres-native (same dialect as ON CONFLICT). The position
    # is the *last* segment, matching lsn_family's rpartition.
    excl_pos = f"NULLIF(regexp_replace({excl}, '^.*:', ''), '')::bigint"
    dest_pos = f"NULLIF(regexp_replace({dest}, '^.*:', ''), '')::bigint"
    filepos_newer = (
        f"(split_part({excl}, ':', 1) > split_part({dest}, ':', 1) "
        f"OR (split_part({excl}, ':', 1) = split_part({dest}, ':', 1) "
        f"AND {excl_pos} > {dest_pos}))"
    )
    both_numeric = f"({excl} ~ '{num_pat}' AND {dest_c} ~ '{num_pat}')"
    excl_scn = _lsn_scn_family_sql(excl, match)
    dest_scn = _lsn_scn_family_sql(dest_c, match)
    scn_newer = (
        f"({excl_scn} AND {dest_scn} "
        f"AND regexp_replace(LOWER({excl}), '^scn:', '')::bigint "
        f"> regexp_replace(LOWER({dest}), '^scn:', '')::bigint)"
    )
    # Opaque: neither side looks like pg / file:pos / scn / all-digits, and both
    # carry the same family prefix so a Mongo token never outranks a GTID set.
    both_opaque = (
        f"({excl} !~ '{pg_pat}' AND {dest_c} !~ '{pg_pat}' "
        f"AND NOT {excl_filepos} AND NOT {dest_filepos} "
        f"AND NOT {excl_scn} AND NOT {dest_scn} "
        f"AND {excl} !~ '{num_pat}' AND {dest_c} !~ '{num_pat}' "
        f"AND {_lsn_same_prefix_sql(excl, dest_c, match)})"
    )
    return (
        f"( "
        f"({excl} ~ '{pg_pat}' AND {dest_c} ~ '{pg_pat}' "
        f"AND {excl}::pg_lsn > COALESCE(NULLIF({dest}, '')::pg_lsn, '0/0'::pg_lsn)) "
        f"OR "
        f"({excl} !~ '{pg_pat}' AND ("
        f"{dest} IS NULL OR {dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR {scn_newer} "
        f"OR ({both_numeric} AND {excl}::bigint > {dest}::bigint) "
        f"OR ({both_opaque} AND {excl} > {dest})"
        f")) "
        f")"
    )


def mysql_lsn_values_newer_sql(lsn_column: str = DF_LSN_COL, *, quote: str = "`") -> str:
    """Boolean SQL: ``VALUES(lsn)`` is strictly newer than the destination cell.

    Handles empty dest, PG ``hi/lo`` hex, ``file:pos`` (file then integer pos),
    numeric versions, and opaque tokens — refuses cross-family invent. Used
    inside ``ON DUPLICATE KEY UPDATE col=IF(<pred>, VALUES(col), col)``.
    """
    col = f"{quote}{lsn_column}{quote}"
    inc = f"VALUES({col})"
    dest = col
    pg_re = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_re = r"^[0-9]+$"

    def match(expr: str, pat: str) -> str:
        return f"LOWER({expr}) REGEXP '{pat}'"

    inc_filepos = _lsn_filepos_family_sql(inc, match)
    dest_filepos = _lsn_filepos_family_sql(dest, match)
    both_filepos = f"({inc_filepos} AND {dest_filepos})"
    filepos_newer = (
        f"(SUBSTRING_INDEX({inc}, ':', 1) > SUBSTRING_INDEX({dest}, ':', 1) "
        f"OR (SUBSTRING_INDEX({inc}, ':', 1) = SUBSTRING_INDEX({dest}, ':', 1) "
        f"AND CAST(SUBSTRING_INDEX({inc}, ':', -1) AS UNSIGNED) "
        f"> CAST(SUBSTRING_INDEX({dest}, ':', -1) AS UNSIGNED)))"
    )
    both_pg = f"({inc} REGEXP '{pg_re}' AND {dest} REGEXP '{pg_re}')"
    # CONV returns a string, so each half is cast to UNSIGNED before compare —
    # otherwise 0/100 sorts before 0/20 under text ordering.
    inc_hi = f"CAST(CONV(SUBSTRING_INDEX({inc}, '/', 1), 16, 10) AS UNSIGNED)"
    dest_hi = f"CAST(CONV(SUBSTRING_INDEX({dest}, '/', 1), 16, 10) AS UNSIGNED)"
    inc_lo = f"CAST(CONV(SUBSTRING_INDEX({inc}, '/', -1), 16, 10) AS UNSIGNED)"
    dest_lo = f"CAST(CONV(SUBSTRING_INDEX({dest}, '/', -1), 16, 10) AS UNSIGNED)"
    pg_newer = (
        f"({inc_hi} > {dest_hi} OR ({inc_hi} = {dest_hi} AND {inc_lo} > {dest_lo}))"
    )
    both_numeric = f"({inc} REGEXP '{num_re}' AND {dest} REGEXP '{num_re}')"
    inc_scn = _lsn_scn_family_sql(inc, match)
    dest_scn = _lsn_scn_family_sql(dest, match)
    scn_newer = (
        f"({inc_scn} AND {dest_scn} "
        f"AND CAST(SUBSTRING(LOWER({inc}), 5) AS UNSIGNED) "
        f"> CAST(SUBSTRING(LOWER({dest}), 5) AS UNSIGNED))"
    )
    both_opaque = (
        f"(NOT {inc_filepos} AND NOT {dest_filepos} "
        f"AND NOT {inc_scn} AND NOT {dest_scn} "
        f"AND {inc} NOT REGEXP '{pg_re}' AND {dest} NOT REGEXP '{pg_re}' "
        f"AND {inc} NOT REGEXP '{num_re}' AND {dest} NOT REGEXP '{num_re}' "
        f"AND {_lsn_same_prefix_sql(inc, dest, match)})"
    )
    return (
        f"({dest} IS NULL OR {dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR {scn_newer} "
        f"OR ({both_pg} AND {pg_newer}) "
        f"OR ({both_numeric} AND CAST({inc} AS UNSIGNED) > CAST({dest} AS UNSIGNED)) "
        f"OR ({both_opaque} AND {inc} > {dest}))"
    )


def sqlite_lsn_update_guard_sql(table_name: str, lsn_column: str = DF_LSN_COL) -> str:
    """WHERE fragment for SQLite ``ON CONFLICT DO UPDATE``.

    Family-aware for PG ``hi/lo`` hex, ``file:pos``, numeric, and opaque.
    Equal-width zero-padded hex strings compare in integer order, so
    ``0/100`` is newer than ``0/20`` (bare text would invert them).
    Writers also run :func:`filter_stale_lsn_rows` / :func:`compare_lsn`
    in Python before bind. Cross-family pairs never invent ``newer``.
    """
    excl = f'excluded."{lsn_column}"'
    dest = f'"{table_name}"."{lsn_column}"'

    def sqlite_filepos(expr: str) -> str:
        """SQLite has no REGEXP: strip trailing digits and require ``…:`` left.

        Equivalent to ``:[0-9]+$`` — ``mongo:826A…04`` loses its trailing digits
        and still ends in hex, so it never reaches the integer position compare.
        """
        trimmed = f"rtrim({expr}, '0123456789')"
        prefixes = " AND ".join(
            f"LOWER(substr({expr}, 1, {len(p)})) <> '{p}'"
            for p in NON_BINLOG_LSN_PREFIXES
        )
        return (
            f"(length({trimmed}) < length({expr}) "
            f"AND substr({trimmed}, -1) = ':' AND length({trimmed}) > 1 "
            f"AND {prefixes})"
        )

    excl_filepos = sqlite_filepos(excl)
    dest_filepos = sqlite_filepos(dest)
    both_filepos = f"({excl_filepos} AND {dest_filepos})"
    # instr/substr — portable without REGEXP extension.
    excl_file = f"substr({excl}, 1, instr({excl}, ':') - 1)"
    dest_file = f"substr({dest}, 1, instr({dest}, ':') - 1)"
    excl_pos = f"CAST(substr({excl}, instr({excl}, ':') + 1) AS INTEGER)"
    dest_pos = f"CAST(substr({dest}, instr({dest}, ':') + 1) AS INTEGER)"
    filepos_newer = (
        f"({excl_file} > {dest_file} "
        f"OR ({excl_file} = {dest_file} AND {excl_pos} > {dest_pos}))"
    )
    # GLOB [0-9]* matches empty too — require at least one digit via length.
    excl_numeric = (
        f"({excl} GLOB '[0-9]*' AND {excl} NOT GLOB '*[^0-9]*' AND length({excl}) > 0)"
    )
    dest_numeric = (
        f"({dest} GLOB '[0-9]*' AND {dest} NOT GLOB '*[^0-9]*' AND length({dest}) > 0)"
    )
    both_numeric = f"({excl_numeric} AND {dest_numeric})"
    excl_hi = f"substr({excl}, 1, instr({excl}, '/') - 1)"
    dest_hi = f"substr({dest}, 1, instr({dest}, '/') - 1)"
    excl_lo = f"substr({excl}, instr({excl}, '/') + 1)"
    dest_lo = f"substr({dest}, instr({dest}, '/') + 1)"
    hex_part = (
        lambda part: f"({part} GLOB '[0-9A-Fa-f]*' AND {part} NOT GLOB '*[^0-9A-Fa-f]*')"
    )
    both_pg = (
        f"({excl} LIKE '%/%' AND {excl} NOT LIKE '%/%/%' AND {excl} NOT LIKE '%:%' "
        f"AND {dest} LIKE '%/%' AND {dest} NOT LIKE '%/%/%' AND {dest} NOT LIKE '%:%' "
        f"AND {hex_part(excl_hi)} AND {hex_part(excl_lo)} "
        f"AND {hex_part(dest_hi)} AND {hex_part(dest_lo)})"
    )
    pad = (
        lambda part: f"substr('0000000000000000' || upper({part}), -16, 16)"
    )
    pg_newer = (
        f"({pad(excl_hi)} > {pad(dest_hi)} "
        f"OR ({pad(excl_hi)} = {pad(dest_hi)} AND {pad(excl_lo)} > {pad(dest_lo)}))"
    )

    def sqlite_scn(expr: str) -> str:
        digits = f"substr({expr}, 5)"
        return (
            f"(LOWER(substr({expr}, 1, 4)) = 'scn:' AND length({digits}) > 0 "
            f"AND {digits} NOT GLOB '*[^0-9]*')"
        )

    excl_scn = sqlite_scn(excl)
    dest_scn = sqlite_scn(dest)
    scn_newer = (
        f"({excl_scn} AND {dest_scn} "
        f"AND CAST(substr({excl}, 5) AS INTEGER) > CAST(substr({dest}, 5) AS INTEGER))"
    )
    excl_opaque = (
        f"(NOT {excl_filepos} AND NOT {excl_scn} "
        f"AND {excl} NOT LIKE '%/%' AND NOT {excl_numeric})"
    )
    dest_opaque = (
        f"(NOT {dest_filepos} AND NOT {dest_scn} "
        f"AND {dest} NOT LIKE '%/%' AND NOT {dest_numeric})"
    )
    same_prefix = " AND ".join(
        f"((LOWER(substr({excl}, 1, {len(p)})) = '{p}') "
        f"= (LOWER(substr({dest}, 1, {len(p)})) = '{p}'))"
        for p in NON_BINLOG_LSN_PREFIXES
    )
    both_opaque = f"({excl_opaque} AND {dest_opaque} AND {same_prefix})"
    return (
        f"({dest} IS NULL OR {dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR {scn_newer} "
        f"OR ({both_pg} AND {pg_newer}) "
        f"OR ({both_numeric} AND CAST({excl} AS INTEGER) > CAST({dest} AS INTEGER)) "
        f"OR ({both_opaque} AND {excl} > {dest}))"
    )


def snowflake_lsn_match_predicate(
    target_alias: str = "t",
    source_alias: str = "s",
    lsn_column: str = DF_LSN_COL,
) -> str:
    """MATCHED guard for Snowflake MERGE — mirrors :func:`compare_lsn` families.

    Bare ``s.lsn > t.lsn`` mis-orders PG ``0/100`` vs ``0/20`` and invents
    cross-family ``newer``. Parse pg / file:pos / numeric; opaque text only
    when both sides are the same opaque family.
    """
    inc = f'{source_alias}."{lsn_column}"'
    dest = f'COALESCE({target_alias}."{lsn_column}", \'\')'
    pg_re = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_re = r"^[0-9]+$"

    # Snowflake REGEXP_LIKE implicitly anchors, so patterns are full-match.
    def match(expr: str, pat: str) -> str:
        return f"REGEXP_LIKE(LOWER({expr}), '{pat}')"

    inc_filepos = _lsn_filepos_family_sql(inc, match, full=True)
    dest_filepos = _lsn_filepos_family_sql(dest, match, full=True)
    both_filepos = f"({inc_filepos} AND {dest_filepos})"
    # SPLIT_PART(..., -1) = last segment (pos) in Snowflake.
    filepos_newer = (
        f"(SPLIT_PART({inc}, ':', 1) > SPLIT_PART({dest}, ':', 1) "
        f"OR (SPLIT_PART({inc}, ':', 1) = SPLIT_PART({dest}, ':', 1) "
        f"AND TRY_TO_NUMBER(SPLIT_PART({inc}, ':', -1)) "
        f"> TRY_TO_NUMBER(SPLIT_PART({dest}, ':', -1))))"
    )
    both_pg = (
        f"(REGEXP_LIKE({inc}, '{pg_re}') AND REGEXP_LIKE({dest}, '{pg_re}'))"
    )
    # Hex hi/lo via TO_NUMBER with hex format mask.
    inc_hi = f"TRY_TO_NUMBER(SPLIT_PART({inc}, '/', 1), 'XXXXXXXXXXXXXXXX')"
    dest_hi = f"TRY_TO_NUMBER(SPLIT_PART({dest}, '/', 1), 'XXXXXXXXXXXXXXXX')"
    inc_lo = f"TRY_TO_NUMBER(SPLIT_PART({inc}, '/', 2), 'XXXXXXXXXXXXXXXX')"
    dest_lo = f"TRY_TO_NUMBER(SPLIT_PART({dest}, '/', 2), 'XXXXXXXXXXXXXXXX')"
    pg_newer = (
        f"({inc_hi} > {dest_hi} OR ({inc_hi} = {dest_hi} AND {inc_lo} > {dest_lo}))"
    )
    both_numeric = (
        f"(REGEXP_LIKE({inc}, '{num_re}') AND REGEXP_LIKE({dest}, '{num_re}'))"
    )
    inc_opaque = (
        f"(NOT REGEXP_LIKE({inc}, '{pg_re}') AND NOT REGEXP_LIKE({inc}, '{num_re}') "
        f"AND NOT {inc_filepos})"
    )
    dest_opaque = (
        f"(NOT REGEXP_LIKE({dest}, '{pg_re}') AND NOT REGEXP_LIKE({dest}, '{num_re}') "
        f"AND NOT {dest_filepos})"
    )
    inc_scn = _lsn_scn_family_sql(inc, match, full=True)
    dest_scn = _lsn_scn_family_sql(dest, match, full=True)
    scn_newer = (
        f"({inc_scn} AND {dest_scn} "
        f"AND TRY_TO_NUMBER(SPLIT_PART({inc}, ':', -1)) "
        f"> TRY_TO_NUMBER(SPLIT_PART({dest}, ':', -1)))"
    )
    both_opaque = (
        f"({inc_opaque} AND {dest_opaque} "
        f"AND NOT {inc_scn} AND NOT {dest_scn} "
        f"AND {_lsn_same_prefix_sql(inc, dest, match, full=True)})"
    )
    return (
        f"({dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR {scn_newer} "
        f"OR ({both_pg} AND {pg_newer}) "
        f"OR ({both_numeric} AND TRY_TO_NUMBER({inc}) > TRY_TO_NUMBER({dest})) "
        f"OR ({both_opaque} AND {inc} > {dest}))"
    )


def bigquery_lsn_match_predicate(
    target_alias: str = "T",
    source_alias: str = "S",
    lsn_column: str = DF_LSN_COL,
) -> str:
    """MATCHED guard for BigQuery MERGE — mirrors :func:`compare_lsn` families.

    Plain ``S.lsn > T.lsn`` is unsafe for PG ``hi/lo`` hex and invents
    cross-family ``newer``. Parse pg / file:pos / numeric; opaque text only
    within the same opaque family.
    """
    inc = f"{source_alias}.`{lsn_column}`"
    dest = f"COALESCE({target_alias}.`{lsn_column}`, '')"
    pg_re = r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$"
    num_re = r"^[0-9]+$"

    def match(expr: str, pat: str) -> str:
        return f"REGEXP_CONTAINS(LOWER({expr}), r'{pat}')"

    inc_filepos = _lsn_filepos_family_sql(inc, match)
    dest_filepos = _lsn_filepos_family_sql(dest, match)
    both_filepos = f"({inc_filepos} AND {dest_filepos})"
    filepos_newer = (
        f"(SPLIT({inc}, ':')[OFFSET(0)] > SPLIT({dest}, ':')[OFFSET(0)] "
        f"OR (SPLIT({inc}, ':')[OFFSET(0)] = SPLIT({dest}, ':')[OFFSET(0)] "
        f"AND SAFE_CAST(ARRAY_REVERSE(SPLIT({inc}, ':'))[OFFSET(0)] AS INT64) "
        f"> SAFE_CAST(ARRAY_REVERSE(SPLIT({dest}, ':'))[OFFSET(0)] AS INT64)))"
    )
    both_pg = (
        f"(REGEXP_CONTAINS({inc}, r'{pg_re}') AND REGEXP_CONTAINS({dest}, r'{pg_re}'))"
    )
    inc_hi = f"SAFE_CAST(CONCAT('0x', SPLIT({inc}, '/')[OFFSET(0)]) AS INT64)"
    dest_hi = f"SAFE_CAST(CONCAT('0x', SPLIT({dest}, '/')[OFFSET(0)]) AS INT64)"
    inc_lo = f"SAFE_CAST(CONCAT('0x', SPLIT({inc}, '/')[OFFSET(1)]) AS INT64)"
    dest_lo = f"SAFE_CAST(CONCAT('0x', SPLIT({dest}, '/')[OFFSET(1)]) AS INT64)"
    pg_newer = (
        f"({inc_hi} > {dest_hi} OR ({inc_hi} = {dest_hi} AND {inc_lo} > {dest_lo}))"
    )
    both_numeric = (
        f"(REGEXP_CONTAINS({inc}, r'{num_re}') AND REGEXP_CONTAINS({dest}, r'{num_re}'))"
    )
    inc_opaque = (
        f"(NOT REGEXP_CONTAINS({inc}, r'{pg_re}') "
        f"AND NOT REGEXP_CONTAINS({inc}, r'{num_re}') "
        f"AND NOT {inc_filepos})"
    )
    dest_opaque = (
        f"(NOT REGEXP_CONTAINS({dest}, r'{pg_re}') "
        f"AND NOT REGEXP_CONTAINS({dest}, r'{num_re}') "
        f"AND NOT {dest_filepos})"
    )
    inc_scn = _lsn_scn_family_sql(inc, match)
    dest_scn = _lsn_scn_family_sql(dest, match)
    scn_newer = (
        f"({inc_scn} AND {dest_scn} "
        f"AND SAFE_CAST(SPLIT({inc}, ':')[OFFSET(1)] AS INT64) "
        f"> SAFE_CAST(SPLIT({dest}, ':')[OFFSET(1)] AS INT64))"
    )
    both_opaque = (
        f"({inc_opaque} AND {dest_opaque} "
        f"AND NOT {inc_scn} AND NOT {dest_scn} "
        f"AND {_lsn_same_prefix_sql(inc, dest, match)})"
    )
    return (
        f"({dest} = '' "
        f"OR ({both_filepos} AND {filepos_newer}) "
        f"OR {scn_newer} "
        f"OR ({both_pg} AND {pg_newer}) "
        f"OR ({both_numeric} AND SAFE_CAST({inc} AS INT64) > SAFE_CAST({dest} AS INT64)) "
        f"OR ({both_opaque} AND {inc} > {dest}))"
    )
