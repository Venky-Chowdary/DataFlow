"""Completeness proof of one rehearsal extract, and the seal over all its parts.

Per table, both sides are measured over their **whole** population with
Gate-8's own canonical cell fingerprints
(:func:`services.reconciliation_api.iter_row_fingerprints`, the kernel behind
Gate-8's checksums) steered by the **source's declared column types**, folded
into order-independent additive digests
(:class:`services.reconciliation_api.FingerprintAccumulator`):

* a text column (CHAR / VARCHAR / TEXT, enums) is compared by its exact text:
  ``'007'`` and ``'7'``, ``' a '`` and ``'a'``, ``'TRUE'`` and ``'true'`` differ.
  Gate-8's type-less folds (numbers, booleans, dates, JSON spelled as text) are
  for comparing a value with its re-rendering in another type; on a text column
  they would hide a changed value;
* every other column goes through Gate-8's typed normalisation for its declared
  type, so a value and its rendering in the extract (a ``timestamptz`` as ISO
  text, a ``numeric`` as a JSON number, bytes as base64) compare equal, by the
  type's own equality;
* a column the digest cannot compare exactly is named and keeps the table from
  "proven": a PostgreSQL ``json`` column (the driver hands back a parsed
  document, so duplicate keys, key order and number spelling are already gone on
  the source side; ``jsonb`` and MySQL ``JSON`` normalise on write and compare as
  documents) and a column with no declared type.

* the row count;
* the key-set digest (the key columns only);
* the row digest (every extracted column);
* control totals (sum, min, max) of every numeric column, as exact decimals.

The source is re-read through its saved connection; the extract is read back
from the sealed bytes (the export bucket or the exports folder) through
Gate-8's artifact reader, and its SHA-256 is recomputed on the same bytes.
Value parity comes from Parity Studio's report on the extract job: its check
method when it compared values in full, and any unexplained difference it
names blocks "proven". Nothing here samples, and nothing measured on part of a
population is called proven: what could not be measured is named.

The seal is an RFC 6962 Merkle tree over the parts.
"""

from __future__ import annotations

import hashlib
import io
import itertools
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, BinaryIO

logger = logging.getLogger(__name__)

MERKLE_ALGORITHM = "sha256-merkle-rfc6962"
FULL_VALUE_CHECKS = frozenset({"full_checksum", "key_diff"})


# --------------------------------------------------------------------- seal


def merkle_leaf(part: dict[str, Any]) -> bytes:
    return f"{part['table']}:{int(part['size_bytes'])}:{part['sha256']}".encode()


def merkle_root(parts: Iterable[dict[str, Any]]) -> str:
    """RFC 6962 Merkle tree hash over the parts, sorted by table name."""
    leaves = [merkle_leaf(p) for p in sorted(parts, key=lambda p: (str(p["table"]), str(p.get("name") or "")))]

    def mth(items: list[bytes]) -> bytes:
        if not items:
            return hashlib.sha256(b"").digest()
        if len(items) == 1:
            return hashlib.sha256(b"\x00" + items[0]).digest()
        k = 1
        while k * 2 < len(items):
            k *= 2
        return hashlib.sha256(b"\x01" + mth(items[:k]) + mth(items[k:])).digest()

    return "sha256:" + mth(leaves).hex()


# -------------------------------------------------------------- measurement


@dataclass
class Profile:
    """One side of a table, measured over its whole population."""

    rows: int = 0
    columns: list[str] = field(default_factory=list)
    key_set_sha256: str | None = None
    row_set_sha256: str | None = None
    totals: dict[str, dict[str, Decimal]] = field(default_factory=dict)
    error: str = ""
    #: column -> why its values are not compared exactly (keeps the table from "proven").
    uncompared: dict[str, str] = field(default_factory=dict)
    #: Gate-8 per-column multiset digests of the row digest (names a differing column).
    column_digests: dict[str, str] = field(default_factory=dict)


_UNTYPED = "has no declared type, so its values could not be compared exactly"
_JSON_TEXT = ("is json: the driver hands back a parsed document, so its exact text (key order, duplicate keys, "
              "number spelling) cannot be compared")


def _declared(types: dict[str, str], column: str) -> str:
    if column in types:
        return str(types[column] or "")
    lowered = column.lower()
    return next((str(v or "") for k, v in types.items() if str(k).lower() == lowered), "")


def digest_plan(columns: list[str], types: dict[str, str], engine: str) -> tuple[set[str], dict[str, str],
                                                                                    dict[str, str]]:
    """(exact-text columns, Gate-8 typed columns -> DDL, uncompared column -> why)."""
    from services.type_system import normalize_logical_type

    exact: set[str] = set()
    typed: dict[str, str] = {}
    uncompared: dict[str, str] = {}
    for col in columns:
        ddl = _declared(types, col).strip()
        if not ddl:
            uncompared[col] = _UNTYPED
            continue
        if ddl.split("(")[0].strip().upper() == "JSON" and engine in {"postgresql", "postgres"}:
            uncompared[col] = _JSON_TEXT
            typed[col] = ddl
        elif normalize_logical_type(ddl) in {"string", "text"}:
            exact.add(col)
        else:
            typed[col] = ddl
    return exact, typed, uncompared


def _exact(value: Any) -> Any:
    """A token Gate-8's normaliser hands back unchanged: the exact text, hex-encoded."""
    if value is None:
        return None
    if isinstance(value, str):
        return "s:" + value.encode("utf-8", "surrogatepass").hex()
    return f"o:{type(value).__name__}:" + str(value).encode("utf-8", "surrogatepass").hex()


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def numeric_columns(sample: Iterable[dict[str, Any]], columns: list[str]) -> list[str]:
    """Columns whose every non-null source value is a number (bool excluded)."""
    seen: dict[str, bool] = {}
    for row in sample:
        for col in columns:
            value = row.get(col)
            if value is None:
                continue
            ok = isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)
            seen[col] = seen.get(col, True) and ok
    return [c for c in columns if seen.get(c)]


def profile_rows(rows: Iterable[dict[str, Any]], columns: list[str], key_columns: list[str],
                 numeric: list[str] | None, *, types: dict[str, str] | None = None,
                 engine: str = "") -> Profile:
    """Count, key-set digest, row digest and control totals in one streamed pass.

    ``types`` are the source's declared column types (both sides use the
    source's: the extract is a file with no catalog) and ``engine`` the source
    engine; see the module docstring for how they steer the digest.
    ``numeric=None`` (the source side) totals every column whose every non-null
    value is a number; the extract side passes the source's numeric columns so
    their text values are parsed as exact decimals.
    """
    from services.reconciliation_api import FingerprintAccumulator, iter_row_fingerprints

    eng = str(engine or "").strip().lower()
    exact, typed, uncompared = digest_plan(list(columns), dict(types or {}), eng)
    prof = Profile(columns=list(columns), uncompared=uncompared)
    keys = [c for c in key_columns if c in columns]
    totals: dict[str, dict[str, Decimal]] = {}

    auto = numeric is None
    candidates = list(columns) if auto else list(numeric)
    not_numeric: set[str] = set()

    def fed() -> Iterator[dict[str, Any]]:
        for row in rows:
            prof.rows += 1
            for col in candidates:
                raw = row.get(col)
                if auto and raw is not None and (isinstance(raw, bool) or not isinstance(raw, (int, float, Decimal))):
                    not_numeric.add(col)
                    continue
                value = _decimal(raw)
                if value is None:
                    continue
                t = totals.setdefault(col, {"sum": Decimal(0), "min": value, "max": value})
                t["sum"] += value
                t["min"], t["max"] = min(t["min"], value), max(t["max"], value)
            yield {c: (_exact(row.get(c)) if c in exact else row.get(c)) for c in columns}

    full_acc = FingerprintAccumulator(keep_fingerprints=False)
    key_acc = FingerprintAccumulator(keep_fingerprints=False)
    # Both digests read the same single pass in lockstep (tee holds one row).
    full_rows, key_source = itertools.tee(fed())
    full = iter_row_fingerprints(full_rows, columns, dest_db_type=eng, dest_types=typed)
    if keys:
        key_fps = iter_row_fingerprints(({k: r.get(k) for k in keys} for r in key_source), keys,
                                        dest_db_type=eng, dest_types=typed)
        for (_rk, fp), (_kk, kfp) in zip(full, key_fps):
            full_acc.add("", fp)
            key_acc.add("", kfp)
    else:
        del key_source
        for _rk, fp in full:
            full_acc.add("", fp)
    prof.row_set_sha256 = full_acc.additive_digest()
    prof.key_set_sha256 = key_acc.additive_digest() if keys else None
    prof.column_digests = full_acc.columns.snapshot()
    prof.totals = {c: t for c, t in totals.items() if c not in not_numeric}
    return prof


class HashingReader(io.RawIOBase):
    """Read-through wrapper that SHA-256s every byte handed to the reader."""

    def __init__(self, raw: BinaryIO) -> None:
        self._raw = raw
        self.digest = hashlib.sha256()
        self.size = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        chunk = self._raw.read(len(buffer))
        if not chunk:
            return 0
        n = len(chunk)
        buffer[:n] = chunk
        self.digest.update(chunk)
        self.size += n
        return n

    def drain(self) -> None:
        while True:
            chunk = self._raw.read(1 << 20)
            if not chunk:
                return
            self.digest.update(chunk)
            self.size += len(chunk)


def open_part(part: dict[str, Any], local_path: str = "") -> BinaryIO:
    """The sealed bytes of a part: the export bucket object, or the exports-folder file."""
    if part.get("storage") == "object_store":
        from services.export_object_store import export_bucket, export_client

        return export_client().get_object(Bucket=export_bucket(), Key=part["object_key"])["Body"]
    return open(local_path, "rb")


def profile_extract(part: dict[str, Any], *, local_path: str, fmt: str, columns: list[str],
                    key_columns: list[str], numeric: list[str], types: dict[str, str] | None = None,
                    engine: str = "") -> tuple[Profile, str, int]:
    """(profile, sha256 of the bytes read, size read). The reader is Gate-8's artifact reader."""
    from services.dest_precount import _iter_artifact_records
    from services.value_serializer import JsonNullToken

    stream = open_part(part, local_path)
    reader = HashingReader(stream)
    try:
        buffered = io.BufferedReader(reader, buffer_size=1 << 20)
        records = _iter_artifact_records(buffered, name=str(part.get("name") or ""), fmt=fmt)
        first: dict[str, Any] | None = None
        rest: Iterator[dict[str, Any]] = iter(())
        iterator = iter(records)
        for first in iterator:
            break
        extract_cols = list(first.keys()) if isinstance(first, dict) else list(columns)

        def rows() -> Iterator[dict[str, Any]]:
            # A top-level JSON null in a JSONL export is the row's SQL NULL (the
            # export writes NULL as null); only nested JSON keeps the token.
            for rec in ([first] if first is not None else []), iterator:
                for row in rec:
                    yield {k: (None if isinstance(v, JsonNullToken) else v) for k, v in row.items()}

        rest = rows()
        prof = profile_rows(rest, columns, key_columns, numeric, types=types, engine=engine)
        prof.columns = extract_cols
        reader.drain()
    finally:
        try:
            stream.close()
        except Exception:  # noqa: BLE001 - closing a finished read decides nothing
            logger.debug("extract stream close failed", exc_info=True)
    return prof, reader.digest.hexdigest(), reader.size


def source_columns_and_rows(endpoint: Any, *, itersize: int = 5000) -> tuple[list[str], Iterator[dict[str, Any]], Any]:
    """(columns, row iterator, closer) of a SQL source table, read in one streamed SELECT."""
    import sqlalchemy as sa

    from connectors.generic_sql import get_sqlalchemy_engine
    from connectors.sql_identifiers import quote_table_ref
    from services.control_totals import _quote_col
    from services.reconciliation_api import iter_select_row_dicts
    from src.transfer.adapters import resolve_connector_config

    cfg = resolve_connector_config(endpoint)
    db_type = str(cfg.get("type") or endpoint.format or "").lower()
    engine = get_sqlalchemy_engine({**dict(cfg), "type": db_type})
    table = quote_table_ref(endpoint.table, getattr(endpoint, "schema", None) or None, dialect=db_type or "ansi")
    conn = engine.connect()
    try:
        columns = list(conn.execute(sa.text(f"SELECT * FROM {table} WHERE 1=0")).keys())  # nosec B608
        select = f"SELECT {', '.join(_quote_col(db_type, c) for c in columns)} FROM {table}"  # nosec B608
        batches = iter_select_row_dicts(conn, sa.text(select), columns, itersize=itersize)
    except Exception:
        conn.close()
        raise

    def rows() -> Iterator[dict[str, Any]]:
        for batch in batches:
            yield from batch

    return columns, rows(), conn.close


# ------------------------------------------------------------------ verdicts


def _sensitive(column: str, totals: dict[str, Decimal]) -> bool:
    """A PII column by name, or a total that is itself a PII value (a card number, an SSN)."""
    from services.pii_guard import detect_pii, is_sensitive_name

    return is_sensitive_name(column) or any(
        detect_pii(format(totals[k], "f")).get("has_pii") for k in ("min", "max") if k in totals)


def _shown(totals: dict[str, Decimal] | None, masked: bool) -> dict[str, str | None] | None:
    if totals is None:
        return None
    return {k: (None if masked else format(v, "f")) for k, v in totals.items()}


def _totals_rows(src: Profile, ext: Profile | None) -> list[dict[str, Any]]:
    """Control totals per numeric column. Sum, min and max are compared on the real values,
    but a PII column's are withheld (``masked``): min / max of an identifier are real values."""
    out = []
    for col in sorted(src.totals):
        s = src.totals[col]
        e = (ext.totals.get(col) if ext else None)
        match = None if e is None and ext is None else bool(
            e and all(s[k] == e[k] for k in ("sum", "min", "max")))
        if ext is not None and e is None and src.rows:
            match = False
        masked = _sensitive(col, s) or bool(e and _sensitive(col, e))
        out.append({
            "column": col,
            "source": _shown(s, masked),
            "extract": _shown(e, masked) if e else None,
            "match": match,
            "masked": masked,
        })
    return out


def resolve_keys(key_columns: list[str], columns: list[str]) -> tuple[list[str], list[str]]:
    """(declared key columns as the table spells them, declared keys that are not columns)."""
    by_lower = {c.lower(): c for c in columns}
    resolved = [by_lower[k.lower()] for k in key_columns if k.lower() in by_lower]
    return resolved, [k for k in key_columns if k.lower() not in by_lower]


def judge_table(
    *,
    name: str,
    source: Profile | None,
    extract: Profile | None,
    key_columns: list[str],
    seal_ok: bool | None,
    parity: dict[str, Any],
    not_measured: list[str],
) -> dict[str, Any]:
    """Status, reason and differences of one extracted table (contract 4.1)."""
    diffs: list[dict[str, Any]] = []
    if seal_ok is False:
        diffs.append({"kind": "seal", "rows": None, "rows_at_least": False,
                      "detail": "The stored part does not match the SHA-256 recorded when it was uploaded."})
    if source is not None and extract is not None:
        if source.rows > extract.rows:
            diffs.append({"kind": "missing_rows", "rows": source.rows - extract.rows, "rows_at_least": True,
                          "detail": f"The extract holds {source.rows - extract.rows} row(s) fewer than the source."})
        elif extract.rows > source.rows:
            diffs.append({"kind": "extra_rows", "rows": extract.rows - source.rows, "rows_at_least": True,
                          "detail": f"The extract holds {extract.rows - source.rows} row(s) more than the source."})
        missing_cols = [c for c in source.columns if c not in extract.columns]
        if missing_cols:
            diffs.append({"kind": "changed_rows", "rows": None, "rows_at_least": False,
                          "detail": f"The extract lacks column(s) {', '.join(missing_cols[:10])}."})
        same_count = source.rows == extract.rows
        if same_count and source.key_set_sha256 and source.key_set_sha256 != extract.key_set_sha256:
            diffs.append({"kind": "changed_rows", "rows": 1, "rows_at_least": True,
                          "detail": "The key sets differ: at least one key is missing and another is extra."})
        elif same_count and source.row_set_sha256 != extract.row_set_sha256:
            named = differing_columns(source, extract)
            where = f" in column(s) {', '.join(named[:10])}" if named else ""
            diffs.append({"kind": "changed_rows", "rows": 1, "rows_at_least": True,
                          "detail": f"The row digests differ{where}: at least one row holds different values "
                                    "(or the source changed during the rehearsal)."})
        for row in _totals_rows(source, extract):
            if row["match"] is False:
                diffs.append({"kind": "control_total", "rows": None, "rows_at_least": False,
                              "detail": f"Control totals of {row['column']} differ."})
    unexplained = int(parity.get("unexplained") or 0)
    if unexplained:
        diffs.append({"kind": "changed_rows", "rows": unexplained, "rows_at_least": False,
                      "detail": f"Parity Studio names {unexplained} unexplained difference(s) on the extract job."})
    method = value_method(parity, source, extract)
    not_measured = list(not_measured) + [f"{col} {why}" for col, why in (source.uncompared if source else {}).items()]
    if diffs:
        status = "incomplete"
        reason = "; ".join(d["detail"] for d in diffs[:4])
    elif not_measured or method not in FULL_VALUE_CHECKS | {"population_digest"}:
        status = "not_proven"
        gaps = list(not_measured) or [f"values were compared only by {method.replace('_', ' ')}"]
        reason = "Nothing differs where measured, but " + "; ".join(gaps) + ", so this table is not proven."
    else:
        status = "proven"
        keyed = "key set, " if source is not None and source.key_set_sha256 else ""
        reason = (f"Rows, {keyed}every value (by {method.replace('_', ' ')}) and control totals match over "
                  f"all {source.rows if source else 0} row(s); the part verified on read.")
    return {"status": status, "reason": reason, "differences": diffs, "value_method": method,
            "control_totals": _totals_rows(source, extract) if source else []}


def differing_columns(source: Profile, extract: Profile) -> list[str]:
    """Columns whose Gate-8 per-column digests differ (diagnostic: names where the rows differ)."""
    src, ext = source.column_digests, extract.column_digests
    if not src or not ext:
        return []
    names = {c.lower(): c for c in source.columns}
    return [names.get(label, label) for label in sorted(set(src) | set(ext)) if src.get(label) != ext.get(label)]


def value_method(parity: dict[str, Any], source: Profile | None, extract: Profile | None) -> str:
    """How values were compared: Gate-8's full check if it ran, else the population digest."""
    check = str(parity.get("check") or "none")
    if check in FULL_VALUE_CHECKS and parity.get("verdict") in {"proven", "explained"}:
        return check
    if source is not None and extract is not None and source.row_set_sha256 and extract.row_set_sha256:
        return "population_digest"
    return check if check in {"sampled", "count_only", "none"} else "none"


def rehearsal_verdict(tables: list[dict[str, Any]]) -> tuple[str, str]:
    """(verdict, reason) over every declared table (contract 4.1)."""
    statuses = [t["status"] for t in tables]
    n = len(tables)
    if any(s in {"pending", "running"} for s in statuses):
        return "running", "The rehearsal is still extracting or verifying."
    count = {s: statuses.count(s) for s in ("proven", "not_proven", "incomplete", "failed")}
    if count["failed"]:
        names = ", ".join(t["name"] for t in tables if t["status"] == "failed")[:300]
        return "failed", f"{count['failed']} of {n} table(s) could not be extracted: {names}."
    if count["incomplete"]:
        names = ", ".join(t["name"] for t in tables if t["status"] == "incomplete")[:300]
        return "incomplete", f"{count['incomplete']} of {n} table(s) differ from the source: {names}."
    if n and count["proven"] == n:
        return "proven", f"Every one of the {n} declared table(s) was extracted and proven complete."
    return "not_proven", (f"{count['proven']} of {n} table(s) proven; {count['not_proven']} could not be "
                          "proven in full (see each table's reason).")
