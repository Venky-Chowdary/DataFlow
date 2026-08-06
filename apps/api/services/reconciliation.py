"""Gate 8 reconciliation — independent target verification."""

from __future__ import annotations

import base64
import hashlib
import heapq
import json
import logging
import os
from services.brand_env import getenv_brand
import re
import struct
import tempfile
from dataclasses import asdict, dataclass
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import time as _time
from datetime import timezone
from decimal import Decimal, InvalidOperation, Overflow
from typing import Any, Callable, Iterable

from services.transform_engine import (
    _DATE_LIKE_RE,
    _parse_date,
    _parse_datetime,
    apply_transform,
)
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

SPILL_THRESHOLD = int(getenv_brand("FINGERPRINT_SPILL_THRESHOLD", "1000000"))

# Quick pre-filter for the expensive Decimal / date normalization in
# normalize_cell.  Most string columns (names, emails, codes) are clearly not
# numbers or dates, so we can skip the exception-heavy Decimal constructor and
# the date regex for them.
_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_DATE_LIKE_CHARS = frozenset("-:/T ")
# RFC 4122 UUID wire — engines differ on case (PG lower, some drivers upper).
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_NULL_SENTINEL = "\x00NULL\x00"


def destination_empty_string_is_null(engine: str | None) -> bool:
    """True when the write destination collapses '' → NULL (Oracle VARCHAR2).

    Fivetran HVR Compare applies **write-location** rules for cross-engine
    ambiguity — Oracle treats zero-length VARCHAR2 as NULL; Postgres/MySQL do not.
    """
    eng = (engine or "").strip().lower()
    if not eng:
        return False
    return eng in {"oracle", "oracledb", "oracle_autonomous"} or eng.startswith("oracle")


@dataclass
class ReconciliationReport:
    passed: bool
    source_rows: int
    target_rows: int
    source_checksum: str
    target_checksum: str
    message: str
    rejected_rows: int = 0
    # Rows kept but with >=1 cell coerced to NULL because a value could not be
    # cast. Data was ALTERED, so this is surfaced even when row counts/checksums
    # match — reconciliation must not claim "100% fidelity" in that case.
    coerced_null_rows: int = 0
    # Rows intentionally not written because they were stale/duplicate without
    # being rejected (e.g. CDC LSN redelivery). They must not be counted as
    # dropped, but they also do not appear in the destination.
    rows_skipped: int = 0
    # Bounded read-back sample (mismatches) for operator drill-down / export.
    sample_compare: dict[str, Any] | None = None
    # Honest post-write phase: verified | writer_ack | failed | skipped | pending
    phase: str = ""
    post_write_pending: bool = False
    preview: bool = False
    # Module 4 honesty: sample authority must never hide checksum mismatch.
    checksum_match: bool | None = None
    population_proof: bool = False
    assurance_level: str = ""

    def to_dict(self) -> dict:
        return stamp_post_write_phase(asdict(self))


def stamp_post_write_phase(report: dict[str, Any]) -> dict[str, Any]:
    """Stamp explicit post-write phase so UIs never confuse writer-ack with Verified.

    Also stamps ``coverage``, ``checksum_match``, ``population_proof``,
    ``assurance_level`` — Gate-8 never invents population RI proof.
      * ``full_checksum`` — independent source↔dest digests match
      * ``sample`` — keyed sample is the authority (checksums missing or diverge)
      * ``writer_ack`` — writer digest only
      * ``none`` / omitted — failed / skipped / pending
    """
    out = dict(report or {})
    if out.get("preview") is True or str(out.get("phase") or "").lower().startswith("pre_write"):
        # Leave preflight simulation alone.
        return out
    if out.get("phase") and not str(out.get("phase")).startswith("post_write"):
        # Unknown custom phase — still normalize pending/preview flags.
        return out

    passed = bool(out.get("passed"))
    src = str(out.get("source_checksum") or "").strip()
    tgt = str(out.get("target_checksum") or "").strip()
    msg = str(out.get("message") or "").lower()
    independent_match = bool(src and tgt and src == tgt)
    # Module 4: always stamp checksum honesty; never invent population RI proof.
    out["checksum_match"] = independent_match if (src and tgt) else False
    out["population_proof"] = False

    if "file export" in msg or ("skipped" in msg and "reconciliation skipped" in msg):
        out["phase"] = "post_write_skipped"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "none"
        out["assurance_level"] = "none"
        return out

    if not passed:
        out["phase"] = "post_write_failed"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "none"
        out["assurance_level"] = "none"
        return out

    writer_only = (
        not tgt
        or "verified by writer" in msg
        or "read-back verifier not available" in msg
        or "read-back" in msg and "unavailable" in msg
    )
    sample = out.get("sample_compare") if isinstance(out.get("sample_compare"), dict) else {}
    sample_ok = bool(sample.get("passed")) and int(sample.get("compared") or 0) > 0
    # GA: diverging independent digests never become sample-verified success.
    # Sample may only classify when digests are missing (not when they disagree).
    if src and tgt and not independent_match:
        out["phase"] = "post_write_failed"
        out["post_write_pending"] = False
        out["preview"] = False
        out["passed"] = False
        out["coverage"] = "none"
        out["assurance_level"] = "none"
        out["checksum_match"] = False
        return out

    sample_authority = sample_ok and (
        writer_only
        or not tgt
        or "sample-verified" in msg
        or "sample verified" in msg
        or "key-aligned" in msg
        or "sample-only assurance" in msg
    )
    if passed and sample_authority and not (src and tgt):
        out["phase"] = "post_write_sample_verified"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "sample"
        out["assurance_level"] = "sample"
        return out

    if independent_match and not writer_only:
        out["phase"] = "post_write_verified"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "full_checksum"
        out["assurance_level"] = "full_checksum"
        return out

    if writer_only or (passed and src and not tgt):
        out["phase"] = "post_write_writer_ack"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "writer_ack"
        out["assurance_level"] = "writer_ack"
        return out

    out["phase"] = "post_write_pending"
    out["post_write_pending"] = True
    out["preview"] = False
    out["coverage"] = "none"
    out["assurance_level"] = "none"
    return out


def _get_case_insensitive(rec: dict[str, Any], key: str | None) -> Any:
    if not key:
        return None
    if key in rec:
        return rec[key]
    lower = key.lower()
    for k, v in rec.items():
        if k.lower() == lower:
            return v
    return None


def _fingerprint_cell(
    value: Any,
    *,
    column: str = "",
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Cell fingerprint — bind-aware when destination types are known."""
    eng = (dest_db_type or "").strip().lower()
    types = dest_types or {}
    ddl = ""
    if column and types:
        ddl = str(types.get(column) or types.get(column.lower()) or "")
        if not ddl:
            for k, v in types.items():
                if str(k).lower() == column.lower():
                    ddl = str(v or "")
                    break
    if eng:
        try:
            # Defined later in this module; resolved at call time.
            # Pass engine even without DDL so Oracle ''↔NULL write rules apply.
            return fingerprint_for_reconcile(value, ddl_type=ddl, engine=eng)
        except Exception:
            pass
    return normalize_cell(value)


def _iter_fingerprints(
    rows: Iterable[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
):
    """Yield (row_key, fingerprint) tuples for each row without materializing the full list.

    When ``dest_db_type`` / ``dest_types`` are set, cells are fingerprinted through
    the same write-path bind helpers as Gate-8 sample compare.
    """
    eng = (dest_db_type or "").strip().lower()
    types = dest_types or {}

    def _fp(val: Any, col: str = "") -> str:
        return _fingerprint_cell(
            val, column=col, dest_db_type=eng, dest_types=types
        )

    if columns is not None:
        cols = columns
        sorted_cols = sorted(cols, key=lambda x: x.lower())
        col_index = {c: i for i, c in enumerate(cols)}
        sort_idx = -1
        if sort_key:
            sort_key_lower = sort_key.lower()
            for i, c in enumerate(cols):
                if c.lower() == sort_key_lower:
                    sort_idx = i
                    break
        for row in rows:
            if isinstance(row, dict):
                parts = [
                    f"{c.lower()}={_fp(row.get(c), c)}" for c in sorted_cols
                ]
                if sort_key:
                    row_key = _fp(row.get(sort_key), sort_key)
                    if row_key is None or row_key == "":
                        for k, v in row.items():
                            if k.lower() == sort_key_lower:
                                row_key = _fp(v, sort_key)
                                break
                else:
                    row_key = ""
            else:
                parts = [
                    f"{c.lower()}={_fp(row[col_index[c]] if col_index[c] < len(row) else None, c)}"
                    for c in sorted_cols
                ]
                row_key = (
                    _fp(
                        row[sort_idx] if sort_idx >= 0 and sort_idx < len(row) else None,
                        sort_key or "",
                    )
                    if sort_key
                    else ""
                )
            fingerprint = "\x1f".join(parts)
            yield (row_key, fingerprint)
    else:
        for row in rows:
            if isinstance(row, dict):
                keys = sorted(row.keys(), key=lambda x: x.lower())
                parts = [f"{k.lower()}={_fp(row.get(k), k)}" for k in keys]
                fingerprint = "\x1f".join(parts)
                row_key = (
                    _fp(_get_case_insensitive(row, sort_key), sort_key or "")
                    if sort_key
                    else ""
                )
            else:
                fingerprint = "|".join(sorted(_fp(v) for v in row))
                row_key = ""
            yield (row_key, fingerprint)


class FingerprintAccumulator:
    """Streaming, order-independent checksum accumulator for arbitrary row counts.

    Keeps fingerprints in memory until ``DATAFLOW_FINGERPRINT_SPILL_THRESHOLD``
    is reached, then spills sorted chunks to disk and merges them at the end.
    This lets the engine compute a strict source checksum for billion-row files
    without holding every row's fingerprint in RAM.
    """

    def __init__(self, threshold: int | None = None) -> None:
        self.threshold = threshold or SPILL_THRESHOLD
        self.buffer: list[tuple[str, str]] = []
        self.chunk_files: list[str] = []
        self.total = 0
        self._tempdir: tempfile.TemporaryDirectory | None = None

    def add(self, key: str, fingerprint: str) -> None:
        self.buffer.append((key, fingerprint))
        self.total += 1
        if len(self.buffer) >= self.threshold:
            self._spill()

    def add_many(self, fingerprints: Iterable[tuple[str, str]]) -> None:
        for key, fingerprint in fingerprints:
            self.add(key, fingerprint)

    def _spill(self) -> None:
        if not self.buffer:
            return
        self.buffer.sort(key=lambda x: (x[0], x[1]))
        if self._tempdir is None:
            self._tempdir = tempfile.TemporaryDirectory(prefix="dataflow_fp_")
        fd, path = tempfile.mkstemp(dir=self._tempdir.name, suffix=".chk")
        with os.fdopen(fd, "wb") as f:
            for key, fp in self.buffer:
                key_b = key.encode("utf-8")
                fp_b = fp.encode("utf-8")
                f.write(struct.pack(">I", len(key_b)))
                f.write(key_b)
                f.write(struct.pack(">I", len(fp_b)))
                f.write(fp_b)
        self.chunk_files.append(path)
        self.buffer = []

    def _read_chunk(self, path: str) -> Iterable[tuple[str, str]]:
        with open(path, "rb") as f:
            while True:
                key_len_b = f.read(4)
                if not key_len_b:
                    break
                key_len = struct.unpack(">I", key_len_b)[0]
                key = f.read(key_len).decode("utf-8")
                fp_len_b = f.read(4)
                if not fp_len_b:
                    break
                fp_len = struct.unpack(">I", fp_len_b)[0]
                fp = f.read(fp_len).decode("utf-8")
                yield (key, fp)

    def _sorted_stream(self) -> Iterable[tuple[str, str]]:
        if not self.chunk_files:
            self.buffer.sort(key=lambda x: (x[0], x[1]))
            yield from self.buffer
            return
        if self.buffer:
            self._spill()
        streams = [self._read_chunk(p) for p in self.chunk_files]
        yield from heapq.merge(*streams, key=lambda x: (x[0], x[1]))

    def digest(self) -> str:
        h = hashlib.sha256()
        for _, fp in self._sorted_stream():
            h.update(fp.encode("utf-8"))
        self.close()
        return h.hexdigest()[:16]

    def close(self) -> None:
        if self._tempdir is not None:
            self._tempdir.cleanup()
            self._tempdir = None
        self.chunk_files = []
        self.buffer = []


def fingerprint_checksum(fingerprints: Iterable[tuple[str, str]]) -> str:
    """Hash a list/iterable of (row_key, fingerprint) tuples.

    For small inputs the in-memory sort+hash path is used; for large or
    streaming inputs an ``FingerprintAccumulator`` spills to disk so the
    checksum stays memory-bounded.
    """
    if isinstance(fingerprints, list) and len(fingerprints) <= SPILL_THRESHOLD:
        return _hash_fingerprints(fingerprints)
    acc = FingerprintAccumulator()
    acc.add_many(fingerprints)
    return acc.digest()


def _hash_fingerprints(fingerprints: list[tuple[str, str]]) -> str:
    fingerprints.sort(key=lambda x: (x[0], x[1]))
    h = hashlib.sha256()
    for _, fp in fingerprints:
        h.update(fp.encode("utf-8"))
    return h.hexdigest()[:16]


def canonical_checksum(
    rows: list[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Stable, order-independent checksum that preserves column identity.

    Accepts either a matrix of values (with an explicit column list) or a list
    of dicts. Column names are included in the row fingerprint so that swapped
    columns cannot collide. Column labels are normalized to lowercase so source
    and target casing differences do not produce false mismatches. When no
    columns are provided, the legacy cell-only fallback is used for matrices.

    Pass ``dest_db_type`` / ``dest_types`` so source wire and destination
    read-back share write-path bind fingerprints (bool/JSON parity).
    """
    if not rows:
        return hashlib.sha256(b"").hexdigest()[:16]
    return _hash_fingerprints(
        list(
            _iter_fingerprints(
                rows,
                columns,
                sort_key=sort_key,
                dest_db_type=dest_db_type,
                dest_types=dest_types,
            )
        )
    )


def canonical_checksum_from_iter(
    rows: Iterable[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    limit: int = 0,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Streaming variant of canonical_checksum with optional sample limit.

    Reads rows lazily and hashes fingerprints in sorted order. A limit of 0
    means process all rows.

    Accumulation goes through :class:`FingerprintAccumulator`, which spills
    sorted chunks to disk past a threshold. It used to append every
    ``(row_key, fingerprint)`` pair to a plain list, which made the strict
    reconcile — the default validation mode, and the one that passes
    ``limit=0`` — allocate roughly 250 bytes per destination row. That is about
    5 GB at 20M rows, and the OOM landed in the *verification* step after the
    data had already been written. The digest is unchanged: with no spill the
    accumulator sorts and hashes exactly as before.
    """
    acc = FingerprintAccumulator()
    for i, (row_key, fp) in enumerate(
        _iter_fingerprints(
            rows,
            columns,
            sort_key=sort_key,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
    ):
        if limit and i >= limit:
            break
        acc.add(row_key, fp)
    return acc.digest()


def checksum_rows(
    rows: list[Any],
    columns: list[str] | None = None,
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Canonical, order-independent checksum over a matrix or list of dicts."""
    return canonical_checksum(
        rows, columns, dest_db_type=dest_db_type, dest_types=dest_types
    )


def aggregate_checksum(
    records: list[dict[str, Any]],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Order-independent checksum for reconciliation with column identity."""
    return canonical_checksum(
        records,
        columns,
        sort_key=sort_key,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )


def reconcile(
    *,
    source_rows: int,
    target_rows: int,
    source_checksum: str,
    target_checksum: str,
    rejected_rows: int = 0,
    strict_checksum: bool = True,
    allow_extra_rows: bool = False,
    sample_compare: dict[str, Any] | None = None,
    coerced_null_rows: int = 0,
    rows_skipped: int = 0,
) -> ReconciliationReport:
    coerced_null_rows = max(int(coerced_null_rows or 0), 0)
    rows_skipped = max(int(rows_skipped or 0), 0)
    # Coerced rows are KEPT in the destination (a cell became NULL), so they do
    # not lower the expected row count — only genuinely DROPPED / held-out rows do.
    # Under quarantine, bad rows are held out of the primary write (rejected >
    # coerced); under coerce_null, rejected == coerced and dropped == 0.
    # Under fail, coerced == 0 so dropped == rejected.
    # Skipped rows are neither dropped nor written (e.g. stale CDC LSN
    # redelivery) and must be excluded from the expected destination count.
    dropped_rows = max(max(rejected_rows, 0) - coerced_null_rows, 0)
    expected_rows = max(source_rows - dropped_rows - rows_skipped, 0)
    row_count_ok = target_rows == expected_rows or (
        allow_extra_rows and target_rows >= expected_rows
    )
    if not row_count_ok:
        extra_note = (
            f" (target has {target_rows - expected_rows} extra rows)"
            if target_rows > expected_rows
            else ""
        )
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                f"Row count mismatch: source {source_rows}, rejected {rejected_rows}, "
                f"skipped {rows_skipped}, expected target {expected_rows} vs target {target_rows}{extra_note}"
            ),
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
        )

    if sample_compare and not sample_compare.get("passed", True):
        mismatches = sample_compare.get("mismatches") or []
        detail = mismatches[0] if mismatches else "value mismatch in read-back sample"
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=f"Read-back sample verification failed: {detail}",
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            sample_compare=sample_compare,
        )

    if source_checksum != target_checksum:
        # Enterprise GA: checksum mismatch always fails Gate-8.
        # Sample compare may attach diagnostics only — never green-pass / override.
        compared = int((sample_compare or {}).get("compared") or 0)
        sample_ok = (
            bool(sample_compare)
            and bool(sample_compare.get("passed", False))
            and compared > 0
        )
        extra_note = ""
        if allow_extra_rows and target_rows > expected_rows:
            extra_note = (
                f" Destination has {target_rows - expected_rows} extra row(s) "
                "(append/upsert); whole-table digests are not comparable."
            )
        sample_note = ""
        if sample_ok:
            sample_note = (
                f" Key-aligned sample compared {compared} row(s) without value "
                "mismatches — diagnostic only; does NOT override checksum failure."
            )
        elif sample_compare:
            sample_note = (
                " Key-aligned sample compare incomplete or failed — not used to "
                "soften checksum mismatch."
            )
        mode_label = "strict" if strict_checksum else "balanced"
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                f"Checksum mismatch ({mode_label}): source {source_checksum} vs "
                f"target {target_checksum}.{extra_note}{sample_note} "
                "Sample success cannot override full-table checksum mismatch."
            ),
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            sample_compare=sample_compare,
            checksum_match=False,
            population_proof=False,
            assurance_level="none",
        )
    if coerced_null_rows:
        # Row counts and checksums can still match here because the SAME failed
        # coercion is applied when re-reading the source for its checksum. That
        # does NOT mean the destination holds the original values — it holds
        # NULLs. Be explicit rather than claiming full fidelity.
        message = (
            f"Transfer completed but NOT full fidelity: {coerced_null_rows} row(s) had a value "
            f"coerced to NULL because it could not be cast to the target type"
        )
        if rejected_rows and rejected_rows != coerced_null_rows:
            message += f"; {rejected_rows} row(s) rejected"
    else:
        message = f"Row fidelity verified — source and target checksums match ({target_rows} rows)"
        if rejected_rows:
            message = f"Transfer verified ({target_rows} rows written, {rejected_rows} rejected)"
    return ReconciliationReport(
        passed=True,
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        message=message,
        rejected_rows=rejected_rows,
        coerced_null_rows=coerced_null_rows,
        rows_skipped=rows_skipped,
        sample_compare=sample_compare,
        checksum_match=True,
        population_proof=False,
        assurance_level="full_checksum",
    )


def _iter_fetchmany(cur, batch_size: int = 5000):
    """Yield rows from a DBAPI cursor without loading the full result set."""
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield row


def verify_postgres_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(
            table_name, schema or "public", dialect="postgresql"
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
            cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
            names = [d[0] for d in cur.description] if cur.description else []
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                _iter_fetchmany(cur),
                columns,
                limit=limit,
                dest_db_type="postgresql",
                dest_types=dest_types,
            )
        conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_pgvector_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Verify pgvector destination by reading source_id/metadata, not the opaque embedding."""
    try:
        from connectors.postgresql_conn import get_connection
        from connectors.sql_identifiers import quote_table_ref

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        table_ref = quote_table_ref(
            table_name, schema or "public", dialect="postgresql"
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
            cur.execute(f"SELECT source_id, metadata FROM {table_ref}")  # nosec B608
            names = (
                [d[0] for d in cur.description]
                if cur.description
                else ["source_id", "metadata"]
            )
            rows: list[dict[str, Any]] = []
            for raw in _iter_fetchmany(cur):
                rec = dict(zip(names, raw))
                source_id = rec.get("source_id", "")
                metadata = rec.get("metadata") or {}
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except Exception:
                        metadata = {}
                if not isinstance(metadata, dict):
                    metadata = {}
                # Reconstruct a source-shaped row from metadata; fall back to source_id for 'id'.
                row: dict[str, Any] = dict(metadata)
                if target_columns:
                    for col in target_columns:
                        if col not in row and col.lower() == "id" and source_id:
                            row[col] = source_id
                    row = {k: v for k, v in row.items() if k in target_columns}
                elif source_id and "id" not in {c.lower() for c in row}:
                    row["id"] = source_id
                rows.append(row)
        conn.close()
        checksum = canonical_checksum_from_iter(
            iter(rows),
            columns=target_columns or None,
            limit=limit,
        )
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_pinecone_namespace(
    *,
    host: str = "",
    connection_string: str = "",
    username: str = "",
    password: str = "",
    api_key: str = "",
    namespace: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Pinecone stats + metadata fetch for Gate-8 (embeddings opaque)."""
    try:
        from connectors.pinecone_writer import _headers, _index_url, _requests_session

        index_url = _index_url(host, connection_string)
        key = api_key or password or username or ""
        if not index_url or not key:
            return -1, ""
        session = _requests_session()
        hdrs = _headers(key)
        ns = (namespace or "").strip()
        stats = session.post(
            f"{index_url}/describe_index_stats",
            data=json.dumps({}),
            headers=hdrs,
            timeout=30,
        )
        if stats.status_code not in {200, 201}:
            return -1, ""
        body = stats.json() if stats.content else {}
        namespaces = body.get("namespaces") or {}
        if ns:
            count = int((namespaces.get(ns) or {}).get("vector_count") or 0)
        else:
            count = int(body.get("totalVectorCount") or body.get("total_vector_count") or 0)
            if not count and isinstance(namespaces, dict):
                count = sum(int((v or {}).get("vector_count") or 0) for v in namespaces.values())

        dict_rows: list[dict[str, Any]] = []
        # limit=0 means "full batch" for Gate-8 — still cap fetch for API safety.
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        if ids:
            params = [("ids", i) for i in ids]
            if ns:
                params.append(("namespace", ns))
            # requests supports list of tuples for repeated ids=
            fetch = session.get(
                f"{index_url}/vectors/fetch",
                params=params,
                headers=hdrs,
                timeout=30,
            )
            if fetch.status_code in {200, 201}:
                vectors = (fetch.json() or {}).get("vectors") or {}
                for vid, payload in vectors.items():
                    meta = payload.get("metadata") if isinstance(payload, dict) else {}
                    if not isinstance(meta, dict):
                        meta = {}
                    dict_rows.append({"id": vid, **meta})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="pinecone",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Pinecone reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_qdrant_collection(
    *,
    host: str = "",
    port: int = 6333,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    collection: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Qdrant collection count + payload scroll/retrieve for Gate-8."""
    try:
        from connectors.qdrant_writer import _base_url, _headers, _requests_session

        api_key = password or username or ""
        base_url = connection_string.rstrip("/") if connection_string else _base_url(host, port, ssl)
        session = _requests_session()
        hdrs = _headers(api_key)
        info = session.get(f"{base_url}/collections/{collection}", headers=hdrs, timeout=15)
        if info.status_code != 200:
            return -1, ""
        body = info.json() or {}
        result = body.get("result") or body
        count = int(
            (result.get("points_count") if isinstance(result, dict) else None)
            or (result.get("indexed_vectors_count") if isinstance(result, dict) else None)
            or 0
        )
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        dict_rows: list[dict[str, Any]] = []
        if ids:
            retrieve = session.post(
                f"{base_url}/collections/{collection}/points",
                data=json.dumps({
                    "ids": ids,
                    "with_payload": True,
                    "with_vector": False,
                }),
                headers=hdrs,
                timeout=30,
            )
            if retrieve.status_code in {200, 201}:
                points = (retrieve.json() or {}).get("result") or []
                for pt in points:
                    if not isinstance(pt, dict):
                        continue
                    payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                    dict_rows.append({"id": pt.get("id"), **payload})
        if not dict_rows:
            scroll = session.post(
                f"{base_url}/collections/{collection}/points/scroll",
                data=json.dumps({
                    "limit": max(1, id_cap if ids else int(limit or 100) or 100),
                    "with_payload": True,
                    "with_vector": False,
                }),
                headers=hdrs,
                timeout=30,
            )
            if scroll.status_code in {200, 201}:
                points = ((scroll.json() or {}).get("result") or {}).get("points") or []
                for pt in points:
                    if not isinstance(pt, dict):
                        continue
                    payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                    dict_rows.append({"id": pt.get("id"), **payload})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="qdrant",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Qdrant reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_weaviate_class(
    *,
    host: str = "",
    port: int = 8080,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    class_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Weaviate aggregate + object list for Gate-8."""
    try:
        from connectors.weaviate_writer import (
            _base_url,
            _class_name,
            _headers,
            _requests_session,
        )

        key = api_key or password or username or ""
        base_url = _base_url(host, port, ssl, connection_string)
        cls = _class_name(class_name)
        session = _requests_session()
        hdrs = _headers(key)
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        dict_rows: list[dict[str, Any]] = []
        if ids:
            for oid in ids:
                resp = session.get(
                    f"{base_url}/v1/objects/{cls}/{oid}",
                    headers=hdrs,
                    timeout=15,
                )
                if resp.status_code not in {200, 201}:
                    # Older Weaviate: /v1/objects/{uuid}
                    resp = session.get(
                        f"{base_url}/v1/objects/{oid}",
                        headers=hdrs,
                        timeout=15,
                    )
                if resp.status_code not in {200, 201}:
                    continue
                obj = resp.json() or {}
                if not isinstance(obj, dict):
                    continue
                props = obj.get("properties") if isinstance(obj.get("properties"), dict) else {}
                dict_rows.append({"id": obj.get("id") or oid, **props})
        if not dict_rows:
            agg = session.get(
                f"{base_url}/v1/objects",
                params={"class": cls, "limit": max(1, int(limit or 100) or 100)},
                headers=hdrs,
                timeout=30,
            )
            if agg.status_code not in {200, 201}:
                return -1, ""
            body = agg.json() or {}
            objects = body.get("objects") or []
            count = int(body.get("totalResults") or len(objects))
            for obj in objects:
                if not isinstance(obj, dict):
                    continue
                props = obj.get("properties") if isinstance(obj.get("properties"), dict) else {}
                dict_rows.append({"id": obj.get("id"), **props})
        else:
            count = len(dict_rows)
            # Prefer class cardinality when list endpoint works.
            try:
                agg = session.get(
                    f"{base_url}/v1/objects",
                    params={"class": cls, "limit": 1},
                    headers=hdrs,
                    timeout=15,
                )
                if agg.status_code in {200, 201}:
                    count = int((agg.json() or {}).get("totalResults") or count)
            except Exception:
                pass
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="weaviate",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Weaviate reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_milvus_collection(
    *,
    host: str = "",
    port: int = 19530,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    database: str = "",
    collection: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Milvus get_collection_stats + query for Gate-8."""
    try:
        from connectors.milvus_writer import (
            _auth_token,
            _base_url,
            _collection_name,
            _headers,
            _ok_response,
            _requests_session,
        )

        coll = _collection_name(collection)
        db_name = (database or "").strip()
        if db_name.lower() in {"", "test_db", "default", "public"}:
            db_name = ""
        token = _auth_token(api_key=api_key, username=username, password=password)
        base_url = _base_url(host, port, ssl, connection_string)
        session = _requests_session()
        hdrs = _headers(token)
        stats_payload: dict[str, Any] = {"collectionName": coll}
        if db_name:
            stats_payload["dbName"] = db_name
        stats = session.post(
            f"{base_url}/v2/vectordb/collections/get_stats",
            data=json.dumps(stats_payload),
            headers=hdrs,
            timeout=30,
        )
        body = stats.json() if stats.content else {}
        if not _ok_response(body if isinstance(body, dict) else {}, stats.status_code):
            return -1, ""
        data = body.get("data") if isinstance(body, dict) else {}
        count = int(
            (data or {}).get("rowCount")
            or (data or {}).get("row_count")
            or (data or {}).get("row_num")
            or 0
        )
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        query_payload: dict[str, Any] = {
            "collectionName": coll,
            "outputFields": ["id", "content", "source_id", "chunk_index"],
            "limit": max(1, id_cap if ids else int(limit or 100) or 100),
        }
        if ids:
            # Prefer keyed filter when writer stamped identities.
            quoted = ", ".join(json.dumps(i) for i in ids)
            query_payload["filter"] = f"id in [{quoted}]"
        else:
            query_payload["filter"] = ""
        if db_name:
            query_payload["dbName"] = db_name
        query = session.post(
            f"{base_url}/v2/vectordb/entities/query",
            data=json.dumps(query_payload),
            headers=hdrs,
            timeout=30,
        )
        qbody = query.json() if query.content else {}
        dict_rows: list[dict[str, Any]] = []
        if _ok_response(qbody if isinstance(qbody, dict) else {}, query.status_code):
            rows = qbody.get("data") if isinstance(qbody, dict) else []
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        dict_rows.append({k: v for k, v in row.items() if k != "vector"})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="milvus",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Milvus reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_mysql_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    try:
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(table_name, dialect="mysql")
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
            cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
            names = [d[0] for d in cur.description] if cur.description else []
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                _iter_fetchmany(cur),
                columns,
                limit=limit,
                dest_db_type="mysql",
                dest_types=dest_types,
            )
        conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_sqlserver_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "dbo",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent SQL Server / Azure SQL Edge read-back for Gate-8 reconcile."""
    try:
        import pymssql

        from connectors.sql_identifiers import quote_table_ref

        conn = pymssql.connect(
            server=host or "127.0.0.1",
            port=int(port or 1433),
            user=username or "sa",
            password=password or "",
            database=database or "master",
            login_timeout=10,
            timeout=30,
        )
        sch = (schema or "dbo").strip() or "dbo"
        table_ref = quote_table_ref(table_name, schema=sch, dialect="sqlserver")
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
            cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
            names = [d[0] for d in cur.description] if cur.description else []
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                _iter_fetchmany(cur),
                columns,
                limit=limit,
                dest_db_type="sqlserver",
                dest_types=dest_types,
            )
        finally:
            cur.close()
            conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("SQL Server reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_oracle_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Oracle read-back for Gate-8 (write-location ''≡NULL fingerprints)."""
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "oracle",
            "host": host or "",
            "port": int(port or 1521),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
        }
        engine = get_sqlalchemy_engine(cfg)
        sch = (schema or username or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect="oracle")
        with engine.connect() as conn:
            count = int(
                conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
                or 0
            )
            result = conn.execute(sa.text(f"SELECT * FROM {table_ref}"))  # nosec B608
            names = list(result.keys())
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                (tuple(row) for row in result),
                columns,
                limit=limit,
                dest_db_type="oracle",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("Oracle reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_bigquery_table(
    *,
    project_id: str,
    dataset_id: str,
    connection_string: str,
    host: str = "",
    port: int = 0,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    try:
        from connectors.bigquery_conn import get_client

        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            host=host,
            port=port,
        )
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table = client.get_table(table_id)
        count = table.num_rows or 0
        field_names = [field.name for field in table.schema] if table.schema else []
        columns = field_names or target_columns or []

        def _row_iter():
            yielded = 0
            for row in client.list_rows(table_id):
                if limit and yielded >= limit:
                    break
                yield list(row.values()) if hasattr(row, "values") else list(row)
                yielded += 1

        return int(count), canonical_checksum_from_iter(
            _row_iter(), columns, limit=limit
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def _rows_from_object_bytes(
    body: bytes, key: str, columns: list[str] | None = None
) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse S3/GCS object payload (JSON, JSONL, CSV) into dict rows and headers."""
    import csv
    import io

    text = body.decode("utf-8", errors="replace")
    lower_key = (key or "").lower()

    if lower_key.endswith(".csv"):
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        headers = reader.fieldnames or []
        return rows, headers

    if lower_key.endswith(".jsonl"):
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(parsed)
            else:
                rows.append({"value": parsed})
        headers = sorted(set(k for r in rows for k in r.keys())) if rows else []
        return rows, headers or (columns or [])

    # Default: JSON array or single JSON object.
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = []
    if isinstance(data, list):
        rows = [r for r in data if isinstance(r, dict)]
        headers = sorted(set(k for r in rows for k in r.keys())) if rows else []
        return rows, headers or (columns or [])
    if isinstance(data, dict):
        return [data], sorted(data.keys())
    return [], columns or []


def verify_s3_object(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile an S3 object by downloading and parsing its contents.

    Multi-chunk writers emit ``part-*`` keys under a stem prefix; aggregate
    those parts when present so Gate-8 does not fall through to writer-ack
    while most rows live only in part objects.
    """
    try:
        from connectors.aws_common import boto3_client
        from connectors.object_store_common import (
            normalize_object_base_key,
            object_parts_prefix,
            object_store_read_keys,
        )
        from connectors.s3_reader import list_objects

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "ssl": ssl,
            "database": bucket,
        }
        client = boto3_client("s3", cfg)
        base = normalize_object_base_key(key)
        parts_prefix = object_parts_prefix(base)
        listed = list_objects(cfg, bucket, parts_prefix) if parts_prefix else []
        read_keys = object_store_read_keys(base, listed)
        all_rows: list[dict[str, Any]] = []
        headers: list[str] = []
        for obj_key in read_keys:
            body = client.get_object(Bucket=bucket, Key=obj_key)["Body"].read()
            rows, hdrs = _rows_from_object_bytes(body, obj_key, target_columns)
            if not headers:
                headers = list(hdrs or [])
            all_rows.extend(rows)
        columns = headers or target_columns or []
        return len(all_rows), canonical_checksum_from_iter(all_rows, columns, limit=limit)
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_sftp_object(
    *,
    host: str = "",
    port: int = 22,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    database: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent SFTP download + parse for Gate-8 (parity with S3/GCS/ADLS)."""
    try:
        from connectors.sftp_common import connect_sftp, parse_sftp_config

        cfg = parse_sftp_config(
            connection_string=connection_string,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            table=table_name,
        )
        if not cfg.host or not cfg.path:
            return -1, ""
        transport, sftp = connect_sftp(cfg)
        try:
            with sftp.file(cfg.path, "rb") as fh:
                body = fh.read()
        finally:
            sftp.close()
            transport.close()
        rows, headers = _rows_from_object_bytes(body, cfg.path, target_columns)
        columns = headers or target_columns or []
        return len(rows), canonical_checksum_from_iter(
            rows,
            columns,
            limit=limit,
            dest_db_type="sftp",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("SFTP reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_gcs_blob(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    connection_string: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a GCS blob by downloading and parsing its contents."""
    try:
        from connectors.gcs_common import gcs_client
        from connectors.gcs_reader import list_objects
        from connectors.object_store_common import (
            normalize_object_base_key,
            object_parts_prefix,
            object_store_read_keys,
        )

        cfg = {
            "host": host,
            "port": port,
            "connection_string": connection_string,
        }
        client = gcs_client(cfg)
        base = normalize_object_base_key(key)
        parts_prefix = object_parts_prefix(base)
        listed = list_objects(cfg, bucket, parts_prefix) if parts_prefix else []
        read_keys = object_store_read_keys(base, listed)
        bucket_obj = client.bucket(bucket)
        all_rows: list[dict[str, Any]] = []
        headers: list[str] = []
        for obj_key in read_keys:
            body = bucket_obj.blob(obj_key).download_as_bytes()
            rows, hdrs = _rows_from_object_bytes(body, obj_key, target_columns)
            if not headers:
                headers = list(hdrs or [])
            all_rows.extend(rows)
        columns = headers or target_columns or []
        return len(all_rows), canonical_checksum_from_iter(all_rows, columns, limit=limit)
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_adls_blob(
    *,
    container: str,
    key: str,
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    service_account: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Azure Blob / ADLS Gen2 read-back (parity with S3/GCS Gate-8)."""
    try:
        from connectors.adls_common import blob_service_client
        from connectors.adls_reader import list_objects
        from connectors.object_store_common import (
            normalize_object_base_key,
            object_parts_prefix,
            object_store_read_keys,
        )

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "service_account": service_account,
            "database": container,
        }
        client = blob_service_client(cfg)
        base = normalize_object_base_key(key)
        parts_prefix = object_parts_prefix(base)
        listed = list_objects(cfg, container, parts_prefix) if parts_prefix else []
        read_keys = object_store_read_keys(base, listed)
        all_rows: list[dict[str, Any]] = []
        headers: list[str] = []
        for obj_key in read_keys:
            body = client.get_blob_client(container, obj_key).download_blob().readall()
            rows, hdrs = _rows_from_object_bytes(body, obj_key, target_columns)
            if not headers:
                headers = list(hdrs or [])
            all_rows.extend(rows)
        columns = headers or target_columns or []
        return len(all_rows), canonical_checksum_from_iter(
            all_rows,
            columns,
            limit=limit,
            dest_db_type="adls",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("ADLS reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_hubspot_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent HubSpot CRM read-back for Gate-8 (reverse-ETL honesty)."""
    try:
        from connectors.hubspot import read_object

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "table": object_name,
            "database": object_name,
        }
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        rows = list(batch.rows or [])
        # Convert matrix rows to dicts when ReadBatch stores tuples.
        dict_rows: list[Any] = []
        for row in rows:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
            else:
                dict_rows.append(row)
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="hubspot",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("HubSpot reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_salesforce_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Salesforce SOQL read-back for Gate-8 reverse-ETL."""
    try:
        from connectors.salesforce import read_object

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
        }
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                # Strip Salesforce attributes envelope when present.
                d = {k: v for k, v in row.items() if k != "attributes"}
                dict_rows.append(d)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="salesforce",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Salesforce reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_airtable_table(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    base_id: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Airtable read-back — flatten fields for Gate-8 fingerprints."""
    try:
        from connectors.airtable import read_object
        from connectors.saas_typed_schema import flatten_airtable_record

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string or base_id,
            "api_key": api_key,
            "database": base_id,
            "table": table_name,
            "type": "airtable",
        }
        batch = read_object(
            cfg=cfg,
            object=table_name,
            limit=max(int(limit or 500), 1),
        )
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                flat, _schema = flatten_airtable_record(row)
                dict_rows.append(flat)
            elif batch.headers:
                headers = list(batch.headers)
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="airtable",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Airtable reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def _verify_saas_read_object(
    *,
    driver: str,
    read_object: Any,
    cfg: dict[str, Any],
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    row_transform: Any = None,
) -> tuple[int, str]:
    """Shared CRM/commerce Gate-8 path: call connector read_object → checksum."""
    try:
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                d = row_transform(row) if row_transform else dict(row)
                dict_rows.append(d)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type=driver,
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("%s reconciliation read-back failed: %s", driver, exc, exc_info=exc)
        return -1, ""


def verify_stripe_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Stripe API read-back for Gate-8 reverse-ETL."""
    from connectors.stripe import read_object

    return _verify_saas_read_object(
        driver="stripe",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "type": "stripe",
        },
        object_name=object_name or "customers",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_shopify_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Shopify Admin REST read-back for Gate-8 reverse-ETL."""
    from connectors.shopify import read_object

    return _verify_saas_read_object(
        driver="shopify",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "shop": host,
            "type": "shopify",
        },
        object_name=object_name or "customers",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_zendesk_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Zendesk Support API read-back for Gate-8 reverse-ETL."""
    from connectors.zendesk import read_object

    return _verify_saas_read_object(
        driver="zendesk",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "type": "zendesk",
        },
        object_name=object_name or "tickets",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_notion_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    database_id: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Notion database/page read-back for Gate-8 reverse-ETL."""
    from connectors.notion import read_object

    return _verify_saas_read_object(
        driver="notion",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": database_id or object_name,
            "type": "notion",
        },
        object_name=object_name or database_id or "",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_kafka_topic(
    *,
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    topic: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    schema_registry_url: str = "",
) -> tuple[int, str]:
    """Bounded Kafka consume for Gate-8 — sample proof, not topic cardinality."""
    try:
        from connectors.kafka_reader import read_topic_batch

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "database": topic,
            "table": topic,
            "group_id": f"dataflow-gate8-verify-{abs(hash(topic)) % 10_000_000}",
            "auto_offset_reset": "earliest",
            "schema_registry_url": schema_registry_url,
        }
        batch, _cursor = read_topic_batch(
            cfg=cfg,
            topic=topic,
            columns=target_columns,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        rows = list(batch.rows or [])
        dict_rows: list[Any] = []
        for row in rows:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or []
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="kafka",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Kafka reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_databricks_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "",
    table_name: str,
    http_path: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Databricks SQL warehouse read-back for Gate-8."""
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "databricks",
            "host": host or "",
            "port": int(port or 443),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
            "http_path": http_path or "",
        }
        engine = get_sqlalchemy_engine(cfg)
        sch = (schema or database or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect="ansi")
        with engine.connect() as conn:
            count = int(
                conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
                or 0
            )
            result = conn.execute(sa.text(f"SELECT * FROM {table_ref}"))  # nosec B608
            names = list(result.keys())
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                (tuple(row) for row in result),
                columns,
                limit=limit,
                dest_db_type="databricks",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("Databricks reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_sqlite_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    host: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a SQLite target by reading the local file."""
    try:
        import sqlite3

        from connectors.sqlite_common import sqlite_file_path

        path = sqlite_file_path(database, connection_string, host)
        if not path:
            return -1, ""
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(table_name, dialect="sqlite")
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
        count = cur.fetchone()[0]
        cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
        names = [d[0] for d in cur.description] if cur.description else []
        columns = names or target_columns or []
        checksum = canonical_checksum_from_iter(
            _iter_fetchmany(cur), columns, limit=limit
        )
        conn.close()
        return int(count), checksum
    except sqlite3.OperationalError as exc:
        # Missing table means the target is empty, not that verification is
        # unavailable. Return 0 so reconciliation can surface the mismatch.
        if "no such table" in str(exc).lower():
            return 0, ""
        return -1, ""
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_duckdb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile a DuckDB target by reading the local file."""
    try:
        import duckdb

        path = connection_string or database
        if not path:
            return -1, ""
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(table_name, dialect="duckdb")
        conn = duckdb.connect(str(path))
        count = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()[0]  # nosec B608
        cur = conn.execute(f"SELECT * FROM {table_ref}")  # nosec B608
        names = [d[0] for d in cur.description] if cur.description else []
        columns = names or target_columns or []
        checksum = canonical_checksum_from_iter(
            _iter_fetchmany(cur), columns, limit=limit
        )
        conn.close()
        return int(count), checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_clickhouse_table(
    *,
    host: str = "",
    port: int = 9000,
    database: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    schema: str = "",
    ssl: bool = False,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent ClickHouse read-back with ``SELECT … FINAL`` (Airbyte-class).

    ReplacingMergeTree upsert is lazy — Gate-8 without FINAL would fingerprint
    duplicate keys and false-fail checksum reconcile.
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import (
            clickhouse_final_table_sql,
            get_sqlalchemy_engine,
        )
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "clickhouse",
            "host": host or "",
            "port": int(port or 9000),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
            "ssl": bool(ssl),
        }
        engine = get_sqlalchemy_engine(cfg)
        table_ref = quote_table_ref(
            table_name,
            schema=schema or None,
            dialect="clickhouse",
        )
        from_sql = clickhouse_final_table_sql(table_ref)
        with engine.connect() as conn:
            count = int(
                conn.execute(
                    sa.text(f"SELECT count() FROM {from_sql}")  # nosec B608
                ).scalar()
                or 0
            )
            result = conn.execute(sa.text(f"SELECT * FROM {from_sql}"))  # nosec B608
            names = list(result.keys())
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                (tuple(row) for row in result),
                columns,
                limit=limit,
                dest_db_type="clickhouse",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("ClickHouse reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_generic_sql_table(
    *,
    dest: dict[str, Any],
    schema: str = "",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    engine_hint: str = "",
) -> tuple[int, str]:
    """Independent SQLAlchemy read-back for catalog ``generic_sql`` engines.

    Covers Teradata / HANA / Vertica / Informix / Trino / Firebird / Netezza and
    other SQLAlchemy-backed SKUs that share ``get_sqlalchemy_engine``. Returns
    ``(-1, "")`` only when the engine cannot connect or the table is missing —
    never invent a checksum from writer rowcounts (Gate-8 honesty).
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        hint = (
            (engine_hint or dest.get("type") or dest.get("engine") or "generic_sql")
            .strip()
            .lower()
        )
        cfg = dict(dest)
        cfg["type"] = hint if hint and hint != "generic_sql" else (
            str(dest.get("type") or "generic_sql")
        )
        engine = get_sqlalchemy_engine(cfg)
        dialect = (
            getattr(getattr(engine, "dialect", None), "name", None) or hint or "ansi"
        ).lower()
        sch = (schema or dest.get("schema") or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect=dialect)
        with engine.connect() as conn:
            count = int(
                conn.execute(
                    sa.text(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
                ).scalar()
                or 0
            )
            result = conn.execute(sa.text(f"SELECT * FROM {table_ref}"))  # nosec B608
            names = list(result.keys())
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                (tuple(row) for row in result),
                columns,
                limit=limit,
                dest_db_type=hint or dialect,
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning(
            "generic_sql reconciliation read-back failed (%s): %s",
            engine_hint or dest.get("type"),
            exc,
            exc_info=exc,
        )
        return -1, ""


def verify_iceberg_table(
    *,
    connection_string: str = "",
    warehouse: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile an Iceberg table by scanning the catalog and fingerprinting rows."""
    try:
        from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

        endpoint = {
            "connection_string": connection_string or "",
            "warehouse": warehouse or "",
            "table": table_name,
            "table_name": table_name,
        }
        cfg = parse_iceberg_catalog_config(endpoint)
        catalog = load_catalog(endpoint)
        identifier = cfg["namespace"] + (cfg["table_name"],)
        tbl = catalog.load_table(identifier)
        count = tbl.scan().count()
        arrow = tbl.scan().to_arrow()
        if limit and len(arrow) > limit:
            arrow = arrow.slice(0, limit)
        rows = arrow.to_pylist()
        columns = target_columns or list(arrow.column_names)
        checksum = fingerprint_checksum(_iter_fingerprints(rows, columns))
        return int(count), checksum
    except Exception as exc:
        logger.warning("Iceberg reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_mongodb_collection(
    *,
    host: str = "",
    port: int = 27017,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    database: str = "",
    ssl: bool = False,
    auth_source: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Reconcile a MongoDB target by counting and fingerprinting documents."""
    try:
        from connectors.mongodb_common import (
            _mongo_client,
            normalize_mongodb_connection_string,
        )

        conn_str = normalize_mongodb_connection_string(
            connection_string or "",
            database=database,
            host=host,
            port=port,
            username=username,
            password=password,
            ssl=ssl,
            auth_source=auth_source,
        )
        client = _mongo_client(conn_str)
        db = client[database or "test"]
        coll = db[table_name]
        count = coll.count_documents({})

        def _doc_iter():
            yielded = 0
            for doc in coll.find({}):
                if limit and yielded >= limit:
                    break
                yield doc
                yielded += 1

        columns = target_columns or sorted(
            set(k for doc in coll.find({}).limit(100) for k in doc.keys())
        )
        checksum = canonical_checksum_from_iter(
            _doc_iter(),
            columns,
            limit=limit,
            dest_db_type="mongodb",
            dest_types=dest_types,
        )
        return int(count), checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_redis_prefix(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    prefix: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile Redis keys under ``prefix:*`` (writer key layout)."""
    try:
        from connectors.redis_reader import _decode, _redis_client

        client = _redis_client(
            {
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "ssl": ssl,
            }
        )
        pattern = f"{prefix}:*" if prefix else "*"
        keys: list[str] = []
        cursor = 0
        while True:
            cursor, batch = client.scan(cursor=cursor, match=pattern, count=500)
            for raw in batch:
                keys.append(raw.decode() if isinstance(raw, bytes) else str(raw))
            if cursor == 0:
                break

        def _row_iter():
            for key in keys:
                raw = client.get(key)
                text = _decode(raw)
                try:
                    payload = (
                        json.loads(text) if text.startswith("{") else {"value": text}
                    )
                except (json.JSONDecodeError, TypeError):
                    payload = {"value": text}
                if isinstance(payload, dict):
                    yield payload
                else:
                    yield {"value": text}

        columns = target_columns or []
        if not columns and keys:
            sample = next(_row_iter(), {})
            columns = sorted(sample.keys()) if sample else ["value"]
        checksum = canonical_checksum_from_iter(_row_iter(), columns, limit=limit)
        return len(keys), checksum
    except Exception as exc:
        logger.warning("Redis reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_elasticsearch_index(
    *,
    host: str = "",
    port: int = 9200,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    index: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Elasticsearch search read-back for Gate-8."""
    try:
        from connectors.elasticsearch_reader import read_index_batch

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "ssl": ssl,
            "api_key": api_key,
        }
        batch, _ = read_index_batch(
            cfg=cfg,
            index=index,
            columns=target_columns,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        # Prefer index cardinality when available (batch may be LIMIT-capped).
        count = len(dict_rows)
        try:
            from connectors.elasticsearch_reader import _client

            es = _client(cfg)
            try:
                count = int(es.count(index=index).get("count", count))
            finally:
                es.close()
        except Exception:
            pass
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="elasticsearch",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Elasticsearch reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_snowflake_table(
    *,
    host: str,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    try:
        from connectors.snowflake_conn import (
            get_connection,
            normalize_account,
            resolve_or_fold_snowflake_table,
            snowflake_qualified_table,
        )

        conn = get_connection(
            account=normalize_account(host),
            username=username,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            connection_string=connection_string,
        )
        from connectors.sql_identifiers import (
            quote_sql_identifier,
            require_safe_identifier,
        )

        with conn.cursor() as cur:
            if warehouse:
                try:
                    wh = require_safe_identifier(warehouse, preserve_case=True)
                    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")
                except Exception as exc:
                    logging.getLogger(__name__).warning(
                        "Exception suppressed: %s", exc, exc_info=exc
                    )
            resolved = resolve_or_fold_snowflake_table(
                cur, schema or "PUBLIC", table_name
            )
            qualified_name = snowflake_qualified_table(schema or "PUBLIC", resolved)
            cur.execute(f"SELECT COUNT(*) FROM {qualified_name}")  # nosec B608
            count = int(cur.fetchone()[0])
            cur.execute(f"SELECT * FROM {qualified_name}")  # nosec B608
            names = [d[0] for d in cur.description] if cur.description else []
            columns = names or target_columns or []
            checksum = canonical_checksum_from_iter(
                _iter_fetchmany(cur),
                columns,
                limit=limit,
                dest_db_type="snowflake",
                dest_types=dest_types,
            )
        conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_dynamodb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    username: str = "local",
    password: str = "local",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Reconcile a DynamoDB target by Scan count and item fingerprint."""
    try:
        import boto3
        from boto3.dynamodb.types import TypeDeserializer

        endpoint = connection_string or "http://localhost:8000"
        client = boto3.client(
            "dynamodb",
            endpoint_url=endpoint,
            aws_access_key_id=username.strip() or "local",
            aws_secret_access_key=password.strip() or "local",
            region_name="us-east-1",
        )
        paginator = client.get_paginator("scan")
        count = sum(
            page["Count"]
            for page in paginator.paginate(TableName=table_name, Select="COUNT")
        )

        deserializer = TypeDeserializer()

        def _item_iter():
            yielded = 0
            for page in paginator.paginate(TableName=table_name):
                for item in page.get("Items", []):
                    if limit and yielded >= limit:
                        break
                    yield {k: deserializer.deserialize(v) for k, v in item.items()}
                    yielded += 1

        columns = target_columns or []
        return int(count), canonical_checksum_from_iter(
            _item_iter(),
            columns,
            limit=limit,
            dest_db_type="dynamodb",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_target(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    fallback_rows: int,
    fallback_checksum: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent destination read-back for Gate-8.

    ``written_ids`` (from writer meta) enables keyed fetch for vector / SaaS
    destinations where full-table scan is unavailable — Fivetran HVR Compare
    class: prove the batch we wrote, not an opaque index count alone.
    """
    # Prefer explicit arg; allow dest cfg stash from reconcile_step.
    ids = written_ids
    if ids is None and isinstance(dest.get("written_ids"), list):
        ids = [str(x) for x in dest["written_ids"] if x is not None]

    if db_type == "iceberg":
        count, chk = verify_iceberg_table(
            connection_string=dest.get("connection_string", ""),
            warehouse=dest.get("warehouse", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "mongodb":
        count, chk = verify_mongodb_collection(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 27017),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            ssl=bool(dest.get("ssl", False)),
            auth_source=dest.get("auth_source", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "dynamodb":
        from connectors.aws_common import resolve_endpoint_url

        conn_str = resolve_endpoint_url(dest) or "http://localhost:8000"
        count, chk = verify_dynamodb_table(
            connection_string=conn_str,
            database=dest.get("database", ""),
            table_name=table_name,
            username=dest.get("username", "local"),
            password=dest.get("password", "local"),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "sqlite":
        count, chk = verify_sqlite_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            host=dest.get("host", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "duckdb":
        count, chk = verify_duckdb_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "generic_sql":
        # Route local file engines / Oracle / ClickHouse by URL or dest.type.
        # Catalog maps clickhouse→generic_sql driver; endpoint.format stays in type.
        engine_hint = str(dest.get("type") or dest.get("engine") or "").lower()
        conn = (dest.get("connection_string") or dest.get("database") or "").lower()
        if "sqlite" in conn or conn.endswith(".db") or conn.endswith(".sqlite"):
            count, chk = verify_sqlite_table(
                connection_string=dest.get("connection_string", ""),
                database=dest.get("database", ""),
                host=dest.get("host", ""),
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
            )
        elif "duckdb" in conn or conn.endswith(".duckdb") or conn.endswith(".duck"):
            count, chk = verify_duckdb_table(
                connection_string=dest.get("connection_string", ""),
                database=dest.get("database", ""),
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
            )
        elif conn.startswith("oracle") or engine_hint.startswith("oracle"):
            count, chk = verify_oracle_table(
                host=dest.get("host", ""),
                port=int(dest.get("port") or 1521),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                schema=schema or dest.get("schema") or "",
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                dest_types=dest_types,
            )
        elif "clickhouse" in engine_hint or "clickhouse" in conn:
            count, chk = verify_clickhouse_table(
                host=dest.get("host", ""),
                port=int(dest.get("port") or 9000),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                schema=schema or dest.get("schema") or "",
                ssl=bool(dest.get("ssl", False)),
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                dest_types=dest_types,
            )
        else:
            # Teradata / HANA / Vertica / Informix / Trino / Firebird / …
            # — independent SQLAlchemy read-back (never invent from writer counts).
            count, chk = verify_generic_sql_table(
                dest=dest,
                schema=schema or dest.get("schema") or "",
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                dest_types=dest_types,
                engine_hint=engine_hint,
            )
    elif db_type == "clickhouse":
        count, chk = verify_clickhouse_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 9000),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "",
            ssl=bool(dest.get("ssl", False)),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "pgvector":
        # pgvector tables store an opaque embedding; reconstruct source rows from
        # the JSON metadata and source_id for an honest checksum reconciliation.
        count, chk = verify_pgvector_table(
            host=dest.get("host", ""),
            port=dest.get("port", 5432),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", False),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type in ("postgresql", "redshift"):
        # Redshift wire protocol is Postgres; local CI uses the PG emulator.
        count, chk = verify_postgres_table(
            host=dest.get("host", ""),
            port=dest.get("port", 5432),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", False)
            if db_type == "postgresql"
            else dest.get("ssl", False),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "redis":
        count, chk = verify_redis_prefix(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 6379),
            database=dest.get("database", "0"),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            prefix=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type in {"elasticsearch", "opensearch", "elastic"}:
        count, chk = verify_elasticsearch_index(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 9200),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            api_key=str(dest.get("api_key") or dest.get("service_account") or ""),
            index=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "snowflake":
        count, chk = verify_snowflake_table(
            host=dest.get("host", ""),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            warehouse=dest.get("warehouse", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "mysql":
        count, chk = verify_mysql_table(
            host=dest.get("host", ""),
            port=int(dest.get("port", 3306)),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", False),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type in {"sqlserver", "mssql", "azure_sql"}:
        count, chk = verify_sqlserver_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 1433),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "dbo",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type in {
        "oracle",
        "oracledb",
        "oracle_db",
        "oracle_autonomous",
        "oracle_autonomous_warehouse",
        "amazon_rds_oracle",
    }:
        count, chk = verify_oracle_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 1521),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "bigquery":
        count, chk = verify_bigquery_table(
            project_id=dest.get("database", ""),
            dataset_id=schema,
            connection_string=dest.get("connection_string", ""),
            host=dest.get("host", ""),
            port=int(dest.get("port", 0) or 0),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "s3":
        count, chk = verify_s3_object(
            bucket=dest.get("database", ""),
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port", 0)),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "gcs":
        count, chk = verify_gcs_blob(
            bucket=dest.get("database", ""),
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port", 0)),
            connection_string=dest.get("connection_string", ""),
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type in {
        "adls",
        "azure_blob_storage",
        "azure_data_lake",
        "azure_data_lake_storage",
    }:
        count, chk = verify_adls_blob(
            container=dest.get("database", "") or schema or "",
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port") or 0),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            service_account=dest.get("service_account", ""),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "sftp":
        count, chk = verify_sftp_object(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 22),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", "") or schema or "",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type in {
        "databricks",
        "databricks_sql",
        "delta",
        "delta_lake",
        "unity_catalog",
        "spark",
    }:
        count, chk = verify_databricks_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 443),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "",
            table_name=table_name,
            http_path=str(dest.get("http_path") or dest.get("warehouse") or ""),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "hubspot":
        count, chk = verify_hubspot_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            object_name=table_name or dest.get("table") or "contacts",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "salesforce":
        count, chk = verify_salesforce_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "Account",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "airtable":
        count, chk = verify_airtable_table(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            base_id=str(dest.get("database") or schema or ""),
            table_name=table_name or dest.get("table") or "",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "stripe":
        count, chk = verify_stripe_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "customers",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "shopify":
        count, chk = verify_shopify_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "customers",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "zendesk":
        count, chk = verify_zendesk_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "tickets",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "notion":
        count, chk = verify_notion_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "",
            database_id=str(dest.get("database") or schema or ""),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "kafka":
        count, chk = verify_kafka_topic(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 9092),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            topic=table_name or dest.get("table") or dest.get("database") or "",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            schema_registry_url=str(
                dest.get("schema_registry_url") or dest.get("registry_url") or ""
            ),
        )
    elif db_type == "pinecone":
        count, chk = verify_pinecone_namespace(
            host=dest.get("host", ""),
            connection_string=dest.get("connection_string", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            api_key=str(dest.get("api_key") or ""),
            namespace=table_name or dest.get("schema") or "",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type == "qdrant":
        count, chk = verify_qdrant_collection(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 6333),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            collection=table_name or dest.get("database") or "dataflow_vectors",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type == "weaviate":
        count, chk = verify_weaviate_class(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 8080),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            api_key=str(dest.get("api_key") or ""),
            class_name=table_name or dest.get("database") or "DataflowChunk",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type == "milvus":
        count, chk = verify_milvus_collection(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 19530),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            api_key=str(dest.get("api_key") or ""),
            database=dest.get("database", "") or schema or "",
            collection=table_name or "dataflow_chunks",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type in {
        "teradata",
        "teradata_vantage",
        "vertica",
        "hana",
        "sap_hana",
        "sap_bw_4hana",
        "informix",
        "firebird",
        "netezza",
        "trino",
        "presto",
        "athena",
        "amazon_athena",
        "hive",
        "impala",
        "sybase",
        "sybase_ase",
        "sap_ase",
        "sap_iq",
    }:
        # Catalog id may arrive before resolve_driver_type → generic_sql.
        count, chk = verify_generic_sql_table(
            dest=dest,
            schema=schema or dest.get("schema") or "",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            engine_hint=db_type,
        )
    else:
        count, chk = -1, ""

    if count >= 0:
        return count, chk
    return fallback_rows, fallback_checksum


def fingerprint_for_reconcile(
    value: Any,
    *,
    ddl_type: str = "",
    engine: str = "",
    transform: str | None = None,
) -> str:
    """Canonical Gate-8 fingerprint: transform → destination bind → normalize_cell.

    Source samples and destination read-back must share this path so Mongo
    ``\"true\"`` / MySQL ``0`` / Postgres ``False`` compare as equal.
    """
    from services.transform_engine import apply_transform
    from services.value_serializer import cell_to_string

    wire: Any = value
    tname = (transform or "").strip().lower()
    if tname and tname not in {"", "none", "identity", "passthrough"}:
        cell = cell_to_string(value, preserve_sql_null=True) if value is not None else None
        if value is None:
            converted, err = None, None
        else:
            converted, err = apply_transform(
                cell if cell is not None else "", transform or "none"
            )
        if err:
            # Quarantine / coerce_null write path stores SQL NULL for failed cells.
            # Fingerprint must match that — not the raw bad wire — or Gate-8
            # false-fails every quarantined coercion as a sample mismatch.
            wire = None
        else:
            wire = converted
    elif value is not None and not isinstance(value, (str, int, float, bool, bytes)):
        wire = cell_to_string(value, preserve_sql_null=True)

    if ddl_type:
        try:
            from connectors.sql_bind import normalize_sql_bind_value

            wire = normalize_sql_bind_value(wire, ddl_type, engine=engine)
        except Exception:
            pass
    return normalize_cell(wire, ddl_type=ddl_type, engine=engine)


def normalize_cell(value: Any, *, ddl_type: str = "", engine: str = "") -> str:
    """Canonical cell text for Gate-8 checksums.

    CHAR/NCHAR blank-pad is a storage artifact (Oracle/SQL Server/MySQL) — rtrim
    spaces so faithful CHAR→VARCHAR migrations do not false-fail (wmk/Airbyte
    class). VARCHAR/TEXT preserve trailing spaces when ``ddl_type`` is known so
    intentional trailing whitespace is not silently equated away.

    When ``engine`` is Oracle, zero-length strings fingerprint as NULL (Oracle
    VARCHAR2 semantics / HVR write-location compare rules). Postgres/MySQL keep
    NULL and ``""`` distinct.
    """
    if value is None:
        # Distinct from empty string — SQL/Dynamo NULL must not checksum as "".
        return _NULL_SENTINEL
    # Dense write materializes absent schemaless fields as SQL NULL; fingerprint
    # must match. Sparse CDC sample_compare skips DF_MISSING columns (omit-from-SET).
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return _NULL_SENTINEL
    if isinstance(value, str) and value.strip().lower() in {
        "__df_sql_null__",
        "__df_ddb_null__",
        "__df_missing__",
    }:
        return _NULL_SENTINEL
    # Oracle write-location: '' is stored/read as NULL — equate before other paths.
    if destination_empty_string_is_null(engine) and isinstance(value, str) and value == "":
        return _NULL_SENTINEL
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, _datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        # Canonical form is UTC wall-clock without a Z marker so TIMESTAMPTZ
        # sources match NTZ sinks that stored the same UTC components, while
        # offset-aware values still differ from a different naive wall clock.
        if value.microsecond:
            return value.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".")
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, _date):
        return _datetime.combine(value, _time.min).strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, float):
        return _canonicalize_number(str(value)) or "nan"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _canonicalize_number(value) or "nan"
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Bytes may be raw payload or a base64-encoded string stored as bytes
        # (common in emulators). When the bytes are a valid base64 string,
        # decode and re-encode so the canonical checksum matches the original
        # encoded text; otherwise base64-encode the raw bytes.
        try:
            decoded = base64.b64decode(value, validate=True)
            re_encoded = base64.b64encode(decoded)
            if re_encoded == value:
                return re_encoded.decode("ascii")
        except Exception:
            logger.debug("Raw bytes are not valid base64; encoding as base64 for checksum")
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return json.dumps(value, sort_keys=True, default=json_default)
    raw_text = str(value)
    if ddl_type:
        try:
            from services.type_system import (
                fold_diacritics,
                fold_kana,
                fold_variation_selectors,
                fold_width_forms,
                is_accent_insensitive_collation,
                is_case_insensitive_collation,
                is_fixed_width_char_carrier,
                is_kana_insensitive_collation,
                is_variation_insensitive_collation,
                is_width_insensitive_collation,
                normalize_logical_type,
            )

            if is_fixed_width_char_carrier(ddl_type):
                # Blank-pad only — do not strip leading spaces (rare but significant).
                text = raw_text.rstrip(" ")
            elif normalize_logical_type(ddl_type) in {"string", "text"}:
                # VARCHAR/TEXT: preserve trailing spaces (significant payload).
                text = raw_text
            else:
                text = raw_text.strip()
            # Collation equality must match the destination engine (CI/AI/WI/KI/VSS).
            if is_width_insensitive_collation(ddl_type):
                text = fold_width_forms(text)
            if is_kana_insensitive_collation(ddl_type):
                text = fold_kana(text)
            if is_variation_insensitive_collation(ddl_type):
                text = fold_variation_selectors(text)
            if is_accent_insensitive_collation(ddl_type):
                text = fold_diacritics(text)
            if is_case_insensitive_collation(ddl_type):
                text = text.casefold()
            # UUID / UNIQUEIDENTIFIER / CHAR(36) UUID carriers — canonicalize
            # braces / 32-hex / case so source wire and dest read-back match
            # (Fivetran HVR compare class: destination storage rules win).
            try:
                from services.type_system import normalize_logical_type

                if normalize_logical_type(ddl_type) == "uuid" or re.search(
                    r"\b(?:uuid|uniqueidentifier|guid)\b", ddl_type, re.I
                ):
                    try:
                        from connectors.sql_bind import coerce_uuid_wire

                        text = coerce_uuid_wire(text) or text
                    except ValueError:
                        if _UUID_RE.match(text):
                            text = text.lower()
            except Exception:
                pass
        except Exception:
            text = raw_text.strip()
    else:
        text = raw_text.strip()
    # Boolean and empty fast paths.
    if not text:
        return ""
    lowered = text.lower()
    # Align with transform_engine strict boolean tokens only. Status enums
    # ("active"/"enabled"/…) must NOT collide with true/false in checksums —
    # that falsely claimed 100% fidelity when status strings met bool columns.
    from services.transform_engine import _STRICT_BOOL_FALSE, _STRICT_BOOL_TRUE

    if lowered in _STRICT_BOOL_TRUE:
        return "1"
    if lowered in _STRICT_BOOL_FALSE:
        return "0"
    # Numeric fast path: only attempt Decimal normalization for strings that look
    # like numbers, avoiding the expensive exception path for names, emails, codes.
    if text[0] in "+-0123456789" and _NUMERIC_RE.match(text):
        canonical = _canonicalize_number(text)
        if canonical is not None:
            return canonical
        return text
    # JSON payloads (e.g. jsonb).
    if text.startswith(("{", "[")):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, sort_keys=True, default=json_default)
        except (json.JSONDecodeError, TypeError):
            pass
    # Date/time normalization: cheap heuristic first to avoid running the date
    # regex on every non-date string.
    if (
        text[0].isdigit()
        and len(text) >= 8
        and _DATE_LIKE_CHARS.intersection(text)
        and _DATE_LIKE_RE.search(text)
    ):
        dtm = _parse_datetime(text)
        if dtm:
            # Fold offsets/Z to UTC wall-clock; keep naive strings as wall-clock.
            # Do not keep a trailing Z — that falsely fails NTZ sink readback
            # against TIMESTAMPTZ sources that share the same UTC components.
            return _checksum_datetime_utc_wall(
                dtm if isinstance(dtm, str) else str(dtm), original=text
            )
        dt = _parse_date(text)
        if dt:
            return f"{dt}T00:00:00"
    return text


def _checksum_datetime_utc_wall(iso_text: str, *, original: str | None = None) -> str:
    """Canonical UTC wall-clock for checksum equality (no Z marker).

    Aware/offset/Z strings fold to UTC components. Naive wall-clock strings
    stay as-is. That equates TIMESTAMPTZ→DATETIME UTC storage without equating
    ``12:00+05:30`` (06:30 UTC) to naive ``12:00``.
    """
    text = (iso_text or "").strip()
    if not text:
        return ""
    src = (original or text).strip()
    aware_or_epoch = bool(
        src.endswith(("Z", "z")) or re.search(r"[+-]\d{2}:?\d{2}\s*$", src)
    )
    try:
        from services.transform_engine import _EPOCH_MS_RE, _EPOCH_S_RE

        aware_or_epoch = aware_or_epoch or bool(
            _EPOCH_MS_RE.match(src) or _EPOCH_S_RE.match(src)
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    try:
        iso = text[:-1] + "+00:00" if text.endswith("Z") else text
        obj = _datetime.fromisoformat(iso)
        if obj.tzinfo is not None or aware_or_epoch:
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=timezone.utc)
            else:
                obj = obj.astimezone(timezone.utc)
            obj = obj.replace(tzinfo=None)
        if obj.microsecond:
            return obj.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".")
        return obj.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        # Fall back: strip invented trailing Z from transform parser.
        if text.endswith("Z") and not aware_or_epoch:
            return text[:-1]
        if text.endswith("Z"):
            return text[:-1]  # still wall-clock form for checksum
        return text


def _checksum_datetime_utc_z(iso_text: str) -> str:
    """Backward-compatible alias — checksums use wall-clock UTC, not Z."""
    return _checksum_datetime_utc_wall(iso_text)


def _canonicalize_number(value: Any) -> str | None:
    """Return a canonical string for numeric values so 9.5 == 9.5000000000."""
    try:
        d = Decimal(value) if not isinstance(value, Decimal) else value
        if d.is_nan():
            return None
        from services.value_serializer import safe_decimal_text

        s = safe_decimal_text(d.normalize() if d.is_finite() else d)
        if s is None:
            return None
        if "." in s and "e" not in s.lower():
            s = s.rstrip("0").rstrip(".")
        return s if s else "0"
    except (InvalidOperation, Overflow, TypeError, ValueError):
        return None


def build_reconciliation_proof(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    *,
    primary_key: str | None = None,
    sample_size: int = 50,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a deterministic proof object for row-level transfer verification.

    The proof is based on exact primary-key matching and normalized mapped-value
    comparison across a bounded sample. It returns a score suitable for a
    preflight/reconciliation gate, not a legal audit guarantee.

    When ``dest_db_type`` / ``dest_types`` are set, sample compare uses the same
    write-path fingerprint as live Gate-8 (bool/JSON bind parity).
    """
    if not source_records and not target_records:
        return {
            "passed": True,
            "matched_key_count": 0,
            "missing_key_count": 0,
            "row_fidelity_score": 1.0,
            "sample_compare": {"passed": True, "compared": 0, "mismatches": []},
        }

    key_col = primary_key
    key_provenance = "explicit" if primary_key else "missing"
    if not key_col:
        # Do not invent "id" — positional / guessed identity produces false
        # high row-fidelity scores (Airbyte-class honesty gap).
        return {
            "passed": False,
            "matched_key_count": 0,
            "missing_key_count": len(source_records),
            "extra_key_count": 0,
            "row_fidelity_score": 0.0,
            "sample_compare": {
                "passed": False,
                "compared": 0,
                "mismatches": [],
                "alignment": "unproven_identity",
            },
            "identity": {
                "column": None,
                "proven": False,
                "reason": "primary_key required for key-aligned fidelity proof",
            },
            "verification_mode": "unproven_identity",
        }

    source_keys = {
        normalize_cell(row.get(key_col))
        for row in source_records
        if row.get(key_col) is not None
    }
    target_keys = {
        normalize_cell(row.get(key_col))
        for row in target_records
        if row.get(key_col) is not None
    }
    # Null / blank keys cannot prove uniqueness — fail closed on fidelity claims.
    source_null_keys = sum(
        1
        for row in source_records
        if row.get(key_col) is None or normalize_cell(row.get(key_col)) == ""
    )
    target_null_keys = sum(
        1
        for row in target_records
        if row.get(key_col) is None or normalize_cell(row.get(key_col)) == ""
    )
    if (
        source_null_keys
        or target_null_keys
        or len(source_keys) < max(1, len(source_records) - source_null_keys)
    ):
        # Duplicate or null identity → positional comparison only, never high confidence.
        sample_compare = sample_compare_rows(
            source_records,
            target_records,
            mappings,
            sample_size=sample_size,
            sort_key=None,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
        sample_compare = {
            **sample_compare,
            "alignment": "positional_only",
            "identity_warning": "weak or non-unique primary key — fidelity is sample/positional only",
        }
        return {
            "passed": False,
            "matched_key_count": 0,
            "missing_key_count": len(source_records),
            "extra_key_count": 0,
            "row_fidelity_score": 0.0,
            "sample_compare": sample_compare,
            "identity": {
                "column": key_col,
                "proven": False,
                "reason": "null or duplicate identity values — refuse key-aligned proof",
            },
            "verification_mode": "positional_only",
        }

    matched_keys = source_keys & target_keys
    missing_keys = source_keys - target_keys
    extra_keys = target_keys - source_keys

    sample_compare = sample_compare_rows(
        source_records,
        target_records,
        mappings,
        sample_size=sample_size,
        sort_key=key_col,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )

    matched_key_count = len(matched_keys)
    missing_key_count = len(missing_keys)
    extra_key_count = len(extra_keys)
    total_keys = max(len(source_keys), 1)
    row_fidelity_score = round(
        max(
            0.0,
            1.0
            - (missing_key_count / total_keys)
            - (extra_key_count / total_keys) * 0.25,
        ),
        4,
    )

    passed = (
        missing_key_count == 0
        and extra_key_count == 0
        and sample_compare.get("passed", True)
        and row_fidelity_score >= 0.95
    )

    return {
        "passed": passed,
        "matched_key_count": matched_key_count,
        "missing_key_count": missing_key_count,
        "extra_key_count": extra_key_count,
        "row_fidelity_score": row_fidelity_score,
        # Stamp alignment on every branch so the UI can distinguish keyed proof
        # from positional comparison without inferring it from absence.
        "sample_compare": {**sample_compare, "alignment": "key_aligned"},
        "identity": {"column": key_col, "proven": True, "provenance": key_provenance},
        "verification_mode": "key_aligned",
    }



def _bucket_member_order(
    idxs: list[int], *, seed: str, bucket_name: str
) -> list[int]:
    """Deterministic intra-bucket order (stable across process restarts)."""
    import hashlib

    return sorted(
        idxs,
        key=lambda i: hashlib.sha256(
            f"{seed}:{bucket_name}:{i}".encode()
        ).hexdigest(),
    )


def _stratified_sample_indices(
    records: list[dict[str, Any]],
    *,
    stratify_col: str,
    sample_size: int,
    seed: str = "",
) -> list[int]:
    """Deterministic per-bucket quota sampling for skewed categoricals.

    Rare classes get a guaranteed slot when buckets fit in ``sample_size``.
    When bucket count exceeds ``sample_size``, prefer the *smallest* buckets
    (rare classes) — never hash-trim across buckets (that reintroduces the
    first-N trap stratification exists to prevent).

    Still a **sample** plan — never population proof.
    """
    import hashlib

    if sample_size <= 0 or not records or not stratify_col:
        return list(range(min(max(sample_size, 0), len(records))))
    buckets: dict[str, list[int]] = {}
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            continue
        raw = rec.get(stratify_col)
        key = normalize_cell(raw) if raw is not None else ""
        buckets.setdefault(key or "<null>", []).append(i)
    names = sorted(buckets.keys())
    if not names:
        return list(range(min(sample_size, len(records))))

    n_buckets = len(names)

    # More strata than slots: keep rare (smallest) buckets — 1 row each.
    if n_buckets > sample_size:
        ranked = sorted(
            names,
            key=lambda name: (
                len(buckets[name]),
                hashlib.sha256(f"{seed}:bucket:{name}".encode()).hexdigest(),
                name,
            ),
        )
        picked: list[int] = []
        for name in ranked[:sample_size]:
            scored = _bucket_member_order(
                buckets[name], seed=seed, bucket_name=name
            )
            if scored:
                picked.append(scored[0])
        return picked[:sample_size]

    # Proportional quota with floor 1 (always possible when n_buckets <= sample_size).
    base = sample_size // n_buckets
    rem = sample_size % n_buckets
    picked = []
    for bi, name in enumerate(names):
        scored = _bucket_member_order(buckets[name], seed=seed, bucket_name=name)
        take = base + (1 if bi < rem else 0)
        take = min(max(take, 1), len(scored))
        picked.extend(scored[:take])

    # Shrink from largest buckets only — never drop a bucket entirely.
    while len(picked) > sample_size:
        # Count current picks per bucket
        membership: dict[str, list[int]] = {n: [] for n in names}
        for i in picked:
            rec = records[i] if i < len(records) else {}
            raw = rec.get(stratify_col) if isinstance(rec, dict) else None
            key = normalize_cell(raw) if raw is not None else ""
            membership.setdefault(key or "<null>", []).append(i)
        # Drop one row from the largest bucket that still has >1
        candidates = [
            (len(idxs), name)
            for name, idxs in membership.items()
            if len(idxs) > 1
        ]
        if not candidates:
            # Should not happen when n_buckets <= sample_size; fail closed trim.
            picked = picked[:sample_size]
            break
        _, drop_name = max(
            candidates,
            key=lambda t: (
                t[0],
                hashlib.sha256(f"{seed}:drop:{t[1]}".encode()).hexdigest(),
            ),
        )
        drop_idxs = membership[drop_name]
        # Drop the last in deterministic bucket order
        ordered = _bucket_member_order(drop_idxs, seed=seed, bucket_name=drop_name)
        drop_i = ordered[-1]
        picked = [i for i in picked if i != drop_i]

    if len(picked) < sample_size:
        used = set(picked)
        for i in range(len(records)):
            if i not in used and isinstance(records[i], dict):
                picked.append(i)
                used.add(i)
            if len(picked) >= sample_size:
                break
    return picked[:sample_size]


def _auto_stratify_source_column(
    source_records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    *,
    sort_key: str | None,
    source_sort_key: str | None,
) -> str | None:
    """Heuristic stratum: skewed low-cardinality mapped column (not the PK).

    Returns ``None`` when no safe candidate exists — caller falls back to
    keyed/positional sample. Never claims population coverage.
    """
    from collections import Counter

    exclude = {
        str(sort_key or "").strip().lower(),
        str(source_sort_key or "").strip().lower(),
        "id",
        "_id",
        "pk",
        "uuid",
        "guid",
    }
    exclude.discard("")
    best_col: str | None = None
    best_key: tuple[float, int, str] | None = None
    for m in mappings:
        src_col = str(m.get("source") or "").strip()
        tgt_col = str(m.get("target") or "").strip()
        if not src_col:
            continue
        if src_col.lower() in exclude or tgt_col.lower() in exclude:
            continue
        vals: list[str] = []
        for r in source_records:
            raw = r.get(src_col)
            cell = normalize_cell(raw) if raw is not None else ""
            vals.append(cell or "<null>")
        if not vals:
            continue
        n_classes = len(set(vals))
        if not (2 <= n_classes <= 20):
            continue
        counts = Counter(vals)
        skew = max(counts.values()) / len(vals)
        # Require imbalance so uniform enums don't pretend to stratify.
        if skew < 0.55:
            continue
        key = (skew, n_classes, src_col.lower())
        if best_key is None or key > best_key:
            best_key = key
            best_col = src_col
    return best_col


def sample_compare_rows(
    source_records: list[dict[str, Any]],
    target_rows: list[dict[str, Any]] | list[tuple[Any, ...]] | list[list[Any]],
    mappings: list[dict[str, Any]],
    *,
    target_columns: list[str] | None = None,
    sample_size: int = 50,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    stratify_by: str | None = None,
) -> dict[str, Any]:
    """
    Compare mapped column values between source records and destination read-back.
    Rows are aligned by a stable key (e.g. primary key) when available, so upserts
    and out-of-order writes compare correctly. Falls back to sorted index alignment.

    When ``dest_db_type`` / ``dest_types`` are provided, both sides are fingerprinted
    through the same write-path bind helpers as MySQL/Postgres/Snowflake writers
    (``fingerprint_for_reconcile``) so Gate-8 does not false-fail on bool/JSON wire.
    """
    if not source_records or not target_rows or not mappings:
        return {"passed": True, "compared": 0, "mismatches": [], "skipped": True}

    dest_types = dest_types or {}
    eng = (dest_db_type or "").strip().lower()

    def _as_dict(tgt_raw: Any) -> dict[str, Any] | None:
        if isinstance(tgt_raw, dict):
            return tgt_raw
        if target_columns and isinstance(tgt_raw, (list, tuple)):
            return {
                col: tgt_raw[i] if i < len(tgt_raw) else None
                for i, col in enumerate(target_columns)
            }
        return None

    target_dicts = [d for d in (_as_dict(t) for t in target_rows) if d is not None]

    # sort_key is a *target* column (e.g. id). Source rows may still use the
    # pre-map name (rec_id → id) — resolve via mappings so key alignment works.
    source_sort_key = sort_key
    if sort_key and mappings:
        sk = sort_key.lower()
        for m in mappings:
            if str(m.get("target") or "").lower() == sk and m.get("source"):
                source_sort_key = str(m["source"])
                break

    target_by_key: dict[str, dict[str, Any]] = {}
    if sort_key:
        for d in target_dicts:
            key = normalize_cell(d.get(sort_key))
            if key and key not in target_by_key:
                target_by_key[key] = d

    def _fingerprint(raw: Any, *, transform: str | None, tgt_col: str) -> str:
        ddl = (
            dest_types.get(tgt_col)
            or dest_types.get(tgt_col.lower())
            or ""
        )
        if not ddl:
            for m in mappings:
                if str(m.get("target") or "") == tgt_col:
                    ddl = str(m.get("target_type") or m.get("inferredType") or "")
                    break
        if eng and ddl:
            try:
                return fingerprint_for_reconcile(
                    raw, ddl_type=ddl, engine=eng, transform=transform
                )
            except Exception:
                pass
        if transform:
            try:
                converted, err = apply_transform(raw, transform)
            except Exception:
                converted, err = raw, "transform_failed"
            if err:
                converted = None
        else:
            converted = raw
        return normalize_cell(converted)

    def _row_key(rec: Any, *, source_side: bool = False) -> Any:
        if isinstance(rec, dict):
            key = source_sort_key if source_side else sort_key
            val = rec.get(key) if key else None
            if val is None and source_side and sort_key and sort_key != source_sort_key:
                val = rec.get(sort_key)
            return val or (rec.get(target_columns[0]) if target_columns else None)
        return rec

    def _sortable(value: Any) -> tuple:
        """Return a stable, type-safe sort key that handles bson.Decimal128."""
        if value is None:
            return (0, Decimal(0))
        if value.__class__.__name__ == "Decimal128":
            try:
                return (0, value.to_decimal())
            except Exception:
                return (1, str(value))
        if isinstance(value, Decimal):
            return (0, value)
        if isinstance(value, bool):
            return (0, Decimal(int(value)))
        if isinstance(value, (int, float)):
            try:
                return (0, Decimal(value))
            except Exception:
                return (1, str(value))
        text = str(value).strip()
        try:
            return (0, Decimal(text))
        except Exception:
            return (1, text.lower())

    import hashlib

    dict_records = [r for r in source_records if isinstance(r, dict)]
    auto_selected = False
    # Auto-stratify: skewed low-cardinality mapped column (never the PK).
    strat_col = (stratify_by or "").strip() or None
    if not strat_col and len(dict_records) > sample_size:
        strat_col = _auto_stratify_source_column(
            dict_records,
            mappings,
            sort_key=sort_key,
            source_sort_key=source_sort_key,
        )
        auto_selected = bool(strat_col)

    if strat_col and dict_records:
        idxs = _stratified_sample_indices(
            dict_records,
            stratify_col=strat_col,
            sample_size=sample_size,
            seed=f"{sort_key or ''}:{strat_col}",
        )
        source_sorted = [dict_records[i] for i in idxs if i < len(dict_records)]
        method = "stratified"
    else:
        source_sorted = sorted(
            dict_records or source_records,
            key=lambda r: _sortable(_row_key(r, source_side=True)),
        )[:sample_size]
        method = "keyed_sorted" if sort_key else "positional_sorted"
        strat_col = None
        auto_selected = False

    # Deterministic seed for auditor replay — sorted PK (or positional index) set.
    pk_values: list[str] = []
    for src in source_sorted:
        key_raw = _row_key(src, source_side=True)
        pk_values.append(normalize_cell(key_raw) if key_raw is not None else "")
    seed_canon = json.dumps(
        {
            "method": method,
            "size": len(source_sorted),
            "sort_key": sort_key or "",
            "source_sort_key": source_sort_key or "",
            "stratify_by": strat_col or "",
            "auto_selected": auto_selected,
            "pk_values": pk_values,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    sample_seed = {
        "method": method,
        "size": len(source_sorted),
        "sort_key": sort_key or "",
        "source_sort_key": source_sort_key or "",
        "stratify_by": strat_col or "",
        "auto_selected": auto_selected,
        "coverage": "sample",
        "population_proof": False,
        "note": (
            "Stratified/keyed sample improves category coverage within the "
            "read-back sample only — not population proof."
            if method == "stratified"
            else "Keyed/positional sample — not population proof."
        ),
        "pk_values": pk_values[:sample_size],
        "content_sha256": hashlib.sha256(seed_canon.encode("utf-8")).hexdigest(),
    }

    mismatches: list[dict[str, str]] = []
    compared = 0
    target_fallback = sorted(target_dicts, key=lambda d: _sortable(_row_key(d)))

    def _result(*, passed: bool) -> dict[str, Any]:
        return {
            "passed": passed,
            "compared": compared,
            "mismatches": mismatches,
            "sample_seed": sample_seed,
            "alignment": "keyed" if (sort_key and target_by_key) else "positional",
        }

    for idx, src in enumerate(source_sorted):
        if sort_key and target_by_key:
            key = normalize_cell(src.get(source_sort_key) if source_sort_key else None)
            if not key and sort_key:
                key = normalize_cell(src.get(sort_key))
            tgt = target_by_key.get(key) if key else None
        else:
            tgt = target_fallback[idx] if idx < len(target_fallback) else None
        if tgt is None:
            continue

        # Case-insensitive target lookup — MySQL/Snowflake cursors may fold names.
        tgt_keys = {str(k).lower(): k for k in tgt.keys()}

        for m in mappings:
            src_col = str(m.get("source") or "")
            tgt_col = str(m.get("target") or "")
            if not src_col or not tgt_col:
                continue
            physical_tgt = tgt_keys.get(tgt_col.lower())
            if physical_tgt is None:
                # Column absent from read-back sample — do not invent NULL mismatch.
                continue
            transform = m.get("transform")
            if not transform or str(transform).strip().lower() in {
                "",
                "none",
                "identity",
                "passthrough",
            }:
                # Mirror writer_common.resolve_transform so quarantine coerces
                # (integer/date/…) fingerprint as NULL like the write path.
                try:
                    from services.transform_resolver import resolve_transform

                    transform = resolve_transform(
                        m,
                        column_types={},
                        dest_types=dest_types,
                    )
                except Exception:
                    transform = m.get("transform")
            # Sparse CDC / STOP_COLUMN / coerce_null: source DF_MISSING means
            # omit-from-SET — skip compare (do not fingerprint as NULL).
            # Destination DF_MISSING is a leak and must still mismatch.
            from services.value_serializer import is_missing_sentinel

            raw_src = src.get(src_col) if isinstance(src, dict) else None
            if is_missing_sentinel(raw_src):
                continue
            raw_tgt = tgt.get(physical_tgt)
            src_val = _fingerprint(
                raw_src, transform=transform, tgt_col=tgt_col
            )
            # Destination already applied bind at write — fingerprint without
            # re-transform so read-back bools/JSON match source write-path form.
            tgt_val = _fingerprint(
                raw_tgt, transform=None, tgt_col=tgt_col
            )
            compared += 1
            if src_val != tgt_val:
                mismatches.append(
                    {
                        "row": str(idx),
                        "source": src_col,
                        "target": tgt_col,
                        "source_value": src_val[:120],
                        "target_value": tgt_val[:120],
                    }
                )
                if len(mismatches) >= 10:
                    return _result(passed=False)

    return _result(passed=len(mismatches) == 0)


class TargetSampleUnavailable(RuntimeError):
    """Destination sample could not be read — distinct from an empty table.

    Callers that treat ``[]`` as "nothing is there" (CDC delete proof, Gate-8
    fidelity) must catch this and fail the gate. Swallowing the error as an
    empty list is what made a missing SELECT grant report delete proof as
    passed while the rows were still live.
    """


def _object_store_target_sample(
    *,
    table_name: str,
    list_keys: Callable[[str], list[str]],
    fetch_bytes: Callable[[str], bytes],
    cols: list[str],
    limit: int,
    sort_key: str,
    keys: Iterable[Any] | None,
) -> list[dict[str, Any]]:
    """Gate-8 sample read shared by S3, GCS and ADLS destinations.

    Object stores have no WHERE clause, so the sample is assembled by reading
    the part objects (or the single legacy object) and filtering in memory.
    Reading every part matters: a multi-chunk write keeps most rows outside the
    base key, and sampling only the base key would compare against a fraction
    of the data while reporting a clean Gate-8.
    """
    from connectors.object_store_common import (
        normalize_object_base_key,
        object_parts_prefix,
        object_store_read_keys,
    )

    base = normalize_object_base_key(table_name)
    listed = list_keys(object_parts_prefix(base))
    read_keys = object_store_read_keys(base, listed)
    lim = max(1, int(limit or 50))
    wanted = {str(k) for k in keys} if keys else set()
    projection = None if cols == ["*"] else cols

    rows: list[dict[str, Any]] = []
    for obj_key in read_keys:
        part_rows, _headers = _rows_from_object_bytes(
            fetch_bytes(obj_key), obj_key, projection
        )
        if wanted and sort_key:
            # Key-targeted sample: keep only the rows Gate-8 asked about, but
            # keep scanning parts because a key can live in any part.
            rows.extend(r for r in part_rows if str(r.get(sort_key)) in wanted)
        else:
            rows.extend(part_rows)
        if len(rows) >= lim and not wanted:
            break
    return rows[:lim]


def read_target_sample(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: list[str] | None = None,
    limit: int = 50,
    sort_key: str | None = None,
    key_values: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Read a small ordered sample from destination for value reconciliation.

    When ``key_values`` is provided with ``sort_key``, prefer a keyed ``IN (...)``
    read so append/upsert Gate-8 can prove fidelity against pre-existing rows
    (ORDER BY … LIMIT alone often misses the batch keys in a large table).

    Returns an empty list only when the destination is genuinely empty (or the
    keyed ``IN`` matched nothing). Read failures raise
    :class:`TargetSampleUnavailable` — never ``[]``.
    """
    from connectors.sql_identifiers import (
        quote_column_list,
        quote_sql_identifier,
        quote_table_ref,
        require_safe_identifier,
    )

    cols = columns or ["*"]
    keys = [k for k in (key_values or []) if k is not None and k != ""][
        : max(1, int(limit or 50))
    ]

    def _row_names(description: Any) -> list[str]:
        # When explicit columns were requested, trust the caller's keys so
        # downstream mapping/reconciliation matches by the mapping target names.
        # Cursor.description names may differ in case (e.g. fakesnow lower-cases
        # quoted identifiers), which would make dict lookups fail for CURRENCY.
        if cols and cols != ["*"]:
            return list(cols)
        return [d[0] for d in (description or [])]

    def _ssl_flag(default: bool = False) -> bool:
        # Match list/probe defaults — ssl=True here previously emptied samples on
        # local / non-TLS hosts while verify_target still counted rows.
        return bool(dest.get("ssl", default))

    try:
        if db_type in ("postgresql", "redshift"):
            # Redshift speaks the Postgres wire protocol; local CI and many
            # managed endpoints use the PG driver. Checksum verify already
            # treated them as one family — sample compare must too, or Gate-8
            # fails closed with "no sample reader" after a successful write.
            from connectors.postgresql_conn import get_connection

            col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            table_ref = quote_table_ref(
                table_name, schema or "public", dialect="postgresql"
            )
            order_sql = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            ssl_flag = _ssl_flag(False)
            last_exc: Exception | None = None
            for attempt_ssl in (ssl_flag, not ssl_flag):
                try:
                    conn = get_connection(
                        host=dest.get("host", ""),
                        port=dest.get("port", 5432),
                        database=dest.get("database", ""),
                        username=dest.get("username", ""),
                        password=dest.get("password", ""),
                        connection_string=dest.get("connection_string", ""),
                        ssl=attempt_ssl,
                    )
                    with conn.cursor() as cur:
                        if keys and sort_key:
                            key_col = quote_sql_identifier(
                                require_safe_identifier(sort_key, preserve_case=True)
                            )
                            placeholders = ",".join(["%s"] * len(keys))
                            cur.execute(
                                f"SELECT {col_sql} FROM {table_ref} "  # nosec B608
                                f"WHERE {key_col} IN ({placeholders}) "
                                f"ORDER BY {order_sql} LIMIT %s",
                                (*keys, int(limit)),
                            )
                        else:
                            cur.execute(
                                f"SELECT {col_sql} FROM {table_ref} ORDER BY {order_sql} LIMIT %s",  # nosec B608
                                (limit,),
                            )
                        names = _row_names(cur.description)
                        rows = cur.fetchall()
                    conn.close()
                    return [dict(zip(names, row)) for row in rows]
                except Exception as exc:
                    last_exc = exc
                    continue
            if last_exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {last_exc}"
                ) from last_exc
            raise TargetSampleUnavailable(
                f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                "postgresql connection failed for both SSL modes"
            )

        if db_type == "mysql":
            from connectors.mysql_conn import get_connection

            mysql_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols],
                    quote_char="`",
                )
            )
            table_ref = quote_table_ref(table_name, dialect="mysql")
            mysql_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True), "`"
                )
                if sort_key
                else "1"
            )
            conn = get_connection(
                host=dest.get("host", ""),
                port=int(dest.get("port", 3306)),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                ssl=dest.get("ssl", False),
            )
            with conn.cursor() as cur:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True), "`"
                    )
                    placeholders = ",".join(["%s"] * len(keys))
                    cur.execute(
                        f"SELECT {mysql_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {mysql_order} LIMIT %s",
                        (*keys, int(limit)),
                    )
                else:
                    cur.execute(
                        f"SELECT {mysql_col_sql} FROM {table_ref} ORDER BY {mysql_order} LIMIT %s",  # nosec B608
                        (limit,),
                    )
                names = _row_names(cur.description)
                rows = cur.fetchall()
            conn.close()
            return [dict(zip(names, row)) for row in rows]

        if db_type in {"sqlserver", "mssql", "azure_sql"}:
            import pymssql

            lim = max(1, int(limit or 50))
            if cols == ["*"]:
                ss_col_sql = "*"
            else:
                ss_col_sql = ", ".join(
                    f"[{require_safe_identifier(c, preserve_case=True).replace(']', ']]')}]"
                    for c in cols
                )
            sch = (schema or dest.get("schema") or "dbo").strip() or "dbo"
            table_ref = quote_table_ref(table_name, schema=sch, dialect="sqlserver")
            ss_order = (
                f"[{require_safe_identifier(sort_key, preserve_case=True).replace(']', ']]')}]"
                if sort_key
                else "1"
            )
            conn = pymssql.connect(
                server=dest.get("host") or "127.0.0.1",
                port=int(dest.get("port") or 1433),
                user=dest.get("username") or "sa",
                password=dest.get("password") or "",
                database=dest.get("database") or "master",
                login_timeout=10,
                timeout=30,
            )
            cur = conn.cursor()
            try:
                if keys and sort_key:
                    key_col = (
                        f"[{require_safe_identifier(sort_key, preserve_case=True).replace(']', ']]')}]"
                    )
                    placeholders = ",".join(["%s"] * len(keys))
                    cur.execute(
                        f"SELECT TOP ({lim}) {ss_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {ss_order}",
                        tuple(keys),
                    )
                else:
                    cur.execute(
                        f"SELECT TOP ({lim}) {ss_col_sql} FROM {table_ref} "  # nosec B608
                        f"ORDER BY {ss_order}"
                    )
                names = _row_names(cur.description)
                rows = cur.fetchall()
            finally:
                cur.close()
                conn.close()
            return [dict(zip(names, row)) for row in rows]

        if db_type in {
            "oracle",
            "oracledb",
            "oracle_db",
            "oracle_autonomous",
            "oracle_autonomous_warehouse",
            "amazon_rds_oracle",
        } or (
            db_type == "generic_sql"
            and (dest.get("connection_string") or "").lower().startswith("oracle")
        ):
            import sqlalchemy as sa

            from connectors.generic_sql import get_sqlalchemy_engine

            lim = max(1, int(limit or 50))
            ora_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            sch = (schema or dest.get("schema") or dest.get("username") or "").strip() or None
            table_ref = quote_table_ref(table_name, schema=sch, dialect="oracle")
            ora_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            engine = get_sqlalchemy_engine(
                {
                    "type": "oracle",
                    "host": dest.get("host", ""),
                    "port": int(dest.get("port") or 1521),
                    "database": dest.get("database", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "schema": schema or dest.get("schema") or "",
                }
            )
            with engine.connect() as conn:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    params: dict[str, Any] = {f"k{i}": k for i, k in enumerate(keys)}
                    params["lim"] = lim
                    placeholders = ",".join(f":k{i}" for i in range(len(keys)))
                    sql = (
                        f"SELECT {ora_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {ora_order} FETCH FIRST :lim ROWS ONLY"
                    )
                else:
                    params = {"lim": lim}
                    sql = (
                        f"SELECT {ora_col_sql} FROM {table_ref} "  # nosec B608
                        f"ORDER BY {ora_order} FETCH FIRST :lim ROWS ONLY"
                    )
                result = conn.execute(sa.text(sql), params)
                names = (
                    list(cols)
                    if cols and cols != ["*"]
                    else list(result.keys())
                )
                rows = result.fetchall()
                return [dict(zip(names, tuple(row))) for row in rows]

        if db_type == "duckdb" or (
            db_type == "generic_sql"
            and (
                "duckdb"
                in (dest.get("connection_string") or dest.get("database") or "").lower()
                or (dest.get("connection_string") or dest.get("database") or "")
                .lower()
                .endswith((".duckdb", ".duck"))
            )
        ):
            import sqlalchemy as sa
            from connectors.generic_sql import get_sqlalchemy_engine

            path = dest.get("connection_string") or dest.get("database", "")
            if not path:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "duckdb path missing (connection_string/database)"
                )
            duckdb_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            table_ref = quote_table_ref(table_name, dialect="duckdb")
            duckdb_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            try:
                engine = get_sqlalchemy_engine(
                    {"type": "duckdb", "connection_string": path}
                )
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc
            with engine.connect() as conn:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    params: dict[str, Any] = {f"k{i}": k for i, k in enumerate(keys)}
                    params["lim"] = int(limit)
                    placeholders = ",".join(f":k{i}" for i in range(len(keys)))
                    sql = (
                        f"SELECT {duckdb_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {duckdb_order} LIMIT :lim"
                    )
                else:
                    params = {"lim": int(limit)}
                    sql = f"SELECT {duckdb_col_sql} FROM {table_ref} ORDER BY {duckdb_order} LIMIT :lim"  # nosec B608
                try:
                    result = conn.execute(sa.text(sql), params)
                    rows = result.mappings().all()
                    # DuckDB returns column labels using the sanitized (underscore)
                    # form of names like "fields.Name".  Re-label them with the
                    # requested target column names so reconciliation keys match
                    # the engine's mapping targets.
                    if cols and cols != ["*"]:
                        return [
                            {cols[i]: list(row.values())[i] for i in range(len(cols))}
                            for row in rows
                        ]
                    return [dict(row) for row in rows]
                except TargetSampleUnavailable:
                    raise
                except Exception as exc:
                    raise TargetSampleUnavailable(
                        f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                    ) from exc

        if db_type == "mongodb":
            from connectors.mongodb_common import (
                _mongo_client,
                normalize_mongodb_connection_string,
            )

            try:
                conn_str = normalize_mongodb_connection_string(
                    dest.get("connection_string", ""),
                    database=dest.get("database", ""),
                    host=dest.get("host", ""),
                    port=int(dest.get("port") or 27017),
                    username=dest.get("username", ""),
                    password=dest.get("password", ""),
                    ssl=bool(dest.get("ssl", False)),
                    auth_source=dest.get("auth_source", ""),
                )
                client = _mongo_client(conn_str)
                db = client[dest.get("database") or "test"]
                coll = db[table_name]
                query_filter: dict[str, Any] = {}
                if keys and sort_key:
                    # Mongo $in is type-sensitive; widened key set matches strings,
                    # integers, and decimals that the writer may have produced.
                    widened: set[Any] = set()
                    for k in keys:
                        widened.add(k)
                        try:
                            if str(k).isdigit():
                                widened.add(int(k))
                        except Exception as exc:
                            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                        try:
                            widened.add(float(k))
                        except Exception as exc:
                            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                        # ObjectId keys from schemaless sources are serialized as hex strings.
                        try:
                            from bson import ObjectId

                            if (
                                isinstance(k, str)
                                and len(k) == 24
                                and all(c in "0123456789abcdefABCDEF" for c in k)
                            ):
                                widened.add(ObjectId(k))
                        except Exception as exc:
                            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                    query_filter = {sort_key: {"$in": list(widened)}}
                cursor = coll.find(query_filter)
                if sort_key:
                    cursor = cursor.sort(sort_key, 1)
                return list(cursor.limit(int(limit)))
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type == "sqlite" or (
            db_type == "generic_sql"
            and (
                "sqlite"
                in (dest.get("connection_string") or dest.get("database") or "").lower()
                or (dest.get("connection_string") or dest.get("database") or "")
                .lower()
                .endswith((".db", ".sqlite"))
            )
        ):
            import sqlite3

            from connectors.sqlite_common import sqlite_file_path

            path = sqlite_file_path(
                dest.get("database") or "",
                dest.get("connection_string") or "",
                dest.get("host") or "",
            )
            if not path:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "sqlite path missing"
                )
            sqlite_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            table_ref = quote_table_ref(table_name, dialect="sqlite")
            sqlite_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            conn = sqlite3.connect(str(path))
            try:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    placeholders = ",".join(["?"] * len(keys))
                    sql = f"SELECT {sqlite_col_sql} FROM {table_ref} WHERE {key_col} IN ({placeholders}) ORDER BY {sqlite_order} LIMIT ?"  # nosec B608
                    cur = conn.execute(sql, [*keys, int(limit)])
                else:
                    sql = f"SELECT {sqlite_col_sql} FROM {table_ref} ORDER BY {sqlite_order} LIMIT ?"  # nosec B608
                    cur = conn.execute(sql, (int(limit),))
                rows = cur.fetchall()
                names = _row_names(cur.description)
                return [dict(zip(names, row)) for row in rows]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc
            finally:
                conn.close()

        if db_type == "redis":
            from connectors.redis_reader import _decode, _redis_client
            from connectors.sql_identifiers import sanitize_identifier

            prefix = table_name or "dataflow"
            cfg = {
                "host": dest.get("host", ""),
                "port": int(dest.get("port") or 6379),
                "database": dest.get("database", "0"),
                "username": dest.get("username", ""),
                "password": dest.get("password", ""),
                "connection_string": dest.get("connection_string", ""),
                "ssl": bool(dest.get("ssl", False)),
            }
            try:
                client = _redis_client(cfg)
                rows_out: list[dict[str, Any]] = []
                if keys and sort_key:
                    # Writer stores keys as ``prefix:<sanitized_id>``.
                    key_names = [
                        f"{prefix}:{sanitize_identifier(str(k), preserve_case=True)}"
                        for k in keys
                    ]
                    for raw in client.mget(key_names):
                        text = _decode(raw)
                        if not text:
                            continue
                        try:
                            payload = json.loads(text)
                        except (json.JSONDecodeError, TypeError):
                            payload = {"value": text}
                        if isinstance(payload, dict):
                            rows_out.append(payload)
                        else:
                            rows_out.append({"value": payload})
                        if len(rows_out) >= limit:
                            break
                else:
                    pattern = f"{prefix}:*" if prefix else "*"
                    cursor = 0
                    while True:
                        cursor, batch = client.scan(
                            cursor=cursor, match=pattern, count=500
                        )
                        for raw_key in batch:
                            key = (
                                raw_key.decode()
                                if isinstance(raw_key, bytes)
                                else str(raw_key)
                            )
                            raw = client.get(key)
                            text = _decode(raw)
                            try:
                                payload = (
                                    json.loads(text)
                                    if text.startswith("{")
                                    else {"value": text}
                                )
                            except (json.JSONDecodeError, TypeError):
                                payload = {"value": text}
                            if isinstance(payload, dict):
                                rows_out.append(payload)
                            else:
                                rows_out.append({"value": payload})
                            if len(rows_out) >= limit:
                                break
                        if cursor == 0 or len(rows_out) >= limit:
                            break
                if columns:
                    rows_out = [
                        {k: v for k, v in row.items() if k in columns}
                        for row in rows_out
                    ]
                return rows_out[:limit]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type in {"elasticsearch", "opensearch", "elastic"}:
            from connectors.elasticsearch_reader import read_index_batch

            cfg = {
                "host": dest.get("host", ""),
                "port": int(dest.get("port") or 9200),
                "username": dest.get("username", ""),
                "password": dest.get("password", ""),
                "connection_string": dest.get("connection_string", ""),
                "ssl": bool(dest.get("ssl", False)),
                "api_key": str(dest.get("api_key") or dest.get("service_account") or ""),
            }
            try:
                batch, _ = read_index_batch(
                    cfg=cfg,
                    index=table_name,
                    columns=None if cols == ["*"] else cols,
                    limit=max(1, int(limit or 50)),
                )
                headers = list(batch.headers or [])
                rows_out: list[dict[str, Any]] = []
                for row in batch.rows or []:
                    if isinstance(row, dict):
                        payload = row
                    elif headers:
                        payload = {
                            headers[i]: row[i] if i < len(row) else None
                            for i in range(len(headers))
                        }
                    else:
                        continue
                    if keys and sort_key:
                        sk = str(payload.get(sort_key, ""))
                        if sk not in {str(k) for k in keys}:
                            continue
                    rows_out.append(payload)
                    if len(rows_out) >= limit:
                        break
                if columns and columns != ["*"]:
                    rows_out = [
                        {k: v for k, v in row.items() if k in columns}
                        for row in rows_out
                    ]
                return rows_out[:limit]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type == "snowflake":
            from connectors.snowflake_conn import (
                get_connection,
                normalize_account,
                resolve_or_fold_snowflake_table,
                snowflake_qualified_table,
            )

            try:
                conn = get_connection(
                    account=normalize_account(dest.get("host", "")),
                    username=dest.get("username", ""),
                    password=dest.get("password", ""),
                    database=dest.get("database", ""),
                    schema=schema or "PUBLIC",
                    warehouse=dest.get("warehouse", ""),
                    connection_string=dest.get("connection_string", ""),
                )
                from connectors.sql_identifiers import (
                    quote_sql_identifier,
                    require_safe_identifier,
                )

                with conn.cursor() as cur:
                    resolved = resolve_or_fold_snowflake_table(
                        cur, schema or "PUBLIC", table_name
                    )
                    qualified_name = snowflake_qualified_table(
                        schema or "PUBLIC", resolved
                    )
                    sf_col_sql = (
                        "*"
                        if cols == ["*"]
                        else quote_column_list(
                            [
                                require_safe_identifier(c, preserve_case=True)
                                for c in cols
                            ]
                        )
                    )
                    sf_order = (
                        quote_sql_identifier(
                            require_safe_identifier(sort_key, preserve_case=True)
                        )
                        if sort_key
                        else "1"
                    )
                    if keys and sort_key:
                        key_col = quote_sql_identifier(
                            require_safe_identifier(sort_key, preserve_case=True)
                        )
                        # Snowflake IN is type-sensitive; widen strings to ints/floats.
                        widened: set[Any] = set()
                        for k in keys:
                            widened.add(k)
                            try:
                                if str(k).isdigit():
                                    widened.add(int(k))
                            except Exception as exc:
                                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                            try:
                                widened.add(float(k))
                            except Exception as exc:
                                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                        placeholders = ",".join(["%s"] * len(widened))
                        cur.execute(
                            f"SELECT {sf_col_sql} FROM {qualified_name} "  # nosec B608
                            f"WHERE {key_col} IN ({placeholders}) "
                            f"ORDER BY {sf_order} LIMIT %s",
                            (*widened, int(limit)),
                        )
                    else:
                        cur.execute(
                            f"SELECT {sf_col_sql} FROM {qualified_name} ORDER BY {sf_order} LIMIT %s",  # nosec B608
                            (int(limit),),
                        )
                    names = _row_names(cur.description)
                    rows = cur.fetchall()
                conn.close()
                return [dict(zip(names, row)) for row in rows]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type == "bigquery":
            from connectors.bigquery_conn import get_client, _is_local_endpoint

            project_id = dest.get("database", "")
            dataset_id = schema or "dataflow"
            is_local, _ = _is_local_endpoint(
                dest.get("host", ""), dest.get("connection_string", "")
            )
            try:
                client = get_client(
                    project_id=project_id,
                    credentials_path=dest.get("connection_string", ""),
                    service_account=dest.get("service_account", ""),
                    host=dest.get("host", ""),
                    port=int(dest.get("port") or 0),
                    connection_string=dest.get("connection_string", ""),
                )
                table_id = f"{project_id}.{dataset_id}.{table_name}"
                if is_local:
                    # Emulator path: scan rows and filter in-process; avoids
                    # query().result() hangs on the goccy emulator for some jobs.
                    out: list[dict[str, Any]] = []
                    scan_limit = (limit or 50) * 10 if (keys and sort_key) else (limit or 50)
                    widened = set()
                    if keys and sort_key:
                        for k in keys:
                            widened.add(k)
                            try:
                                if str(k).isdigit():
                                    widened.add(int(k))
                            except Exception as exc:
                                logger.debug("Could not widen key %r to int: %s", k, exc)
                            try:
                                widened.add(float(k))
                            except Exception as exc:
                                logger.debug("Could not widen key %r to float: %s", k, exc)
                    for row in client.list_rows(table_id, max_results=scan_limit):
                        d = dict(row.items()) if hasattr(row, "items") else {k: v for k, v in zip(cols, row)}
                        if cols and cols != ["*"]:
                            d = {k: v for k, v in d.items() if k in cols}
                        if keys and sort_key:
                            if d.get(sort_key) in widened:
                                out.append(d)
                        else:
                            out.append(d)
                        if len(out) >= (limit or 50):
                            break
                    return out
                # Production: use a real BigQuery query with a bounded timeout.
                col_sql = (
                    "*"
                    if cols == ["*"]
                    else quote_column_list(
                        [require_safe_identifier(c, preserve_case=True) for c in cols]
                    )
                )
                bq_order = (
                    quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    if sort_key
                    else "1"
                )
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    placeholders = ",".join(["%s"] * len(keys))
                    sql = (
                        f"SELECT {col_sql} FROM `{table_id}` "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {bq_order} LIMIT %s"
                    )
                    params = (*keys, int(limit))
                else:
                    sql = f"SELECT {col_sql} FROM `{table_id}` ORDER BY {bq_order} LIMIT %s"  # nosec B608
                    params = (int(limit),)
                res = client.query(sql, timeout=60).result()
                names = list(res.schema) if res.schema else cols
                if names and names[0] and not isinstance(names[0], str):
                    names = [f.name for f in names]
                return [
                    {k: v for k, v in dict(row.items()).items() if k in (cols if cols != ["*"] else dict(row.items()).keys())}
                    for row in res
                ]
            except TargetSampleUnavailable:
                raise
            except Exception as exc:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
                ) from exc

        if db_type in {
            "adls",
            "azure_blob_storage",
            "azure_data_lake",
            "azure_data_lake_storage",
        }:
            from connectors.adls_common import blob_service_client
            from connectors.adls_reader import list_objects

            container = (dest.get("database") or schema or "").strip()
            if not container or not table_name:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "ADLS container or blob path missing"
                )
            cfg_adls = {
                "host": dest.get("host", ""),
                "port": int(dest.get("port") or 0),
                "username": dest.get("username", ""),
                "password": dest.get("password", ""),
                "connection_string": dest.get("connection_string", ""),
                "service_account": dest.get("service_account", ""),
                "database": container,
            }
            client = blob_service_client(cfg_adls)
            return _object_store_target_sample(
                table_name=table_name,
                list_keys=lambda prefix: list_objects(cfg_adls, container, prefix),
                fetch_bytes=lambda k: (
                    client.get_blob_client(container, k).download_blob().readall()
                ),
                cols=cols,
                limit=limit,
                sort_key=sort_key,
                keys=keys,
            )

        if db_type in {"s3", "minio", "s3_compatible", "aws_s3"}:
            from connectors.aws_common import boto3_client
            from connectors.s3_reader import list_objects

            bucket = (dest.get("database") or schema or "").strip()
            if not bucket or not table_name:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "S3 bucket or object key missing"
                )
            cfg_s3 = {
                "host": dest.get("host", ""),
                "port": int(dest.get("port") or 0),
                "username": dest.get("username", ""),
                "password": dest.get("password", ""),
                "connection_string": dest.get("connection_string", ""),
                "ssl": bool(dest.get("ssl", False)),
                "database": bucket,
                "endpoint_url": dest.get("endpoint_url", "") or "",
                "path_style": bool(dest.get("path_style", False)),
                "region": dest.get("region", "") or "",
            }
            client = boto3_client("s3", cfg_s3)
            return _object_store_target_sample(
                table_name=table_name,
                list_keys=lambda prefix: list_objects(cfg_s3, bucket, prefix),
                fetch_bytes=lambda k: client.get_object(Bucket=bucket, Key=k)["Body"].read(),
                cols=cols,
                limit=limit,
                sort_key=sort_key,
                keys=keys,
            )

        if db_type in {"gcs", "google_cloud_storage"}:
            from connectors.gcs_common import gcs_client
            from connectors.gcs_reader import list_objects

            bucket = (dest.get("database") or schema or "").strip()
            if not bucket or not table_name:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "GCS bucket or object key missing"
                )
            cfg_gcs = {
                "host": dest.get("host", ""),
                "port": int(dest.get("port") or 0),
                "connection_string": dest.get("connection_string", ""),
                "service_account": dest.get("service_account", ""),
                "password": dest.get("password", ""),
            }
            bucket_obj = gcs_client(cfg_gcs).bucket(bucket)
            return _object_store_target_sample(
                table_name=table_name,
                list_keys=lambda prefix: list_objects(cfg_gcs, bucket, prefix),
                fetch_bytes=lambda k: bucket_obj.blob(k).download_as_bytes(),
                cols=cols,
                limit=limit,
                sort_key=sort_key,
                keys=keys,
            )

        if db_type in {
            "databricks",
            "databricks_sql",
            "delta",
            "delta_lake",
            "unity_catalog",
            "spark",
        }:
            import sqlalchemy as sa

            from connectors.generic_sql import get_sqlalchemy_engine

            lim = max(1, int(limit or 50))
            db_col_sql = (
                "*"
                if cols == ["*"]
                else quote_column_list(
                    [require_safe_identifier(c, preserve_case=True) for c in cols]
                )
            )
            sch = (schema or dest.get("schema") or dest.get("database") or "").strip() or None
            table_ref = quote_table_ref(table_name, schema=sch, dialect="ansi")
            db_order = (
                quote_sql_identifier(
                    require_safe_identifier(sort_key, preserve_case=True)
                )
                if sort_key
                else "1"
            )
            engine = get_sqlalchemy_engine(
                {
                    "type": "databricks",
                    "host": dest.get("host", ""),
                    "port": int(dest.get("port") or 443),
                    "database": dest.get("database", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "schema": schema or dest.get("schema") or "",
                    "http_path": str(dest.get("http_path") or dest.get("warehouse") or ""),
                }
            )
            with engine.connect() as conn:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True)
                    )
                    params = {f"k{i}": k for i, k in enumerate(keys)}
                    params["lim"] = lim
                    placeholders = ",".join(f":k{i}" for i in range(len(keys)))
                    sql = (
                        f"SELECT {db_col_sql} FROM {table_ref} "  # nosec B608
                        f"WHERE {key_col} IN ({placeholders}) "
                        f"ORDER BY {db_order} LIMIT :lim"
                    )
                else:
                    params = {"lim": lim}
                    sql = (
                        f"SELECT {db_col_sql} FROM {table_ref} "  # nosec B608
                        f"ORDER BY {db_order} LIMIT :lim"
                    )
                result = conn.execute(sa.text(sql), params)
                names = list(cols) if cols and cols != ["*"] else list(result.keys())
                return [dict(zip(names, tuple(row))) for row in result.fetchall()]

        if db_type == "hubspot":
            from connectors.hubspot import read_object

            batch = read_object(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "table": table_name,
                    "database": table_name,
                },
                object=table_name or "contacts",
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or (cols if cols != ["*"] else []) or [])
            out_rows: list[dict[str, Any]] = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = dict(row)
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "salesforce":
            from connectors.salesforce import read_object

            batch = read_object(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "api_key": dest.get("api_key", ""),
                    "table": table_name,
                    "database": table_name,
                },
                object=table_name or "Account",
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or [])
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = {k: v for k, v in row.items() if k != "attributes"}
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "airtable":
            from connectors.airtable import read_object
            from connectors.saas_typed_schema import flatten_airtable_record

            batch = read_object(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "api_key": dest.get("api_key", ""),
                    "database": dest.get("database") or schema or "",
                    "table": table_name,
                    "type": "airtable",
                },
                object=table_name,
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d, _ = flatten_airtable_record(row)
                elif batch.headers:
                    headers = list(batch.headers)
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type in {"stripe", "shopify", "zendesk", "notion"}:
            if db_type == "stripe":
                from connectors.stripe import read_object as _saas_read
            elif db_type == "shopify":
                from connectors.shopify import read_object as _saas_read
            elif db_type == "zendesk":
                from connectors.zendesk import read_object as _saas_read
            else:
                from connectors.notion import read_object as _saas_read

            batch = _saas_read(
                cfg={
                    "host": dest.get("host", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "api_key": dest.get("api_key", ""),
                    "table": table_name,
                    "database": dest.get("database") or schema or table_name,
                    "shop": dest.get("host", ""),
                    "type": db_type,
                },
                object=table_name
                or (
                    "customers"
                    if db_type in {"stripe", "shopify"}
                    else ("tickets" if db_type == "zendesk" else "")
                ),
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or [])
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = dict(row)
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "kafka":
            from connectors.kafka_reader import read_topic_batch

            batch, _ = read_topic_batch(
                cfg={
                    "host": dest.get("host", ""),
                    "port": int(dest.get("port") or 9092),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "database": table_name,
                    "table": table_name,
                    "group_id": f"dataflow-gate8-sample-{abs(hash(table_name)) % 10_000_000}",
                    "auto_offset_reset": "earliest",
                    "schema_registry_url": str(
                        dest.get("schema_registry_url") or dest.get("registry_url") or ""
                    ),
                },
                topic=table_name,
                columns=None if cols == ["*"] else cols,
                limit=max(1, int(limit or 50)) * 5 if keys else max(1, int(limit or 50)),
            )
            headers = list(batch.headers or [])
            out_rows = []
            for row in batch.rows or []:
                if isinstance(row, dict):
                    d = dict(row)
                elif headers:
                    d = {
                        headers[i]: row[i] if i < len(row) else None
                        for i in range(len(headers))
                    }
                else:
                    continue
                if keys and sort_key and d.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    d = {k: d.get(k) for k in cols}
                out_rows.append(d)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "pinecone":
            from connectors.pinecone_writer import _headers, _index_url, _requests_session

            index_url = _index_url(dest.get("host", ""), dest.get("connection_string", ""))
            key = str(
                dest.get("api_key") or dest.get("password") or dest.get("username") or ""
            )
            if not index_url or not key:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "pinecone index URL or API key missing"
                )
            session = _requests_session()
            hdrs = _headers(key)
            ns = (table_name or dest.get("schema") or "").strip()
            ids = [str(k) for k in keys] if keys else []
            if not ids:
                return []
            params = [("ids", i) for i in ids[: max(1, int(limit or 50))]]
            if ns:
                params.append(("namespace", ns))
            fetch = session.get(
                f"{index_url}/vectors/fetch",
                params=params,
                headers=hdrs,
                timeout=30,
            )
            if fetch.status_code not in {200, 201}:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    f"pinecone fetch HTTP {fetch.status_code}"
                )
            vectors = (fetch.json() or {}).get("vectors") or {}
            out_rows = []
            for vid, payload in vectors.items():
                meta = payload.get("metadata") if isinstance(payload, dict) else {}
                if not isinstance(meta, dict):
                    meta = {}
                row = {"id": vid, **meta}
                if cols and cols != ["*"]:
                    row = {k: row.get(k) for k in cols}
                out_rows.append(row)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "qdrant":
            from connectors.qdrant_writer import _base_url, _headers, _requests_session

            api_key = dest.get("password") or dest.get("username") or ""
            base_url = (
                dest.get("connection_string", "").rstrip("/")
                if dest.get("connection_string")
                else _base_url(
                    dest.get("host", ""),
                    int(dest.get("port") or 6333),
                    bool(dest.get("ssl", False)),
                )
            )
            collection = table_name or dest.get("database") or "dataflow_vectors"
            session = _requests_session()
            hdrs = _headers(str(api_key))
            out_rows: list[dict[str, Any]] = []
            if keys:
                retrieve = session.post(
                    f"{base_url}/collections/{collection}/points",
                    data=json.dumps({
                        "ids": [str(k) for k in keys[: max(1, int(limit or 50))]],
                        "with_payload": True,
                        "with_vector": False,
                    }),
                    headers=hdrs,
                    timeout=30,
                )
                points = (
                    (retrieve.json() or {}).get("result") or []
                    if retrieve.status_code in {200, 201}
                    else []
                )
            else:
                scroll = session.post(
                    f"{base_url}/collections/{collection}/points/scroll",
                    data=json.dumps({
                        "limit": max(1, int(limit or 50)),
                        "with_payload": True,
                        "with_vector": False,
                    }),
                    headers=hdrs,
                    timeout=30,
                )
                points = (
                    ((scroll.json() or {}).get("result") or {}).get("points") or []
                    if scroll.status_code in {200, 201}
                    else []
                )
            for pt in points:
                if not isinstance(pt, dict):
                    continue
                payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                row = {"id": pt.get("id"), **payload}
                if cols and cols != ["*"]:
                    row = {k: row.get(k) for k in cols}
                out_rows.append(row)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

        if db_type == "weaviate":
            from connectors.weaviate_writer import (
                _base_url,
                _class_name,
                _headers,
                _requests_session,
            )

            key = str(
                dest.get("api_key") or dest.get("password") or dest.get("username") or ""
            )
            base_url = _base_url(
                dest.get("host", ""),
                int(dest.get("port") or 8080),
                bool(dest.get("ssl", False)),
                dest.get("connection_string", ""),
            )
            cls = _class_name(table_name or dest.get("database") or "DataflowChunk")
            session = _requests_session()
            hdrs = _headers(key)
            out_rows = []
            if keys:
                for oid in keys[: max(1, int(limit or 50))]:
                    resp = session.get(
                        f"{base_url}/v1/objects/{cls}/{oid}",
                        headers=hdrs,
                        timeout=15,
                    )
                    if resp.status_code not in {200, 201}:
                        resp = session.get(
                            f"{base_url}/v1/objects/{oid}",
                            headers=hdrs,
                            timeout=15,
                        )
                    if resp.status_code not in {200, 201}:
                        continue
                    obj = resp.json() or {}
                    if not isinstance(obj, dict):
                        continue
                    props = (
                        obj.get("properties")
                        if isinstance(obj.get("properties"), dict)
                        else {}
                    )
                    row = {"id": obj.get("id") or oid, **props}
                    if cols and cols != ["*"]:
                        row = {k: row.get(k) for k in cols}
                    out_rows.append(row)
            else:
                agg = session.get(
                    f"{base_url}/v1/objects",
                    params={"class": cls, "limit": max(1, int(limit or 50))},
                    headers=hdrs,
                    timeout=30,
                )
                if agg.status_code in {200, 201}:
                    for obj in (agg.json() or {}).get("objects") or []:
                        if not isinstance(obj, dict):
                            continue
                        props = (
                            obj.get("properties")
                            if isinstance(obj.get("properties"), dict)
                            else {}
                        )
                        row = {"id": obj.get("id"), **props}
                        if cols and cols != ["*"]:
                            row = {k: row.get(k) for k in cols}
                        out_rows.append(row)
                        if len(out_rows) >= int(limit or 50):
                            break
            return out_rows

        if db_type == "milvus":
            from connectors.milvus_writer import (
                _auth_token,
                _base_url,
                _collection_name,
                _headers,
                _ok_response,
                _requests_session,
            )

            coll = _collection_name(table_name or "dataflow_chunks")
            db_name = (dest.get("database") or schema or "").strip()
            if db_name.lower() in {"", "test_db", "default", "public"}:
                db_name = ""
            token = _auth_token(
                api_key=str(dest.get("api_key") or ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
            )
            base_url = _base_url(
                dest.get("host", ""),
                int(dest.get("port") or 19530),
                bool(dest.get("ssl", False)),
                dest.get("connection_string", ""),
            )
            session = _requests_session()
            hdrs = _headers(token)
            query_payload: dict[str, Any] = {
                "collectionName": coll,
                "outputFields": ["id", "content", "source_id", "chunk_index"],
                "limit": max(1, int(limit or 50)),
            }
            if keys:
                quoted = ", ".join(json.dumps(str(k)) for k in keys[: max(1, int(limit or 50))])
                query_payload["filter"] = f"id in [{quoted}]"
            else:
                query_payload["filter"] = ""
            if db_name:
                query_payload["dbName"] = db_name
            query = session.post(
                f"{base_url}/v2/vectordb/entities/query",
                data=json.dumps(query_payload),
                headers=hdrs,
                timeout=30,
            )
            qbody = query.json() if query.content else {}
            out_rows = []
            if _ok_response(qbody if isinstance(qbody, dict) else {}, query.status_code):
                for row in (qbody.get("data") if isinstance(qbody, dict) else []) or []:
                    if not isinstance(row, dict):
                        continue
                    d = {k: v for k, v in row.items() if k != "vector"}
                    if cols and cols != ["*"]:
                        d = {k: d.get(k) for k in cols}
                    out_rows.append(d)
                    if len(out_rows) >= int(limit or 50):
                        break
            return out_rows

        _engine_hint = str(dest.get("type") or dest.get("engine") or "").lower()
        _conn_hint = str(dest.get("connection_string") or dest.get("database") or "").lower()
        if db_type == "clickhouse" or (
            db_type == "generic_sql"
            and ("clickhouse" in _engine_hint or "clickhouse" in _conn_hint)
        ):
            import sqlalchemy as sa

            from connectors.generic_sql import (
                clickhouse_final_table_sql,
                get_sqlalchemy_engine,
            )
            from connectors.sql_identifiers import (
                quote_sql_identifier,
                quote_table_ref,
                require_safe_identifier,
            )

            engine = get_sqlalchemy_engine(
                {
                    "type": "clickhouse",
                    "host": dest.get("host", ""),
                    "port": int(dest.get("port") or 9000),
                    "database": dest.get("database", ""),
                    "username": dest.get("username", ""),
                    "password": dest.get("password", ""),
                    "connection_string": dest.get("connection_string", ""),
                    "schema": schema or dest.get("schema") or "",
                    "ssl": bool(dest.get("ssl", False)),
                }
            )
            table_ref = quote_table_ref(
                table_name,
                schema=schema or dest.get("schema") or None,
                dialect="clickhouse",
            )
            from_sql = clickhouse_final_table_sql(table_ref)
            col_sql = (
                "*"
                if cols == ["*"]
                else ", ".join(
                    quote_sql_identifier(
                        require_safe_identifier(c, preserve_case=True), "`"
                    )
                    for c in cols
                )
            )
            with engine.connect() as conn:
                if keys and sort_key:
                    key_col = quote_sql_identifier(
                        require_safe_identifier(sort_key, preserve_case=True), "`"
                    )
                    placeholders = ", ".join(f":k{i}" for i in range(len(keys)))
                    params = {f"k{i}": k for i, k in enumerate(keys)}
                    result = conn.execute(
                        sa.text(
                            f"SELECT {col_sql} FROM {from_sql} "  # nosec B608
                            f"WHERE {key_col} IN ({placeholders}) "
                            f"LIMIT {int(limit or 50)}"
                        ),
                        params,
                    )
                else:
                    result = conn.execute(
                        sa.text(
                            f"SELECT {col_sql} FROM {from_sql} "  # nosec B608
                            f"LIMIT {int(limit or 50)}"
                        )
                    )
                names = list(result.keys()) if result.keys() else (
                    list(cols) if cols != ["*"] else []
                )
                return [dict(zip(names, row)) for row in result.fetchall()]

        if db_type == "pgvector":
            from connectors.postgresql_conn import get_connection
            from connectors.sql_identifiers import (
                quote_sql_identifier,
                quote_table_ref,
                require_safe_identifier,
            )

            table_ref = quote_table_ref(
                table_name, schema or "public", dialect="postgresql"
            )
            conn = get_connection(
                host=dest.get("host", ""),
                port=dest.get("port", 5432),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                ssl=bool(dest.get("ssl", False)),
            )
            try:
                with conn.cursor() as cur:
                    if keys and sort_key:
                        key_col = quote_sql_identifier(
                            require_safe_identifier(sort_key, preserve_case=True)
                        )
                        placeholders = ",".join(["%s"] * len(keys))
                        cur.execute(
                            f"SELECT id, content, source_id, chunk_index, metadata "  # nosec B608
                            f"FROM {table_ref} WHERE {key_col} IN ({placeholders}) LIMIT %s",
                            (*keys, int(limit or 50)),
                        )
                    else:
                        cur.execute(
                            f"SELECT id, content, source_id, chunk_index, metadata "  # nosec B608
                            f"FROM {table_ref} LIMIT %s",
                            (int(limit or 50),),
                        )
                    names = [d[0] for d in cur.description] if cur.description else []
                    out_rows = []
                    for raw in cur.fetchall():
                        rec = dict(zip(names, raw))
                        meta = rec.get("metadata") or {}
                        if isinstance(meta, str):
                            try:
                                meta = json.loads(meta)
                            except Exception:
                                meta = {}
                        if not isinstance(meta, dict):
                            meta = {}
                        row = {
                            "id": rec.get("id"),
                            "content": rec.get("content"),
                            "source_id": rec.get("source_id"),
                            "chunk_index": rec.get("chunk_index"),
                            **meta,
                        }
                        if cols and cols != ["*"]:
                            row = {k: row.get(k) for k in cols}
                        out_rows.append(row)
                    return out_rows
            finally:
                conn.close()

        if db_type == "sftp":
            from connectors.sftp_common import connect_sftp, parse_sftp_config

            cfg = parse_sftp_config(
                connection_string=dest.get("connection_string", ""),
                host=dest.get("host", ""),
                port=int(dest.get("port") or 22),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                database=dest.get("database", "") or schema or "",
                table=table_name,
            )
            if not cfg.host or not cfg.path:
                raise TargetSampleUnavailable(
                    f"Could not read destination sample from {db_type!r}.{table_name!r}: "
                    "sftp host or path missing"
                )
            transport, sftp = connect_sftp(cfg)
            try:
                with sftp.file(cfg.path, "rb") as fh:
                    body = fh.read()
            finally:
                sftp.close()
                transport.close()
            rows, headers = _rows_from_object_bytes(
                body, cfg.path, None if cols == ["*"] else cols
            )
            out_rows: list[dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    if headers:
                        row = {
                            headers[i]: row[i] if i < len(row) else None
                            for i in range(len(headers))
                        }
                    else:
                        continue
                if keys and sort_key and row.get(sort_key) not in set(keys):
                    continue
                if cols and cols != ["*"]:
                    row = {k: row.get(k) for k in cols}
                out_rows.append(row)
                if len(out_rows) >= int(limit or 50):
                    break
            return out_rows

    except TargetSampleUnavailable:
        raise
    except Exception as exc:
        raise TargetSampleUnavailable(
            f"Could not read destination sample from {db_type!r}.{table_name!r}: {exc}"
        ) from exc
    raise TargetSampleUnavailable(
        f"No sample reader is wired for destination type {db_type!r} "
        f"(table {table_name!r}); refusing to treat as empty"
    )
