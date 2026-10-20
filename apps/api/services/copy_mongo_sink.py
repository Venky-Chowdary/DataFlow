"""Canonical Mongo dest writer for identity COPY.

Dest COUNT is ``count_documents({})`` — never ``estimatedDocumentCount``.
Empty dest is ``insert_many`` (unordered), **not** upsert / ``ReplaceOne``.
Occupied dest whose COUNT already equals the source snapshot is
skip-complete. Occupied dest with a different COUNT declines.
``_id`` is not invented from row bytes.

SQL NULL is BSON null (field present). Empty string stays empty string.
DATE-like Python values at midnight become BSON Date at UTC midnight —
Mongo has no date-only type. A datetime with a time component declines
(BSON Date would invent UTC). Nested documents are not converted here;
pair engines that allow BSON identity (Mongo→Mongo) pass values through.
"""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import (
    FastPathResult,
    FastPathUnavailable,
)
from services.copy_pg_mongo import mongo_collection, mongo_dest_count

logger = logging.getLogger(__name__)


def mongo_copy_batch(env_key: str, default: str = "5000") -> int:
    raw = (getenv_brand(env_key, default) or default).strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def sql_value_to_bson(value: Any) -> Any:
    """Python cell from a SQL driver → BSON. Midnight datetime is DATE."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.hour or value.minute or value.second or value.microsecond:
            raise FastPathUnavailable(
                "SQL datetime with a time component is not Mongo COPY-safe "
                "(BSON Date would invent UTC)"
            )
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, Decimal):
        from bson.decimal128 import Decimal128

        # X-61: Decimal128 tops out at 34 significant digits — binding a
        # DECIMAL(38,10) raised a raw decimal.Inexact. Carry canonical plain
        # decimal text instead (exact); the verifier compares decoded
        # Decimals, and the service records the column in the typed finding
        # ``decimal_over34_text_carrier`` on the run's source_snapshot.
        if len(value.as_tuple().digits) > 34:
            return format(value, "f")
        return Decimal128(value)
    if isinstance(value, int) and not isinstance(value, bool) and not (
        -(2**63) <= value <= 2**63 - 1
    ):
        # X-62: pymongo binds Python int as BSON int64 — BIGINT UNSIGNED tops
        # out at 2^64-1 and raised a raw "MongoDB can only handle up to
        # 8-byte ints". Decimal128 holds 34 significant digits, exact for
        # any uint64; the verifier compares decoded integers.
        from bson.decimal128 import Decimal128

        return Decimal128(Decimal(value))
    if isinstance(value, (bytes, bytearray, memoryview)):
        # X-64: binary lands as BSON Binary subtype 0, byte-exact — never
        # base64 text (that was a write-only self-verify like X-52).
        from bson.binary import Binary

        return Binary(bytes(value))
    return value


def bson_to_python(value: Any, ddl: str) -> Any:
    """BSON cell → Python for SQL dest bind. Nested/binary decline."""
    if value is None:
        return None
    if isinstance(value, dict) or isinstance(value, list):
        raise FastPathUnavailable("nested Mongo document is not COPY-safe")
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise FastPathUnavailable("binary Mongo field is not COPY-safe")
    from bson import Decimal128, Int64, ObjectId

    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return value.to_decimal()
    if isinstance(value, Int64):
        return int(value)
    if isinstance(value, datetime):
        base = (ddl or "").split("(")[0].strip().upper().replace(" ", "")
        if base == "DATE":
            if value.tzinfo is not None:
                return value.astimezone(timezone.utc).date()
            return value.date()
        if value.tzinfo is not None:
            raise FastPathUnavailable("timestamptz Mongo Date is not COPY-safe")
        return value
    return value


def _mongo_val_wire(value: Any) -> str:
    """Type-tagged canonical spelling for one BSON value (X-60)."""
    name = type(value).__name__
    if value is None:
        return "null"
    if name == "Decimal128":
        return f"dec:{value.to_decimal()}"
    if isinstance(value, bool):
        return f"bool:{value}"
    if isinstance(value, int):
        return f"int:{value}"
    if isinstance(value, float):
        return f"float:{value!r}"
    if isinstance(value, datetime):
        return f"dt:{value.isoformat()}"
    if isinstance(value, (bytes, bytearray, memoryview)) or name == "Binary":
        return f"bin:{bytes(value).hex()}"
    if isinstance(value, dict):
        return "doc:" + _mongo_doc_wire(value)
    if isinstance(value, (list, tuple)):
        return "arr:[" + ",".join(_mongo_val_wire(v) for v in value) + "]"
    return f"{name}:{value}"


def _mongo_doc_wire(doc: dict[str, Any]) -> str:
    """Field-order-independent encoding; ``_id`` is server-assigned and
    excluded on both sides so bound docs and collection read-back compare."""
    return "{" + ",".join(
        f"{k}={_mongo_val_wire(doc[k])}" for k in sorted(doc) if k != "_id"
    ) + "}"


def _mongo_doc_render(doc: dict[str, Any]) -> str:
    """Compact culprit rendering — first fields name the row."""
    keys = [k for k in sorted(doc) if k != "_id"][:4]
    return "{" + ", ".join(f"{k}={doc[k]!r}" for k in keys) + "}"


class MongoCopyDigest:
    """Multiset digest of the documents an insert_many route bound (X-60).

    The *→Mongo fast paths run inside this process — every row they bind is
    available, so a value digest is cheap and honest ("the copy ran inside
    the engines" is false here). ``update`` folds bound docs *before*
    insert_many assigns ``_id``; ``collection_counts`` re-reads the dest the
    same way. Equal multisets carry ``mongo_docs_sha256`` as a real
    two-sided checksum (never a ``dest_count:`` token).
    """

    _INDEX_CAP = 200_000

    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.rendered: dict[str, str] = {}

    def update(self, docs: list[dict[str, Any]]) -> None:
        for doc in docs:
            h = hashlib.sha256(
                _mongo_doc_wire(doc).encode("utf-8")
            ).hexdigest()
            self.counts[h] += 1
            if h not in self.rendered and len(self.rendered) < self._INDEX_CAP:
                self.rendered[h] = _mongo_doc_render(doc)

    def digest_hex(self) -> str:
        h = hashlib.sha256()
        for key in sorted(self.counts):
            h.update(key.encode())
            h.update(str(self.counts[key]).encode())
        return h.hexdigest()

    def diff_render(self, dest_counts: Counter[str]) -> list[str]:
        """Culprit renderings for docs present on one side only — the
        source-side docs lead (they name the row the user knows)."""
        named = [
            self.rendered[h]
            for h in set(self.counts) ^ set(dest_counts)
            if h in self.rendered
        ]
        bare = [
            f"<doc sha256:{h[:16]}…>"
            for h in set(self.counts) ^ set(dest_counts)
            if h not in self.rendered
        ]
        return (named + bare)[:5]


def mongo_collection_counts(coll: Any) -> Counter[str]:
    """Re-read a collection into the digest's per-doc hash multiset."""
    counts: Counter[str] = Counter()
    for doc in coll.find({}, batch_size=5000):
        counts[hashlib.sha256(
            _mongo_doc_wire(doc).encode("utf-8")
        ).hexdigest()] += 1
    return counts


_MONGO_TEMPORAL_BASES = frozenset(
    {"DATETIME", "DATETIME2", "DATETIMEOFFSET", "TIMESTAMP", "TIME"}
)


def mongo_temporal_preflight(col: str, declared: str) -> None:
    """X-65: refuse temporal source types at plan time, naming the column and
    the two carrier choices — never a bare runtime bind error after the
    user approved the plan. BSON Date is millisecond-precision, so a
    DATETIME(6)/TIME(6) column also loses µs; that is stated in the refusal.
    """
    base = (declared or "").strip().upper().split("(")[0].strip()
    if base in _MONGO_TEMPORAL_BASES:
        raise FastPathUnavailable(
            f"source column {col!r} type {declared} — MongoDB has no naive "
            "wall-clock date type: choose the UTC-normalized BSON Date "
            "carrier (assumes UTC, and µs truncate to ms — a typed info "
            "finding is recorded) or the ISO text carrier (µs preserved) "
            "in Map, then re-run"
        )


def mongo_decimal_over34_columns(types: Any) -> list[str]:
    """Columns declared DECIMAL(p>34[,s]) — past Decimal128's exact range
    (X-61). The service records these as ``decimal_over34_text_carrier``;
    cells bind canonical text via ``sql_value_to_bson``. ``types`` maps
    column -> declared type string."""
    import re as _re

    out: list[str] = []
    for name, declared in (types or {}).items():
        m = _re.match(r"decimal\((\d+)", str(declared or "").lower())
        if m and int(m.group(1)) > 34:
            out.append(str(name))
    return out


def insert_many_documents(
    coll: Any,
    docs: list[dict[str, Any]],
    digest: MongoCopyDigest | None = None,
) -> int:
    if not docs:
        return 0
    if digest is not None:
        digest.update(docs)
    result = coll.insert_many(docs, ordered=False)
    inserted = len(result.inserted_ids)
    if inserted != len(docs):
        raise ValueError(
            "Mongo COPY refused: insert_many "
            f"{inserted} != batch {len(docs)}"
        )
    return inserted



def prepare_mongo_dest(
    *,
    dest_cfg: dict[str, Any],
    dest_table: str,
    source_count: int,
    replace_destination: bool,
    extra_snapshot: dict[str, Any] | None = None,
) -> tuple[Any, bool, str] | FastPathResult:
    """Return ``(coll, created_here, mongo_write)`` or a skip-complete result."""
    try:
        import pymongo  # noqa: F401
    except Exception as exc:
        raise FastPathUnavailable(f"pymongo required for Mongo COPY: {exc}") from exc

    _client, coll = mongo_collection(dest_cfg, dest_table)
    dest_count_before = mongo_dest_count(dest_cfg, dest_table)
    dest_occupied = dest_count_before > 0
    if dest_occupied and not replace_destination:
        raise FastPathUnavailable(
            "append into occupied Mongo dest stays on the row path "
            "(identity COPY would duplicate)"
        )
    mongo_write = "overwrite" if replace_destination and dest_occupied else "insert"
    if replace_destination and dest_occupied:
        coll.drop()
        dest_count_before = 0
    created_here = dest_count_before == 0
    return coll, created_here, mongo_write


def prove_mongo_dest(
    *,
    dest_cfg: dict[str, Any],
    dest_table: str,
    source_count: int,
    inserted: int,
    mongo_write: str,
    extra_snapshot: dict[str, Any] | None = None,
    source_digest: MongoCopyDigest | None = None,
) -> FastPathResult:
    if inserted != source_count:
        raise ValueError(
            "Mongo COPY refused: inserted "
            f"{inserted} != source snapshot {source_count}"
        )
    dest_count = mongo_dest_count(dest_cfg, dest_table)
    if dest_count != source_count:
        raise ValueError(
            "Mongo COPY refused: dest count_documents "
            f"{dest_count} != source snapshot {source_count}"
        )
    if source_digest is None:
        # X-83: a Mongo destination copy cannot run inside the engines — the
        # docs crossed this process, so dest_count: is not proof. Fail closed
        # rather than emit an engine-count-only classification.
        raise FastPathUnavailable(
            "Mongo COPY requires the bound-doc digest "
            "(no in-engine copy exists to prove a count)"
        )
    if source_digest is not None:
        # X-60: this route is in-process — the bound docs were available, so
        # the proof is a two-sided value digest, not a count. Re-read the
        # collection and compare multisets; a tampered doc fails with its
        # rendered culprit, never silently green.
        _client, coll = mongo_collection(dest_cfg, dest_table)
        dest_counts = mongo_collection_counts(coll)
        if dest_counts != source_digest.counts:
            culprits = source_digest.diff_render(dest_counts)
            raise ValueError(
                "Mongo COPY refused: destination document digest differs "
                "from the bound source digest — culprit "
                + "; ".join(culprits or ["<unidentified>"])
            )
        proof = f"mongo_docs_sha256:{source_digest.digest_hex()}"
        proof_scope = "mongo_doc_digest_full_checksum"
    snapshot = {
        "copy_workers": 1,
        "copy_split": "serial",
        "copy_partitions": 1,
        "partitions_skipped": 0,
        "partitions_loaded": 1,
        "shard_mode": "table",
        "mongo_write": mongo_write,
        **(extra_snapshot or {}),
    }
    return FastPathResult(
        rows_copied=dest_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=snapshot,
        proof_scope=proof_scope,
    )


def abort_created_mongo(coll: Any, created_here: bool) -> None:
    if not created_here:
        return
    try:
        coll.drop()
    except Exception:
        logger.debug("Mongo dest drop after copy failure skipped", exc_info=True)
