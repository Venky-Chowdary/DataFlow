"""The document-shaped fixture the NoSQL scale matrix moves, and its checksum.

Why this shape
--------------
A flat ``id``/``amount`` fixture proves throughput and nothing else. The values
that actually break document and analytical engines are the ones a relational
fixture cannot express: integers past the IEEE-754 exact range, a decimal wider
than a double, a naive timestamp next to a zoned one, unicode in a *key* rather
than a value, and a nested document with arrays of scalars **and** of objects
that has no flat relational shape at all.

Checksum
--------
``content_checksum`` is deliberately **additive and order-independent**: each
row contributes ``int(sha256(projection)[:16], 16)`` and the row contributions
are summed mod 2**64. Two properties fall out of that, and both are used as
assertions by the matrix rather than as a convenience:

* a destination read in a different order than the source still matches, so the
  checksum measures content and not ordering;
* under ``full_refresh_append`` a row-addressed destination holding the fixture
  twice checksums to exactly ``2 * source``, and a key-addressed one to
  ``1 *`` — the same distinction ``sync_mode_probe.expected_rows`` draws for
  counts, now proven over content as well.

A checksum computed from the writer's own buffer would prove nothing, so every
engine module computes it by reading back through that engine's own client.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterator

from services.type_system import create_new_mapping_target_type

MASK64 = (1 << 64) - 1

#: Beyond 2**53 (9007199254740992): a float64 carrier cannot hold these
#: exactly, so any engine that lands them through a double is a fidelity
#: defect rather than a rounding preference.
BIG_INT_BASE = 9_007_199_254_740_993

#: Naive wall-clock. It must not be silently reinterpreted as UTC — the Map
#: step has an ``assume_timezone`` control for declaring the intent, and a
#: writer that guesses is the defect this column exists to catch.
NAIVE_EPOCH = datetime(2021, 3, 14, 1, 59, 26, 535000)
ZONED_EPOCH = datetime(2021, 3, 14, 1, 59, 26, 535000, tzinfo=timezone.utc)

#: Every column is mapped. Leaving one out is not an option: the engine's G13
#: refuses a run with unmapped source columns rather than dropping them, so an
#: "only the easy columns" matrix cannot even execute.
MAPPED_COLUMNS = (
    "id",
    "uid",
    "big_int",
    "amount",
    "ts_naive",
    "ts_zoned",
    "unicode_key",
    "payload",
)

#: Columns the content checksum covers. The two timestamps are deliberately
#: **excluded** and reported separately (``temporal_observation``): BSON ``date``
#: is milliseconds and has no zone, so a microsecond zoned source cannot
#: round-trip through it. Folding that into one number would either hide the
#: collapse or fail every document cell for a documented carrier limit; naming
#: it keeps both facts visible.
CHECKSUM_COLUMNS = ("id", "uid", "big_int", "amount", "unicode_key", "payload")

# Back-compat alias for callers that want "the mapped projection".
PROJECTION = MAPPED_COLUMNS

RELATIONAL_DDL_PG = """
  id BIGINT PRIMARY KEY,
  uid TEXT NOT NULL,
  big_int BIGINT NOT NULL,
  amount NUMERIC(24,6) NOT NULL,
  ts_naive TIMESTAMP NOT NULL,
  ts_zoned TIMESTAMPTZ NOT NULL,
  unicode_key TEXT NOT NULL,
  payload JSONB NOT NULL
"""

RELATIONAL_DDL_MYSQL = """
  id BIGINT PRIMARY KEY,
  uid VARCHAR(64) NOT NULL,
  big_int BIGINT NOT NULL,
  amount DECIMAL(24,6) NOT NULL,
  ts_naive DATETIME(6) NOT NULL,
  ts_zoned TIMESTAMP(6) NOT NULL,
  unicode_key VARCHAR(128) NOT NULL,
  payload JSON NOT NULL
"""


def uid_for(seq: int) -> str:
    """Deterministic UUID text so a re-seed reproduces the same checksum."""
    digest = hashlib.sha1(f"dataflow-scale-uid-{seq}".encode()).hexdigest()
    return (
        f"{digest[0:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"
    )


def unicode_key_for(seq: int) -> str:
    """Unicode in the key position, including CJK, Cyrillic and an emoji."""
    return f"ключ-日本語-🚀-{seq}"


def amount_for(seq: int) -> Decimal:
    """Wider than a double can hold exactly at these magnitudes."""
    return Decimal(f"{seq}.{seq % 1_000_000:06d}")


def big_int_for(seq: int) -> int:
    return BIG_INT_BASE + seq


def payload_for(seq: int) -> dict[str, Any]:
    """A document that exceeds the flat relational shape.

    Heterogeneity is on purpose and varies by ``seq % 4``:

    * arrays of scalars and arrays of objects in the same document;
    * ``mixed`` holds a different JSON *type* per document (int / str / bool /
      null), which a column-typed destination cannot absorb without either a
      declared JSON carrier or a quarantine;
    * ``only_some`` is missing from three quarters of the documents, and
      ``extra_*`` appears in one quarter only.
    """
    variant = seq % 4
    doc: dict[str, Any] = {
        "profile": {
            "name": f"acct-{seq}",
            "tags": [f"t{seq % 7}", f"t{seq % 11}", "全部"],
            "addresses": [
                {"kind": "home", "city": f"city-{seq % 97}", "zip": seq % 100000},
                {"kind": "work", "city": f"ville-{seq % 89}", "zip": (seq * 7) % 100000},
            ],
            "deep": {"l2": {"l3": {"l4": {"leaf": f"depth-{seq}"}}}},
        },
        "mixed": [seq, f"s{seq}", True, None][variant],
        "счёт": seq % 13,
    }
    if variant == 0:
        doc["only_some"] = {"present": True, "seq": seq}
    if variant == 1:
        doc[f"extra_{seq % 5}"] = [seq, seq + 1]
    return doc


def row_for(seq: int) -> dict[str, Any]:
    """One fixture row in canonical (python) types."""
    return {
        "id": seq,
        "uid": uid_for(seq),
        "big_int": big_int_for(seq),
        "amount": amount_for(seq),
        "ts_naive": NAIVE_EPOCH + timedelta(seconds=seq),
        "ts_zoned": ZONED_EPOCH + timedelta(seconds=seq),
        "unicode_key": unicode_key_for(seq),
        "payload": payload_for(seq),
    }


def iter_rows(rows: int, *, start: int = 1) -> Iterator[dict[str, Any]]:
    for seq in range(start, start + rows):
        yield row_for(seq)


def normalize_amount(value: Any) -> str:
    """Canonical decimal text so PG ``numeric`` and Mongo ``Decimal128`` agree.

    A ``float`` reaching here is not silently repaired: it is normalized the
    same way, which makes a double-carried decimal fall out of the checksum
    instead of quietly matching.
    """
    if isinstance(value, Decimal):
        dec = value
    elif isinstance(value, float):
        dec = Decimal(repr(value))
    else:
        text = str(value)
        try:
            dec = Decimal(text)
        except Exception:  # noqa: BLE001 — an unparseable amount is a result
            return f"!{text}"
    return f"{dec.quantize(Decimal('0.000001')):f}"


def normalize_payload(value: Any) -> str:
    """Canonical JSON text for the nested document, key-sorted.

    Every engine gets the same treatment, so a store that dropped a nested key
    or flattened one away changes the checksum rather than passing.
    """
    import json

    if value is None:
        return "!missing"
    if isinstance(value, (dict, list)):
        obj = value
    else:
        try:
            obj = json.loads(str(value))
        except Exception:  # noqa: BLE001 — unparseable payload is a result
            return f"!{str(value)[:64]}"
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def row_fingerprint(
    *,
    id_value: Any,
    uid: Any,
    big_int: Any,
    amount: Any,
    unicode_key: Any,
    payload: Any,
) -> int:
    """Content fingerprint of one row's mapped projection."""
    parts = [
        str(int(id_value)),
        str(uid),
        str(int(big_int)),
        normalize_amount(amount),
        str(unicode_key),
        normalize_payload(payload),
    ]
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def content_checksum(rows: Any) -> tuple[int, int]:
    """``(row_count, additive checksum)`` over an iterable of projection dicts."""
    total = 0
    count = 0
    for row in rows:
        count += 1
        total = (
            total
            + row_fingerprint(
                id_value=row["id"],
                uid=row["uid"],
                big_int=row["big_int"],
                amount=row["amount"],
                unicode_key=row["unicode_key"],
                payload=row["payload"],
            )
        ) & MASK64
    return count, total


def expected_checksum(rows: int, *, start: int = 1) -> tuple[int, int]:
    """The checksum the fixture itself carries — computed, never stored."""
    return content_checksum(
        {
            "id": r["id"],
            "uid": r["uid"],
            "big_int": r["big_int"],
            "amount": r["amount"],
            "unicode_key": r["unicode_key"],
            "payload": r["payload"],
        }
        for r in iter_rows(rows, start=start)
    )


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def temporal_observation(seq: int, ts_naive: Any, ts_zoned: Any) -> dict[str, Any]:
    """How the two timestamps of row ``seq`` actually landed.

    Reported rather than folded into the checksum, and it distinguishes the
    three outcomes that matter:

    * ``exact`` — same instant, same precision, and the naive value still has
      no zone;
    * ``truncated_ms`` — a millisecond carrier (BSON ``date``) dropped the
      microseconds; a documented carrier limit, not a silent loss, and it is
      named here so it cannot be quoted as an exact round trip;
    * ``naive_reinterpreted`` — the naive value came back zone-aware. That is
      the failure the ``assume_timezone`` control exists to prevent, because
      the store guessed a zone the source never declared.
    """
    expected_naive = NAIVE_EPOCH + timedelta(seconds=seq)
    expected_zoned = ZONED_EPOCH + timedelta(seconds=seq)
    notes: list[str] = []

    naive_got = ts_naive
    if isinstance(naive_got, str):
        try:
            naive_got = datetime.fromisoformat(naive_got.replace("Z", "+00:00"))
        except ValueError:
            notes.append("naive_unparseable")
    if isinstance(naive_got, (int, float)):
        naive_got = datetime.fromtimestamp(float(naive_got) / 1000.0, tz=timezone.utc)
        notes.append("naive_from_epoch_millis")
    if isinstance(naive_got, datetime):
        if naive_got.tzinfo is not None:
            notes.append("naive_reinterpreted")
            naive_cmp = naive_got.replace(tzinfo=None)
        else:
            naive_cmp = naive_got
        if naive_cmp != expected_naive:
            if naive_cmp == expected_naive.replace(microsecond=(expected_naive.microsecond // 1000) * 1000):
                notes.append("naive_truncated_ms")
            else:
                notes.append("naive_shifted")

    zoned_got = ts_zoned
    if isinstance(zoned_got, str):
        try:
            zoned_got = datetime.fromisoformat(zoned_got.replace("Z", "+00:00"))
        except ValueError:
            notes.append("zoned_unparseable")
    if isinstance(zoned_got, (int, float)):
        zoned_got = datetime.fromtimestamp(float(zoned_got) / 1000.0, tz=timezone.utc)
    if isinstance(zoned_got, datetime):
        if zoned_got.tzinfo is None:
            notes.append("zoned_lost_zone")
            zoned_cmp = zoned_got.replace(tzinfo=timezone.utc)
        else:
            zoned_cmp = zoned_got.astimezone(timezone.utc)
        if zoned_cmp != expected_zoned:
            if zoned_cmp.replace(microsecond=0) == expected_zoned.replace(microsecond=0):
                notes.append("zoned_truncated_ms")
            else:
                notes.append("zoned_shifted")

    return {
        "seq": seq,
        "ts_naive_expected": _iso(expected_naive),
        "ts_naive_got": _iso(ts_naive),
        "ts_zoned_expected": _iso(expected_zoned),
        "ts_zoned_got": _iso(ts_zoned),
        "verdict": "exact" if not notes else ",".join(sorted(set(notes))),
    }


#: The physical source DDL each engine actually holds the fixture in. Map stamps
#: a create-new target type from the *source* type, so an under-declared mapping
#: is not a shortcut: it makes the engine infer a carrier from sampled values,
#: and the fixture's ``9007199254740993`` then plans as INTEGER and fail-closes
#: on the declared narrowing. The harness therefore declares what it seeded.
SOURCE_TYPES: dict[str, dict[str, str]] = {
    "postgresql": {
        "id": "BIGINT",
        "uid": "TEXT",
        "big_int": "BIGINT",
        "amount": "NUMERIC(24,6)",
        "ts_naive": "TIMESTAMP",
        "ts_zoned": "TIMESTAMPTZ",
        "unicode_key": "TEXT",
        "payload": "JSONB",
    },
    "mysql": {
        "id": "BIGINT",
        "uid": "VARCHAR(64)",
        "big_int": "BIGINT",
        "amount": "DECIMAL(24,6)",
        "ts_naive": "DATETIME(6)",
        "ts_zoned": "TIMESTAMP(6)",
        "unicode_key": "VARCHAR(128)",
        "payload": "JSON",
    },
    "duckdb": {
        "id": "BIGINT",
        "uid": "VARCHAR",
        "big_int": "BIGINT",
        "amount": "DECIMAL(24,6)",
        "ts_naive": "TIMESTAMP",
        "ts_zoned": "TIMESTAMPTZ",
        "unicode_key": "VARCHAR",
        "payload": "VARCHAR",
    },
    "mongodb": {
        "id": "BIGINT",
        "uid": "VARCHAR",
        "big_int": "BIGINT",
        "amount": "DECIMAL(24,6)",
        "ts_naive": "TIMESTAMP",
        "ts_zoned": "TIMESTAMP",
        "unicode_key": "VARCHAR",
        "payload": "JSON",
    },
    # Redis stores one JSON string per key and DynamoDB stores S/N attributes:
    # both carry the projection as text, which is why the content checksum —
    # not the declared type — is what proves the values survived.
    "redis": {col: "VARCHAR" for col in MAPPED_COLUMNS} | {"payload": "JSON"},
    "dynamodb": {
        "id": "VARCHAR",
        "uid": "VARCHAR",
        "big_int": "DECIMAL(38,0)",
        "amount": "DECIMAL(24,6)",
        "ts_naive": "VARCHAR",
        "ts_zoned": "VARCHAR",
        "unicode_key": "VARCHAR",
        "payload": "JSON",
    },
    "bigquery_emulator": {
        "id": "INT64",
        "uid": "STRING",
        "big_int": "INT64",
        "amount": "NUMERIC",
        "ts_naive": "DATETIME",
        "ts_zoned": "TIMESTAMP",
        "unicode_key": "STRING",
        "payload": "JSON",
    },
    "elasticsearch": {col: "VARCHAR" for col in MAPPED_COLUMNS} | {"payload": "JSON"},
}


def source_types(engine: str) -> dict[str, str]:
    """Physical types the fixture was seeded with on ``engine``."""
    return dict(SOURCE_TYPES.get(engine, SOURCE_TYPES["postgresql"]))


def mappings(
    *,
    assume_timezone: str | None = None,
    source_engine: str = "",
    dest_engine: str = "",
) -> list[dict[str, Any]]:
    """Identity Map over the projection.

    ``source_engine``/``dest_engine`` stamp each mapping's ``target_type``
    through the canonical Map owner
    (``services.type_system.create_new_mapping_target_type``) instead of letting
    the harness invent DDL. That is what the Map step does in the product, and it
    matters: without the stamp the create-new carrier is inferred from sampled
    values, so ``9007199254740993`` in a BIGINT column plans as INTEGER and the
    run correctly fail-closes on a narrowing the fixture never asked for.

    ``payload`` carries no ``struct_policy``, which is the default
    ``store_as_json``: the nested document lands whole. Flattening is a
    *declared* choice — see ``flatten_mappings`` — never something the engine
    does on its own.

    ``assume_timezone`` puts ``assume_timezone:<zone>`` on ``ts_naive`` only.
    Instant-only destinations (BSON ``date``, an Elasticsearch ``date``) have no
    zoneless carrier, so without a declaration the engine quarantines the naive
    column rather than stamping UTC on it. Passing a zone here is the operator
    asserting what the source meant — the run then lands the column *because it
    was declared*, which is the whole point of the control. Routes that pass
    ``None`` are the proof that the guess never happens on its own.
    """
    src_types = source_types(source_engine) if source_engine else {}
    out: list[dict[str, Any]] = []
    for col in MAPPED_COLUMNS:
        m: dict[str, Any] = {"source": col, "target": col, "confidence": 0.99}
        src_type = src_types.get(col, "")
        if src_type and dest_engine:
            m["source_type"] = src_type
            m["target_type"] = create_new_mapping_target_type(
                src_type, dest_engine, source_db=source_engine
            )
        if assume_timezone and col == "ts_naive":
            m["transform"] = f"assume_timezone:{assume_timezone}"
        out.append(m)
    return out


def flatten_mappings(
    *,
    assume_timezone: str | None = None,
    source_engine: str = "",
    dest_engine: str = "",
) -> list[dict[str, Any]]:
    """Same Map with the nested document's flattening declared explicitly."""
    out = mappings(
        assume_timezone=assume_timezone,
        source_engine=source_engine,
        dest_engine=dest_engine,
    )
    for m in out:
        if m["source"] == "payload":
            m["struct_policy"] = "flatten_top_level_keys"
    return out
