"""Canonical primary-key / identity-key resolution for preflight + writers.

One helper — G6 DDL, G8 dry-run reconcile, and G9 integrity must agree.
Connector-specific patches that reinvent ``*_id`` heuristics cause false blocks
(Mongo ``user_id`` dupes) or silent misses. Prefer explicit contract keys; fall
back only to exact ``id`` / ``_id`` (and mode-gated ``*_id`` for required-nulls).

Key-addressed sinks (Redis, DynamoDB, Elasticsearch, vector upserts) always
require a unique identity — append still collides on the key. Redis uses
natural-key ranking (``code`` / ``iso`` / ``name``) — never Mongo's ``_id``-only
rule and never weak attributes like ``capital``.
"""

from __future__ import annotations

from typing import Any, Iterable, Literal

from services.db_type_utils import SCHEMALESS_DESTS, normalize_dest_kind

Purpose = Literal["uniqueness", "required_nulls"]

_EXACT_SQL_KEYS = ("id", "_id", "uuid", "pk", "key")
# Mongo only — Dynamo uses writer-aligned HASH ranking (id/_id/pk/sk/*_id).
_DOCUMENT_STORE_DESTS = frozenset({"mongodb"})
# Same preferred names as ``dynamodb_writer._pick_hash_key`` / ``_resolve_key_schema``.
_DYNAMODB_PREFERRED_KEYS = ("id", "_id", "pk", "sk", "uuid", "key")

# Destinations where every write is identity-keyed (SET/PutItem/upsert by id).
# Validate must always enforce uniqueness — append still collides on the key.
KEY_ADDRESSED_DESTS = frozenset({
    "redis",
    "dynamodb",
    "elasticsearch",
    "pinecone",
    "qdrant",
    "weaviate",
    "milvus",
    "pgvector",
})

# Sync modes that must enforce identity uniqueness on the Validate sample for
# SQL / document stores. Key-addressed sinks always require uniqueness.
_UNIQUE_IDENTITY_SYNC_MODES = frozenset({
    "upsert",
    "incremental_deduped",
    "cdc",
    "scd2",
    "mirror",
    "full_refresh_mirror",
    "reverse_etl",
})

# Redis key ranking — shared by Validate (G6/G8/G9) and redis_writer Execute.
_REDIS_IDENTITY_EXACT = frozenset(
    {"id", "_id", "pk", "key", "uuid", "guid", "uid", "oid"}
)
_REDIS_NATURAL_KEYS = (
    "code",
    "iso",
    "iso2",
    "iso3",
    "iso_code",
    "country_code",
    "countrycode",
    "airport_code",
    "iata",
    "icao",
    "sku",
    "slug",
    "external_id",
    "externalid",
    "name",
    "country",
    "airport",
)
_REDIS_WEAK_KEYS = frozenset(
    {
        "capital",
        "city",
        "continent",
        "region",
        "currency",
        "language",
        "color",
        "status",
        "type",
        "description",
        "comment",
        "notes",
        "population",
        "area",
        "latitude",
        "longitude",
        "lat",
        "lon",
        "lng",
    }
)


def sync_requires_unique_identity(
    sync_mode: str | None,
    dest_kind: str | None = None,
) -> bool:
    """True when Validate must fail-closed on duplicate identity keys.

    Key-addressed destinations (Redis, DynamoDB, ES, vector upserts) always
    return True: every write is keyed, so append still collides on duplicates.
    """
    kind = normalize_dest_kind(dest_kind) if dest_kind else ""
    if kind in KEY_ADDRESSED_DESTS:
        return True
    mode = (sync_mode or "").strip().lower()
    try:
        from services.sync_cursor import normalize_sync_mode

        mode = normalize_sync_mode(sync_mode)
    except Exception:
        pass
    return mode in _UNIQUE_IDENTITY_SYNC_MODES


def pick_dynamodb_identity_column(candidates: list[str]) -> str | None:
    """Pick DynamoDB HASH key — same ranking as ``dynamodb_writer``.

    Prefers ``id`` / ``_id`` / ``pk`` / ``sk`` / ``uuid`` / ``key``, then the
    first ``*_id`` column. Returns ``None`` rather than inventing a weak attr.
    """
    if not candidates:
        return None
    lower_map = {c.lower(): c for c in candidates}
    for name in _DYNAMODB_PREFERRED_KEYS:
        if name in lower_map:
            return lower_map[name]
    for c in candidates:
        if c.lower().endswith("_id"):
            return c
    return None


def pick_redis_identity_column(candidates: list[str]) -> str | None:
    """Pick the best Redis key column from an ordered candidate list.

    Returns ``None`` when only weak (non-unique) attributes remain — force the
    operator to set an explicit primary key rather than invent ``capital``.
    """
    if not candidates:
        return None
    lower_map = {c.lower(): c for c in candidates}
    for name in _REDIS_IDENTITY_EXACT:
        if name in lower_map:
            return lower_map[name]
    for c in candidates:
        if c.lower().endswith("_id") and c.lower() not in _REDIS_WEAK_KEYS:
            return c
    for name in _REDIS_NATURAL_KEYS:
        if name in lower_map:
            return lower_map[name]
    for c in candidates:
        if c.lower() not in _REDIS_WEAK_KEYS:
            return c
    return None


def infer_redis_conflict_columns(
    target_cols: list[str],
    mappings: list[dict[str, Any]] | None,
    conflict_columns: list[str] | None = None,
) -> list[str]:
    """Return Redis key column(s) — same ranking for Validate and Execute."""
    if conflict_columns:
        cols = [c for c in conflict_columns if c in target_cols]
        if cols:
            return cols

    source_to_target: dict[str, str] = {}
    for m in mappings or []:
        src = str(m.get("source") or "")
        if src:
            source_to_target[src] = str(m.get("target") or src)

    for src, tgt in source_to_target.items():
        if src.lower() in _REDIS_IDENTITY_EXACT and tgt in target_cols:
            return [tgt]
    for src, tgt in source_to_target.items():
        if src.lower().endswith("_id") and tgt in target_cols:
            return [tgt]
    src_lower = {s.lower(): t for s, t in source_to_target.items()}
    for name in _REDIS_NATURAL_KEYS:
        tgt = src_lower.get(name)
        if tgt and tgt in target_cols:
            return [tgt]

    picked = pick_redis_identity_column(list(target_cols))
    return [picked] if picked else []


def _mapping_pairs(mappings: Iterable[Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for m in mappings or []:
        if isinstance(m, dict):
            src = str(m.get("source") or "")
            tgt = str(m.get("target") or "")
        else:
            src = str(getattr(m, "source", "") or "")
            tgt = str(getattr(m, "target", "") or "")
        if src and tgt:
            pairs.append((src, tgt))
    return pairs


def extract_contract_primary_key_columns(
    stream_contracts: Iterable[Any] | None,
    *,
    stream_name: str = "",
) -> list[str]:
    """Ordered composite primary key columns from Studio ``stream_contracts``.

    Prefer this over :func:`extract_contract_primary_key` when callers need the
    full composite (CDC deletes, uniqueness probes). Empty list when unset.
    """
    contracts = [
        c
        for c in (stream_contracts or [])
        if isinstance(c, dict) and c.get("selected", True)
    ]
    if not contracts:
        return []
    chosen: dict[str, Any] | None = None
    want = (stream_name or "").strip()
    if want:
        for c in contracts:
            name = str(c.get("name") or c.get("stream") or "").strip()
            if name == want:
                chosen = c
                break
    c = chosen or contracts[0]
    raw = c.get("primary_key") if c else None
    if raw is None and c:
        raw = c.get("primary_keys")
    if isinstance(raw, (list, tuple)):
        out = [str(item).strip() for item in raw if str(item or "").strip()]
        return out
    name = str(raw or "").strip()
    if not name:
        return []
    # Comma-joined composite form used by some Studio payloads.
    if "," in name:
        return [p.strip() for p in name.split(",") if p.strip()]
    return [name]


def extract_contract_primary_key(
    stream_contracts: Iterable[Any] | None,
    *,
    stream_name: str = "",
) -> str | None:
    """Single identity column for legacy callers (first of a composite).

    Prefer :func:`extract_contract_primary_key_columns` when the full composite
    is required (CDC deletes, uniqueness probes).
    """
    cols = extract_contract_primary_key_columns(stream_contracts, stream_name=stream_name)
    return cols[0] if cols else None


def resolve_identity_key(
    *,
    mappings: Iterable[Any],
    source_columns: list[str] | None = None,
    dest_kind: str = "",
    validation_mode: str = "strict",
    purpose: Purpose = "uniqueness",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
) -> tuple[str | None, str | None]:
    """Return ``(source_column, target_column)`` for the identity key, or ``(None, None)``.

    Rules:
    * Redis: natural-key ranking (``id`` / ``code`` / ``iso`` / ``name`` …) — never
      prefer weak attributes (``capital``) when a stronger key exists. Same ranking
      as ``redis_writer``.
    * Mongo: only ``_id`` — other ``*_id`` fields are FKs.
    * DynamoDB: writer-aligned HASH ranking (``id``/``_id``/``pk``/``sk``/``*_id``).
    * Operator ``contract_primary_key`` wins when present.
    * Prefer introspected destination primary-key columns when mapped.
    * SQL uniqueness: exact ``id`` / ``_id``; sole ``*_id`` when unambiguous.
    """
    kind = normalize_dest_kind(dest_kind)
    mode = (validation_mode or "strict").strip().lower()
    pairs = _mapping_pairs(mappings)
    srcs = [s for s, _ in pairs]
    if source_columns:
        for c in source_columns:
            if c not in srcs:
                srcs.append(c)
    tgts = [t for _, t in pairs]
    tgt_by_src = {s: t for s, t in pairs}
    src_by_tgt = {t: s for s, t in pairs}
    tgt_lower = {t.lower(): t for t in tgts}
    src_lower = {s.lower(): s for s in srcs}

    # Operator identity (Advanced / stream contract) — never silently swap for ``id``.
    contract = str(contract_primary_key or "").strip()
    if contract:
        matched_src = src_lower.get(contract.lower())
        if matched_src:
            return matched_src, tgt_by_src.get(matched_src, matched_src)
        matched_tgt = tgt_lower.get(contract.lower())
        if matched_tgt:
            return src_by_tgt.get(matched_tgt, matched_tgt), matched_tgt
        return contract, contract

    # Redis: key-value identity (not Mongo _id-only).
    if kind == "redis":
        mapping_dicts = [{"source": s, "target": t} for s, t in pairs]
        target_pool = list(tgts) if tgts else list(srcs)
        inferred = infer_redis_conflict_columns(target_pool, mapping_dicts, None)
        if not inferred:
            return None, None
        tgt = inferred[0]
        src = src_by_tgt.get(tgt) or src_lower.get(tgt.lower()) or tgt
        return src, tgt

    # DynamoDB: match dynamodb_writer HASH selection — never invent capital/city.
    if kind == "dynamodb":
        for pk in destination_pk_columns or []:
            name = str(pk or "").strip()
            if not name:
                continue
            matched = tgt_lower.get(name.lower())
            if matched:
                return src_by_tgt.get(matched, matched), matched
            matched_src = src_lower.get(name.lower())
            if matched_src:
                return matched_src, tgt_by_src.get(matched_src, matched_src)
        target_pool = list(tgts) if tgts else list(srcs)
        picked = pick_dynamodb_identity_column(target_pool)
        if not picked:
            return None, None
        # Prefer target name when it came from mappings; else treat as source.
        if picked in src_by_tgt or picked.lower() in {t.lower() for t in tgts}:
            tgt = tgt_lower.get(picked.lower(), picked)
            src = src_by_tgt.get(tgt) or src_lower.get(picked.lower()) or picked
            return src, tgt
        src = src_lower.get(picked.lower(), picked)
        return src, tgt_by_src.get(src, src)

    # Mongo document stores: only ``_id``.
    if kind in _DOCUMENT_STORE_DESTS or (
        kind in SCHEMALESS_DESTS and kind not in {"redis", "dynamodb"}
    ):
        for t in tgts:
            if t.lower() == "_id":
                src = next((s for s, tt in pairs if tt == t), "_id")
                return src, t
        for s in srcs:
            if s.lower() == "_id":
                return s, tgt_by_src.get(s, s)
        return None, None

    # Destination contract wins: first mapped introspected PK column.
    for pk in destination_pk_columns or []:
        name = str(pk or "").strip()
        if not name:
            continue
        matched = tgt_lower.get(name.lower())
        if matched:
            return src_by_tgt.get(matched, matched), matched

    # Prefer exact target names first (destination contract wins).
    exact = _EXACT_SQL_KEYS if purpose == "required_nulls" else ("id", "_id")
    for key in exact:
        for t in tgts:
            if t.lower() == key:
                src = next((s for s, tt in pairs if tt == t), key)
                return src, t
        for s in srcs:
            if s.lower() == key:
                return s, tgt_by_src.get(s, s)

    # Sole ``*_id`` natural key — never invent a PK when several FK-like columns compete.
    star_id_srcs = [
        s for s in srcs
        if s.lower().endswith("_id") and s.lower() not in {"id", "_id"}
    ]
    if purpose == "uniqueness" and len(star_id_srcs) == 1:
        s = star_id_srcs[0]
        return s, tgt_by_src.get(s, s)

    if purpose == "required_nulls" and mode in {"strict", "maximum"} and star_id_srcs:
        s = star_id_srcs[0]
        return s, tgt_by_src.get(s, s)

    return None, None


def resolve_primary_key_target(
    mappings: Iterable[Any],
    dest_kind: str,
    *,
    validation_mode: str = "strict",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
) -> str | None:
    """Target-side identity column for uniqueness probes (DDL / G8)."""
    _src, tgt = resolve_identity_key(
        mappings=mappings,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
        purpose="uniqueness",
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
    )
    return tgt


def resolve_primary_key_source(
    mappings: Iterable[Any],
    source_columns: list[str] | None,
    dest_kind: str,
    *,
    validation_mode: str = "strict",
    purpose: Purpose = "required_nulls",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
) -> str | None:
    """Source-side identity column for integrity / null / duplicate audits."""
    src, _tgt = resolve_identity_key(
        mappings=mappings,
        source_columns=source_columns,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
        purpose=purpose,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
    )
    return src
