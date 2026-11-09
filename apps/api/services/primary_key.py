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

from typing import Any, Iterable, Literal, Sequence

from services.db_type_utils import SCHEMALESS_DESTS, normalize_dest_kind

Purpose = Literal["uniqueness", "required_nulls"]

_EXACT_SQL_KEYS = ("id", "_id", "uuid", "pk", "key")
# Mongo only — Dynamo uses writer-aligned HASH ranking (id/_id/pk/sk/*_id).
_DOCUMENT_STORE_DESTS = frozenset({"mongodb"})
# Same preferred names as ``dynamodb_writer._pick_hash_key`` / ``_resolve_key_schema``.
_DYNAMODB_PREFERRED_KEYS = ("id", "_id", "pk", "sk", "uuid", "key")

# Destinations where every write is identity-keyed (SET/PutItem/upsert by id).
# Validate must always enforce uniqueness — append still collides on the key.
# MongoDB ``_id`` is the document primary key: duplicate ``_id`` in a batch is
# silent drop / last-write-wins under idempotent insert — always hard-block.
KEY_ADDRESSED_DESTS = frozenset({
    "redis",
    "dynamodb",
    "elasticsearch",
    "mongodb",
    "pinecone",
    "qdrant",
    "weaviate",
    "milvus",
    "pgvector",
})

# Of those, the ones that *derive* the key from row data: two rows that resolve
# to the same key silently overwrite each other, so a write with no identity
# loses rows no matter which sync mode asked for it.
#
# MongoDB is deliberately absent. When ``_id`` is unmapped the server assigns a
# unique ObjectId per document, so an insert-only run cannot collide — which is
# what ``mongodb_writer._idempotent_insert_many`` documents and relies on.
# Demanding an identity there blocked plain overwrite and append loads that
# Execute would have completed correctly. A *mapped* ``_id`` still has to be
# unique in every mode, because duplicates within one batch are last-write-wins;
# that is ``sync_requires_unique_identity``, which still covers MongoDB.
_DERIVED_KEY_DESTS = KEY_ADDRESSED_DESTS - {"mongodb"}


def _identity_dest_kind(dest_kind: str | None) -> str:
    """Canonical kind for identity rules; a vector sink stays itself.

    ``normalize_dest_kind`` folds ``pgvector`` into the PostgreSQL family, which
    would drop the vector upsert's key-addressed rule.
    """
    raw = (dest_kind or "").strip().lower()
    if raw in KEY_ADDRESSED_DESTS:
        return raw
    return normalize_dest_kind(dest_kind) if dest_kind else ""

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


def sync_collapses_in_file_duplicates(sync_mode: str | None) -> bool:
    """Upsert keeps the last in-file copy of a key. Append and insert do not.

    ``incremental_upsert`` normalizes to ``incremental_deduped``. CDC, mirror,
    and SCD2 still require a unique identity and are not collapsed here.
    """
    return _normalized_mode(sync_mode) in {"upsert", "incremental_deduped"}


#: Sources whose read order IS the change order within a key: an upsert keeps the
#: last read copy of a key, which is its latest record. Kafka gives that order
#: per partition, so it holds for a topic whose messages are keyed (one key,
#: one partition) or that has a single partition — the shape of a changelog
#: topic. The finding text states the assumption.
ORDERED_LOG_SOURCES = frozenset({"kafka"})
ORDERED_LOG_NOTE = (
    "Repeated keys collapsed to the last message read: Kafka orders messages "
    "within a partition, so this assumes a keyed (per-key partition) or "
    "single-partition topic. Declare in_file_dedupe=block to refuse repeats instead."
)


def _stream_scope(stream_contracts: list[dict] | None, stream_name: str = "") -> list[dict]:
    """The contract(s) a decision is about: the named stream, else every selected one."""
    selected = [
        c for c in (stream_contracts or [])
        if isinstance(c, dict) and c.get("selected") is not False
    ]
    if stream_name:
        named = [
            c for c in selected
            if str(c.get("name") or c.get("stream") or "").strip() == stream_name.strip()
        ]
        if named:
            return named
    return selected


def latest_wins_dedupe(
    in_file_dedupe: str | None,
    sync_mode: str | None,
    *,
    stream_contracts: list[dict] | None = None,
    source_type: str = "",
    stream_name: str = "",
) -> str:
    """``keep_last`` when an upsert's winner per key is defined by read order.

    ``incremental_deduped`` keeps the latest version of each key. With a
    declared cursor the incremental read is ordered by it, and a changelog
    topic is read in offset order, so the last copy read IS the latest one.
    Refusing a repeated key there blocked the normal shape of a changelog
    (D89) and of a versioned table (E2-023: 3 versions per key). An explicit
    operator policy (``block`` / ``keep_last``) always wins.

    Decided per stream: ``stream_name`` picks that contract; without it every
    selected contract must declare a cursor, so a cursorless stream in a
    multi-stream request still refuses repeated keys.
    """
    explicit = str(in_file_dedupe or "").strip()
    if explicit or not sync_collapses_in_file_duplicates(sync_mode):
        return explicit
    from src.transfer.engine_family import engine_family

    if engine_family(source_type) in ORDERED_LOG_SOURCES:
        return "keep_last"
    scope = _stream_scope(stream_contracts, stream_name)
    if scope and all(
        str(c.get("cursor_field") or c.get("cursor") or "").strip() for c in scope
    ):
        return "keep_last"
    return explicit


def table_reads_latest_wins(
    sync_mode: str | None,
    *,
    stream_contracts: list[dict] | None = None,
    source_type: str = "",
    stream_name: str = "",
) -> bool:
    """True when this read of a table/log defines "the last copy of a key".

    The same answer :func:`latest_wins_dedupe` gives an unset policy: a
    collapsing sync whose read is cursor-ordered (a versioned table, E2-023)
    or offset-ordered (a changelog topic, D89). There an operator's explicit
    ``keep_last`` is the automatic choice spelled out — schedules always
    stamp ``block`` or ``keep_last``, so refusing it left a versioned-table
    schedule no setting that runs (QA C2 review 3).
    """
    return (
        latest_wins_dedupe(
            None,
            sync_mode,
            stream_contracts=stream_contracts,
            source_type=source_type,
            stream_name=stream_name,
        )
        == "keep_last"
    )


def latest_wins_note(source_type: str = "") -> str:
    """The assumption behind a read-order collapse, for the finding text."""
    from src.transfer.engine_family import engine_family

    return ORDERED_LOG_NOTE if engine_family(source_type) in ORDERED_LOG_SOURCES else ""


def operator_collapses_in_file_duplicates(
    sync_mode: str | None,
    in_file_dedupe: str | None = "",
) -> bool:
    """Collapse when the sync mode does, unless the operator asked to block.

    An explicit ``block`` policy still fails the load. ``keep_last`` collapses
    even on append. An empty policy follows the sync mode.
    """
    policy = str(in_file_dedupe or "").strip().lower().replace("-", "_")
    if policy == "block":
        return False
    if policy == "keep_last":
        return True
    return sync_collapses_in_file_duplicates(sync_mode)


def sync_requires_unique_identity(
    sync_mode: str | None,
    dest_kind: str | None = None,
) -> bool:
    """True when Validate must fail-closed on duplicate identity keys.

    Key-addressed destinations (Redis, DynamoDB, ES, vector upserts) always
    return True: every write is keyed, so append still collides on duplicates.
    """
    kind = _identity_dest_kind(dest_kind)
    if kind in KEY_ADDRESSED_DESTS:
        return True
    return _normalized_mode(sync_mode) in _UNIQUE_IDENTITY_SYNC_MODES


def dest_is_key_addressed(
    dest_kind: str | None,
    key_columns: Sequence[str] | None = None,
) -> bool:
    """True when every write to this destination is addressed by identity.

    Redis ``SET``, DynamoDB ``PutItem``, Elasticsearch index-by-``_id`` and
    vector upserts replace the value held at the key. Re-writing a key that
    already exists is the destination's defined behaviour, not a lost row, so
    cardinality growth cannot account for such a write.

    MongoDB is addressed by ``_id`` alone: an insert whose ``_id`` is unmapped
    gets a server-assigned ObjectId per document, so an append accumulates
    rows exactly like a SQL table and its cardinality must be closed by
    COUNT(*) delta, not a key census over some other mapped column.
    """
    kind = _identity_dest_kind(dest_kind)
    if kind == "mongodb":
        return any((c or "").strip() == "_id" for c in (key_columns or ()))
    return kind in KEY_ADDRESSED_DESTS


def _normalized_mode(sync_mode: str | None) -> str:
    mode = (sync_mode or "").strip().lower()
    try:
        from services.sync_cursor import normalize_sync_mode

        return normalize_sync_mode(sync_mode)
    except Exception:
        return mode


def missing_identity_blocks(
    sync_mode: str | None,
    dest_kind: str | None = None,
) -> bool:
    """True when having *no* identity key at all must fail the transfer.

    Distinct from :func:`sync_requires_unique_identity`, which answers whether a
    key that *is* bound has to be unique. A sink that derives its key from row
    data cannot accept a keyless write without losing rows. A sink that assigns
    its own key can, for insert-only modes — the row still lands exactly once.
    Upsert-class modes need a key everywhere: there is nothing to match on.
    """
    kind = _identity_dest_kind(dest_kind)
    if kind in _DERIVED_KEY_DESTS:
        return True
    return _normalized_mode(sync_mode) in _UNIQUE_IDENTITY_SYNC_MODES


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
        # Fail closed — never soft-shrink a composite Redis key on case/partial miss.
        from connectors.writer_common import resolve_conflict_targets

        return resolve_conflict_targets(conflict_columns, target_cols, strict=True)

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


def identity_source_columns(
    pk_target_columns: Sequence[str] | None,
    mappings: Iterable[Any] | None,
) -> list[str]:
    """Source column for every destination key component, in key order.

    Batch audits read source-keyed cells, while the write key is named by the
    destination. A component with no mapping keeps its own name (identity map).
    """
    target_to_source: dict[str, str] = {}
    for src, tgt in _mapping_pairs(mappings or []):
        target_to_source.setdefault(tgt.lower(), src)
    out: list[str] = []
    for col in pk_target_columns or []:
        name = str(col or "").strip()
        if not name:
            continue
        src = target_to_source.get(name.lower(), name)
        if src not in out:
            out.append(src)
    return out


# Elasticsearch document ids are explicit. A lone ``order_id`` is not one:
# inventing ``_id`` from the only ``*_id`` column collides rows the operator
# never keyed, and Validate must not green-light a key the writer will refuse.
_ELASTICSEARCH_KINDS = frozenset({
    "elasticsearch",
    "opensearch",
    "amazon_elasticsearch",
    "elastic_cloud",
})


def _is_elasticsearch_kind(dest_kind: str | None) -> bool:
    raw = (dest_kind or "").strip().lower().replace(" ", "_").replace("-", "_")
    if raw in _ELASTICSEARCH_KINDS:
        return True
    return _identity_dest_kind(dest_kind) in _ELASTICSEARCH_KINDS


def _flagged_identity_names(mappings: Iterable[Any] | None) -> list[str]:
    """Source names a Map row flagged as identity, in mapping order.

    ``primary_key`` / ``is_primary_key`` / ``identity``. Omitted rows are not a key.
    """
    names: list[str] = []
    for item in mappings or []:
        if isinstance(item, dict):
            if item.get("intentional_omit") or item.get("omit"):
                continue
            flagged = (
                item.get("primary_key")
                or item.get("is_primary_key")
                or item.get("identity")
            )
            name = str(item.get("source") or item.get("target") or "").strip()
        else:
            if getattr(item, "intentional_omit", None) or getattr(item, "omit", None):
                continue
            flagged = (
                getattr(item, "primary_key", None)
                or getattr(item, "is_primary_key", None)
                or getattr(item, "identity", None)
            )
            name = str(getattr(item, "source", "") or getattr(item, "target", "") or "").strip()
        if not flagged or not name or name in names:
            continue
        names.append(name)
    return names


def _extra_identity_names(extra: Any) -> list[str]:
    """Operator identity on the destination endpoint.

    ``primary_key`` / ``primary_keys`` only. ``primary_key_columns`` is catalog
    evidence and is not read here — adopting it would invent a document id.
    """
    if not isinstance(extra, dict):
        return []
    raw = extra.get("primary_key")
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        raw = extra.get("primary_keys")
    if isinstance(raw, (list, tuple)):
        parts = [str(item).strip() for item in raw if str(item or "").strip()]
    else:
        text = str(raw or "").strip()
        parts = (
            [p.strip() for p in text.replace(";", ",").split(",") if p.strip()]
            if text
            else []
        )
    out: list[str] = []
    for name in parts:
        if name not in out:
            out.append(name)
    return out


def explicit_identity_target_columns(
    mappings: Iterable[Any] | None,
    *,
    conflict_columns: Sequence[str] | None = None,
    stream_contracts: Iterable[Any] | None = None,
    contract_primary_key: str | None = None,
    destination_extra: Any = None,
    stream_name: str = "",
) -> tuple[list[str], str | None]:
    """Target columns an operator named as document identity.

    Each source is optional:

    * ``conflict_columns`` — the write key the stream already resolved.
    * stream contract / ``contract_primary_key`` — Advanced settings and the
      Map step's identity field (one contract).
    * ``destination.extra.primary_key`` — the same identity stored on the endpoint.
    * mapping ``primary_key`` / ``is_primary_key`` / ``identity`` — a Map-row flag.

    Returns ``([], None)`` when nothing was named. Does not invent ``id`` or
    ``*_id``. When two named sources disagree, returns ``([], message)``.
    """
    mapping_list = list(mappings or [])

    def _targets(names: list[str]) -> list[str]:
        return _map_identity_names_to_target(names, mappings=mapping_list)

    named: list[tuple[str, list[str]]] = []
    conflict = [str(c).strip() for c in (conflict_columns or []) if str(c or "").strip()]
    if conflict:
        named.append(("write key", _targets(conflict)))
    contract_names = extract_contract_primary_key_columns(
        stream_contracts,
        stream_name=stream_name,
        fallback_first=not bool((stream_name or "").strip()),
    )
    raw_contract = str(contract_primary_key or "").strip()
    if not contract_names and raw_contract:
        contract_names = [
            p.strip() for p in raw_contract.replace(";", ",").split(",") if p.strip()
        ]
    if contract_names:
        named.append(("Advanced identity", _targets(contract_names)))
    extra_names = _extra_identity_names(destination_extra)
    if extra_names:
        named.append(("destination identity", _targets(extra_names)))
    flagged = _flagged_identity_names(mapping_list)
    if flagged:
        named.append(("Map primary key", _targets(flagged)))

    chosen: list[str] | None = None
    chosen_key: tuple[str, ...] | None = None
    labels: list[str] = []
    for label, cols in named:
        if not cols:
            continue
        key = tuple(c.casefold() for c in cols)
        labels.append(f"{label} ({', '.join(cols)})")
        if chosen is None:
            chosen = cols
            chosen_key = key
            continue
        if key != chosen_key:
            return [], (
                "Elasticsearch document identity disagrees — "
                + "; ".join(labels)
                + ". Set one key on Map or in Advanced settings."
            )
    return list(chosen or []), None


def bind_explicit_elasticsearch_identity(
    dest_kind: str | None,
    mappings: Iterable[Any] | None,
    current_targets: Sequence[str] | None,
    *,
    stream_contracts: Iterable[Any] | None = None,
    contract_primary_key: str | None = None,
    destination_extra: Any = None,
    stream_name: str = "",
) -> list[str]:
    """Fold explicit identity into the Elasticsearch write key.

    Other destinations keep ``current_targets``. A disagreement raises
    ``ValueError`` before any document is indexed.
    """
    current = [str(c).strip() for c in (current_targets or []) if str(c or "").strip()]
    if not _is_elasticsearch_kind(dest_kind):
        return current
    cols, err = explicit_identity_target_columns(
        mappings,
        conflict_columns=current,
        stream_contracts=stream_contracts,
        contract_primary_key=contract_primary_key,
        destination_extra=destination_extra,
        stream_name=stream_name,
    )
    if err:
        raise ValueError(err)
    return cols or current


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
    fallback_first: bool = True,
) -> list[str]:
    """Ordered composite primary key columns from Studio ``stream_contracts``.

    Prefer this over :func:`extract_contract_primary_key` when callers need the
    full composite (CDC deletes, uniqueness probes). Empty list when unset.

    When ``stream_name`` is set and no contract matches, ``fallback_first=False``
    returns ``[]`` instead of silently using ``contracts[0]`` (wrong-stream probe).
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
        if chosen is None and not fallback_first:
            return []
    c = chosen or (contracts[0] if fallback_first else None)
    if not c:
        return []
    raw = c.get("primary_key")
    if raw is None:
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
    if contract and "," in contract:
        # "id,tenant_id" is not a column. The composite resolvers own that
        # string; returning it whole made the uniqueness probe look up a
        # name that does not exist and then fall through to a guessed id.
        return None, None
    if contract:
        matched_src = src_lower.get(contract.lower())
        if matched_src:
            return matched_src, tgt_by_src.get(matched_src, matched_src)
        matched_tgt = tgt_lower.get(contract.lower())
        if matched_tgt:
            return src_by_tgt.get(matched_tgt, matched_tgt), matched_tgt
        return contract, contract

    # Elasticsearch is key-addressed but does not derive ``_id`` from a lone
    # ``*_id`` column. The operator names the key (contract or Map flag), or a
    # mapped field is literally ``_id`` / ``id`` / ``pk``.
    if _is_elasticsearch_kind(dest_kind):
        flagged = _flagged_identity_names(mappings)
        if len(flagged) == 1:
            mapped = _map_identity_names_to_target(flagged, mappings=list(mappings or []))
            tgt = mapped[0] if mapped else flagged[0]
            src = src_lower.get(flagged[0].lower()) or src_by_tgt.get(tgt) or flagged[0]
            return src, tgt
        if len(flagged) > 1:
            return None, None
        for key in ("_id", "id", "pk"):
            for t in tgts:
                if t.lower() == key:
                    src = next((s for s, tt in pairs if tt == t), key)
                    return src, t
            for s in srcs:
                if s.lower() == key:
                    return s, tgt_by_src.get(s, s)
        return None, None

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


def _map_identity_names_to_source(
    names: list[str],
    *,
    mappings: Iterable[Any],
    source_columns: list[str] | None = None,
) -> list[str]:
    """Map dest/contract identity names onto source columns when possible."""
    pairs = _mapping_pairs(mappings)
    src_by_tgt = {t: s for s, t in pairs}
    srcs = [s for s, _ in pairs]
    if source_columns:
        for c in source_columns:
            if c not in srcs:
                srcs.append(c)
    src_lower = {s.lower(): s for s in srcs}
    tgt_lower = {t.lower(): t for t in src_by_tgt}
    out: list[str] = []
    for raw in names:
        name = str(raw or "").strip()
        if not name:
            continue
        matched_src = src_lower.get(name.lower())
        if matched_src:
            out.append(matched_src)
            continue
        matched_tgt = tgt_lower.get(name.lower())
        if matched_tgt:
            out.append(src_by_tgt.get(matched_tgt, matched_tgt))
            continue
        # Unmapped contract name — keep as-is so the probe can still GROUP BY.
        out.append(name)
    # Preserve order, drop empties/dupes.
    seen: set[str] = set()
    deduped: list[str] = []
    for c in out:
        key = c.lower()
        if not c or key in seen:
            continue
        seen.add(key)
        deduped.append(c)
    return deduped


def mapped_catalog_upsert_key(
    primary_key_columns: Sequence[str] | None,
    mappings: Iterable[Any],
) -> tuple[list[str], list[str]]:
    """Map a source-catalog primary key through the write mapping.

    Returns ``(source_columns, target_columns)``. Empty when the catalog has
    no key, or when any key column is unmapped — upserting on a partial key
    would match rows on the wrong identity. This does not invent ``id``.
    """
    from services.sync_cursor import map_source_to_target

    keys = [str(c).strip() for c in (primary_key_columns or []) if str(c or "").strip()]
    mapping_list = [m for m in (mappings or []) if isinstance(m, dict)]
    if not keys or not mapping_list:
        return [], []
    by_lower: dict[str, str] = {}
    for item in mapping_list:
        src = str(item.get("source") or "").strip()
        if src:
            by_lower.setdefault(src.lower(), src)
    sources: list[str] = []
    targets: list[str] = []
    for key in keys:
        src = by_lower.get(key.lower())
        if not src:
            return [], []
        tgt = str(map_source_to_target(src, mapping_list) or "").strip()
        if not tgt:
            return [], []
        sources.append(src)
        targets.append(tgt)
    return sources, targets


def resolve_primary_key_source_columns(
    mappings: Iterable[Any],
    source_columns: list[str] | None,
    dest_kind: str,
    *,
    validation_mode: str = "strict",
    purpose: Purpose = "uniqueness",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
    stream_contracts: Iterable[Any] | None = None,
    stream_name: str = "",
) -> list[str]:
    """Full composite source identity columns for uniqueness probes.

    Prefer this over :func:`resolve_primary_key_source` when the covering key is
    multi-column — probing only the first column false-fails (same org_id) or
    false-greens (first col unique, tuple not).
    """
    # 1. Stream contracts (Studio) — authoritative composite when selected.
    # Never fall back to contracts[0] when a stream_name was requested but missed
    # — that probes the wrong stream's identity on multi-table Validate/Execute.
    want_stream = bool((stream_name or "").strip())
    contract_cols = extract_contract_primary_key_columns(
        stream_contracts,
        stream_name=stream_name,
        fallback_first=not want_stream,
    )
    if len(contract_cols) > 1:
        return _map_identity_names_to_source(
            contract_cols, mappings=mappings, source_columns=source_columns
        )

    # 2. Comma-joined operator contract string.
    raw_contract = str(contract_primary_key or "").strip()
    if raw_contract and "," in raw_contract:
        parts = [p.strip() for p in raw_contract.split(",") if p.strip()]
        if len(parts) > 1:
            return _map_identity_names_to_source(
                parts, mappings=mappings, source_columns=source_columns
            )

    # 3. Introspected destination PK composite — map each column to source.
    dest_pks = [
        str(p).strip() for p in (destination_pk_columns or []) if str(p or "").strip()
    ]
    if len(dest_pks) > 1:
        mapped = _map_identity_names_to_source(
            dest_pks, mappings=mappings, source_columns=source_columns
        )
        if len(mapped) > 1:
            return mapped

    # 4. Single-column stream/contract / resolver fallback.
    # Elasticsearch Map-row flags are an explicit key when the contract is empty.
    # They are not consulted once a contract names a key — the contract wins,
    # and the writer refuses the pair when the flag names a different column.
    if (
        _is_elasticsearch_kind(dest_kind)
        and not contract_cols
        and not str(contract_primary_key or "").strip()
    ):
        flagged = _flagged_identity_names(mappings)
        if flagged:
            return _map_identity_names_to_source(
                flagged, mappings=mappings, source_columns=source_columns
            )

    if len(contract_cols) == 1:
        mapped = _map_identity_names_to_source(
            contract_cols, mappings=mappings, source_columns=source_columns
        )
        if mapped:
            return mapped

    src = resolve_primary_key_source(
        mappings,
        source_columns,
        dest_kind,
        validation_mode=validation_mode,
        purpose=purpose,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
    )
    return [src] if src else []


def _map_identity_names_to_target(
    names: list[str],
    *,
    mappings: Iterable[Any],
) -> list[str]:
    """Map source/contract identity names onto destination columns."""
    pairs = _mapping_pairs(mappings)
    tgt_by_src = {s.lower(): t for s, t in pairs}
    tgt_lower = {t.lower(): t for _, t in pairs}
    out: list[str] = []
    seen: set[str] = set()
    for raw in names:
        name = str(raw or "").strip()
        if not name:
            continue
        tgt = tgt_by_src.get(name.lower()) or tgt_lower.get(name.lower()) or name
        key = tgt.lower()
        if not tgt or key in seen:
            continue
        seen.add(key)
        out.append(tgt)
    return out


def resolve_primary_key_columns(
    mappings: Iterable[Any],
    source_columns: list[str] | None,
    dest_kind: str,
    *,
    validation_mode: str = "strict",
    purpose: Purpose = "uniqueness",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
    stream_contracts: Iterable[Any] | None = None,
    stream_name: str = "",
) -> tuple[list[str], list[str]]:
    """``(source_columns, target_columns)`` for the full identity.

    Same owner as :func:`resolve_primary_key_source_columns`. An unmapped
    composite part is kept, not dropped, and does not fall through to a
    guessed ``id``. Empty when no identity can be named.
    """
    sources = resolve_primary_key_source_columns(
        mappings,
        source_columns,
        dest_kind,
        validation_mode=validation_mode,
        purpose=purpose,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
        stream_contracts=stream_contracts,
        stream_name=stream_name,
    )
    if not sources:
        return [], []
    return sources, _map_identity_names_to_target(sources, mappings=mappings)


def resolve_primary_key_target_columns(
    mappings: Iterable[Any],
    source_columns: list[str] | None,
    dest_kind: str,
    *,
    validation_mode: str = "strict",
    purpose: Purpose = "uniqueness",
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
    stream_contracts: Iterable[Any] | None = None,
    stream_name: str = "",
) -> list[str]:
    """Target-side composite identity for the sample uniqueness probe (G6)."""
    _sources, targets = resolve_primary_key_columns(
        mappings,
        source_columns,
        dest_kind,
        validation_mode=validation_mode,
        purpose=purpose,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
        stream_contracts=stream_contracts,
        stream_name=stream_name,
    )
    return targets
