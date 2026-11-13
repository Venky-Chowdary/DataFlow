"""Redis document writes that never lose data silently.

The Redis writer used to ``SET`` every row's JSON over its key. That dropped
the fields an upsert does not map (contract G15) and the key's TTL, replaced a
hash with a string, let an append overwrite a stored key, and — because the
legacy key layout sanitizes the id — let a second id that sanitizes to the same
key replace the first id's document.

``apply_redis_docs`` writes one batch with an outcome per row:

* ``upsert``: merge onto the stored document. A JSON string is read, merged
  and written back by a compare-and-set script that re-applies the key's TTL
  (the CAS fails if the value moved since the read; the row is retried). A hash
  takes ``HSET`` of the mapped fields (``HDEL`` for a mapped NULL). Any other
  type is refused, never replaced.
* ``append``: ``SET NX``; a stored key is a rejected row.
* ``skip`` (duplicate policy ``skip_duplicates``): ``SET NX``; a key stored by
  the same id is left as stored and counted skipped.
* ``overwrite``: plain ``SET`` (the run cleared the prefix first).

Every mode first checks key ownership: a stored document whose identity
fields name a different id is a key collision and the row is refused with
both ids named.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: GET-compare then SET, keeping the TTL (PTTL re-applied: works on any server
#: version, unlike SET KEEPTTL which needs Redis 6).
_CAS_SET = """
local cur = redis.call('GET', KEYS[1])
if cur ~= ARGV[1] then return 0 end
local ttl = redis.call('PTTL', KEYS[1])
redis.call('SET', KEYS[1], ARGV[2])
if ttl > 0 then redis.call('PEXPIRE', KEYS[1], ttl) end
return 1
"""

_ROUNDS = 5

WRITTEN = "written"
SKIPPED = "skipped"
REJECTED = "rejected"
FAILED = "error"


@dataclass
class RedisDocPlan:
    row: int  # 1-based source row number
    key: str
    identity: str  # the id text (composite parts joined with ``|``)
    doc: dict[str, Any]
    parts: tuple[str, ...] = ()  # identity per key column (``|`` may be data)


def _decode(raw: Any) -> str:
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8", "replace")
    return "" if raw is None else str(raw)


def _doc_text(doc: dict[str, Any]) -> str:
    from services.value_serializer import json_dumps_exact_text, sanitize_json_value

    return json_dumps_exact_text(sanitize_json_value(doc), ensure_ascii=True)


def identity_parts(values: list[Any]) -> tuple[str, ...] | None:
    """Comparable identity of a document: its key columns' text, per column."""
    from services.value_serializer import present_cell_text

    if not values or any(v is None for v in values):
        return None
    return tuple(present_cell_text(v) or "" for v in values)


def _prefetch(
    client: Any, keys: list[str], id_cols: list[str]
) -> list[tuple[str, Any, tuple[str, ...] | None]]:
    """``(type, raw string value or None, stored identity or None)`` per key."""
    from connectors.redis_reader import load_redis_json_doc

    pipe = client.pipeline(transaction=False)
    for key in keys:
        pipe.type(key)
    kinds = [_decode(k) for k in pipe.execute()]
    pipe = client.pipeline(transaction=False)
    reads: list[int] = []
    for i, (key, kind) in enumerate(zip(keys, kinds)):
        if kind == "string":
            pipe.get(key)
            reads.append(i)
        elif kind == "hash" and id_cols:
            pipe.hmget(key, id_cols)
            reads.append(i)
    values = pipe.execute(raise_on_error=False) if reads else []
    out: list[tuple[str, Any, tuple[str, ...] | None]] = [(kind, None, None) for kind in kinds]
    for i, value in zip(reads, values):
        kind = kinds[i]
        if isinstance(value, Exception):
            out[i] = ("moved", None, None)
        elif kind == "string":
            doc = load_redis_json_doc(value)
            ident = (
                identity_parts([doc.get(c) for c in id_cols])
                if isinstance(doc, dict) and id_cols and all(c in doc for c in id_cols)
                else None
            )
            out[i] = (kind, value, ident)
        else:
            out[i] = (kind, None, identity_parts([None if v is None else _decode(v) for v in value]))
    return out


def _collision(plan: RedisDocPlan, owner: tuple[str, ...]) -> tuple[str, str]:
    held = owner[0] if len(owner) == 1 else list(owner)
    mine = plan.parts[0] if len(plan.parts) == 1 else list(plan.parts)
    return REJECTED, (
        f"Redis key collision: id {mine!r} maps to key {plan.key!r}, which "
        f"already holds id {held!r} — refused, never overwritten. Set the connector "
        "key_format to raw to key documents by their exact id text."
    )


def apply_redis_docs(
    client: Any, plans: list[RedisDocPlan], *, mode: str, id_cols: list[str]
) -> list[tuple[str, str]]:
    """Write ``plans``; ``(outcome, reason)`` per plan (see module docstring)."""
    outcomes: list[tuple[str, str] | None] = [None] * len(plans)
    cas = client.register_script(_CAS_SET)
    pending = list(range(len(plans)))
    for _round in range(_ROUNDS):
        if not pending:
            break
        stored = _prefetch(client, [plans[i].key for i in pending], id_cols)
        held = (
            _stored_hash_fields(client, plans, pending, stored) if mode == "append" else {}
        )
        pipe = client.pipeline(transaction=False)
        sent: list[tuple[int, str]] = []
        for pos, (kind, raw, owner) in zip(pending, stored):
            plan = plans[pos]
            if owner is not None and plan.parts and owner != plan.parts:
                outcomes[pos] = _collision(plan, owner)
                continue
            if kind == "moved":
                continue  # changed type between reads: next round
            exists = kind != "none"
            if mode == "overwrite":
                pipe.set(plan.key, _doc_text(plan.doc))
                sent.append((pos, "set"))
            elif mode in {"append", "skip"} or not exists:
                if exists:
                    outcomes[pos] = (
                        (SKIPPED, "")
                        if mode == "skip" or _same_document(plan, kind, raw, held.get(pos))
                        else (
                            REJECTED,
                            f"Redis key {plan.key!r} already stored — append never "
                            "overwrites an existing key; use upsert to update it",
                        )
                    )
                    continue
                pipe.set(plan.key, _doc_text(plan.doc), nx=True)
                sent.append((pos, "nx"))
            elif kind == "string":
                from connectors.redis_reader import load_redis_json_doc

                existing = load_redis_json_doc(raw)
                merged = {**existing, **plan.doc} if isinstance(existing, dict) else plan.doc
                cas(keys=[plan.key], args=[raw, _doc_text(merged)], client=pipe)
                sent.append((pos, "cas"))
            elif kind == "hash":
                _hash_merge(pipe, plan)
                sent.append((pos, "hash"))
            else:
                outcomes[pos] = (
                    REJECTED,
                    f"Redis key {plan.key!r} holds a {kind}; upsert merges JSON string "
                    "or hash documents only — refused, never replaced",
                )
        results = pipe.execute(raise_on_error=False) if len(pipe) else []
        retry: list[int] = []
        it = iter(results)
        for pos, op in sent:
            res = next(it)
            if op == "hash":
                res2 = next(it) if _hash_has_deletes(plans[pos]) else None
                res = res if not isinstance(res2, Exception) else res2
            if isinstance(res, Exception):
                if "WRONGTYPE" in str(res):
                    retry.append(pos)
                else:
                    outcomes[pos] = (FAILED, str(res))
            elif op in {"nx", "cas"} and not res:
                retry.append(pos)  # created / changed since the read
            else:
                outcomes[pos] = (WRITTEN, "")
        pending = retry + [p for p, (_k, *_r) in zip(pending, stored) if _k == "moved"]
    for pos in pending:
        if outcomes[pos] is None:
            outcomes[pos] = (FAILED, f"Redis key {plans[pos].key!r} kept changing during the write")
    return [o or (FAILED, "not written") for o in outcomes]


def legacy_layout_refusal(
    client: Any, prefix: str, plans: list[RedisDocPlan], id_cols: list[str]
) -> str:
    """Why a ``raw``-format batch must not write onto this prefix, or "".

    A new connector defaults to ``key_format=raw`` (``prefix:7``). If it
    replaces a legacy connector on the same prefix, the legacy document of id
    ``7`` sits at ``prefix:col_7``: writing ``prefix:7`` would create a second
    document per id that precount and Gate-8 (raw) never see. The legacy key is
    a legacy document of *this* row only when the document there carries this
    row's identity — under raw keys another id may legitimately live at that
    spelling (``A_1`` vs ``A-1``, ``col_7`` vs ``7``). Read the same way as the
    collision check: one pipelined TYPE, then GET / HMGET of the stored ones.
    """
    from connectors.redis_reader import redis_key_for

    pairs: list[tuple[str, RedisDocPlan]] = []
    for plan in plans:
        legacy = redis_key_for(prefix, plan.identity, key_format="legacy")
        if legacy != plan.key and plan.parts:
            pairs.append((legacy, plan))
    if not pairs or not id_cols:
        return ""
    stored = _prefetch(client, [key for key, _plan in pairs], id_cols)
    found = [
        key
        for (key, plan), (kind, _raw, owner) in zip(pairs, stored)
        if kind != "none" and owner == plan.parts
    ]
    if not found:
        return ""
    sample = ", ".join(repr(k) for k in found[:3])
    return (
        f"Redis prefix {prefix!r} already holds legacy-layout keys ({sample}"
        f"{', …' if len(found) > 3 else ''}) for ids this batch writes; this connector "
        "writes key_format=raw keys, which would create a second document per id. Set "
        "the connector's key_format to legacy to continue those keys, or migrate them "
        "to the raw layout first. Nothing in this batch was written."
    )


def _stored_hash_fields(
    client: Any, plans: list[RedisDocPlan], pending: list[int], stored: list[tuple]
) -> dict[int, list[Any]]:
    """Append: the mapped fields of stored hashes, one pipelined HMGET."""
    hashes = [pos for pos, (kind, _r, _o) in zip(pending, stored) if kind == "hash"]
    if not hashes:
        return {}
    pipe = client.pipeline(transaction=False)
    for pos in hashes:
        pipe.hmget(plans[pos].key, list(plans[pos].doc))
    return dict(zip(hashes, pipe.execute(raise_on_error=False)))


def _same_document(plan: RedisDocPlan, kind: str, raw: Any, hash_values: Any) -> bool:
    """Append: the stored document already holds this row (its mapped fields).

    A key-addressed store re-run with an unchanged source must not be a
    rejected row. JSON compares parsed values (key order, spacing and unmapped
    fields do not matter); a hash compares the mapped fields with the text the
    writer would store. Different content stays a rejected row.
    """
    import json

    from connectors.redis_reader import load_redis_json_doc
    from services.value_serializer import json_dumps_exact_text, sanitize_json_value

    if kind == "string":
        existing = load_redis_json_doc(raw)
        incoming = json.loads(_doc_text(plan.doc))
        return isinstance(existing, dict) and all(
            name in existing and existing[name] == value for name, value in incoming.items()
        )
    if kind != "hash" or not isinstance(hash_values, list):
        return False
    for (name, value), held in zip(sanitize_json_value(plan.doc).items(), hash_values):
        if value is None:
            if held is not None:
                return False
            continue
        text = value if isinstance(value, str) else json_dumps_exact_text(value)
        if held is None or _decode(held) != text:
            return False
    return True


def _hash_has_deletes(plan: RedisDocPlan) -> bool:
    return any(v is None for v in plan.doc.values())


def _hash_merge(pipe: Any, plan: RedisDocPlan) -> None:
    """HSET mapped fields (text as stored by the reader), HDEL mapped NULLs."""
    from services.value_serializer import json_dumps_exact_text, sanitize_json_value

    sets: dict[str, str] = {}
    for name, value in sanitize_json_value(plan.doc).items():
        if value is None:
            continue
        sets[name] = value if isinstance(value, str) else json_dumps_exact_text(value)
    if sets:
        pipe.hset(plan.key, mapping=sets)
    else:
        pipe.exists(plan.key)
    nulls = [name for name, value in plan.doc.items() if value is None]
    if nulls:
        pipe.hdel(plan.key, *nulls)
