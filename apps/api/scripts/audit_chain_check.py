"""Read-only production check of the audit hash chain (D221).

Answers the question behind "N audit-chain verification failures": were they
written that way (forks from concurrent writer processes, records signed under
a different key) or changed afterwards (tampering)? It only ever reads: every
query is a ``find``/``aggregate``/``count`` through a wrapper that refuses
anything else, aggregation pipelines may not contain ``$out``/``$merge``, and
no index, collection or file is created.

    # structure only: forks, duplicate positions, records per signing key
    python scripts/audit_chain_check.py --db datatransfer

    # also recompute each record's HMAC with the keys in this shell's env
    # (DATAFLOW_AUDIT_HMAC_KEY, DATAFLOW_AUDIT_HMAC_RETIRED_KEYS, DATAFLOW_AUTH_SECRET)
    python scripts/audit_chain_check.py --db datatransfer --hmac --json

The Mongo URI comes from ``--uri`` or, like the API, from ``MONGODB_URI`` /
``MONGO_URL`` / ``MONGO_PRIVATE_URL``. Key material is never printed; keys are
shown by their public id (``ak_…``). See docs/AUDIT_CHAIN_PRODUCTION_CHECK.md
for the equivalent mongosh queries.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

API = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API))

from services.audit_chain_keys import LEGACY_KEY_LABEL, hmac_hex, key_id_for  # noqa: E402
from services.brand_env import getenv_brand  # noqa: E402

_READ_METHODS = frozenset(
    {"find", "aggregate", "count_documents", "estimated_document_count", "list_indexes"}
)
_WRITE_STAGES = ("$out", "$merge")


class ReadOnlyCollection:
    """Exposes only the read methods of a pymongo collection."""

    def __init__(self, coll: Any) -> None:
        self._coll = coll

    def __getattr__(self, name: str) -> Any:
        if name not in _READ_METHODS:
            raise PermissionError(f"audit_chain_check is read-only; {name!r} refused")
        method = getattr(self._coll, name)
        if name == "aggregate":
            def aggregate(pipeline: list[dict[str, Any]], **kwargs: Any) -> Any:
                for stage in pipeline:
                    if any(op in stage for op in _WRITE_STAGES):
                        raise PermissionError("audit_chain_check refuses $out/$merge")
                return method(pipeline, **kwargs)

            return aggregate
        return method


def _mongo_uri(explicit: str) -> str:
    if explicit:
        return explicit
    for key in ("MONGODB_URI", "MONGO_URL", "MONGO_PRIVATE_URL", "MONGODB_URL", "MONGO_PUBLIC_URL"):
        val = os.getenv(key, "").strip()
        if val and "mongo" in val.lower():
            return val
    return "mongodb://localhost:27017/"


def _env(name: str) -> str:
    return (getenv_brand(name, "") or "").strip()


def _keys_from_env() -> dict[str, Any]:
    current = _env("AUDIT_HMAC_KEY").encode("utf-8") or None
    retired = [p.strip().encode("utf-8") for p in _env("AUDIT_HMAC_RETIRED_KEYS").split(",") if p.strip()]
    login = (getenv_brand("AUTH_SECRET", "dev-change-me-before-production") or "").encode("utf-8")
    legacy = [login]
    if login == b"dev-change-me-before-production":
        legacy.append(b"dev-only-not-for-production")  # what the old writer fell back to
    return {"current": current, "retired": retired, "login": legacy}


def _count_groups(coll: ReadOnlyCollection, field: str, match: dict[str, Any]) -> dict[str, Any]:
    rows = list(
        coll.aggregate(
            [
                {"$match": match},
                {"$group": {"_id": f"${field}", "n": {"$sum": 1}}},
                {"$match": {"n": {"$gt": 1}}},
                {"$sort": {"n": -1}},
            ],
            allowDiskUse=True,
        )
    )
    return {
        "groups": len(rows),
        "records_beyond_first": sum(int(r["n"]) - 1 for r in rows),
        "examples": [{"value": str(r["_id"])[:16], "records": int(r["n"])} for r in rows[:5]],
    }


def structure_report(coll: ReadOnlyCollection, current_key_id: str | None) -> dict[str, Any]:
    total = coll.count_documents({})
    by_version = {
        ("none (before R7)" if r["_id"] is None else f"v{r['_id']}"): int(r["n"])
        for r in coll.aggregate([{"$group": {"_id": "$chain_v", "n": {"$sum": 1}}}])
    }
    prev = _count_groups(coll, "prev_hash", {"prev_hash": {"$type": "string", "$ne": ""}})
    ws_prev = _count_groups(coll, "ws_prev_hash", {"ws_prev_hash": {"$type": "string", "$ne": ""}})
    seq = _count_groups(coll, "chain_seq", {"chain_seq": {"$gt": 0}})
    by_key = {
        (r["_id"] or LEGACY_KEY_LABEL): int(r["n"])
        for r in coll.aggregate(
            [{"$group": {"_id": "$key_id", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]
        )
    }
    not_current = (
        total - coll.count_documents({"key_id": current_key_id}) if current_key_id else None
    )
    return {
        "total_records": total,
        "by_chain_version": by_version,
        "forks": {
            "predecessors_claimed_more_than_once": prev["groups"],
            "records_beyond_first_claim": prev["records_beyond_first"],
            "duplicate_positions": seq["groups"],
            "records_in_duplicate_positions_beyond_first": seq["records_beyond_first"],
            "workspace_line_predecessors_claimed_more_than_once": ws_prev["groups"],
            "examples": prev["examples"],
        },
        "keys": {
            "current_key_id": current_key_id,
            "by_key_id": by_key,
            "records_not_under_current_key": not_current,
        },
    }


def hmac_report(coll: ReadOnlyCollection, keys: dict[str, Any]) -> dict[str, Any]:
    """Which key, if any, reproduces each record's stored HMAC."""
    current = keys["current"]
    ring = {key_id_for(k): k for k in keys["retired"]}
    if current:
        ring[key_id_for(current)] = current
    counts = {
        "verified_under_current_key": 0,
        "verified_under_retired_key": 0,
        "verified_under_legacy_login_secret": 0,
        "verified_under_no_configured_key": 0,
        "key_id_not_held": 0,
        "no_event_hash": 0,
    }
    for doc in coll.find({}, batch_size=500):
        stored = str(doc.get("event_hash") or "")
        if not stored:
            counts["no_event_hash"] += 1
            continue
        kid = doc.get("key_id")
        if kid:
            key = ring.get(str(kid))
            if key is None:
                counts["key_id_not_held"] += 1
            elif hmac_hex(doc, key) != stored:
                counts["verified_under_no_configured_key"] += 1
            elif current and key == current:
                counts["verified_under_current_key"] += 1
            else:
                counts["verified_under_retired_key"] += 1
            continue
        if any(hmac_hex(doc, k) == stored for k in keys["login"]):
            counts["verified_under_legacy_login_secret"] += 1
        elif any(hmac_hex(doc, k) == stored for k in keys["retired"]):
            counts["verified_under_retired_key"] += 1
        else:
            counts["verified_under_no_configured_key"] += 1
    counts["note"] = (
        "verified_under_no_configured_key is the only count that can mean an "
        "altered record; it also counts records signed with a secret not in this "
        "shell's env (e.g. a login secret rotated before the chain had its own key)."
    )
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--uri", default="", help="Mongo URI (default: MONGODB_URI / MONGO_URL env)")
    parser.add_argument("--db", default="datatransfer", help="database name (default: datatransfer)")
    parser.add_argument("--collection", default="audit_events")
    parser.add_argument("--hmac", action="store_true", help="also recompute HMACs with env keys")
    parser.add_argument("--json", action="store_true", help="print JSON only")
    args = parser.parse_args(argv)

    import pymongo

    client = pymongo.MongoClient(_mongo_uri(args.uri), serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        print(f"error: cannot reach MongoDB: {type(exc).__name__}", file=sys.stderr)
        return 2
    from services.value_serializer import control_plane_codec_options

    # Read with the codec the API reads with, so recomputed HMACs see the same values.
    db = client.get_database(
        args.db, codec_options=control_plane_codec_options(client[args.db].codec_options)
    )
    coll = ReadOnlyCollection(db[args.collection])
    keys = _keys_from_env()
    current_kid = key_id_for(keys["current"]) if keys["current"] else None
    report: dict[str, Any] = {
        "read_only": True,
        "database": args.db,
        "collection": args.collection,
        **structure_report(coll, current_kid),
    }
    if args.hmac:
        report["hmac"] = hmac_report(coll, keys)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return 0
    forks = report["forks"]
    print(f"audit chain check (read-only) - {args.db}.{args.collection}")
    print(f"  records:                              {report['total_records']}")
    print(f"  by chain version:                     {report['by_chain_version']}")
    print(f"  predecessors claimed more than once:  {forks['predecessors_claimed_more_than_once']}"
          f" ({forks['records_beyond_first_claim']} extra record(s)) <- forks")
    print(f"  duplicate chain positions:            {forks['duplicate_positions']}")
    print(f"  workspace-line forks:                 "
          f"{forks['workspace_line_predecessors_claimed_more_than_once']}")
    print(f"  records per signing key:              {report['keys']['by_key_id']}")
    print(f"  records not under the current key:    {report['keys']['records_not_under_current_key']}")
    if args.hmac:
        for name, value in report["hmac"].items():
            if name != "note":
                print(f"  hmac {name.replace('_', ' ')}: {value}")
        print(f"  ({report['hmac']['note']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
