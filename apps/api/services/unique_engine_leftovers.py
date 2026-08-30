"""Unique-engine leftover mappings — Mongo ``_id`` and SQLite TEXT carrier.

Competitors still leave these implicit:

* Fivetran MongoDB merges on ``_id`` and marks leftovers ``_fivetran_deleted``.
* Debezium emits ``_id`` in change events; dest apply is the operator's problem.
* SQLite affinity stores DECIMAL as REAL unless the wire is TEXT — our own
  CREATE path already picks TEXT. Inferring ``DECIMAL(p,s)`` back from those
  digits invents capacity (``schema_introspect._sqlite_text_over_numeric_samples``).

DataFlow leftover MERGE is dest-key-addressed. Every source field is mapped or
``intentional_omit``. Dropping Mongo ``_id`` quietly is G13 silent loss.
``100%`` means a named fixture, not marketing. In this slice that fixture is
the leftover mapping cartesian (``test_unique_engine_leftovers``) plus the
sqlite TEXT live write dest COUNT (``test_unique_engine_leftovers_live``).
Not a 5×5 live matrix. Not PRODUCTION_SKU tenant execute.
"""

from __future__ import annotations

from typing import Any

from services.mapping_constraints import is_intentional_omit, write_mappings
from services.type_system import ddl_type

MONGO_OBJECT_ID_OMISSION: dict[str, Any] = {
    "source": "_id",
    "target": "",
    "confidence": 0.0,
    "intentional_omit": True,
}

CORE_ENGINES = ("postgresql", "mysql", "mongodb", "sqlite")


def _norm(name: str) -> str:
    return str(name or "").strip().lower()


def leftover_column_mappings(
    *,
    source_format: str,
    dest_format: str,
    source_columns: list[str] | None = None,
    dest_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Map a leftover cartesian cell without inventing dest types.

    Mongo source ``_id`` is omitted (G13) unless the dest is Mongo and the
    business key binds to ``_id``. SQLite dest ``amount`` stays TEXT. Typed
    warehouse dests may bind NUMERIC with ``source_type=TEXT`` — Validate still
    owns fit. Dest-exists is write-by-name; this helper never invents create-new.
    """
    src = _norm(source_format)
    dst = _norm(dest_format)
    sources = [str(c) for c in (source_columns or ["id", "amount", "_id"]) if str(c).strip()]
    dests = [str(c) for c in (dest_columns or []) if str(c).strip()]
    dest_l = {_norm(c) for c in dests}
    maps: list[dict[str, Any]] = []

    has_id = any(_norm(c) == "id" for c in sources)
    has_mongo_id = any(_norm(c) == "_id" for c in sources)
    has_amount = any(_norm(c) == "amount" for c in sources)

    if dst == "mongodb":
        if has_id:
            maps.append(
                {
                    "source": "id",
                    "target": "_id",
                    "confidence": 0.99,
                    "source_type": "TEXT",
                    "target_type": "TEXT",
                }
            )
        elif has_mongo_id:
            maps.append(
                {
                    "source": "_id",
                    "target": "_id",
                    "confidence": 0.99,
                    "source_type": "TEXT",
                    "target_type": "TEXT",
                }
            )
    elif has_id:
        maps.append(
            {
                "source": "id",
                "target": "id",
                "confidence": 0.99,
                "source_type": "TEXT",
                "target_type": "BIGINT" if dst in {"postgresql", "mysql"} else "TEXT",
            }
        )

    if has_amount:
        if dst == "sqlite":
            amount_type = "TEXT"
        elif dst in {"postgresql", "mysql"}:
            amount_type = ddl_type(dst, "DECIMAL(18,2)") or "NUMERIC(18,2)"
        else:
            amount_type = "TEXT"
        maps.append(
            {
                "source": "amount",
                "target": "amount",
                "confidence": 0.99,
                "source_type": "TEXT" if src == "sqlite" else "NUMERIC",
                "target_type": amount_type,
            }
        )

    if src == "mongodb" and has_mongo_id:
        already = any(
            _norm(m.get("source")) == "_id" and not is_intentional_omit(m) for m in maps
        )
        if not already:
            maps.append(dict(MONGO_OBJECT_ID_OMISSION))

    if dest_l:
        for m in write_mappings(maps):
            tgt = str(m.get("target") or "").strip()
            if tgt and _norm(tgt) not in dest_l and not (
                dst == "mongodb" and _norm(tgt) == "_id"
            ):
                m["create_new"] = False
    return maps


def leftover_g13_accounted(mappings: list[dict[str, Any]], source_columns: list[str]) -> bool:
    """True when every source column is a write mapping or an intentional omit."""
    accounted = {
        _norm(str(m.get("source") or ""))
        for m in mappings
        if str(m.get("source") or "").strip()
        and (is_intentional_omit(m) or str(m.get("target") or "").strip())
    }
    return all(_norm(c) in accounted for c in source_columns if str(c).strip())


def leftover_sqlite_dest_invents_decimal(mappings: list[dict[str, Any]], dest_format: str) -> bool:
    """True when a SQLite dest mapping invents DECIMAL affinity (forbidden)."""
    if _norm(dest_format) != "sqlite":
        return False
    for m in write_mappings(mappings):
        tgt_type = str(m.get("target_type") or "").upper()
        if "DECIMAL" in tgt_type or tgt_type.startswith("NUMERIC"):
            return True
    return False
