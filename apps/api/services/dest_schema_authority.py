"""Destination carrier provenance — sampled profile vs declared catalog.

D1: a schemaless destination's shape is inferred from a bounded value sample
and then compared as if the destination had *declared* it, so run 2 of a route
refuses what run 1 correctly wrote. Object-store writers also enforce the
probed width at write time, so suppressing the Map verdict alone would fail
open (green Map, quarantined rows).

Authority is a first-class fact that leaves the probe:

* ``declared`` — a real catalog (SQL DDL, Elasticsearch mapping, DynamoDB key
  type, operator-authored target). Enforced. Narrowing is still lossy.
* ``sampled`` — a page-measured profile. It may *widen* to the source
  declaration; it is never a ceiling.
* ``unknown`` — the probe failed. Not compatible, not invented. Map/Validate
  keep ``dest_type_unread``.

``is_lossy_coercion`` is not weakened. A sampled ``DECIMAL(2,2)`` is rewritten
to the widened profile *before* that function is asked, so it answers the
question the destination actually poses.
"""

from __future__ import annotations

from typing import Any, Mapping

CARRIER_DECLARED = "declared"
CARRIER_SAMPLED = "sampled"
CARRIER_UNKNOWN = "unknown"

ORIGIN_CATALOG = "destination_catalog"
ORIGIN_SAMPLED = "sampled_profile"


def destination_schema_is_sampled(db_type: str | None) -> bool:
    """True when this engine's live schema is a value sample, not a catalog.

    Elasticsearch is excluded: an index mapping is a declared catalog (D16).
    Document stores stay sampled (Mongo/Dynamo attributes, Redis, Kafka).
    Object stores and file exports have no DDL — a reread is always a sample.
    """
    from services.type_system import (
        destination_carriers_are_inferred,
        destination_is_file_export,
    )

    if destination_is_file_export(db_type):
        return True
    return destination_carriers_are_inferred(db_type)


def default_column_authority(db_type: str | None) -> str:
    """Engine-level default when the probe did not stamp per-column authority."""
    if destination_schema_is_sampled(db_type):
        return CARRIER_SAMPLED
    return CARRIER_DECLARED


def column_carrier_authority(
    db_type: str | None,
    column: str,
    *,
    authority_map: Mapping[str, str] | None = None,
) -> str:
    """Per-column authority, folding to the engine default when unstamped."""
    name = str(column or "").strip()
    if not name:
        return CARRIER_UNKNOWN
    raw = ""
    if authority_map:
        raw = str(authority_map.get(name) or "").strip().lower()
        if not raw:
            folded = {str(k).casefold(): str(v) for k, v in authority_map.items()}
            raw = str(folded.get(name.casefold()) or "").strip().lower()
    if raw in {CARRIER_DECLARED, CARRIER_SAMPLED, CARRIER_UNKNOWN}:
        return raw
    return default_column_authority(db_type)


def authority_from_batch_meta(
    headers: list[str],
    meta: Mapping[str, Any] | None,
    db_type: str | None = "",
) -> dict[str, str]:
    """Per-header authority from a ReadBatch.

    Explicit ``native_types_authority`` wins. Keys present in ``native_types``
    inherit the engine default (Elasticsearch → declared, object store →
    sampled). Headers the mapping/sample did not type stay sampled on a
    sampled engine and unknown on a catalog engine — never silently declared.
    """
    headers = [str(h) for h in (headers or []) if str(h).strip()]
    payload = dict(meta or {})
    stamped = payload.get("native_types_authority")
    native = payload.get("native_types")
    native_keys = {
        str(k)
        for k, v in (native.items() if isinstance(native, dict) else [])
        if k and str(v or "").strip()
    }
    explicit: dict[str, str] = {}
    if isinstance(stamped, dict):
        for key, value in stamped.items():
            token = str(value or "").strip().lower()
            if token in {CARRIER_DECLARED, CARRIER_SAMPLED, CARRIER_UNKNOWN}:
                explicit[str(key)] = token
    engine_default = default_column_authority(db_type)
    out: dict[str, str] = {}
    for header in headers:
        if header in explicit:
            out[header] = explicit[header]
        elif header in native_keys:
            out[header] = engine_default
        elif destination_schema_is_sampled(db_type):
            out[header] = CARRIER_SAMPLED
        else:
            # Catalog engine, field not in the mapping: placeholder, not a
            # declaration. D16: dynamic ES fields must not acquire a type.
            out[header] = CARRIER_SAMPLED
    return out


def declared_native_types_meta(types: Mapping[str, str]) -> dict[str, Any]:
    """ReadBatch.meta for a catalog that declared its fields (ES mapping)."""
    native = {
        str(k): str(v)
        for k, v in (types or {}).items()
        if k and str(v or "").strip()
    }
    return {
        "native_types": native,
        "native_types_authority": {name: CARRIER_DECLARED for name in native},
    }


def widen_sampled_dest_carrier(
    source_type: str,
    sampled_type: str,
    dest_db: str = "",
) -> str:
    """Widen a sampled dest carrier so it can hold the source declaration.

    Same-family width (``DECIMAL(2,2)`` vs ``DECIMAL(12,2)``, ``INTEGER`` vs
    ``BIGINT``, ``VARCHAR(4)`` vs ``VARCHAR(64)``) is re-projected. A numeric
    sample that observed integers while the source declared a decimal widens
    along the integer⊂decimal lattice. Incompatible families (decimal vs date,
    bool vs string-that-is-not-a-widening) stay as sampled — ``is_lossy_coercion``
    still answers that question. Never invents a type from an empty sample.
    """
    sampled = (sampled_type or "").strip()
    source = (source_type or "").strip()
    if not sampled or not source:
        return sampled
    from connectors.schema_drift import is_wider_type
    from services.decision_kernel.type_invent import (
        create_new_mapping_target_type,
        promote_create_new_capacity_stamp,
    )
    from services.type_system import is_lossy_coercion, normalize_logical_type

    if not is_lossy_coercion(source, sampled, dest_db=dest_db):
        return sampled

    src_logical = normalize_logical_type(source)
    sampled_logical = normalize_logical_type(sampled)

    if src_logical == sampled_logical:
        promoted = promote_create_new_capacity_stamp(source, sampled, dest_db)
        if promoted and not is_lossy_coercion(source, promoted, dest_db=dest_db):
            return promoted
        if is_wider_type(sampled, source, dest_db=dest_db):
            return source
        return sampled

    # Observed a narrower numeric family than the source declared (first page
    # of integers, later page / declared DECIMAL). The profile widens.
    if sampled_logical == "integer" and src_logical in {"decimal", "integer"}:
        projected = create_new_mapping_target_type(source, dest_db) or source
        if projected and not is_lossy_coercion(source, projected, dest_db=dest_db):
            return projected
        if is_wider_type(sampled, source, dest_db=dest_db):
            return source
    if sampled_logical == "decimal" and src_logical == "integer":
        # Sample is already the wider family; keep it (never narrow).
        return sampled
    if is_wider_type(sampled, source, dest_db=dest_db):
        return source
    return sampled


def apply_sampled_profile_to_dest_types(
    dest_types: Mapping[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
    authority_map: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Replace sampled dest widths with the widened profile; leave declared.

    An operator override is a ceiling and is not widened. Missing origin on a
    sampled engine is still a profile — that is the D1 write path: Studio
    posted ``DECIMAL(2,2)`` with no provenance stamp.
    """
    from services.mapping_constraints import write_mappings

    out = {str(k): str(v) for k, v in dict(dest_types or {}).items() if k}
    if not out:
        return out
    by_target: dict[str, dict[str, Any]] = {}
    for mapping in write_mappings(list(mappings or [])):
        tgt = str(mapping.get("target") or "").strip()
        if tgt:
            by_target[tgt] = mapping
            by_target.setdefault(tgt.casefold(), mapping)
    for name, current in list(out.items()):
        mapping = by_target.get(name) or by_target.get(name.casefold()) or {}
        if mapping.get("user_override") or mapping.get("userOverride"):
            continue
        if column_carrier_authority(
            dest_db, name, authority_map=authority_map
        ) != CARRIER_SAMPLED:
            continue
        source_type = str(mapping.get("source_type") or "").strip()
        if not source_type:
            continue
        out[name] = widen_sampled_dest_carrier(source_type, current, dest_db)
    return out


def probe_failed_schema_is_unknown(
    *,
    schema: Mapping[str, str] | None,
    table_exists: bool | None,
    probe_error: str = "",
) -> bool:
    """True when the probe produced no authoritative types and must stay unread.

    A MinIO ``NoSuchKey`` (or any read failure) must not become a compatible
    schema. Empty types plus unknown existence is unread; empty types plus
    proven-absent is create-new (not this helper).
    """
    if table_exists is False:
        return False
    if schema:
        return False
    return table_exists is None or bool(str(probe_error or "").strip())
