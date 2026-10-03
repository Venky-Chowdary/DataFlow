"""Destination referential-integrity proof: the parent rows actually exist.

A carried foreign key is a *promise*; it is only worth anything if the engine
was enforcing it while the rows landed. Two real migration outcomes this
module separates, which a catalog diff alone cannot:

``enforced``   the destination carries a foreign key the catalog says was
               checked against existing rows — no scan needed. PostgreSQL
               NOT VALID, a SQL Server untrusted or disabled key, and an
               Oracle NOT VALIDATED key are not this proof; those rows are
               scanned. SQLAlchemy's PostgreSQL reflection omits NOT VALID,
               so the validation bit is read from the catalog probe. Redshift,
               Snowflake, BigQuery, and Databricks store the constraint and
               do not check rows against it, so a catalog hit is not this
               proof either. A Snowflake hybrid table does enforce a foreign
               key; the dialect name does not say the table is hybrid, so
               that catalog hit is not the proof either.
``scanned``    the destination has no such constraint (dropped for load speed,
               or never created), so the child rows are anti-joined against
               the parent and orphans are counted for real

Composite keys are scanned as a tuple under the catalog match type. Unreported
is SQL ``MATCH SIMPLE``: a child row with any NULL in the key is unconstrained
and is not an orphan. A source ``MATCH FULL`` is that scan, including when the
destination constraint is enforced as ``MATCH SIMPLE``. ``MATCH PARTIAL`` is
not a completed scan.

An enforced catalog FK counts only when it is the same relationship: the same
parent table and the same (child column, parent column) pairs. A constraint on
the same child columns that points at a different parent column is not that
promise, and the child rows are scanned.

Anything else — parent table missing, unreadable catalog, failed scan — is
reported unavailable with a reason. An unproven relationship never counts as
clean, because "no orphans found" and "no scan ran" look identical in a report
and only one of them is true.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import sqlalchemy as sa

from services.fk_tuple_scan import _table_col, alias_parent_if_self_ref
from services.foreign_key_metadata import (
    enforced_relationship_identities,
    probe_foreign_keys,
    validation_catalog_dialect,
)
from services.fk_tuple_scan import orphan_example_text as _orphan_example_text  # noqa: F401
from services.fk_tuple_scan import match_scan_refusal, normalize_match, scan_orphan_anti_join
from services.foreign_key_identity import (
    fk_identity as _fk_identity,
    fold as _fold,
    parent_label as _parent_label,
    parse_foreign_key,
    relationship_identity,
    same_relationship as _same_relationship,
)
from services.physical_state_diff import catalog_table_names, resolve_stored_name

logger = logging.getLogger(__name__)

GATE_ID = "g22_dest_referential_integrity"
REPORT_SCHEMA = "dest_referential_integrity_v1"
_PROVEN_STATUSES = frozenset({"enforced", "scanned"})

__all__ = [
    "verify_destination_referential_integrity",
    "relationship_identity",
    "referential_integrity_proven",
    "build_dest_ri_gate",
    "build_dest_ri_validate_gate",
    "apply_dest_ri_to_reconcile",
    "GATE_ID",
]


def _destination_match(
    identity: tuple[str, tuple[tuple[str, str], ...]] | None,
    measured: Any,
    inspector_fks: list[Any],
) -> str:
    """Match type the destination catalog recorded for this relationship.

    The metadata probe wins. Inspector ``options["match"]`` is the fallback
    SQLAlchemy keeps when the DDL names the clause. Empty means unreported.
    """
    if identity is None:
        return ""
    if measured is not None and getattr(measured, "measured", False):
        named = ""
        for item in measured.items:
            ident = _fk_identity(
                {
                    "constrained_columns": item.columns,
                    "referred_schema": item.referenced_schema,
                    "referred_table": item.referenced_table,
                    "referred_columns": item.referenced_columns,
                    "match": item.match,
                }
            )
            if not _same_relationship(identity, ident):
                continue
            kind = normalize_match(item.match)
            if kind and named and kind != named:
                return "unknown"
            if kind:
                named = kind
        if named:
            return named
    for fk in inspector_fks:
        if not isinstance(fk, dict):
            continue
        parsed = parse_foreign_key(fk)
        if parsed.conflict or not _same_relationship(identity, _fk_identity(fk)):
            continue
        return normalize_match(parsed.match)
    return ""


def _orphan_scan(
    conn: Any,
    *,
    child: Any,
    child_columns: list[str],
    parent: Any,
    parent_columns: list[str],
    match: str = "",
) -> dict[str, Any]:
    """Anti-join via ``fk_tuple_scan`` using the catalog match type."""
    try:
        c_cols = [_table_col(child, name) for name in child_columns]
        p_cols = [_table_col(parent, name) for name in parent_columns]
    except KeyError:
        return {"available": False, "reason": "join column missing from catalog"}
    return scan_orphan_anti_join(
        conn,
        child=child,
        child_columns=c_cols,
        parent=parent,
        parent_columns=p_cols,
        match=match,
    )


def _reflect(conn: Any, meta: sa.MetaData, name: str, schema: str | None) -> Any:
    return sa.Table(name, meta, autoload_with=conn, schema=schema)


def verify_destination_referential_integrity(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str = "",
    table: str,
    foreign_keys: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Prove every source relationship still holds in the destination data.

    ``foreign_keys`` are the relationships the *source* guaranteed (each with
    ``constrained_columns``, ``referred_table``, ``referred_columns``). When
    omitted, the destination's own catalog FKs are used — those are enforced by
    definition, so the interesting call passes the source's.
    """
    if not table:
        return {"verified": False, "reason": "no table name to inspect"}

    from connectors.generic_sql import get_sqlalchemy_engine

    try:
        engine = get_sqlalchemy_engine({**cfg, "type": db_type})
    except Exception as exc:  # noqa: BLE001 — a refused connection is evidence
        return {"verified": False, "reason": f"cannot connect: {exc}"}

    relations: list[dict[str, Any]] = []
    schema_arg = schema or None
    with engine.connect() as conn:
        inspector = sa.inspect(conn)
        child_name = resolve_stored_name(
            catalog_table_names(
                inspector, schema_arg, conn=conn, dialect=str(db_type)
            ),
            table,
        )
        if child_name is None:
            return {
                "verified": False,
                "reason": f"table {table} not found in destination catalog",
            }

        dest_fks = inspector.get_foreign_keys(child_name, schema=schema_arg)
        measured = None
        catalog_dialect = validation_catalog_dialect(db_type)
        if catalog_dialect:
            measured = probe_foreign_keys(
                catalog_dialect, conn, schema_arg or "", child_name
            )
        enforced = enforced_relationship_identities(db_type, dest_fks, measured)
        wanted = list(foreign_keys if foreign_keys is not None else dest_fks)
        if not wanted:
            return {
                "verified": True,
                "reason": "source declares no foreign keys",
                "relations": [],
            }

        meta = sa.MetaData()
        table_names = catalog_table_names(
            inspector, schema_arg, conn=conn, dialect=str(db_type)
        )
        for fk in wanted:
            parsed = parse_foreign_key(fk)
            child_cols = list(parsed.child_columns)
            parent_cols = list(parsed.parent_columns)
            parent_schema = _fold(parsed.parent_schema)
            parent_table = _fold(parsed.parent_table)
            rel: dict[str, Any] = {
                "columns": child_cols,
                "referred_table": parent_table,
                "referred_columns": parent_cols,
            }
            if parsed.conflict:
                rel.update(
                    status="unavailable",
                    available=False,
                    reason=parsed.conflict,
                )
                relations.append(rel)
                continue
            key = relationship_identity(
                child_cols, _parent_label(parent_schema, parent_table), parent_cols
            )
            source_match = normalize_match(parsed.match)
            refusal = match_scan_refusal(source_match)
            if refusal:
                rel.update(
                    status="unavailable",
                    available=False,
                    match=source_match,
                    reason=refusal,
                )
                relations.append(rel)
                continue
            # An enforced MATCH SIMPLE constraint does not prove MATCH FULL.
            # A partial NULL is legal there and is an orphan under the source rule.
            dest_match = _destination_match(key, measured, dest_fks)
            simple_promise = source_match != "full"
            if any(_same_relationship(key, known) for known in enforced) and (
                simple_promise or dest_match == "full"
            ):
                rel.update(status="enforced", available=True, orphan_count=0)
                relations.append(rel)
                continue
            if not child_cols or len(child_cols) != len(parent_cols):
                rel.update(
                    status="unavailable",
                    available=False,
                    reason="relationship has no usable column pairing",
                )
                relations.append(rel)
                continue
            parent_lookup_schema = parent_schema or None
            if parent_schema and _fold(parent_schema) != _fold(schema_arg or ""):
                parent_names = catalog_table_names(
                    inspector,
                    parent_lookup_schema,
                    conn=conn,
                    dialect=str(db_type),
                )
            else:
                parent_names = table_names
            stored_parent = resolve_stored_name(parent_names, parent_table)
            if stored_parent is None:
                qualified = _parent_label(parent_schema, parent_table) or parent_table
                rel.update(
                    status="unavailable",
                    available=False,
                    reason=f"parent table {qualified} absent from destination",
                )
                relations.append(rel)
                continue
            try:
                child_tbl = _reflect(conn, meta, child_name, schema_arg)
                parent_tbl = alias_parent_if_self_ref(
                    child_tbl,
                    _reflect(
                        conn,
                        meta,
                        stored_parent,
                        parent_lookup_schema or schema_arg,
                    ),
                )
                resolved_child = [
                    resolve_stored_name([c.name for c in child_tbl.columns], name)
                    for name in child_cols
                ]
                resolved_parent = [
                    resolve_stored_name([c.name for c in parent_tbl.columns], name)
                    for name in parent_cols
                ]
                if any(c is None for c in resolved_child) or any(
                    p is None for p in resolved_parent
                ):
                    raise LookupError("join column not resolvable in destination")
                scan = _orphan_scan(
                    conn,
                    child=child_tbl,
                    child_columns=[str(c) for c in resolved_child],
                    parent=parent_tbl,
                    parent_columns=[str(p) for p in resolved_parent],
                    match=source_match,
                )
            except Exception as exc:  # noqa: BLE001 — a failed scan is evidence
                logger.warning("destination RI scan failed: %s", exc)
                scan = {"available": False, "reason": f"scan failed: {exc}"}
            if scan.get("available"):
                rel.update(status="scanned", **scan)
            else:
                rel.update(status="unavailable", **scan)
            relations.append(rel)

    orphaned = [r for r in relations if int(r.get("orphan_count") or 0) > 0]
    unavailable = [r for r in relations if not r.get("available")]
    return {
        "verified": not orphaned and not unavailable,
        "relations": relations,
        "orphan_relations": [
            f"{'+'.join(r['columns'])}->{r['referred_table']}" for r in orphaned
        ],
        "unavailable_relations": [
            f"{'+'.join(r['columns'])}->{r['referred_table']}" for r in unavailable
        ],
        "orphan_rows": sum(int(r.get("orphan_count") or 0) for r in relations),
    }


def referential_integrity_proven(evidence: Mapping[str, Any] | None) -> bool:
    """True only when dest-side relations were enforced or scanned with 0 orphans.

    No relationships is not proven of nothing. Schema-only ``constraint_fk``
    and a source sample orphan probe never set this.
    """
    if not isinstance(evidence, dict):
        return False
    relations = [
        r for r in list(evidence.get("relations") or []) if isinstance(r, dict)
    ]
    if not relations:
        return False
    if not evidence.get("verified"):
        return False
    for rel in relations:
        status = str(rel.get("status") or "").strip().lower()
        if status not in _PROVEN_STATUSES:
            return False
        if not rel.get("available"):
            return False
        if int(rel.get("orphan_count") or 0) > 0:
            return False
    return True


def build_dest_ri_validate_gate(*, has_relationships: bool) -> dict[str, Any]:
    """Validate never claims dest population RI — that is a post-write Gate-8 proof."""
    if not has_relationships:
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                "No foreign keys were declared on this route — destination "
                "referential integrity was not asked."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": False,
                "rule_id": f"{GATE_ID}.undeclared",
            },
        }
    return {
        "id": GATE_ID,
        "status": "skip",
        "message": (
            "Source relationships are present. Destination-side referential "
            "integrity (enforced FK or anti-join orphan scan) is a post-write "
            "Gate-8 proof — Validate schema coverage (constraint_fk) and a "
            "sample orphan probe are not that proof."
        ),
        "duration_ms": 0,
        "details": {
            "schema": REPORT_SCHEMA,
            "declared": True,
            "evidence": "unmeasured",
            "rule_id": f"{GATE_ID}.post_write",
        },
    }


def build_dest_ri_gate(
    evidence: Mapping[str, Any] | None,
    *,
    has_relationships: bool | None = None,
) -> dict[str, Any]:
    """Execute Gate-8 G22 from dest RI evidence. Fail closed on orphans/unproven."""
    relations = []
    if isinstance(evidence, Mapping):
        relations = [
            r for r in list(evidence.get("relations") or []) if isinstance(r, dict)
        ]
    asked = bool(has_relationships) or bool(relations)
    if not asked:
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                "No foreign keys were declared on this route — destination "
                "referential integrity was not asked."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": False,
                "rule_id": f"{GATE_ID}.undeclared",
            },
        }
    if not isinstance(evidence, Mapping) or not evidence:
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                "Destination referential integrity is unproven — the dest-side "
                "orphan scan did not run. Fail closed."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": True,
                "rule_id": f"{GATE_ID}.unproven",
            },
        }
    if referential_integrity_proven(evidence):
        n = len(relations)
        return {
            "id": GATE_ID,
            "status": "pass",
            "message": (
                f"Destination referential integrity holds on {n} relationship(s) "
                "(enforced FK or anti-join scan, 0 orphans)."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": True,
                "relations": relations,
                "rule_id": f"{GATE_ID}.proven",
            },
        }
    orphan_rows = int(evidence.get("orphan_rows") or 0)
    orphan_rels = list(evidence.get("orphan_relations") or [])
    unavailable = list(evidence.get("unavailable_relations") or [])
    if orphan_rows > 0 or orphan_rels:
        named = ", ".join(str(r) for r in orphan_rels[:4]) or "relationship"
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"Destination referential integrity failed: {orphan_rows} orphan "
                f"row(s) on {named}. A matching row count does not prove parents exist."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": True,
                "orphan_rows": orphan_rows,
                "orphan_relations": orphan_rels,
                "relations": relations,
                "rule_id": f"{GATE_ID}.orphans",
            },
        }
    reason = str(evidence.get("reason") or "")
    named_unavail = ", ".join(str(r) for r in unavailable[:4])
    return {
        "id": GATE_ID,
        "status": "block",
        "message": (
            "Destination referential integrity is unproven"
            + (f" ({named_unavail})" if named_unavail else "")
            + (f": {reason}" if reason else "")
            + ". Fail closed."
        ),
        "duration_ms": 0,
        "details": {
            "schema": REPORT_SCHEMA,
            "declared": True,
            "unavailable_relations": unavailable,
            "reason": reason,
            "relations": relations,
            "rule_id": f"{GATE_ID}.unproven",
        },
    }


def apply_dest_ri_to_reconcile(
    stamped: dict[str, Any],
    *,
    evidence: Mapping[str, Any] | None = None,
    has_relationships: bool | None = None,
) -> dict[str, Any]:
    """Stamp G22 onto a Gate-8 report and fail the job on dest orphans/unproven."""
    phys = stamped.get("physical_state") if isinstance(stamped.get("physical_state"), dict) else {}
    ri = evidence
    if not isinstance(ri, Mapping):
        ri = phys.get("referential_integrity") if isinstance(phys, dict) else None
    gate = build_dest_ri_gate(
        ri if isinstance(ri, Mapping) else None,
        has_relationships=has_relationships,
    )
    out = dict(stamped)
    out["g22_dest_referential_integrity"] = gate
    if gate.get("status") == "block":
        out["passed"] = False
        prior = str(out.get("message") or "").rstrip()
        extra = str(gate.get("message") or "G22 destination referential integrity failed")
        out["message"] = f"{prior} {extra}".strip() if prior else extra
    return out

