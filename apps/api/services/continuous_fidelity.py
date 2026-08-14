"""Continuous Fidelity — prove two live datasets carry the same population.

The industry's own best practice for a mainframe or ERP cutover is *parallel
running*: keep the old system and the new one live side by side and reconcile
them continuously for months, so a divergence is caught while both systems can
still be compared rather than after the old one is gone. Teams do this by
hand-writing SQL; nobody sells it as a product.

This module is that product's engine. Given a source and a destination — two
tables that may sit in different databases, moved by us or by someone else — it
proves, or disproves, that each column still carries the same population, and
names every divergence by column. It is not tied to a transfer: it reads both
sides as they are right now, which is exactly what a parallel-run reconciliation
needs.

It is built on ``services.column_profile`` (the same engine-side, any-scale
aggregate parity the reconcile ladder uses) rather than a parallel comparison
path, and it is honest about its reach:

* **Same-engine** routes compare row count, per-column NULL rate, and numeric and
  temporal min/max/sum.
* **Cross-engine** routes (a Zero-ETL supervisor attaching to, say, a Postgres
  source and a MySQL replica) narrow to the statistics that survive a change of
  engine — row count, NULL rate, canonicalized numeric min/max/sum — and decline
  temporal/text ordering, whose rendering, time-zone and collation semantics
  differ. A declined statistic is stated, never silently skipped.

The report carries a content digest so a stored or forwarded result is
tamper-evident without any key management: recompute the digest over the report
minus the digest field and it must match.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.transfer.models import EndpointConfig

logger = logging.getLogger(__name__)

#: Assurance an individual fidelity run reached.
ASSURANCE_ENGINE_PROFILE = "engine_column_profile"
ASSURANCE_UNSUPPORTED = "unsupported_engine"
ASSURANCE_UNAVAILABLE = "unavailable"
ASSURANCE_NO_COLUMNS = "no_columns"


@dataclass(frozen=True)
class ColumnDivergence:
    """One column statistic that did not match, named for the operator."""

    column: str
    statistic: str  # null_count | non_null_count | min_value | max_value | sum_value | missing_side
    source: Any = None
    target: Any = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FidelityReport:
    """The result of one parallel-run parity check between two datasets."""

    run_id: str
    checked_at: str
    passed: bool
    assurance_level: str
    cross_engine: bool
    source: dict[str, Any]
    destination: dict[str, Any]
    source_rows: int | None
    target_rows: int | None
    row_balance_passed: bool
    columns_compared: int
    divergent_columns: list[str]
    divergences: list[ColumnDivergence]
    compared_statistics: list[str]
    not_compared: list[str]
    message: str
    notes: list[str] = field(default_factory=list)
    report_digest: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "run_id": self.run_id,
            "checked_at": self.checked_at,
            "passed": self.passed,
            "assurance_level": self.assurance_level,
            "cross_engine": self.cross_engine,
            "source": self.source,
            "destination": self.destination,
            "source_rows": self.source_rows,
            "target_rows": self.target_rows,
            "row_balance_passed": self.row_balance_passed,
            "columns_compared": self.columns_compared,
            "divergent_columns": list(self.divergent_columns),
            "divergences": [d.to_dict() for d in self.divergences],
            "compared_statistics": list(self.compared_statistics),
            "not_compared": list(self.not_compared),
            "message": self.message,
            "notes": list(self.notes),
        }
        payload["report_digest"] = _digest_report(payload)
        return payload


def _digest_report(payload: dict[str, Any]) -> str:
    """SHA-256 over the canonical report so a stored result is tamper-evident."""
    body = {k: v for k, v in payload.items() if k != "report_digest"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _identity(engine: str, schema: str, table: str) -> dict[str, Any]:
    return {"engine": engine, "schema": schema or "", "table": table or ""}


def _pairs_and_types(
    mappings: list[dict] | None, column_types: dict[str, str] | None
) -> tuple[list[tuple[str, str]], dict[str, str]]:
    """Resolve ``(source, target)`` pairs and target types from the mapping.

    Column types stamped on the mapping are used first; an explicit
    ``column_types`` override (keyed by target) wins, because a caller that knows
    the destination's physical type should be trusted over an inference.
    """
    from services.mapping_constraints import is_intentional_omit

    pairs: list[tuple[str, str]] = []
    types: dict[str, str] = {}
    for m in mappings or []:
        if is_intentional_omit(m):
            continue
        src = str(m.get("source") or "").strip()
        tgt = str(m.get("target") or src).strip()
        if not src or not tgt:
            continue
        pairs.append((src, tgt))
        tt = str(m.get("target_type") or m.get("inferredType") or "").strip()
        if tt:
            types[tgt] = tt
    for k, v in (column_types or {}).items():
        if v:
            types[str(k)] = str(v)
    if not pairs and column_types:
        # No mapping supplied, but the caller named the columns and their types —
        # treat it as an identity mapping so a same-name parallel run still runs.
        pairs = [(c, c) for c in column_types]
    return pairs, types


def _divergences_from_ladder(ladder: dict[str, Any]) -> list[ColumnDivergence]:
    l2 = (ladder.get("layers") or {}).get("L2") or {}
    details = l2.get("details") or {}
    out: list[ColumnDivergence] = []
    for entry in details.get("mismatches") or []:
        if not isinstance(entry, dict):
            continue
        column = str(entry.get("column") or "")
        if entry.get("reason") == "missing_side":
            out.append(ColumnDivergence(column=column, statistic="missing_side"))
            continue
        for stat, values in (entry.get("diffs") or {}).items():
            values = values if isinstance(values, dict) else {}
            out.append(
                ColumnDivergence(
                    column=column,
                    statistic=str(stat),
                    source=values.get("source"),
                    target=values.get("target"),
                )
            )
    return out


def run_fidelity_check(
    *,
    source: "EndpointConfig",
    destination: "EndpointConfig",
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
    workspace_id: str = "",
) -> FidelityReport:
    """Compare two live datasets right now and report every column divergence.

    Reads both sides as they are — there is no transfer in the loop — so this is
    the primitive a continuous parallel-run reconciliation or a Zero-ETL
    supervisor schedules. Returns a :class:`FidelityReport` in every case,
    including the ones it cannot prove (an unsupported engine, an unreadable
    endpoint), so a scheduler always has a structured result to act on.
    """
    from src.transfer.connector_capabilities import resolve_driver_type

    from services.column_profile import engine_profile_ladder, profile_supported

    run_id = "fid_" + uuid.uuid4().hex[:16]
    checked_at = _now_iso()

    source_engine = resolve_driver_type(source.format or "")
    dest_engine = resolve_driver_type(destination.format or "")
    # Identity is built from the request first so the early returns never need to
    # resolve a saved connector or open a connection to report why they declined.
    source_schema = str(source.schema or "public")
    dest_schema = str(destination.schema or "public")
    source_table = str(source.table or source.collection or "")
    dest_table = str(destination.table or destination.collection or "")

    def _report(**kwargs: Any) -> FidelityReport:
        base = dict(
            run_id=run_id,
            checked_at=checked_at,
            cross_engine=(
                profile_supported(source_engine)
                and profile_supported(dest_engine)
                and _family(source_engine) != _family(dest_engine)
            ),
            source=_identity(source_engine, source_schema, source_table),
            destination=_identity(dest_engine, dest_schema, dest_table),
            source_rows=None,
            target_rows=None,
            row_balance_passed=False,
            columns_compared=0,
            divergent_columns=[],
            divergences=[],
            compared_statistics=[],
            not_compared=[],
            notes=[],
        )
        base.update(kwargs)
        return FidelityReport(**base)

    # Fail fast before touching a connector: an unsupported engine or an unnamed
    # table needs no connection to decline.
    if not (profile_supported(source_engine) and profile_supported(dest_engine)):
        return _report(
            passed=False,
            assurance_level=ASSURANCE_UNSUPPORTED,
            message=(
                "Continuous fidelity currently profiles PostgreSQL and MySQL/MariaDB; "
                f"{source_engine or 'unknown'} → {dest_engine or 'unknown'} is not yet supported."
            ),
        )

    pairs, types = _pairs_and_types(mappings, column_types)
    if not pairs:
        return _report(
            passed=False,
            assurance_level=ASSURANCE_NO_COLUMNS,
            message="No columns to compare — supply a mapping or column_types.",
        )

    from src.transfer.adapters import resolve_connector_config

    src_cfg = resolve_connector_config(source, workspace_id or None)
    dst_cfg = resolve_connector_config(destination, workspace_id or None)
    source_schema = str(source.schema or src_cfg.get("schema") or "public")
    dest_schema = str(destination.schema or dst_cfg.get("schema") or "public")
    source_table = str(source.table or source.collection or src_cfg.get("table") or "")
    dest_table = str(destination.table or destination.collection or dst_cfg.get("table") or "")

    if not source_table or not dest_table:
        return _report(
            passed=False,
            assurance_level=ASSURANCE_NO_COLUMNS,
            message="Source and destination tables must both be named to compare them.",
        )

    ladder = engine_profile_ladder(
        source_engine=source_engine,
        source_cfg=src_cfg,
        source_schema=source_schema,
        source_table=source_table,
        dest_engine=dest_engine,
        dest_cfg=dst_cfg,
        dest_schema=dest_schema,
        dest_table=dest_table,
        pairs=pairs,
        types=types,
    )
    if ladder is None:
        return _report(
            passed=False,
            assurance_level=ASSURANCE_UNAVAILABLE,
            message=(
                "Could not read a profile from one or both endpoints; fidelity is "
                "unproven for this run (the endpoints may be unreachable or the "
                "tables absent)."
            ),
        )

    l1 = (ladder.get("layers") or {}).get("L1") or {}
    l2 = (ladder.get("layers") or {}).get("L2") or {}
    l1_details = l1.get("details") or {}
    l2_details = l2.get("details") or {}
    divergences = _divergences_from_ladder(ladder)
    divergent_columns = list(ladder.get("localization", {}).get("columns") or [])
    passed = bool(ladder.get("passed"))
    row_ok = bool(l1.get("passed"))
    source_rows = _as_int(l1_details.get("source_rows"))
    target_rows = _as_int(l1_details.get("target_rows"))

    if passed:
        message = (
            f"Parity holds: {source_rows} rows on each side, "
            f"{l2_details.get('columns_compared', 0)} columns in agreement."
        )
    elif not row_ok:
        message = (
            f"Row counts diverge: source {source_rows} vs destination {target_rows}."
        )
    else:
        message = "Column divergence: " + (
            ladder.get("localization_summary") or ", ".join(divergent_columns)
        )

    return _report(
        passed=passed,
        assurance_level=ASSURANCE_ENGINE_PROFILE,
        cross_engine=bool(ladder.get("cross_engine")),
        source_rows=source_rows,
        target_rows=target_rows,
        row_balance_passed=row_ok,
        columns_compared=int(l2_details.get("columns_compared") or 0),
        divergent_columns=divergent_columns,
        divergences=divergences,
        compared_statistics=list(l2_details.get("compared_statistics") or []),
        not_compared=list(l2_details.get("not_compared") or []),
        message=message,
    )


def _family(engine: str) -> str:
    from services.column_profile import profile_engine_family

    return profile_engine_family(engine)


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
