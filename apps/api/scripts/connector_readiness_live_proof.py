#!/usr/bin/env python3
"""Track E — measured connector readiness cells through the real engine.

One command, env-gated: every route whose store is not reachable is recorded as
``skip`` with the exact reason instead of being dropped or assumed green.

    cd apps/api && python scripts/connector_readiness_live_proof.py --rows 100000

Each cell does exactly what a client audit would do:

1. seed the source with the store's **own driver** (never the engine) and
   measure it independently — that is the source count;
2. run ``UniversalTransferEngine.execute_tracked`` (no bypass, preflight on);
3. measure the destination with a **second, independent driver connection** —
   count plus a checksum over ``id``/``amount`` so a mangled value fails the
   cell even when the row count matches;
4. record rejected / quarantined / coerced-null / skipped from the engine's own
   row accounting, plus elapsed, rows/sec, sync mode and run id.

The reverse direction reuses the rows the forward direction wrote, so the
destination store is proven as a source over data the product itself produced.

Writes ``data/proofs/connector_readiness_live.json``, consumed by
``scripts/connector_readiness_matrix.py`` to mark ``proven-live-here``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

ARTIFACT = _API_ROOT / "data" / "proofs" / "connector_readiness_live.json"

#: The fixture's declared types. A schemaless source (documents, KV blobs, CSV
#: objects) declares none, so CREATE types would be peeked from the sample and
#: preflight fail-closes when later rows do not fit the peek. Stamping these is
#: the Map decision an operator makes in Studio, not a bypass: every gate,
#: coercion and reconcile still runs.
FIXTURE_TYPES = {
    "id": "INTEGER",
    "name": "VARCHAR(64)",
    "amount": "DECIMAL(12,2)",
    "code": "VARCHAR(8)",
    "flag": "BOOLEAN",
}


def _reconciled_source_type(declared: str, introspected: str) -> str:
    """The type an operator would keep in Map for one schemaless source column.

    A schemaless store gives introspection two different kinds of answer, and
    only one of them is authority:

    * a **carrier** (DynamoDB ``N``, Redis text) — the store's own domain, so it
      beats the fixture's declaration: reading ``N`` back really can return a
      38-digit decimal, and stamping BIGINT would be a narrowing.
    * a **sample peek** (``DECIMAL(3,2)`` from the first documents) — narrower
      than the population, which is what preflight fail-closes on. The
      operator widens it, which is what the declared fixture type is.

    Same logical family plus declared parameters means a peek; no parameters
    means the carrier's full capacity.
    """
    from services.type_system import (
        LOGICAL_STRING,
        LOGICAL_TEXT,
        normalize_logical_type,
    )

    if not introspected:
        return declared
    src_logical = normalize_logical_type(introspected)
    if src_logical != normalize_logical_type(declared):
        if src_logical in {LOGICAL_STRING, LOGICAL_TEXT}:
            # An untyped text carrier (Elasticsearch ``text``, a Redis JSON
            # blob) is the store's absence of typing, not a statement that the
            # column is text. The fixture's domain stands and the engine still
            # grades the parse on real values.
            return declared
        return introspected
    return declared if "(" in introspected else introspected


def _declared_source_types(src: Any, src_name: str) -> dict[str, str]:
    """Fixture column types for a schemaless source, reconciled with the store."""
    introspected = _introspected_source_types(src, src_name)
    return {
        col: _reconciled_source_type(declared, introspected.get(col, ""))
        for col, declared in FIXTURE_TYPES.items()
    }


def _schemaless_mappings(
    src: Any,
    src_name: str,
    dest_driver: str,
) -> list[dict[str, Any]]:
    """Map stamps for a source that declares no types, in dest dialect DDL.

    The source type is whatever the product's own introspection reports for the
    seeded fixture — the operator in Studio maps what Map shows, and a store's
    carrier is the truth about fidelity (DynamoDB's ``N`` is an exact decimal,
    not an INTEGER promise). Falling back to the declared fixture type only
    covers a column introspection does not surface.

    Store-assigned columns (Mongo's ``_id``, Elasticsearch's ``_index``, Redis
    key metadata) are declared omitted rather than left unaccounted, because
    G13 must block a column that would leave silently.
    """
    from services.type_system import materialize_dest_ddl

    introspected = _introspected_source_types(src, src_name)
    source_types = _declared_source_types(src, src_name)
    mappings: list[dict[str, Any]] = []
    for col, source_type in source_types.items():
        mappings.append({
            "source": col,
            "target": col,
            "source_type": source_type,
            "target_type": materialize_dest_ddl(dest_driver, source_type),
            "confidence": 1.0,
        })
    omitted = {c for c in src.omit_columns} | {
        c for c in introspected if c not in FIXTURE_TYPES
    }
    mappings += [
        {"source": col, "target": "", "intentional_omit": True}
        for col in sorted(omitted)
    ]
    return mappings


def _introspected_source_types(src: Any, src_name: str) -> dict[str, str]:
    """Source columns and types as the product's introspection reports them."""
    from src.transfer.endpoint_intelligence import introspect_endpoint

    out = introspect_endpoint(src.endpoint(src_name))
    schema = out.get("schema") or {}
    return {
        str(col): str(typ)
        for col, typ in schema.items()
        if str(col) and str(typ)
    }


# Routes worth measuring locally: a relational hub (PostgreSQL) against every
# other locally-startable store, plus the relational pairs clients migrate
# between most often. Each entry is measured in both directions when both
# stores can take both roles.
ROUTES: tuple[tuple[str, str], ...] = (
    ("postgresql", "mysql"),
    ("postgresql", "sqlite"),
    ("postgresql", "duckdb"),
    ("postgresql", "mongodb"),
    ("postgresql", "sqlserver"),
    ("postgresql", "redis"),
    ("postgresql", "s3"),
    ("postgresql", "dynamodb"),
    ("postgresql", "elasticsearch"),
    ("mysql", "mongodb"),
    ("mysql", "sqlite"),
)


def _row_accounting(result: Any) -> dict[str, int]:
    """Engine-reported accounting; absent keys stay absent rather than zeroed."""
    acct = getattr(result, "row_accounting", None) or {}
    out: dict[str, int] = {}
    for key in (
        "rejected", "quarantined", "coerced_null", "coerced_nulls",
        "skipped", "written", "read",
    ):
        if key in acct:
            try:
                out[key] = int(acct[key] or 0)
            except (TypeError, ValueError):
                continue
    summary = getattr(result, "destination_summary", None) or {}
    for key in ("rejected_rows", "quarantined_rows"):
        if key in summary and key not in out:
            try:
                out[key] = int(summary[key] or 0)
            except (TypeError, ValueError):
                continue
    return out


def _object_key(store: Any, name: str) -> str:
    """Object stores address by key, not table name."""
    return f"{name}.csv" if store.driver in {"s3", "gcs", "adls"} else name


def run_cell(
    src_key: str,
    dst_key: str,
    rows: int,
    *,
    sync_mode: str = "full_refresh_overwrite",
    seeded_name: str | None = None,
) -> dict[str, Any]:
    from tests.connector_readiness_stores import STORES
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    src, dst = STORES[src_key], STORES[dst_key]
    cell: dict[str, Any] = {
        "source": src_key,
        "destination": dst_key,
        "sync_mode": sync_mode,
        "requested_rows": rows,
        "schema_shape": "id INT PK, name VARCHAR(64), amount DECIMAL(12,2), code VARCHAR(8), flag BOOL",
    }

    ok, reason = src.available()
    if not ok:
        return {**cell, "status": "skip", "reason": reason}
    ok, reason = dst.available()
    if not ok:
        return {**cell, "status": "skip", "reason": reason}
    if not src.can_source:
        return {**cell, "status": "skip", "reason": f"{src_key} has no reader (source-incapable)"}
    if not dst.can_dest:
        return {**cell, "status": "skip", "reason": f"{dst_key} has no writer (destination-incapable)"}
    if seeded_name is None and src.seed is None:
        return {
            **cell,
            "status": "skip",
            "reason": f"{src_key} cannot be seeded by its own driver in this harness; "
                      "run the forward direction first so the engine populates it",
        }

    stem = f"trk_e_{src_key}_{dst_key}_{uuid.uuid4().hex[:6]}"
    src_name = _object_key(src, f"{stem}_s")
    dst_name = _object_key(dst, f"{stem}_d")

    try:
        if seeded_name is None:
            src.seed(src_name, rows)  # type: ignore[misc]
        else:
            src_name = seeded_name
        source_count, source_checksum = src.measure(src_name)
        cell["source_count"] = source_count
        cell["source_checksum"] = source_checksum

        dst.drop(dst_name)
        column_types = (
            _declared_source_types(src, src_name) if src.schemaless else {}
        )
        mappings = (
            _schemaless_mappings(src, src_name, dst.driver) if src.schemaless else []
        )
        request = TransferRequest(
            source=src.endpoint(src_name),
            destination=dst.endpoint(dst_name),
            sync_mode=sync_mode,
            validation_mode="strict",
            column_types=column_types,
            mappings=mappings,
            stream_contracts=[{
                "name": "track_e",
                "sync_mode": sync_mode,
                "primary_key": "id",
                "selected": True,
            }],
        )
        run_id = uuid.uuid4().hex[:24]
        started = time.perf_counter()
        result = UniversalTransferEngine().execute_tracked(request, run_id)
        elapsed = time.perf_counter() - started

        # Record the engine verdict *before* measuring: when a destination
        # object is missing, the measurement error would otherwise mask the
        # engine error that explains why.
        cell.update({
            "run_id": run_id,
            "engine_success": bool(getattr(result, "success", False)),
            "engine_error": (getattr(result, "error", "") or "")[:400],
            "engine_records_transferred": int(getattr(result, "records_transferred", 0) or 0),
        })
        try:
            dest_count, dest_checksum = dst.measure(dst_name)
        except Exception as exc:
            return {
                **cell,
                "status": "fail",
                "reason": f"destination unmeasurable ({type(exc).__name__}: {exc}); "
                          f"engine_success={cell['engine_success']} "
                          f"engine_error={cell['engine_error']}"[:600],
            }
        cell.update({
            "dest_count": dest_count,
            "dest_checksum": dest_checksum,
            "elapsed_seconds": round(elapsed, 2),
            "rows_per_second": round(dest_count / elapsed, 1) if elapsed > 0 else None,
            "row_accounting": _row_accounting(result),
            "preflight_ran": bool(getattr(result, "validation_plan", None)),
            "dest_object": dst_name,
            "source_object": src_name,
            "declared_column_types": bool(column_types),
        })
        conserved = dest_count == source_count
        checksum_match = dest_checksum == source_checksum
        cell["rows_conserved"] = conserved
        cell["checksum_match"] = checksum_match
        if cell["engine_success"] and conserved and checksum_match:
            cell["status"] = "pass"
        else:
            cell["status"] = "fail"
            cell["reason"] = (
                f"engine_success={cell['engine_success']} "
                f"source={source_count} dest={dest_count} "
                f"checksum_match={checksum_match} err={cell['engine_error']}"
            )
        return cell
    except Exception as exc:  # measured failure, never a silent skip
        return {**cell, "status": "fail", "reason": f"{type(exc).__name__}: {exc}"[:500]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument(
        "--only", default="", help="comma-separated store ids to restrict the matrix to"
    )
    parser.add_argument("--keep", action="store_true", help="keep destination objects for inspection")
    args = parser.parse_args()

    from tests.connector_readiness_stores import STORES

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    cells: list[dict[str, Any]] = []

    for a, b in ROUTES:
        if only and not ({a, b} & only):
            continue
        rows_a = min(args.rows, STORES[a].max_rows or args.rows)
        rows = min(rows_a, STORES[b].max_rows or args.rows)

        forward = run_cell(a, b, rows)
        cells.append(forward)
        print(json.dumps(forward)[:600], flush=True)

        # Reverse direction over the rows the forward run just wrote.
        reverse_seed = forward.get("dest_object") if forward.get("status") == "pass" else None
        if reverse_seed:
            reverse = run_cell(b, a, rows, seeded_name=reverse_seed)
        else:
            reverse = run_cell(b, a, rows)
            if reverse.get("status") == "skip" and forward.get("status") != "pass":
                reverse["reason"] = (
                    f"forward direction did not pass ({forward.get('status')}: "
                    f"{forward.get('reason', '')[:200]}), so no engine-written "
                    f"{b} fixture exists to read back"
                )
        cells.append(reverse)
        print(json.dumps(reverse)[:600], flush=True)

        if not args.keep:
            leftovers = (
                (a, forward.get("source_object")),
                (b, forward.get("dest_object")),
                (b, reverse.get("source_object")),
                (a, reverse.get("dest_object")),
            )
            for key, name in leftovers:
                if name:
                    try:
                        STORES[key].drop(name)
                    except Exception:
                        pass

    # A restricted rerun must not delete the cells it did not run: keep the
    # previous artifact's cells and replace only the routes measured now, so a
    # single-route re-measure after a fix cannot shrink the evidence set.
    if only:
        measured = {(c.get("source"), c.get("destination")) for c in cells}
        try:
            previous = json.loads(ARTIFACT.read_text(encoding="utf-8")).get("cells") or []
        except (OSError, ValueError):
            previous = []
        cells = [
            c
            for c in previous
            if (c.get("source"), c.get("destination")) not in measured
        ] + cells

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": 1,
        "requested_rows_per_cell": args.rows,
        "engine_entrypoint": "src.transfer.engine.UniversalTransferEngine.execute_tracked",
        "pass": sum(1 for c in cells if c["status"] == "pass"),
        "fail": sum(1 for c in cells if c["status"] == "fail"),
        "skip": sum(1 for c in cells if c["status"] == "skip"),
        "cells": cells,
    }
    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("pass", "fail", "skip")}, indent=2))
    print(f"artifact: {ARTIFACT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
