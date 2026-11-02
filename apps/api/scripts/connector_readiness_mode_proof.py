#!/usr/bin/env python3
"""Track E — what each sync mode actually does on the second run.

A transfer that returns ``success=True`` proves the writer ran, not the mode.
A mode *is* its second-run behaviour, and only the destination row count after
two identical runs separates them (``tests/sync_mode_probe`` owns that table):

* ``full_refresh_overwrite`` replaces — N rows stay N;
* ``full_refresh_append`` accumulates — the second run delivers a disjoint id
  range (a real feed's shape), so N rows become 2N on every sink, key-addressed
  or not;
* ``incremental_append`` reads past the watermark — N stays N;
* ``upsert`` is keyed and idempotent — N stays N.

Each cell seeds the source with the store's own driver, runs
``UniversalTransferEngine.execute_tracked`` twice against the *same*
destination object (never dropping it in between), and counts the destination
through an independent connection. A mode that silently appends under ``upsert``
doubles the customer's data; one that appends under ``incremental`` re-reads the
whole source every tick. Both are recorded as ``fail`` with the measured counts.

    cd apps/api && python scripts/connector_readiness_mode_proof.py --rows 10000

Env-gated: an unreachable store is ``skip`` with the exact reason. Modes the
harness cannot express honestly (``cdc``, ``scd2``, ``mirror``) are recorded as
``skip`` naming what they would need, never as a pass.

Writes ``data/proofs/connector_readiness_modes.json``.
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

from scripts.connector_readiness_live_proof import (  # noqa: E402
    _declared_source_types,
    _object_key,
    _row_accounting,
    _schemaless_mappings,
)

ARTIFACT = _API_ROOT / "data" / "proofs" / "connector_readiness_modes.json"

#: Modes whose second-run semantics this harness measures.
MEASURED_MODES: tuple[str, ...] = (
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental_append",
    "upsert",
)

#: Modes that need evidence this harness cannot produce, with the reason that
#: goes in the matrix verbatim. Silence here would read as "not supported"; a
#: pass would be an invention.
UNMEASURED_MODES: dict[str, str] = {
    "cdc": (
        "needs a running log-based capture (Postgres logical slot / MySQL binlog "
        "reader / SQL Server CT) plus a mutation workload; the two-run row-count "
        "probe cannot distinguish at-least-once redelivery from a fresh read"
    ),
    "scd2": (
        "needs a versioned dimension fixture (effective_from/effective_to plus a "
        "changed attribute run) — row count alone cannot prove history retention"
    ),
    "mirror": (
        "needs source deletes replicated as dest-owned lattice ``_deleted`` rows; "
        "the fixture is insert-only so a mirror pass would be unproven"
    ),
}

#: Destinations addressed by key rather than by row (see ``expected_rows``).
KEY_ADDRESSED = frozenset({"redis", "dynamodb", "elasticsearch"})

#: Routes to probe. Kept to stores this repo can start locally, one per writer
#: family so a mode defect in a shared writer path cannot hide behind a single
#: dialect.
ROUTES: tuple[tuple[str, str], ...] = (
    ("postgresql", "mysql"),
    ("postgresql", "sqlserver"),
    ("postgresql", "sqlite"),
    ("postgresql", "duckdb"),
    ("postgresql", "mongodb"),
    ("postgresql", "redis"),
    ("postgresql", "s3"),
    ("postgresql", "dynamodb"),
    ("postgresql", "elasticsearch"),
    ("mysql", "postgresql"),
)


def _expected(mode: str, rows: int, *, key_addressed: bool) -> int:
    if mode == "full_refresh_append":
        # The second batch carries ids the destination does not hold, so a
        # key-addressed sink accumulates exactly like a row-addressed one.
        return rows * 2
    return rows


def run_mode_cell(src_key: str, dst_key: str, mode: str, rows: int) -> dict[str, Any]:
    """Two runs of one mode, measured from an independent connection."""
    from tests.connector_readiness_stores import STORES
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    src, dst = STORES[src_key], STORES[dst_key]
    key_addressed = dst.driver in KEY_ADDRESSED
    cell: dict[str, Any] = {
        "source": src_key,
        "destination": dst_key,
        "sync_mode": mode,
        "requested_rows": rows,
        "key_addressed_destination": key_addressed,
        "expected_rows_after_two_runs": _expected(
            mode, rows, key_addressed=key_addressed
        ),
    }

    for store, key in ((src, src_key), (dst, dst_key)):
        ok, reason = store.available()
        if not ok:
            return {**cell, "status": "skip", "reason": reason}
    if not src.can_source:
        return {**cell, "status": "skip", "reason": f"{src_key} has no reader"}
    if not dst.can_dest:
        return {**cell, "status": "skip", "reason": f"{dst_key} has no writer"}
    if src.seed is None:
        return {
            **cell,
            "status": "skip",
            "reason": f"{src_key} cannot be seeded by its own driver in this harness",
        }

    stem = f"trk_e_mode_{src_key}_{dst_key}_{uuid.uuid4().hex[:6]}"
    src_name = _object_key(src, f"{stem}_s")
    dst_name = _object_key(dst, f"{stem}_d")

    try:
        src.seed(src_name, rows)
        source_count, _ = src.measure(src_name)
        cell["source_count"] = source_count

        dst.drop(dst_name)
        column_types = _declared_source_types(src, src_name) if src.schemaless else {}
        mappings = (
            _schemaless_mappings(src, src_name, dst.driver) if src.schemaless else []
        )
        contract: dict[str, Any] = {
            "name": "track_e",
            "sync_mode": mode,
            "selected": True,
        }
        if mode != "full_refresh_append":
            # Append is the one mode with no row identity: declaring a key would
            # make the second run of the *same* batch a key collision, which the
            # engine correctly refuses — that refusal measures the gate, not the
            # mode. Every other mode is keyed on ``id``.
            contract["primary_key"] = "id"
        if mode.startswith("incremental"):
            # Without a cursor the engine has no watermark to seek past and
            # "incremental" degrades into a full re-read. The meaning of the
            # cursor is the operator's declaration, and for this fixture it is
            # the truth: ``id`` is a generated sequence assigned on insert.
            contract["cursor_field"] = "id"
            contract["cursor_semantics"] = "monotonic_sequence"

        run_ids: list[str] = []
        after: list[int] = []
        transferred: list[int] = []
        started = time.perf_counter()
        for attempt in (1, 2):
            if attempt == 2 and mode == "full_refresh_append":
                # Append delivers rows the destination does not hold yet. A
                # second identical batch would collide on the key the CREATE
                # mirrored from the source, and the engine's refusal would be
                # measuring that gate rather than the mode, so the second batch
                # is a disjoint id range — the shape a real feed has.
                src.seed(src_name, rows, start=rows + 1)
            request = TransferRequest(
                source=src.endpoint(src_name),
                destination=dst.endpoint(dst_name),
                sync_mode=mode,
                validation_mode="strict",
                column_types=column_types,
                mappings=mappings,
                stream_contracts=[dict(contract)],
            )
            run_id = uuid.uuid4().hex[:24]
            run_ids.append(run_id)
            result = UniversalTransferEngine().execute_tracked(request, run_id)
            cell["engine_success"] = bool(getattr(result, "success", False))
            cell["engine_error"] = (getattr(result, "error", "") or "")[:400]
            cell["row_accounting"] = _row_accounting(result)
            transferred.append(int(getattr(result, "records_transferred", 0) or 0))
            if not cell["engine_success"]:
                break
            landed, _ = dst.measure(dst_name)
            after.append(int(landed))
        elapsed = time.perf_counter() - started

        cell.update({
            "run_ids": run_ids,
            "records_transferred": transferred,
            "dest_rows_after_run": after,
            "elapsed_seconds": round(elapsed, 2),
            "source_object": src_name,
            "dest_object": dst_name,
        })
        if not cell.get("engine_success"):
            cell["status"] = "fail"
            cell["reason"] = (
                f"run {len(run_ids)} of 2 failed: {cell.get('engine_error', '')}"
            )
            return cell
        landed_final = after[-1]
        cell["status"] = (
            "pass" if landed_final == cell["expected_rows_after_two_runs"] else "fail"
        )
        if cell["status"] == "fail":
            cell["reason"] = (
                f"after two {mode} runs the destination holds {landed_final} rows, "
                f"expected {cell['expected_rows_after_two_runs']} "
                f"(source {source_count}, key-addressed={key_addressed})"
            )
        return cell
    except Exception as exc:  # a measured failure, never a silent skip
        return {**cell, "status": "fail", "reason": f"{type(exc).__name__}: {exc}"[:500]}
    finally:
        try:
            src.drop(src_name)
            dst.drop(dst_name)
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument(
        "--only", default="", help="comma-separated store ids to restrict the matrix to"
    )
    parser.add_argument(
        "--modes", default="", help="comma-separated sync modes (default: all measured)"
    )
    args = parser.parse_args()

    from tests.connector_readiness_stores import STORES

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    modes = tuple(m.strip() for m in args.modes.split(",") if m.strip()) or MEASURED_MODES
    cells: list[dict[str, Any]] = []

    for a, b in ROUTES:
        if only and not ({a, b} & only):
            continue
        rows = min(
            args.rows,
            STORES[a].max_rows or args.rows,
            STORES[b].max_rows or args.rows,
        )
        for mode in modes:
            cell = run_mode_cell(a, b, mode, rows)
            cells.append(cell)
            print(json.dumps(cell)[:600], flush=True)
        for mode, reason in UNMEASURED_MODES.items():
            cells.append({
                "source": a,
                "destination": b,
                "sync_mode": mode,
                "requested_rows": rows,
                "status": "skip",
                "reason": reason,
            })

    if (only or args.modes) and ARTIFACT.exists():
        # A restricted rerun must not erase the cells it did not touch: the
        # matrix doc cites this artifact, and a shrunken file would read as
        # "those routes were never measured".
        prior = json.loads(ARTIFACT.read_text(encoding="utf-8")).get("cells", [])
        fresh = {(c["source"], c["destination"], c["sync_mode"]) for c in cells}
        cells = [
            c for c in prior
            if (c["source"], c["destination"], c["sync_mode"]) not in fresh
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
