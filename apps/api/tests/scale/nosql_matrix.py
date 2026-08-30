"""Track C matrix runner: live NoSQL / analytical engines at >= 100K rows.

One command, one JSON artifact per run:

    DATAFLOW_SCALE_NOSQL=1 PYTHONPATH=. python -m tests.scale.nosql_matrix \
        --engines mongodb,redis,dynamodb,duckdb --rows 100000 \
        --out /tmp/track_c.json

What a cell proves
------------------
Each cell runs the *same* mode twice through
``src.transfer.engine.UniversalTransferEngine.execute_tracked`` — never a
bypass — and then measures the destination with that store's own driver:

* ``dest_count`` — independent count, compared against
  ``sync_mode_probe.expected_rows_scaled`` for the mode and the destination's
  addressing (a Redis keyspace keyed by id cannot double under append);
* ``dest_checksum`` — additive content checksum over the mapped projection,
  compared against the fixture's own checksum times the number of copies the
  mode is supposed to have landed. A count alone would pass a destination that
  held the right number of *wrong* rows;
* ``temporal`` — how the naive and zoned timestamps actually landed, reported
  separately because an instant-only carrier (BSON date) cannot hold a zoneless
  wall clock and the honest outcome is a named carrier note, not a silent pass;
* the engine's own row accounting (rejected / quarantined / coerced-null /
  skipped) so a cell that landed fewer rows says where they went.

A cell is ``pass`` only when the transfer succeeded, the independent count
equals the expected count, and the independent checksum equals the expected
checksum. Anything else is ``fail`` with the measured numbers, or
``skip (reason)`` when the engine is not available — never a rounded-up green.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
import traceback
import uuid
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from preflight.risk_contract import (
    make_clearing_risk_contract,
    mapping_is_structural_review,
    mapping_requires_risk_contract,
    mapping_review_cleared,
)
from services.data_profiler import source_types_are_authoritative
from services.mapping_pipeline import run_mapping_pipeline
from src.transfer.endpoint_intelligence import introspect_endpoint
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from tests.scale.engines_nosql import ALL_ENGINES, RELATIONAL, TRACK_C, EngineSpec
from tests.scale.nosql_fixture import (
    MAPPED_COLUMNS,
    MASK64,
    row_fingerprint,
    source_types,
    temporal_observation,
)
from tests.sync_mode_probe import expected_rows_scaled, stream_contract

MODES = (
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental_append",
    "upsert",
)
TEMPORAL_SAMPLE = 5


@dataclass
class Cell:
    route: str
    source: str
    destination: str
    mode: str
    rows: int
    status: str = "pending"
    source_count: int | None = None
    dest_count: int | None = None
    expected_count: int | None = None
    source_checksum: int | None = None
    dest_checksum: int | None = None
    expected_checksum: int | None = None
    checksum_match: bool | None = None
    rejected: int = 0
    quarantined: int = 0
    coerced_null: int = 0
    skipped: int = 0
    elapsed_s: float = 0.0
    rows_per_s: float = 0.0
    run_ids: list[str] = field(default_factory=list)
    schema_shape: str = ""
    struct_policy: str = "store_as_json"
    assume_timezone: str | None = None
    key_addressed: bool = False
    signed_reviews: list[str] = field(default_factory=list)
    temporal: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""


def build_mappings(
    *,
    source: Any,
    destination: Any,
    src_name: str,
    dst_name: str,
    assume_timezone: str | None,
    struct_policy: str,
    reviewed: list[str] | None = None,
    src_samples: dict[str, list[Any]] | None = None,
    src_db: str = "",
    dst_db: str = "",
) -> list[dict[str, Any]]:
    """Map through the product's own owners, not through harness-invented DDL.

    ``introspect_endpoint`` supplies the live source (and, when it exists,
    destination) schema and ``services.mapping_pipeline.run_mapping_pipeline``
    — the Map step Transfer Studio calls, over the ``map_columns`` SSOT —
    stamps each pair's ``target_type``. Declaring types in the harness
    instead was measurably wrong: the guessed carrier disagreed with the live
    column (``payload JSON -> TEXT``) and Validate fail-closed on a collapse
    nobody had asked for. ``SOURCE_TYPES`` remains only as the fallback for a
    store whose introspect returns no typed schema.
    """
    src_probe = introspect_endpoint(
        replace(source, extra={**(source.extra or {}), "introspect_purpose": "source"})
    )
    src_live = {str(k): str(v) for k, v in (src_probe.get("schema") or {}).items()}
    src_schema = {
        k: v for k, v in src_live.items() if k in MAPPED_COLUMNS
    } or source_types(src_name)
    # Probe the destination in the destination role, the way Transfer Studio
    # does: same table, different question — the carrier the DDL declares, in
    # the operator's own namespace, not the shape of the rows already there.
    dst_probe = introspect_endpoint(
        replace(
            destination,
            extra={
                **(destination.extra or {}),
                "introspect_purpose": "destination",
            },
        )
    )
    dst_schema = {
        str(k): str(v)
        for k, v in (dst_probe.get("schema") or {}).items()
        if str(k) in MAPPED_COLUMNS
    }
    # Samples are handed to the Map SSOT because the execution pipeline profiles
    # the same rows and *upgrades* an untyped text carrier (a Redis / DynamoDB
    # value is text or a bare Number) to the type the values prove. Mapping
    # without them stamped ``string -> TEXT`` while Validate profiled
    # ``DECIMAL(9,6)`` and then refused its own plan as a collapse. One source
    # carrier, evidenced the same way on both sides.
    source_schemas = [
        {
            "name": col,
            "inferred_type": src_schema.get(col, "VARCHAR"),
            "samples": list((src_samples or {}).get(col) or []),
        }
        for col in MAPPED_COLUMNS
    ]
    target_schemas = (
        [
            {"name": col, "inferred_type": dst_schema[col], "samples": []}
            for col in MAPPED_COLUMNS
            if col in dst_schema
        ]
        if dst_schema
        else None
    )
    # Map through ``run_mapping_pipeline`` — the entry the Transfer Studio Map
    # step calls — not through ``map_columns`` directly. The pipeline is where a
    # non-authoritative source's *values* are profiled and merged into the
    # source carrier (Redis hands every field back as ``string``), and Validate
    # profiles the same rows the same way. Calling the inner mapper skipped that
    # merge, so Map stamped ``string -> TEXT`` while preflight graded a profiled
    # ``DECIMAL(9,6) -> TEXT`` and refused the plan as its own fidelity collapse.
    mapped = (
        run_mapping_pipeline(
            list(MAPPED_COLUMNS),
            list(dst_schema) if dst_schema else list(MAPPED_COLUMNS),
            source_schemas=source_schemas,
            target_schemas=target_schemas,
            use_llm=False,
            source_samples={
                col: [r for r in ((src_samples or {}).get(col) or [])]
                for col in MAPPED_COLUMNS
            },
            destination_db_type=dst_db or dst_name,
            destination_table_exists=bool(dst_schema),
            source_db_type=src_db or src_name,
            source_types_authoritative=source_types_are_authoritative(
                src_db or src_name, src_db or src_name
            ),
        ).get("mappings")
        or []
    )
    by_source = {str(m.get("source") or m.get("source_column")): m for m in mapped}
    out: list[dict[str, Any]] = []
    # A document source introspects wider than the relational projection: Mongo
    # adds its generated ``_id`` and one flattened column per nested key. G13
    # refuses to write while those are unaccounted, which is the correct gate —
    # so the harness records the operator decision (``intentional_omit``) for
    # every live source column outside the checksummed projection instead of
    # letting them disappear.
    for col in sorted(src_live):
        if col in MAPPED_COLUMNS:
            continue
        out.append(
            {
                "source": col,
                "target": "",
                "intentional_omit": True,
                "reasoning": (
                    "Track C projection is the 8 fixture columns; this column is "
                    "a store-generated key or a flattened view of `payload`, "
                    "which is carried whole."
                ),
            }
        )
    for col in MAPPED_COLUMNS:
        m = dict(by_source.get(col) or {"source": col, "target": col})
        m.setdefault("source", col)
        m.setdefault("target", col)
        if assume_timezone and col == "ts_naive":
            m["transform"] = f"assume_timezone:{assume_timezone}"
        if struct_policy != "store_as_json" and col == "payload":
            m["struct_policy"] = struct_policy
        # A mapping the Map SSOT flags for review (naive wall clock into an
        # instant-only BSON date, decimal into a store with no decimal carrier,
        # a declared structural flatten) is *not* silently executed: the
        # product demands a signed continue-policy Migration Risk Contract, so
        # the harness signs one — the same control an operator uses — and names
        # the column in the evidence. The contract only clears the review; it
        # does not relax the writer, and the independent checksum still judges
        # whether the value actually landed intact.
        # Ask the product's own clearance owner, not a Map-time hint field: a
        # review demand can also be raised by the gate (a declared structural
        # flatten, an invented decimal scale) after Map has returned.
        needs_contract = mapping_requires_risk_contract(m) or mapping_is_structural_review(m)
        if (
            m.get("requires_review") or needs_contract
        ) and not mapping_review_cleared(m):
            m["risk_contract"] = make_clearing_risk_contract(
                column=str(m.get("target") or col),
                source_type=str(m.get("source_type") or ""),
                destination_type=str(m.get("target_type") or ""),
                approved_by="track-c-scale-harness",
                reason=(
                    f"Track C scale matrix {src_name}->{dst_name}: declared "
                    f"carrier decision for {col}"
                ),
                execution_policy="CAST_AND_CONTINUE",
            )
            # An *ambiguous* (non-lossy) mapping clears with an explicit
            # operator confirmation instead of a contract — sign both so the
            # harness never depends on which branch the gate takes.
            m["operator_override"] = True
            m["user_override"] = True
            if reviewed is not None:
                reviewed.append(f"{col}:{m.get('source_type')}->{m.get('target_type')}")
        out.append(m)
    return out


def _measure(spec: EngineSpec, table: str) -> tuple[int, int, list[dict[str, Any]]]:
    """Independent count + content checksum + a temporal sample."""
    count = 0
    total = 0
    temporal: list[dict[str, Any]] = []

    for row in spec.read_projection(table):
        count += 1
        total = (
            total
            + row_fingerprint(
                id_value=row["id"],
                uid=row["uid"],
                big_int=row["big_int"],
                amount=row["amount"],
                unicode_key=row["unicode_key"],
                payload=row["payload"],
            )
        ) & MASK64
        if len(temporal) < TEMPORAL_SAMPLE:
            try:
                temporal.append(
                    temporal_observation(
                        int(row["id"]), row.get("ts_naive"), row.get("ts_zoned")
                    )
                )
            except Exception as exc:  # noqa: BLE001 — a bad instant is a result
                temporal.append({"error": f"{type(exc).__name__}: {exc}"[:200]})
    return count, total, temporal


def _sample_rows(spec: EngineSpec, table: str, limit: int = 8) -> list[dict[str, Any]]:
    """A few real rows from the store's own driver, for the Map step's evidence."""
    out: list[dict[str, Any]] = []
    for row in spec.read_projection(table):
        out.append(row)
        if len(out) >= limit:
            break
    return out


def _accounting(result: Any) -> dict[str, int]:
    acct = result.row_accounting or {}
    summary = result.destination_summary or {}
    return {
        "rejected": int(summary.get("rejected_rows") or 0),
        "quarantined": int(acct.get("rows_quarantined") or 0),
        "coerced_null": int(acct.get("rows_coerced_null") or 0),
        "skipped": int(acct.get("rows_skipped") or 0),
    }


def run_cell(
    *,
    src_spec: EngineSpec,
    dst_spec: EngineSpec,
    mode: str,
    rows: int,
    suffix: str,
    struct_policy: str = "store_as_json",
) -> Cell:
    """Seed the source, run ``mode`` twice, then measure the destination."""
    route = f"{src_spec.name}->{dst_spec.name}"
    cell = Cell(
        route=route,
        source=src_spec.name,
        destination=dst_spec.name,
        mode=mode,
        rows=rows,
        struct_policy=struct_policy,
        assume_timezone=dst_spec.assume_timezone,
        key_addressed=dst_spec.key_addressed,
    )
    src_table = f"scale_src_{src_spec.name[:6]}_{suffix}"
    dst_table = f"scale_dst_{dst_spec.name[:6]}_{suffix}"

    ok, reason = src_spec.availability()
    if not ok:
        cell.status = "skip"
        cell.error = f"source {src_spec.name} unavailable: {reason}"
        return cell
    ok, reason = dst_spec.availability()
    if not ok:
        cell.status = "skip"
        cell.error = f"destination {dst_spec.name} unavailable: {reason}"
        return cell
    if src_spec.seed is None:
        cell.status = "skip"
        cell.error = (
            f"{src_spec.name} has no independent seeding driver in this harness, "
            "so it cannot be proven as a source"
        )
        return cell

    # ``full_refresh_append`` into a row-addressed table means "land this
    # population *as well as* what is already there". Re-offering the identical
    # population cannot mean that on a destination that carries the source's
    # PRIMARY KEY (create-new carries it, correctly), so the second run of an
    # append cell offers a *disjoint* population and the cell asserts N+N rows
    # and the sum of both checksums. A key-addressed store keeps the identical
    # population, because "append lands N not 2N" is exactly what has to be
    # proven there.
    append_disjoint = mode == "full_refresh_append" and not dst_spec.key_addressed
    started = time.perf_counter()
    try:
        src_spec.seed(src_table, rows)
        first_sum = _measure(src_spec, src_table)[1] if append_disjoint else 0
        src_rows = _sample_rows(src_spec, src_table)
        if dst_spec.drop is not None:
            dst_spec.drop(dst_table)
        source = src_spec.endpoint(src_table)
        destination = dst_spec.endpoint(dst_table)
        maps = build_mappings(
            source=source,
            destination=destination,
            src_name=src_spec.name,
            dst_name=dst_spec.name,
            assume_timezone=dst_spec.assume_timezone,
            struct_policy=struct_policy,
            reviewed=cell.signed_reviews,
            src_samples={
                col: [r.get(col) for r in src_rows]
                for col in MAPPED_COLUMNS
            },
            src_db=src_spec.db_type,
            dst_db=dst_spec.db_type,
        )
        engine = UniversalTransferEngine()
        for _run in (1, 2):
            # Each run submits its own mapping payload, exactly as the API does
            # per request: the pipeline hydrates and re-binds the list it is
            # handed, so re-submitting a mutated list made run 2 fail DDL
            # identity against run 1's rebound stamps.
            run_maps = copy.deepcopy(maps)
            if _run == 2 and append_disjoint:
                src_spec.seed(src_table, rows, start=rows + 1)
            request = TransferRequest(
                source=source,
                destination=destination,
                sync_mode=mode,
                skip_preflight=False,
                validation_mode="balanced",
                stream_contracts=[stream_contract(src_table, mode)],
                mappings=run_maps,
            )
            run_id = uuid.uuid4().hex[:24]
            cell.run_ids.append(run_id)
            result = engine.execute_tracked(request, run_id)
            acct = _accounting(result)
            for key, value in acct.items():
                setattr(cell, key, getattr(cell, key) + value)
            if not result.success:
                cell.status = "fail"
                cell.error = str(result.error or "")[:600]
                cell.elapsed_s = round(time.perf_counter() - started, 2)
                return cell
            cell.schema_shape = str(
                (result.destination_summary or {}).get("shape")
                or (result.destination_summary or {}).get("dest_exists_shape")
                or cell.schema_shape
                or ("create_new" if _run == 1 else "dest_exists")
            )
        cell.elapsed_s = round(time.perf_counter() - started, 2)
        cell.rows_per_s = round((rows * 2) / cell.elapsed_s, 1) if cell.elapsed_s else 0

        src_count, src_sum, _ = _measure(src_spec, src_table)
        cell.source_count = src_count
        cell.source_checksum = src_sum
        dst_count, dst_sum, temporal = _measure(dst_spec, dst_table)
        cell.dest_count = dst_count
        cell.dest_checksum = dst_sum
        cell.temporal = temporal

        expected = expected_rows_scaled(
            mode, rows=rows, key_addressed=dst_spec.key_addressed
        )
        cell.expected_count = expected
        if append_disjoint:
            # Two distinct populations landed, so the expected content is the
            # sum of the two independently measured checksums.
            cell.expected_checksum = (first_sum + src_sum) & MASK64
        else:
            copies = 2 if expected == rows * 2 else 1
            cell.expected_checksum = (src_sum * copies) & MASK64
        cell.checksum_match = cell.dest_checksum == cell.expected_checksum
        if struct_policy == "flatten_top_level_keys":
            # The flattened projection is not the JSON document, so the
            # document checksum cannot apply; the cell proves count + no
            # silent drop, and says so instead of comparing incomparables.
            cell.checksum_match = None
            cell.status = "pass" if dst_count == expected else "fail"
            if cell.status == "fail":
                cell.error = (
                    f"count {dst_count} != expected {expected} under declared "
                    "flatten_top_level_keys"
                )
        elif dst_count == expected and cell.checksum_match:
            cell.status = "pass"
        else:
            cell.status = "fail"
            cell.error = (
                f"independent count {dst_count} vs expected {expected}; "
                f"checksum {cell.dest_checksum} vs expected {cell.expected_checksum}"
            )
    except Exception as exc:  # noqa: BLE001 — an exception is a measured failure
        cell.status = "fail"
        cell.error = f"{type(exc).__name__}: {exc}"[:600]
        cell.elapsed_s = round(time.perf_counter() - started, 2)
        traceback.print_exc()
    finally:
        try:
            if src_spec.drop is not None:
                src_spec.drop(src_table)
            if dst_spec.drop is not None:
                dst_spec.drop(dst_table)
        except Exception:  # noqa: BLE001 — cleanup must not mask a result
            pass
    return cell


def run_round_trip(
    *, doc_spec: EngineSpec, rows: int, suffix: str
) -> Cell:
    """relational -> document -> relational, checksummed at both relational ends.

    This is the fidelity question a single hop cannot answer: the document
    store may accept everything and still hand back a decimal as a double or
    drop a nested key. Both ends are read with ``psycopg2``, so the comparison
    is between two independent measurements of the same population.
    """
    pg = RELATIONAL["postgresql"]
    cell = Cell(
        route=f"postgresql->{doc_spec.name}->postgresql",
        source="postgresql",
        destination="postgresql",
        mode="full_refresh_overwrite",
        rows=rows,
        assume_timezone=doc_spec.assume_timezone,
        key_addressed=doc_spec.key_addressed,
        schema_shape="round_trip",
    )
    ok, reason = doc_spec.availability()
    if not ok:
        cell.status = "skip"
        cell.error = f"{doc_spec.name} unavailable: {reason}"
        return cell
    src_table = f"scale_rt_src_{suffix}"
    mid_table = f"scale_rt_mid_{suffix}"
    back_table = f"scale_rt_back_{suffix}"
    started = time.perf_counter()
    try:
        pg.seed(src_table, rows)
        if doc_spec.drop:
            doc_spec.drop(mid_table)
        pg.drop(back_table)
        engine = UniversalTransferEngine()
        legs = [
            (
                pg.endpoint(src_table),
                doc_spec.endpoint(mid_table),
                src_table,
                "postgresql",
                doc_spec.name,
                doc_spec.assume_timezone,
                "postgresql",
                doc_spec.db_type,
            ),
            (
                doc_spec.endpoint(mid_table),
                pg.endpoint(back_table),
                mid_table,
                doc_spec.name,
                "postgresql",
                None,
                doc_spec.db_type,
                "postgresql",
            ),
        ]
        for (
            source,
            destination,
            stream,
            src_name,
            dst_name,
            atz,
            src_db,
            dst_db,
        ) in legs:
            maps = build_mappings(
                source=source,
                destination=destination,
                src_name=src_name,
                dst_name=dst_name,
                src_db=src_db,
                dst_db=dst_db,
                assume_timezone=atz,
                struct_policy="store_as_json",
                reviewed=cell.signed_reviews,
            )
            run_id = uuid.uuid4().hex[:24]
            cell.run_ids.append(run_id)
            result = engine.execute_tracked(
                TransferRequest(
                    source=source,
                    destination=destination,
                    sync_mode="full_refresh_overwrite",
                    skip_preflight=False,
                    validation_mode="balanced",
                    stream_contracts=[
                        stream_contract(stream, "full_refresh_overwrite")
                    ],
                    mappings=copy.deepcopy(maps),
                ),
                run_id,
            )
            acct = _accounting(result)
            for key, value in acct.items():
                setattr(cell, key, getattr(cell, key) + value)
            if not result.success:
                cell.status = "fail"
                cell.error = str(result.error or "")[:600]
                return cell
        cell.elapsed_s = round(time.perf_counter() - started, 2)
        cell.rows_per_s = round((rows * 2) / cell.elapsed_s, 1) if cell.elapsed_s else 0
        src_count, src_sum, _ = _measure(pg, src_table)
        back_count, back_sum, temporal = _measure(pg, back_table)
        cell.source_count = src_count
        cell.source_checksum = src_sum
        cell.dest_count = back_count
        cell.dest_checksum = back_sum
        cell.expected_count = src_count
        cell.expected_checksum = src_sum
        cell.checksum_match = src_sum == back_sum
        cell.temporal = temporal
        cell.status = (
            "pass" if src_count == back_count and cell.checksum_match else "fail"
        )
        if cell.status == "fail":
            cell.error = (
                f"round trip counts {src_count} -> {back_count}; checksums "
                f"{src_sum} vs {back_sum}"
            )
    except Exception as exc:  # noqa: BLE001
        cell.status = "fail"
        cell.error = f"{type(exc).__name__}: {exc}"[:600]
        traceback.print_exc()
    finally:
        for table in (src_table, back_table):
            try:
                pg.drop(table)
            except Exception:  # noqa: BLE001
                pass
        try:
            if doc_spec.drop:
                doc_spec.drop(mid_table)
        except Exception:  # noqa: BLE001
            pass
    return cell


def routes_for(engine: str) -> list[tuple[str, str]]:
    """Both directions against both relational partners, plus the same-engine hop."""
    spec = TRACK_C[engine]
    pairs: list[tuple[str, str]] = []
    for partner in ("postgresql", "mysql"):
        if spec.dest_role:
            pairs.append((partner, engine))
        if spec.source_role and spec.seed is not None:
            pairs.append((engine, partner))
    if spec.source_role and spec.dest_role and spec.seed is not None:
        pairs.append((engine, engine))
    return pairs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Track C scale matrix")
    parser.add_argument(
        "--engines",
        default=",".join(TRACK_C),
        help="comma-separated Track C engines to run",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=int(os.environ.get("DATAFLOW_SCALE_ROWS", "100000")),
    )
    parser.add_argument("--modes", default=",".join(MODES))
    parser.add_argument("--out", default="")
    parser.add_argument(
        "--skip-round-trip", action="store_true", help="omit the relational round trip"
    )
    parser.add_argument(
        "--flatten",
        action="store_true",
        help="also run one declared flatten_top_level_keys cell per engine",
    )
    args = parser.parse_args(argv)

    if os.environ.get("DATAFLOW_SCALE_NOSQL") != "1":
        print("DATAFLOW_SCALE_NOSQL != 1 — refusing to run (env-gated)")
        return 2

    rows = args.rows
    suffix = uuid.uuid4().hex[:6]
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    cells: list[Cell] = []

    for engine in engines:
        if engine not in TRACK_C:
            print(f"unknown engine {engine}", file=sys.stderr)
            return 2
        spec = TRACK_C[engine]
        available, reason = spec.availability()
        for src_name, dst_name in routes_for(engine):
            for mode in modes:
                if not available:
                    cells.append(
                        Cell(
                            route=f"{src_name}->{dst_name}",
                            source=src_name,
                            destination=dst_name,
                            mode=mode,
                            rows=rows,
                            status="skip",
                            error=f"{engine} unavailable: {reason}",
                            key_addressed=spec.key_addressed,
                        )
                    )
                    continue
                cell = run_cell(
                    src_spec=ALL_ENGINES[src_name],
                    dst_spec=ALL_ENGINES[dst_name],
                    mode=mode,
                    rows=rows,
                    suffix=f"{suffix}{len(cells):02d}",
                )
                cells.append(cell)
                print(json.dumps(asdict(cell), default=str))
                sys.stdout.flush()
        if args.flatten and available and spec.dest_role:
            cell = run_cell(
                src_spec=RELATIONAL["postgresql"],
                dst_spec=spec,
                mode="full_refresh_overwrite",
                rows=rows,
                suffix=f"{suffix}f{len(cells):02d}",
                struct_policy="flatten_top_level_keys",
            )
            cells.append(cell)
            print(json.dumps(asdict(cell), default=str))
            sys.stdout.flush()
        if not args.skip_round_trip and spec.source_role and spec.seed is not None:
            cell = run_round_trip(
                doc_spec=spec, rows=rows, suffix=f"{suffix}r{len(cells):02d}"
            )
            cells.append(cell)
            print(json.dumps(asdict(cell), default=str))
            sys.stdout.flush()

    summary = {
        "rows": rows,
        "pass": sum(1 for c in cells if c.status == "pass"),
        "fail": sum(1 for c in cells if c.status == "fail"),
        "skip": sum(1 for c in cells if c.status == "skip"),
        "cells": [asdict(c) for c in cells],
    }
    text = json.dumps(summary, default=str, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"wrote {args.out}")
    print(
        f"pass={summary['pass']} fail={summary['fail']} skip={summary['skip']}"
    )
    return 0 if summary["fail"] == 0 else 1


if __name__ == "__main__":  # pragma: no cover — CLI entry
    raise SystemExit(main())
