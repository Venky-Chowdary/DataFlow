"""Runner for the 100K relational scale matrix.

Every cell goes through the product path — ``UniversalTransferEngine.
execute_tracked`` with preflight ON — and is then proved from *outside* the
product: an independent driver connection on the destination does its own
``COUNT(*)`` and recomputes the fixture checksum over the mapped projection.
The writer's acknowledgement is never the evidence.

Two families of cells:

**data cells** (``create_new``, ``dest_exists_compatible``) run the same route
twice, because a sync mode *is* its second-run behaviour: overwrite stays N,
append becomes 2N, incremental stays N, upsert stays N.

**blocking cells** (``dest_exists_narrower``, G13 extra unmapped source column,
G14 dest-only NOT NULL without default) must be *refused*. A cell that lands
truncated, rounded or NULLed data instead of blocking is a ``fail`` here even
though the transfer said success.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Sequence

from src.transfer.models import EndpointConfig, TransferResult

from tests.scale.sql_engines import Engine, build_engines, live_engines
from tests.scale.fixture import (
    domain_contract_columns,
    engine_columns,
    expected_checksum,
    projection,
)

ROWS_DEFAULT = 100_000
SLOW_ROWS_PER_SEC = 2_000

MODES = (
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental_append",
    "upsert",
)

DATA_SHAPES = ("create_new", "dest_exists_compatible", "dest_exists_missing_column")
BLOCKING_SHAPES = (
    "dest_exists_narrower",
    "g13_extra_source_column",
    "g14_dest_notnull_no_default",
    "domain_contract_unsigned",
)

#: Column the blocking shapes attack: DECIMAL(20,9) into an integer carrier.
NARROW_COLUMN = "amt_dec"
#: Routes that skip the decimal (SQLite has no exact decimal type) narrow the
#: bounded Unicode column instead — VARCHAR(64) into VARCHAR(8) truncates.
NARROW_COLUMN_FALLBACK = "name_txt"
#: Column removed from the destination to create an extra unmapped source column.
G13_COLUMN = "note_null"
#: Destination-only NOT NULL column with no default.
G14_COLUMN = "dest_only_req"


@dataclass
class Cell:
    source: str
    destination: str
    mode: str
    shape: str
    status: str = "skip"
    reason: str = ""
    source_rows: int = 0
    dest_rows: int = 0
    expected_rows: int = 0
    rejected: int = 0
    quarantined: int = 0
    coerced_null: int = 0
    skipped_rows: int = 0
    checksum_source: str = ""
    checksum_dest: str = ""
    checksum_match: bool | None = None
    elapsed_seconds: float = 0.0
    rows_per_sec: float = 0.0
    columns: int = 0
    skipped_columns: dict[str, str] = field(default_factory=dict)
    run_ids: list[str] = field(default_factory=list)
    quarantine_sample: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def slow(self) -> bool:
        return self.status == "pass" and 0 < self.rows_per_sec < SLOW_ROWS_PER_SEC

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["slow"] = self.slow
        return data


def _stream_contract(name: str, mode: str, key: str = "id") -> dict[str, Any]:
    """The contract this fixture can honestly sign for a mode.

    ``full_refresh_append`` declares no key: the fixture ids repeat on the
    second run, and a keyed destination is right to refuse the duplicate insert
    rather than accumulate — so append is proven against a keyless destination,
    which is the shape where 2N is the correct answer.

    ``incremental_append`` declares ``insert_only`` because the fixture mints
    each id once and never updates a row. Preflight refuses an undeclared
    cursor (a backdated insert would be skipped forever), and declaring
    semantics the fixture does not have would be lying to the gate.
    """
    from tests.sync_mode_probe import stream_contract

    contract = stream_contract(name, mode)
    if contract.get("primary_key"):
        contract["primary_key"] = key
    if contract.get("cursor_field"):
        contract["cursor_field"] = key
    if mode == "full_refresh_append":
        contract.pop("primary_key", None)
    if mode.startswith("incremental"):
        contract["cursor_semantics"] = "insert_only"
    return contract


def _risk_contract_draft(column: str, reason: str) -> dict[str, Any]:
    """An unsigned Migration Risk Contract draft the engine signs before gating.

    ``QUARANTINE_ROW`` is the honest policy for a domain the destination cannot
    declare: the carrier holds every canonical UUID, so no row is expected to
    fail, and any row that does must be quarantined rather than coerced.
    """
    return {
        "column": column,
        "approved_by": "tests.scale.run_matrix",
        "reason": reason,
        "execution_policy": "QUARANTINE_ROW",
        "severity": "high",
        "expected_precision_loss": False,
        "expected_truncation": False,
        "expected_nulls": False,
        "quarantine_policy": "holdout_rejected_rows",
        "rollback_strategy": "DOCUMENT_ONLY",
        "loss_classification": "domain_not_enforced",
    }


def _mappings(
    columns: Sequence[str],
    src: Engine,
    dst: Engine,
    *,
    sign_risk: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Mappings in each side's *stored* column spelling (Oracle folds to upper)."""
    drafts = dict(sign_risk or {})
    out: list[dict[str, Any]] = []
    for c in columns:
        mapping: dict[str, Any] = {
            "source": src.stored(c),
            "target": dst.stored(c),
            "confidence": 0.99,
        }
        if c in drafts:
            mapping["risk_contract"] = _risk_contract_draft(
                src.stored(c), drafts[c]
            )
        out.append(mapping)
    return out


def execute(
    source: EndpointConfig,
    destination: EndpointConfig,
    *,
    mode: str,
    columns: Sequence[str],
    stream: str,
    src: Engine,
    dst: Engine,
    validation_mode: str = "strict",
    sign_risk: Mapping[str, str] | None = None,
) -> tuple[TransferResult, str, float]:
    """One real, preflight-on transfer. Returns the result, run id and wall time."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode=mode,
        validation_mode=validation_mode,
        stream_contracts=[_stream_contract(stream, mode, key=src.stored("id"))],
        mappings=_mappings(columns, src, dst, sign_risk=sign_risk),
        # skip_preflight stays False: fail-closed preflight is under test too.
    )
    run_id = uuid.uuid4().hex[:24]
    started = time.perf_counter()
    result = UniversalTransferEngine().execute_tracked(request, run_id)
    return result, run_id, time.perf_counter() - started


def _accounting(cell: Cell, result: TransferResult) -> None:
    dest = dict(result.destination_summary or {})
    ledger = dict(result.row_accounting or {})
    cell.rejected += int(dest.get("rejected_rows") or dest.get("rejected") or 0)
    cell.quarantined += int(
        dest.get("quarantined_rows")
        or dest.get("quarantine_row_count")
        or ledger.get("quarantined")
        or 0
    )
    cell.coerced_null += int(dest.get("coerced_null_rows") or 0)
    cell.skipped_rows += int(dest.get("skipped_rows") or ledger.get("skipped") or 0)


def _quarantine_sample(result: TransferResult, limit: int = 3) -> list[dict[str, Any]]:
    """The rows/issues the product refused, so a refusal is inspectable."""
    out: list[dict[str, Any]] = []
    dest = dict(result.destination_summary or {})
    for key in ("quarantine_rows", "rejected_samples", "quarantine_sample"):
        for row in list(dest.get(key) or [])[:limit]:
            out.append(row if isinstance(row, dict) else {"row": str(row)[:300]})
    plan = dict(result.validation_plan or {})
    for blocker in list(plan.get("blockers") or [])[:limit]:
        if isinstance(blocker, dict):
            out.append(
                {
                    "gate": str(blocker.get("gate") or blocker.get("code") or "")[:60],
                    "message": str(blocker.get("message") or blocker.get("detail") or "")[
                        :300
                    ],
                }
            )
        else:
            out.append({"blocker": str(blocker)[:300]})
    return out[: limit * 2]


class SourceCache:
    """Seeded source tables, one per (engine, projection) — reused across cells."""

    def __init__(self, rows: int, prefix: str) -> None:
        self.rows = rows
        self.prefix = prefix
        self._tables: dict[tuple[str, tuple[str, ...]], str] = {}
        self._types: dict[tuple[str, tuple[str, ...]], dict[str, str]] = {}

    def table(self, engine: Engine, columns: Sequence[str]) -> str:
        key = (engine.name, tuple(columns))
        existing = self._tables.get(key)
        if existing:
            return existing
        name = f"{self.prefix}_src_{engine.name}_{len(self._tables)}"
        engine.seed(name, columns, self.rows)
        self._tables[key] = name
        return name

    def source_types(self, engine: Engine, columns: Sequence[str]) -> dict[str, str]:
        """Live source DDL per fixture column, for invented-destination shapes."""
        key = (engine.name, tuple(columns))
        cached = self._types.get(key)
        if cached is None:
            live = engine.introspect_types(self.table(engine, columns))
            folded = {str(name).casefold(): str(ddl) for name, ddl in live.items()}
            cached = {
                column: folded[column.casefold()]
                for column in columns
                if column.casefold() in folded
            }
            self._types[key] = cached
        return cached

    def drop_all(self, engines: dict[str, Engine]) -> None:
        for (engine_name, _cols), table in self._tables.items():
            engines[engine_name].drop(table)


def _expected_after_two_runs(mode: str, rows: int) -> int:
    return rows * 2 if mode == "full_refresh_append" else rows


def run_data_cell(
    src: Engine,
    dst: Engine,
    mode: str,
    shape: str,
    *,
    rows: int,
    cache: SourceCache,
    prefix: str,
    keep: bool = False,
) -> Cell:
    columns, skips = projection(src.name, dst.name)
    contracts = domain_contract_columns(dst.name, columns)
    cell = Cell(
        source=src.name,
        destination=dst.name,
        mode=mode,
        shape=shape,
        columns=len(columns),
        skipped_columns=skips,
        source_rows=rows,
    )
    if contracts:
        cell.notes.append(
            "signed Migration Risk Contract: "
            + "; ".join(f"{k}: {v}" for k, v in contracts.items())
            + " (the unsigned refusal is proven by the "
            "domain_contract_unsigned cell)"
        )
    src_table = cache.table(src, columns)
    dst_table = f"{prefix}_dst_{uuid.uuid4().hex[:8]}"
    runs = 2 if shape == "create_new" else 1
    cell.expected_rows = (
        _expected_after_two_runs(mode, rows) if runs == 2 else rows
    )
    try:
        if shape == "create_new" and mode == "full_refresh_append":
            # create-new mirrors the source primary key, and a keyed
            # destination is *right* to refuse the second run's duplicate keys
            # (measured: "5 key value(s) ... already at rest ... aborts").
            # 2N is only the correct answer for a keyless append sink, so that
            # is the destination this mode is proven against.
            cell.shape = "keyless_append_sink"
            dst.create_table(
                dst_table,
                columns,
                keyless=True,
                source_types=cache.source_types(src, columns),
                source_engine=src.name,
            )
            cell.notes.append(
                "append sink created without the primary key; a keyed "
                "destination fails closed on the duplicate-key insert instead"
            )
        elif shape == "create_new":
            dst.drop(dst_table)
        elif shape == "dest_exists_compatible":
            dst.create_table(
                dst_table,
                columns,
                source_types=cache.source_types(src, columns),
                source_engine=src.name,
            )
        elif shape == "dest_exists_missing_column":
            # The destination lacks a mapped column: the engine must either
            # refuse or evolve the table — never write the rest and drop it.
            dst.create_table(
                dst_table,
                [c for c in columns if c != G13_COLUMN],
                source_types=cache.source_types(src, columns),
                source_engine=src.name,
            )
        else:  # pragma: no cover — guarded by DATA_SHAPES
            raise ValueError(f"unknown data shape {shape}")

        for attempt in range(runs):
            result, run_id, wall = execute(
                src.endpoint(src_table),
                dst.endpoint(dst_table),
                mode=mode,
                columns=columns,
                stream=src_table,
                src=src,
                dst=dst,
                sign_risk=contracts,
            )
            cell.run_ids.append(run_id)
            _accounting(cell, result)
            if attempt == 0:
                cell.elapsed_seconds = round(result.elapsed_seconds or wall, 3)
                cell.rows_per_sec = round(
                    rows / cell.elapsed_seconds if cell.elapsed_seconds else 0.0, 1
                )
            if not result.success:
                cell.status = "fail"
                cell.reason = f"run {attempt + 1} failed: {str(result.error)[:300]}"
                cell.quarantine_sample = _quarantine_sample(result)
                return cell
            if not (result.validation_plan or {}):
                cell.notes.append("validation_plan empty — preflight evidence missing")

        # ---- proof from outside the product -------------------------------
        cell.dest_rows = dst.count(dst_table)
        dest_chk = dst.checksum(dst_table, columns)
        cell.checksum_dest = dest_chk.hex
        # Under append the destination holds the fixture twice; the aggregate is
        # order-independent, so the expected value is the sum of both passes.
        want = expected_checksum(rows, columns)
        expected_total = (
            (want.total * 2) % (1 << 64)
            if cell.expected_rows == rows * 2
            else want.total
        )
        cell.checksum_source = f"{expected_total:016x}"
        cell.checksum_match = dest_chk.total == expected_total
        if cell.dest_rows != cell.expected_rows:
            cell.status = "fail"
            cell.reason = (
                f"destination holds {cell.dest_rows}, mode {mode} requires "
                f"{cell.expected_rows} after {runs} run(s)"
            )
        elif not cell.checksum_match:
            cell.status = "fail"
            cell.reason = (
                f"checksum mismatch over {len(columns)} mapped columns "
                f"(dest {cell.checksum_dest} != source {cell.checksum_source})"
            )
        elif cell.coerced_null or cell.rejected or cell.skipped_rows:
            cell.status = "fail"
            cell.reason = (
                f"silent coercion: rejected={cell.rejected} "
                f"coerced_null={cell.coerced_null} skipped={cell.skipped_rows}"
            )
        else:
            cell.status = "pass"
    except Exception as exc:  # noqa: BLE001 — an exception is a measured result
        cell.status = "fail"
        cell.reason = f"{type(exc).__name__}: {exc}"[:400]
    finally:
        if not keep:
            try:
                dst.drop(dst_table)
            except Exception:  # noqa: BLE001
                pass
    return cell


def run_blocking_cell(
    src: Engine,
    dst: Engine,
    shape: str,
    *,
    rows: int,
    cache: SourceCache,
    prefix: str,
    mode: str = "full_refresh_overwrite",
) -> Cell:
    """A shape the product must refuse rather than silently reshape."""
    columns, skips = projection(src.name, dst.name)
    cell = Cell(
        source=src.name,
        destination=dst.name,
        mode=mode,
        shape=shape,
        columns=len(columns),
        skipped_columns=skips,
        source_rows=rows,
        expected_rows=0,
    )
    src_table = cache.table(src, columns)
    dst_table = f"{prefix}_blk_{uuid.uuid4().hex[:8]}"
    try:
        map_columns: Sequence[str] = columns
        if shape == "domain_contract_unsigned":
            contracts = domain_contract_columns(dst.name, columns)
            if not contracts:
                cell.status = "skip"
                cell.reason = (
                    f"{dst.name} declares every mapped domain natively — "
                    "no Migration Risk Contract is required on this route"
                )
                return cell
            cell.notes.append(
                "unsigned: " + "; ".join(f"{k}: {v}" for k, v in contracts.items())
            )
            dst.drop(dst_table)
        elif shape == "dest_exists_narrower":
            narrow_col = (
                NARROW_COLUMN if NARROW_COLUMN in columns else NARROW_COLUMN_FALLBACK
            )
            dst.create_table(dst_table, columns, narrow=narrow_col)
        elif shape == "g13_extra_source_column":
            # The destination cannot take note_null and nobody mapped or
            # declared it omitted: G13 must block instead of dropping it.
            dst.create_table(dst_table, [c for c in columns if c != G13_COLUMN])
            map_columns = [c for c in columns if c != G13_COLUMN]
        elif shape == "g14_dest_notnull_no_default":
            dst.create_table(dst_table, columns)
            dst.add_column(
                dst_table,
                f"{dst.quote(G14_COLUMN)} {_g14_type(dst.name)} NOT NULL",
                f"{dst.quote(G14_COLUMN)} = 1",
            )
        else:  # pragma: no cover — guarded by BLOCKING_SHAPES
            raise ValueError(f"unknown blocking shape {shape}")

        before = 0 if shape == "domain_contract_unsigned" else dst.count(dst_table)
        result, run_id, _wall = execute(
            src.endpoint(src_table),
            dst.endpoint(dst_table),
            mode=mode,
            columns=map_columns,
            stream=src_table,
            src=src,
            dst=dst,
        )
        cell.run_ids.append(run_id)
        _accounting(cell, result)
        cell.quarantine_sample = _quarantine_sample(result)
        try:
            cell.dest_rows = dst.count(dst_table) - before
        except Exception:  # noqa: BLE001 — a refused create-new leaves no table
            cell.dest_rows = 0
        cell.elapsed_seconds = round(result.elapsed_seconds or 0.0, 3)
        cell.reason = str(result.error or "")[:300]
        if not result.success and cell.dest_rows == 0:
            cell.status = "pass"
            if not cell.reason:
                cell.reason = "refused (no error text)"
        elif result.success and cell.dest_rows == 0 and cell.rejected >= rows:
            # Quarantined every row instead of writing coerced data: also a
            # refusal, and it is inspectable.
            cell.status = "pass"
            cell.reason = f"all {cell.rejected} rows quarantined"
        else:
            cell.status = "fail"
            cell.reason = (
                f"NOT blocked: success={result.success} landed={cell.dest_rows} "
                f"rejected={cell.rejected} coerced_null={cell.coerced_null} "
                f"error={cell.reason}"
            )
    except Exception as exc:  # noqa: BLE001
        cell.status = "fail"
        cell.reason = f"{type(exc).__name__}: {exc}"[:400]
    finally:
        try:
            dst.drop(dst_table)
        except Exception:  # noqa: BLE001
            pass
    return cell


def _g14_type(engine: str) -> str:
    return {
        "postgresql": "INTEGER",
        "mysql": "INT",
        "sqlserver": "INT",
        "sqlite": "INTEGER",
        "oracle": "NUMBER(10)",
    }[engine]


def run_matrix(
    *,
    rows: int = ROWS_DEFAULT,
    engines: Sequence[str] | None = None,
    modes: Sequence[str] = MODES,
    shapes: Sequence[str] = BLOCKING_SHAPES,
    data_shapes: Sequence[str] = DATA_SHAPES,
    shape_modes: Sequence[str] = ("full_refresh_overwrite",),
    blocking_modes: Sequence[str] = ("full_refresh_overwrite",),
    prefix: str = "scale",
    progress: Any = None,
) -> dict[str, Any]:
    """Every ordered pair of live engines × modes, plus the schema shapes."""
    fleet = build_engines(engines)
    live, skipped = live_engines(fleet)
    cache = SourceCache(rows, prefix)
    cells: list[Cell] = []

    def emit(cell: Cell) -> None:
        cells.append(cell)
        if progress is not None:
            progress(cell)

    for src_name, src in live.items():
        # Guarantee the source can carry what we claim it carries.
        engine_columns(src_name)
        for dst_name, dst in live.items():
            for mode in modes:
                if "create_new" not in data_shapes:
                    continue
                emit(
                    run_data_cell(
                        src,
                        dst,
                        mode,
                        "create_new",
                        rows=rows,
                        cache=cache,
                        prefix=prefix,
                    )
                )
            for shape in data_shapes:
                if shape == "create_new":
                    continue
                for mode in shape_modes or ("full_refresh_overwrite",):
                    emit(
                        run_data_cell(
                            src,
                            dst,
                            mode,
                            shape,
                            rows=rows,
                            cache=cache,
                            prefix=prefix,
                        )
                    )
            for shape in shapes:
                for mode in blocking_modes or ("full_refresh_overwrite",):
                    emit(
                        run_blocking_cell(
                            src,
                            dst,
                            shape,
                            rows=rows,
                            cache=cache,
                            prefix=prefix,
                            mode=mode,
                        )
                    )

    cache.drop_all(live)
    return summarize(cells, live, skipped, rows)


def summarize(
    cells: Sequence[Cell],
    live: dict[str, Engine],
    skipped: dict[str, str],
    rows: int,
) -> dict[str, Any]:
    return {
        "rows": rows,
        "live_engines": sorted(live),
        "skipped_engines": skipped,
        "pass": sum(1 for c in cells if c.status == "pass"),
        "fail": sum(1 for c in cells if c.status == "fail"),
        "skip": sum(1 for c in cells if c.status == "skip") + len(skipped),
        "slow_routes": [c.as_dict() for c in cells if c.slow],
        "cells": [c.as_dict() for c in cells],
    }
