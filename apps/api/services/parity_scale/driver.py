"""The scan driver: mode R (key ranges), mode H (hash buckets), leaves, judge.

A scan is a queue of small work items. Each item is one or two bounded
statements per side; after each one the driver checkpoints its whole state to
a :class:`~services.parity_scale.store.ScanStore`. That makes a scan:

* **asynchronous** — :meth:`ParityScan.run` works for at most ``max_seconds``
  and returns progress; the caller (a worker, a follow-on phase after the
  write, owner decision 2) calls it again later or from another process, and
  the result arrives whenever the queue is empty;
* **resumable** — a worker killed at any point resumes from the last
  checkpoint and reaches the same result (only ``resumed`` differs);
* **bounded in memory** — the queue holds unit bounds, the leaf merge holds at
  most a few leaf thresholds of ``(key, a, b)``, the judge pages 1,000 keys,
  stored keys are capped (counts never are) and the streamed pass spills to
  disk.

Honesty rules: a unit whose statement times out is halved and retried; after
``max_retries`` it is recorded as **not compared** with the reason, and the
scan's coverage drops below 100% — it is never a pass. A systematic difference
(most units differ) stops the drill-down instead of fetching the table, and the
counts become "at least".
"""

from __future__ import annotations

import math
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from services.parity_scale import require_enabled
from services.parity_scale.ce1 import ALGORITHM, CE_VERSION, HASH_SPEC
from services.parity_scale.dialects.base import EMPTY, LeafRow, Side, UnitTimeout, add
from services.parity_scale.plan import PlanError, TablePlan, require_pair
from services.parity_scale.store import ScanStore, decode_key, encode_key, utc_now

STATE_VERSION = 1


@dataclass(frozen=True)
class ScanConfig:
    """Tunables (data-diff's ``--bisection-factor`` / ``--bisection-threshold``)."""

    #: ``auto`` picks R when the key is order-portable, else H; ``R`` / ``H`` force.
    mode: str = "auto"
    segment_rows: int = 50_000
    bisection_factor: int = 16
    leaf_threshold: int = 10_000
    #: A unit that still holds more than this many rows and cannot be split
    #: (one key repeated) is not compared rather than loaded.
    leaf_hard_cap: int = 80_000
    max_levels: int = 5
    #: Mode H scans each side in chunks of its own native key order.
    chunk_rows: int = 250_000
    #: Level-0 boundaries (mode R): ``auto`` reads exact quantiles below
    #: ``large_table_rows`` (engine estimate) and, above it, splits a single
    #: integer key linearly or samples the keys (TABLESAMPLE).
    boundary_strategy: str = "auto"
    large_table_rows: int = 2_000_000
    statement_timeout_s: float | None = 120.0
    max_retries: int = 2
    max_wall_seconds: float | None = None
    systematic_unit_ratio: float = 0.05
    #: …and at least this many differing level-0 units (small tables drill).
    systematic_min_units: int = 20
    systematic_keys: int = 50_000
    keys_stored_cap: int = 10_000
    judge_page: int = 1_000
    examples_cap: int = 20
    stream_batch: int = 5_000
    spill_dir: str | None = None
    #: Run each statement on both sides at once (one connection per side).
    parallel_sides: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ScanConfig":
        fields = cls.__dataclass_fields__
        return cls(**{k: v for k, v in (data or {}).items() if k in fields})


SideFactory = Callable[[str, TablePlan], Side]
"""``factory(role, plan) -> Side``: reconnects on resume, in any process."""


class ScanStateError(RuntimeError):
    """The stored state cannot be resumed (missing, other plan, other version)."""


def _new_state(scan_id: str, plan: TablePlan, config: ScanConfig, mode: str, mode_reason: str) -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "scan_id": scan_id,
        "algorithm": ALGORITHM,
        "ce_version": CE_VERSION,
        "hash": HASH_SPEC,
        "plan": plan.to_dict(),
        "plan_hash": plan.plan_hash,
        "config": asdict(config),
        "mode": mode,
        "mode_reason": mode_reason,
        "check_kind": "multiset_digest" if plan.keyless else "segment_diff",
        "status": "running",
        "phase": "plan",
        "queue": [{"t": "start"}],
        "items_done": 0,
        "statements": 0,
        "units": {"level0": 0, "level0_differing": 0, "compared": 0, "differing": 0,
                  "leaves": 0, "max_level": 0},
        "not_compared": [],
        "rows": {"source": 0, "destination": 0, "level0_complete": False},
        "held_out": None,
        "diff": {
            "missing": 0, "extra": 0, "changed": 0, "duplicate": 0,
            "missing_keys": [], "extra_keys": [], "changed_keys": [], "duplicate_keys": [],
            "keys_capped": False,
            "missing_content_hashes": [], "extra_content_hashes": [],
        },
        "judge": {"rows_judged": 0, "ce_stricter_than_fp": 0, "explained_rows": 0,
                  "unexplained_rows": 0, "cells_by_column": {}, "causes": {},
                  "row_causes": {}, "examples": [], "not_judged": 0},
        "stream": None,
        "examples": {"missing_content": [], "extra_content": []},
        "systematic": None,
        "resumed": 0,
        "started_at": utc_now(),
        "finished_at": None,
        "elapsed_s": 0.0,
    }


class ParityScan:
    """One table's scan. Build with :meth:`start` or :meth:`resume`."""

    def __init__(
        self,
        state: dict[str, Any],
        *,
        store: ScanStore,
        open_side: SideFactory,
        fault: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.state = state
        self.store = store
        self.plan = TablePlan.from_dict(state["plan"])
        self.config = ScanConfig.from_dict(state["config"])
        self._open_side = open_side
        self._sides: dict[str, Side] = {}
        self._pool: Any = None
        #: Test hook: called before each statement with ``(op, item)``; it may
        #: raise :class:`UnitTimeout` to simulate a slow statement.
        self._fault = fault

    # construction ----------------------------------------------------------------
    @classmethod
    def start(
        cls,
        plan: TablePlan,
        *,
        store: ScanStore,
        open_side: SideFactory,
        config: ScanConfig | None = None,
        scan_id: str | None = None,
        fault: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> "ParityScan":
        require_enabled()
        require_pair(plan)
        config = config or ScanConfig()
        mode, reason = cls._choose_mode(plan, config)
        state = _new_state(scan_id or f"psd1-{uuid.uuid4().hex[:16]}", plan, config, mode, reason)
        store.save(state["scan_id"], state)
        return cls(state, store=store, open_side=open_side, fault=fault)

    @classmethod
    def resume(
        cls,
        scan_id: str,
        *,
        store: ScanStore,
        open_side: SideFactory,
        plan: TablePlan | None = None,
        fault: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> "ParityScan":
        require_enabled()
        state = store.load(scan_id)
        if state is None:
            raise ScanStateError(f"no stored scan {scan_id!r}")
        if state.get("version") != STATE_VERSION:
            raise ScanStateError("stored scan has another state version: restart it")
        if plan is not None and plan.plan_hash != state.get("plan_hash"):
            raise ScanStateError("the plan changed since the scan started (schema or mapping): restart it")
        if state.get("status") == "running":
            state["resumed"] = int(state.get("resumed") or 0) + 1
            store.save(scan_id, state)
        return cls(state, store=store, open_side=open_side, fault=fault)

    @staticmethod
    def _choose_mode(plan: TablePlan, config: ScanConfig) -> tuple[str, str]:
        if plan.keyless:
            if plan.stream_columns or not plan.engine_columns:
                return "S", "keyless table with streamed columns: multiset of row fingerprints"
            return "H", "keyless table: multiset digest over row-hash buckets"
        if not plan.key_encodable:
            return "S", "key not encodable in-engine: every column by streamed fingerprint"
        if config.mode == "R":
            if not plan.order_portable:
                raise PlanError(f"mode R needs an order-portable key: {plan.order_reason}")
            return "R", "forced"
        if config.mode == "H":
            return "H", "forced"
        if plan.order_portable:
            return "R", plan.order_reason
        return "H", plan.order_reason

    # sides -----------------------------------------------------------------------
    def side(self, role: str) -> Side:
        if role not in self._sides:
            side = self._open_side(role, self.plan)
            side.set_timeout(self.config.statement_timeout_s)
            self._sides[role] = side
        return self._sides[role]

    def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=True)
            self._pool = None
        for side in self._sides.values():
            side.close()
        self._sides.clear()

    def _parallel(self, calls: list[Callable[[], Any]]) -> list[Any]:
        """Run one statement per side at once (each side has its own connection)."""
        if not self.config.parallel_sides or len(calls) < 2:
            return [call() for call in calls]
        if self._pool is None:
            from concurrent.futures import ThreadPoolExecutor

            self._pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="psd1-side")
        futures = [self._pool.submit(call) for call in calls]
        results: list[Any] = []
        error: BaseException | None = None
        for future in futures:
            try:
                results.append(future.result())
            except BaseException as exc:  # noqa: BLE001 — re-raised below, after both sides finished
                error = error or exc
                results.append(None)
        if error is not None:
            raise error
        return results

    def _both(self, op: str, item: dict[str, Any], call: Callable[[Side], Any]) -> tuple[Any, Any]:
        if self._fault is not None:
            self._fault(op, item)
        src, dst = self.side("source"), self.side("destination")
        self.state["statements"] += 2
        left, right = self._parallel([lambda: call(src), lambda: call(dst)])
        return left, right

    # public loop -------------------------------------------------------------------
    @property
    def done(self) -> bool:
        return self.state["status"] != "running"

    def run(self, max_seconds: float | None = None) -> dict[str, Any]:
        """Work until the queue is empty or ``max_seconds`` pass; return progress."""
        started = time.monotonic()
        try:
            while not self.done:
                if max_seconds is not None and time.monotonic() - started >= max_seconds:
                    break
                wall = self.config.max_wall_seconds
                if wall is not None and float(self.state["elapsed_s"]) >= wall:
                    self._abandon_queue("scan wall-clock budget exhausted")
                    continue
                t0 = time.monotonic()
                item = self.state["queue"].pop(0) if self.state["queue"] else None
                if item is None:
                    self._finish()
                else:
                    self._process(item)
                    self.state["items_done"] += 1
                self.state["elapsed_s"] = round(float(self.state["elapsed_s"]) + time.monotonic() - t0, 6)
                self.store.save(self.state["scan_id"], self.state)
        finally:
            if self.done:
                self.close()
        return self.progress()

    def progress(self) -> dict[str, Any]:
        s = self.state
        diff = s["diff"]
        return {
            "scan_id": s["scan_id"],
            "status": s["status"],
            "phase": s["phase"],
            "mode": s["mode"],
            "items_done": s["items_done"],
            "items_queued": len(s["queue"]),
            "units": dict(s["units"]),
            "not_compared": len(s["not_compared"]),
            "differences_so_far": diff["missing"] + diff["extra"] + diff["changed"] + diff["duplicate"],
            "elapsed_s": s["elapsed_s"],
            "resumed": s["resumed"],
        }

    def result(self) -> dict[str, Any]:
        from services.parity_scale.evidence import build_evidence

        return build_evidence(self.state)

    # dispatch ----------------------------------------------------------------------
    def _process(self, item: dict[str, Any]) -> None:
        kind = item["t"]
        handler = {
            "start": self._start,
            "R": self._range_unit,
            "RL": self._range_leaf,
            "H": self._bucket_level,
            "HL": self._bucket_leaf,
            "held": self._held,
            "stream": self._stream,
            "judge": self._judge,
            "examples": self._examples,
            "level0_done": self._after_level0,
        }[kind]
        handler(item)
        if not self.done:
            self._maybe_queue_judge()

    def _enqueue(self, *items: dict[str, Any]) -> None:
        self.state["queue"].extend(items)

    def _not_compared(self, unit: dict[str, Any], reason: str) -> None:
        self.state["not_compared"].append({"unit": unit, "reason": reason})

    def _abandon_queue(self, reason: str) -> None:
        for item in self.state["queue"]:
            # A scan stopped before it even started compared nothing: never a match.
            if item["t"] in {"R", "RL", "H", "HL", "stream", "start", "held"}:
                self._not_compared(item, reason)
            elif item["t"] == "judge":
                self.state["judge"]["not_judged"] += len(item.get("keys") or [])
        self.state["queue"] = []
        self._finish()

    def _finish(self) -> None:
        self.state["status"] = "done"
        self.state["phase"] = "done"
        self.state["finished_at"] = utc_now()

    # start ---------------------------------------------------------------------------
    def _start(self, _item: dict[str, Any]) -> None:
        from services.parity_scale.certification import assess

        mode = self.state["mode"]
        self.state["certification"] = assess(
            self.plan,
            getattr(self.side("source"), "server", self.plan.source_engine),
            getattr(self.side("destination"), "server", self.plan.dest_engine),
        )
        tail: list[dict[str, Any]] = []
        if any(c.decimal_fit for c in self.plan.engine_columns):
            tail.append({"t": "held"})
        if mode == "S":
            self.state["phase"] = "stream"
            self._enqueue({"t": "stream", "shape": "multiset" if self.plan.keyless else "keyed"}, *tail)
            return
        if self.plan.stream_columns and not self.plan.keyless:
            tail.append({"t": "stream", "shape": "changed"})
        self.state["phase"] = "level0"
        if mode == "R":
            src = self.side("source")
            try:
                if self._fault is not None:
                    self._fault("split", {"t": "R0"})
                points, strategy = src.boundaries(
                    self.config.segment_rows,
                    strategy=self.config.boundary_strategy,
                    large_table_rows=self.config.large_table_rows,
                )
                self.state["boundary_strategy"] = strategy
                self.state["statements"] += 1
            except UnitTimeout as exc:
                self._not_compared({"t": "R", "lo": None, "hi": None, "lvl": 0},
                                   f"segment boundaries timed out: {exc}")
                self._enqueue(*tail)
                return
            bounds = [None, *[encode_key(p) for p in points], None]
            units = [
                {"t": "R", "lo": bounds[i], "hi": bounds[i + 1], "lvl": 0, "retry": 0,
                 "est": self.config.segment_rows}
                for i in range(len(bounds) - 1)
            ]
            self.state["units"]["level0"] = len(units)
            self.state["deferred"] = []
            self._enqueue(*units, {"t": "level0_done"})
        else:
            self._enqueue({"t": "H", "lvl": 0, "parents": [], "retry": 0})
        self._enqueue(*tail)

    # mode R ---------------------------------------------------------------------------
    def _range_unit(self, item: dict[str, Any]) -> None:
        units = self.state["units"]
        if "known" in item:
            src, dst = tuple(item["known"][0]), tuple(item["known"][1])
        else:
            lo, hi = decode_key(item["lo"]), decode_key(item["hi"])
            try:
                src, dst = self._both("range_digest", item, lambda s: s.range_digest(lo, hi))
            except UnitTimeout as exc:
                self._retry_range(item, str(exc))
                return
            if item["lvl"] == 0:
                self.state["rows"]["source"] += src[0]
                self.state["rows"]["destination"] += dst[0]
            units["max_level"] = max(units["max_level"], int(item["lvl"]))
            if src == dst:
                units["compared"] += 1
                return
            units["differing"] += 1
            if item["lvl"] == 0:
                # Level 0 is breadth first, so a systematic difference is seen
                # before any drill-down starts.
                units["level0_differing"] += 1
                self.state["deferred"].append({**item, "known": [list(src), list(dst)]})
                return
        if self.state.get("systematic"):
            self._not_compared(item, "systematic difference: not localised")
            return
        lo, hi = decode_key(item["lo"]), decode_key(item["hi"])
        biggest = max(src[0], dst[0])
        if biggest <= self.config.leaf_threshold or int(item["lvl"]) >= self.config.max_levels:
            self._leaf_or_skip(item, biggest, {"t": "RL", "lo": item["lo"], "hi": item["hi"]})
            return
        side = self.side("source" if src[0] >= dst[0] else "destination")
        step = max(1, math.ceil(biggest / self.config.bisection_factor))
        try:
            if self._fault is not None:
                self._fault("split", item)
            points = side.split_points(lo, hi, step)
            self.state["statements"] += 1
        except UnitTimeout as exc:
            self._not_compared(item, f"split timed out: {exc}")
            return
        if not points:
            self._leaf_or_skip(item, biggest, {"t": "RL", "lo": item["lo"], "hi": item["hi"]})
            return
        bounds = [item["lo"], *[encode_key(p) for p in points], item["hi"]]
        children = [
            {"t": "R", "lo": bounds[i], "hi": bounds[i + 1], "lvl": int(item["lvl"]) + 1,
             "retry": 0, "est": step}
            for i in range(len(bounds) - 1)
        ]
        # Depth first below level 0 keeps the frontier at ``f × levels``.
        self.state["queue"][0:0] = children

    def _leaf_or_skip(self, item: dict[str, Any], rows: int, leaf: dict[str, Any]) -> None:
        if rows > self.config.leaf_hard_cap:
            self._not_compared(item, f"unit holds {rows} rows and cannot be narrowed further")
            return
        self.state["queue"].insert(0, leaf)

    def _retry_range(self, item: dict[str, Any], reason: str) -> None:
        if int(item.get("retry") or 0) >= self.config.max_retries:
            self._not_compared(item, f"timed out after {self.config.max_retries} retries: {reason}")
            return
        lo, hi = decode_key(item["lo"]), decode_key(item["hi"])
        est = max(2, int(item.get("est") or self.config.segment_rows))
        try:
            points = self.side("source").split_points(lo, hi, max(1, math.ceil(est / 2)))
            self.state["statements"] += 1
        except UnitTimeout as exc:
            self._not_compared(item, f"timed out, and halving it timed out too: {exc}")
            return
        bounds = [item["lo"], *[encode_key(p) for p in points[:1]], item["hi"]]
        halves = [
            {**item, "lo": bounds[i], "hi": bounds[i + 1], "retry": int(item.get("retry") or 0) + 1,
             "est": max(1, est // 2)}
            for i in range(len(bounds) - 1)
        ]
        self.state["queue"][0:0] = halves

    def _after_level0(self, _item: dict[str, Any] | None = None) -> None:
        units = self.state["units"]
        self.state["rows"]["level0_complete"] = not any(
            n["unit"].get("lvl") == 0 for n in self.state["not_compared"]
        )
        self._check_systematic(units["level0"], units["level0_differing"])
        self.state["phase"] = "drill"
        deferred = self.state.pop("deferred", [])
        if self.state.get("systematic"):
            # Localise one unit as a sample (names the column); the rest are
            # differing but not enumerated, so counts become "at least".
            for unit in deferred[1:]:
                self._not_compared(unit, "systematic difference: not localised")
            deferred = deferred[:1]
            if deferred:
                src, dst = deferred[0]["known"]
                if max(src[0], dst[0]) <= self.config.leaf_threshold:
                    self.state["queue"].insert(0, {"t": "RL", "lo": deferred[0]["lo"],
                                                   "hi": deferred[0]["hi"], "sample": True})
                else:
                    self._not_compared(deferred[0], "systematic difference: not localised")
            return
        self.state["queue"][0:0] = deferred

    def _check_systematic(self, total: int, differing: int) -> None:
        cfg = self.config
        # Both: enough differing units that drilling is costly, and a share of
        # them that row-level drift does not explain.
        if differing >= cfg.systematic_min_units and differing / max(total, 1) > cfg.systematic_unit_ratio:
            self.state["systematic"] = {
                "units_total": total,
                "units_differing": differing,
                "reason": f"{differing} of {total} level-0 units differ (more than "
                f"{cfg.systematic_unit_ratio:.0%}): a column-wide cause, not row-level drift",
            }

    def _range_leaf(self, item: dict[str, Any]) -> None:
        lo, hi = decode_key(item["lo"]), decode_key(item["hi"])
        try:
            src, dst = self._both("range_leaf", item, lambda s: s.range_leaf(lo, hi))
        except UnitTimeout as exc:
            self._not_compared(item, f"leaf read timed out: {exc}")
            return
        self.state["units"]["leaves"] += 1
        self._merge(src, dst)

    # mode H ---------------------------------------------------------------------------
    def _chunks(self, role: str) -> list[tuple[Any, Any]]:
        """This side's scan chunks in its own native key order (additivity)."""
        if self.plan.keyless:
            return [(None, None)]
        cache = self.state.setdefault("chunks", {})
        if role not in cache:
            points = self.side(role).split_points(None, None, self.config.chunk_rows)
            self.state["statements"] += 1
            bounds = [None, *[encode_key(p) for p in points], None]
            cache[role] = [[bounds[i], bounds[i + 1]] for i in range(len(bounds) - 1)]
        return [(decode_key(lo), decode_key(hi)) for lo, hi in cache[role]]

    def _chunked(self, role: str, item: dict[str, Any], call: Callable[[Side, Any, Any], Any], merge):
        """One side's scan, chunk by chunk; returns ``(merged result, statements)``.

        Runs on a worker thread: it touches only its own side and returns its
        statement count instead of writing to the shared state.
        """
        side = self.side(role)
        total = None
        statements = 0
        pending = [(lo, hi, 0) for lo, hi in self._chunk_bounds[role]]
        while pending:
            lo, hi, retry = pending.pop(0)
            try:
                if self._fault is not None:
                    self._fault(f"chunk:{role}", {**item, "chunk_retry": retry})
                part = call(side, lo, hi)
                statements += 1
            except UnitTimeout:
                if retry >= self.config.max_retries or self.plan.keyless:
                    raise
                points = side.split_points(lo, hi, max(1, self.config.chunk_rows // (2 ** (retry + 1))))
                statements += 1
                bounds = [lo, *points[:1], hi]
                pending[0:0] = [(bounds[i], bounds[i + 1], retry + 1) for i in range(len(bounds) - 1)]
                continue
            total = part if total is None else merge(total, part)
        return total, statements

    def _both_chunked(self, item: dict[str, Any], call: Callable[[Side, Any, Any], Any], merge, empty):
        # Chunk bounds are read (and cached in the state) on this thread first.
        self._chunk_bounds: dict[str, list[tuple[Any, Any]]] = {role: self._chunks(role) for role in ("source", "destination")}
        self.side("source"), self.side("destination")
        results = self._parallel([
            lambda: self._chunked("source", item, call, merge),
            lambda: self._chunked("destination", item, call, merge),
        ])
        out = []
        for total, statements in results:
            self.state["statements"] += statements
            out.append(empty if total is None else total)
        return out

    @staticmethod
    def _merge_digests(left: dict, right: dict) -> dict:
        out = dict(left)
        for k, v in right.items():
            out[k] = add(out.get(k, EMPTY), v)
        return out

    def _bucket_level(self, item: dict[str, Any]) -> None:
        level = int(item["lvl"])
        parents = [tuple(p) for p in item["parents"]]
        try:
            per_side = self._both_chunked(
                item, lambda s, lo, hi: s.bucket_digests(level, parents, lo, hi), self._merge_digests, {}
            )
        except UnitTimeout as exc:
            self._not_compared(item, f"bucket level {level} timed out after retries: {exc}")
            return
        src, dst = per_side
        units = self.state["units"]
        units["max_level"] = max(units["max_level"], level)
        paths = sorted(set(src) | set(dst))
        if level == 0:
            units["level0"] = 1024
            self.state["rows"]["source"] = sum(d[0] for d in src.values())
            self.state["rows"]["destination"] = sum(d[0] for d in dst.values())
            self.state["rows"]["level0_complete"] = True
        compared = 1024 * max(1, len(parents)) if level else 1024
        differing = [p for p in paths if src.get(p, EMPTY) != dst.get(p, EMPTY)]
        units["compared"] += compared - len(differing)
        units["differing"] += len(differing)
        if level == 0:
            units["level0_differing"] = len(differing)
            self._check_systematic(1024, len(differing))
            self.state["phase"] = "drill"
        if not differing:
            return
        if self.state.get("systematic"):
            first = differing[0]
            size = max(src.get(first, EMPTY)[0], dst.get(first, EMPTY)[0])
            if size <= self.config.leaf_threshold:
                self.state["queue"].insert(0, {"t": "HL", "paths": [list(first)], "sample": True})
            for path in differing[1:]:
                self._not_compared({"t": "H", "lvl": level, "path": list(path)},
                                   "systematic difference: not localised")
            return
        leaves: list[list[int]] = []
        deeper: list[list[int]] = []
        for path in differing:
            size = max(src.get(path, EMPTY)[0], dst.get(path, EMPTY)[0])
            if size <= self.config.leaf_threshold or level + 1 >= min(self.config.max_levels, 6):
                if size > self.config.leaf_hard_cap:
                    self._not_compared({"t": "H", "lvl": level, "path": list(path)},
                                       f"bucket holds {size} rows and cannot be narrowed further")
                    continue
                leaves.append(list(path))
            else:
                deeper.append(list(path))
        work: list[dict[str, Any]] = []
        batch: list[list[int]] = []
        budget = 0
        for path in leaves:
            size = max(src.get(tuple(path), EMPTY)[0], dst.get(tuple(path), EMPTY)[0])
            if batch and budget + size > 4 * self.config.leaf_threshold:
                work.append({"t": "HL", "paths": batch})
                batch, budget = [], 0
            batch.append(path)
            budget += size
        if batch:
            work.append({"t": "HL", "paths": batch})
        for start in range(0, len(deeper), 64):
            work.append({"t": "H", "lvl": level + 1, "parents": deeper[start : start + 64], "retry": 0})
        self.state["queue"][0:0] = work

    def _bucket_leaf(self, item: dict[str, Any]) -> None:
        paths = [tuple(p) for p in item["paths"]]
        try:
            per_side = self._both_chunked(
                item, lambda s, lo, hi: s.bucket_leaf(paths, lo, hi), lambda a, b: [*a, *b], []
            )
        except UnitTimeout as exc:
            self._not_compared(item, f"bucket leaf timed out after retries: {exc}")
            return
        self.state["units"]["leaves"] += len(paths)
        self._merge(per_side[0], per_side[1])

    # leaf merge ---------------------------------------------------------------------------
    def _merge(self, src_rows: Sequence[LeafRow], dst_rows: Sequence[LeafRow]) -> None:
        if self.plan.keyless:
            self._merge_multiset(src_rows, dst_rows)
            return
        diff = self.state["diff"]
        cap = self.config.keys_stored_cap
        src: dict[str, list[LeafRow]] = defaultdict(list)
        dst: dict[str, list[LeafRow]] = defaultdict(list)
        for row in src_rows:
            src[row.ident].append(row)
        for row in dst_rows:
            dst[row.ident].append(row)

        def keep(bucket: str, key: tuple[Any, ...] | None) -> None:
            stored = diff[f"{bucket}_keys"]
            if len(stored) < cap:
                stored.append(encode_key(key))
            else:
                diff["keys_capped"] = True

        for ident in sorted(set(src) | set(dst)):
            s_rows, d_rows = src.get(ident, []), dst.get(ident, [])
            if not d_rows:
                diff["missing"] += len(s_rows)
                keep("missing", s_rows[0].key)
                continue
            if not s_rows:
                diff["extra"] += len(d_rows)
                keep("extra", d_rows[0].key)
                continue
            if len(d_rows) > 1:
                diff["duplicate"] += len(d_rows) - 1
                keep("duplicate", d_rows[0].key)
            words = {(r.a, r.b) for r in s_rows}
            if not any((r.a, r.b) in words for r in d_rows):
                diff["changed"] += 1
                keep("changed", s_rows[0].key)
        if diff["changed"] + diff["missing"] + diff["extra"] > self.config.systematic_keys and not self.state["systematic"]:
            self.state["systematic"] = {
                "reason": f"more than {self.config.systematic_keys} differing keys: the drill-down stopped",
            }
            self._drop_drill_queue("systematic difference: not localised")

    def _drop_drill_queue(self, reason: str) -> None:
        keep: list[dict[str, Any]] = []
        for item in self.state["queue"]:
            if item["t"] in {"R", "RL", "H", "HL"}:
                self._not_compared(item, reason)
            else:
                keep.append(item)
        self.state["queue"] = keep

    def _merge_multiset(self, src_rows: Sequence[LeafRow], dst_rows: Sequence[LeafRow]) -> None:
        diff = self.state["diff"]
        counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        for row in src_rows:
            counts[row.ident][0] += 1
        for row in dst_rows:
            counts[row.ident][1] += 1
        for ident in sorted(counts):
            s, d = counts[ident]
            if s > d:
                diff["missing"] += s - d
                if len(diff["missing_content_hashes"]) < self.config.examples_cap:
                    diff["missing_content_hashes"].append(ident)
            elif d > s:
                diff["extra"] += d - s
                if len(diff["extra_content_hashes"]) < self.config.examples_cap:
                    diff["extra_content_hashes"].append(ident)

    # held-out ledger -----------------------------------------------------------------------
    def _held(self, _item: dict[str, Any]) -> None:
        try:
            if self._fault is not None:
                self._fault("held", _item)
            self.state["held_out"] = {"fit_excluded": self.side("source").held_count()}
            self.state["statements"] += 1
        except UnitTimeout as exc:
            self.state["held_out"] = {"fit_excluded": None, "reason": f"count timed out: {exc}"}

    # streamed pass ---------------------------------------------------------------------------
    def _stream(self, item: dict[str, Any]) -> None:
        from services.parity_scale.stream import stream_compare

        self.state["phase"] = "stream"
        shape = item["shape"]
        columns = None if shape != "changed" else list(self.plan.stream_columns)
        try:
            if self._fault is not None:
                self._fault("stream", item)
            result = stream_compare(
                self.side("source"), self.side("destination"), self.plan,
                shape=shape, columns=columns, keys_cap=self.config.keys_stored_cap,
                examples_cap=self.config.examples_cap, batch=self.config.stream_batch,
                spill_dir=self.config.spill_dir,
            )
        except UnitTimeout as exc:
            self._not_compared(item, f"streamed pass timed out: {exc}")
            return
        self.state["stream"] = {
            "shape": shape,
            "columns": [c.target for c in (columns or self.plan.compared_columns)],
            "rows_source": result.rows_source,
            "rows_destination": result.rows_destination,
        }
        diff = self.state["diff"]
        if shape == "multiset":
            diff["missing"] += result.missing
            diff["extra"] += result.extra
            self.state["examples"]["missing_content"] = result.missing_examples
            self.state["examples"]["extra_content"] = result.extra_examples
            self.state["rows"].update(source=result.rows_source, destination=result.rows_destination,
                                      level0_complete=True)
            return
        if shape == "keyed":
            self.state["rows"].update(source=result.rows_source, destination=result.rows_destination,
                                      level0_complete=True)
            for name in ("missing", "extra", "duplicate"):
                diff[name] += getattr(result, name)
                diff[f"{name}_keys"].extend(getattr(result, f"{name}_keys"))
        known = {tuple(map(repr, k)) for k in diff["changed_keys"]}
        for key in result.changed_keys:
            if tuple(map(repr, key)) not in known:
                diff["changed_keys"].append(key)
                diff["changed"] += 1
        # Keys beyond the stored cap were counted by the pass but not named.
        extra_unnamed = result.changed - len(result.changed_keys)
        if extra_unnamed > 0:
            diff["changed"] += extra_unnamed
            diff["keys_capped"] = True

    # judge ------------------------------------------------------------------------------------
    def _queue_judge(self) -> None:
        diff = self.state["diff"]
        page = self.config.judge_page
        keys = diff["changed_keys"]
        jobs = [{"t": "judge", "keys": keys[i : i + page]} for i in range(0, len(keys), page)]
        if self.plan.keyless and (diff["missing_content_hashes"] or diff["extra_content_hashes"]):
            jobs.append({"t": "examples"})
        self.state["queue"].extend(jobs)

    def _judge(self, item: dict[str, Any]) -> None:
        from services.parity_scale.judge import judge_row, key_text

        self.state["phase"] = "judge"
        keys = [decode_key(k) for k in item["keys"]]
        judge = self.state["judge"]
        try:
            src, dst = self._both("fetch_rows", item, lambda s: s.fetch_rows(keys))
        except UnitTimeout as exc:
            judge["not_judged"] += len(keys)
            self._not_compared({"t": "judge", "keys": len(keys)}, f"row fetch timed out: {exc}")
            return
        src_side = self.side("source")
        dst_side = self.side("destination")
        for key in keys:
            s_rows = src.get(src_side.key_hex(key)) or []
            d_rows = dst.get(dst_side.key_hex(key)) or []
            if not s_rows or not d_rows:
                # The row moved between the digest and the fetch: in flight.
                judge["not_judged"] += 1
                continue
            text = key_text(self.plan, key)
            verdict = judge_row(self.plan, s_rows[0], d_rows[0], key_text=text)
            judge["rows_judged"] += 1
            if verdict.equal_by_fingerprint:
                judge["ce_stricter_than_fp"] += 1
                continue
            if verdict.explained:
                judge["explained_rows"] += 1
            else:
                judge["unexplained_rows"] += 1
            by_column = judge.setdefault("by_column", {})
            for cell in verdict.cells:
                col = cell["column"]
                cause = cell["cause"]
                code = cause.get("code") or "unexplained"
                judge["cells_by_column"][col] = judge["cells_by_column"].get(col, 0) + 1
                judge["causes"][code] = judge["causes"].get(code, 0) + 1
                # Per column and cause: what the Trust Report groups a difference by.
                slot = by_column.setdefault(col, {}).setdefault(code, {
                    "rows": 0, "explained": bool(cause.get("explained")), "label": cause.get("label") or code,
                    "detail": cause.get("detail") or "", "keys": [],
                })
                slot["rows"] += 1
                if len(slot["keys"]) < 20:
                    slot["keys"].append(text)
            if len(judge["examples"]) < self.config.examples_cap:
                judge["examples"].append({"key": text, "cells": verdict.cells})

    def _examples(self, _item: dict[str, Any]) -> None:
        from services.parity_scale.judge import wire_text

        diff = self.state["diff"]
        for role, name in (("source", "missing"), ("destination", "extra")):
            hashes = diff[f"{name}_content_hashes"]
            if not hashes:
                continue
            try:
                rows = self.side(role).keyless_examples(hashes, self.config.examples_cap)
                self.state["statements"] += 1
            except UnitTimeout:
                continue
            self.state["examples"][f"{name}_content"] = [
                [wire_text(v) for v in values] for _h, values in rows
            ]

    # the queue's tail: judge after every unit is merged ---------------------------------------------
    def _maybe_queue_judge(self) -> None:
        if not self.state.get("judge_queued") and not any(
            i["t"] in {"R", "RL", "H", "HL", "stream", "held", "level0_done", "start"}
            for i in self.state["queue"]
        ):
            self.state["judge_queued"] = True
            self._queue_judge()
