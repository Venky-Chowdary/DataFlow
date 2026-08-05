"""Wall-clock accounting for the phases of a single transfer.

Job records already carry total elapsed seconds and a rows/second figure, but
both are computed once at the end. When an operator asks "why was this slow?"
there was nothing to answer with: no split between reading the source,
transforming and writing, and re-reading for the checksum. Support could only
guess, and the three fixes for those three causes are completely different
(source index, worker count, validation mode).

This is the one place that accounting lives. Readers and writers must not each
grow their own timer — a per-connector stopwatch is how the same transfer ends
up reporting two different durations.

Timings are wall-clock and phases overlap by design: chunk writers run on a
pool while the reader fetches ahead, so the phase totals sum to more than the
elapsed time. ``share_of_busy`` reports each phase against total measured busy
time rather than against elapsed, so the numbers stay interpretable instead of
implying the job spent 180% of its life working.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Iterator

#: Canonical phase names. Keep this list short — it is an operator-facing
#: vocabulary, not an internal call graph.
PHASE_READ = "read"
PHASE_TRANSFORM_WRITE = "transform_write"
PHASE_CHECKSUM = "checksum"
PHASE_INTROSPECT = "introspect"
PHASE_DDL = "ddl"

_PHASE_LABELS = {
    PHASE_READ: "Reading source",
    PHASE_TRANSFORM_WRITE: "Transforming and writing",
    PHASE_CHECKSUM: "Verifying checksum",
    PHASE_INTROSPECT: "Inspecting schema",
    PHASE_DDL: "Applying DDL",
}


class PhaseProfile:
    """Thread-safe accumulator of per-phase wall time and call counts."""

    def __init__(self) -> None:
        self._seconds: dict[str, float] = defaultdict(float)
        self._calls: dict[str, int] = defaultdict(int)
        self._rows: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()
        self._started = time.perf_counter()

    def add(self, phase: str, seconds: float, *, rows: int = 0) -> None:
        if seconds < 0:
            return
        with self._lock:
            self._seconds[phase] += seconds
            self._calls[phase] += 1
            self._rows[phase] += max(0, rows)

    @contextmanager
    def measure(self, phase: str, *, rows: int = 0) -> Iterator[None]:
        """Time a block and attribute it to ``phase``.

        Records the elapsed time even when the block raises, so a failed
        transfer still shows where it spent its time before dying. When
        OpenTelemetry is enabled the same block becomes a child span of the
        transfer root, named after the phase — that is how an APM view and
        the job's phase_profile card stay in lockstep.
        """
        start = time.perf_counter()
        try:
            from services.tracing import set_span_attribute, start_span
        except Exception:
            start_span = None  # type: ignore[assignment]

        span_cm = (
            start_span(
                f"transfer.phase.{phase}",
                attributes={
                    "dataflow.phase": phase,
                    "dataflow.phase_label": _PHASE_LABELS.get(phase, phase),
                },
            )
            if start_span is not None
            else _null_cm()
        )
        with span_cm as span:
            try:
                yield
            finally:
                elapsed = time.perf_counter() - start
                self.add(phase, elapsed, rows=rows)
                if start_span is not None:
                    set_span_attribute(span, "dataflow.phase_seconds", round(elapsed, 4))
                    set_span_attribute(span, "dataflow.phase_rows", max(0, rows))

    @property
    def elapsed_seconds(self) -> float:
        return max(0.0, time.perf_counter() - self._started)

    def snapshot(self) -> dict[str, Any]:
        """Operator-facing breakdown; safe to embed in a job record."""
        with self._lock:
            seconds = dict(self._seconds)
            calls = dict(self._calls)
            rows = dict(self._rows)

        busy = sum(seconds.values())
        phases = []
        for name, secs in sorted(seconds.items(), key=lambda kv: -kv[1]):
            phase_rows = rows.get(name, 0)
            phases.append({
                "phase": name,
                "label": _PHASE_LABELS.get(name, name),
                "seconds": round(secs, 3),
                "calls": calls.get(name, 0),
                "rows": phase_rows,
                "share_of_busy": round(secs / busy, 4) if busy > 0 else 0.0,
                "rows_per_second": round(phase_rows / secs, 1) if secs > 0 and phase_rows else 0.0,
            })
        return {
            "phases": phases,
            "busy_seconds": round(busy, 3),
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "dominant_phase": phases[0]["phase"] if phases else "",
            # Phases overlap (reader fetches ahead of the writer pool), so this
            # is deliberately allowed to exceed 1.0. Surfacing it beats an
            # unexplained mismatch between the numbers.
            "overlap_factor": (
                round(busy / self.elapsed_seconds, 2) if self.elapsed_seconds > 0 else 0.0
            ),
        }

    def summary(self) -> str:
        """One line an operator can read in a log or a job header."""
        snap = self.snapshot()
        if not snap["phases"]:
            return "no phase timings recorded"
        parts = [
            f"{p['label']} {p['seconds']:.1f}s ({p['share_of_busy'] * 100:.0f}%)"
            for p in snap["phases"]
        ]
        return " · ".join(parts)


def _null_cm():
    from contextlib import nullcontext

    return nullcontext()


class NullPhaseProfile(PhaseProfile):
    """No-op profile for call sites that do not want the bookkeeping."""

    def add(self, phase: str, seconds: float, *, rows: int = 0) -> None:  # noqa: D102
        return
