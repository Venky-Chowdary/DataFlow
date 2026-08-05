"""Tests for per-transfer phase accounting.

The operator-facing question is "why was this slow?". Before this existed the
job record had one end-of-run rows/second number, which cannot separate a slow
source scan from a slow destination write from a doubled cost in strict
verification — and those three have different fixes.
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.phase_profile import (  # noqa: E402
    PHASE_CHECKSUM,
    PHASE_READ,
    PHASE_TRANSFORM_WRITE,
    NullPhaseProfile,
    PhaseProfile,
)


class TestPhaseProfileAccounting:
    def test_measure_records_time_and_rows(self):
        p = PhaseProfile()
        with p.measure(PHASE_READ, rows=100):
            time.sleep(0.01)
        snap = p.snapshot()
        assert len(snap["phases"]) == 1
        phase = snap["phases"][0]
        assert phase["phase"] == PHASE_READ
        assert phase["seconds"] >= 0.009
        assert phase["rows"] == 100
        assert phase["calls"] == 1

    def test_time_is_recorded_even_when_the_block_raises(self):
        """A failed transfer must still show where it spent its time."""
        p = PhaseProfile()
        with pytest.raises(RuntimeError):
            with p.measure(PHASE_TRANSFORM_WRITE):
                time.sleep(0.01)
                raise RuntimeError("destination refused the write")
        snap = p.snapshot()
        assert snap["phases"][0]["seconds"] >= 0.009

    def test_shares_are_relative_to_busy_time(self):
        p = PhaseProfile()
        p.add(PHASE_READ, 1.0, rows=10)
        p.add(PHASE_TRANSFORM_WRITE, 3.0, rows=10)
        snap = p.snapshot()
        shares = {ph["phase"]: ph["share_of_busy"] for ph in snap["phases"]}
        assert shares[PHASE_TRANSFORM_WRITE] == pytest.approx(0.75)
        assert shares[PHASE_READ] == pytest.approx(0.25)
        assert sum(shares.values()) == pytest.approx(1.0)

    def test_dominant_phase_is_the_biggest_cost(self):
        p = PhaseProfile()
        p.add(PHASE_READ, 0.5)
        p.add(PHASE_CHECKSUM, 9.0)
        p.add(PHASE_TRANSFORM_WRITE, 2.0)
        assert p.snapshot()["dominant_phase"] == PHASE_CHECKSUM

    def test_overlap_factor_is_reported_not_hidden(self):
        """Reader and writer pool run concurrently, so busy can exceed elapsed."""
        p = PhaseProfile()
        p.add(PHASE_READ, 5.0)
        p.add(PHASE_TRANSFORM_WRITE, 5.0)
        snap = p.snapshot()
        assert snap["busy_seconds"] == pytest.approx(10.0)
        # Elapsed here is near zero, so overlap must be large rather than clamped.
        assert snap["overlap_factor"] > 1.0

    def test_rows_per_second_is_per_phase(self):
        p = PhaseProfile()
        p.add(PHASE_READ, 2.0, rows=1000)
        assert p.snapshot()["phases"][0]["rows_per_second"] == pytest.approx(500.0)

    def test_empty_profile_is_safe_to_serialize(self):
        snap = PhaseProfile().snapshot()
        assert snap["phases"] == []
        assert snap["dominant_phase"] == ""
        assert snap["busy_seconds"] == 0.0

    def test_summary_is_human_readable(self):
        p = PhaseProfile()
        p.add(PHASE_TRANSFORM_WRITE, 8.0)
        p.add(PHASE_READ, 2.0)
        text = p.summary()
        assert "Transforming and writing" in text
        assert "80%" in text

    def test_negative_durations_are_ignored(self):
        p = PhaseProfile()
        p.add(PHASE_READ, -5.0)
        assert p.snapshot()["phases"] == []

    def test_concurrent_writers_do_not_lose_samples(self):
        """Chunk writers record from the pool; totals must not race."""
        p = PhaseProfile()

        def record() -> None:
            for _ in range(100):
                p.add(PHASE_TRANSFORM_WRITE, 0.001, rows=1)

        threads = [threading.Thread(target=record) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        phase = p.snapshot()["phases"][0]
        assert phase["calls"] == 800
        assert phase["rows"] == 800

    def test_null_profile_records_nothing(self):
        p = NullPhaseProfile()
        with p.measure(PHASE_READ, rows=10):
            pass
        assert p.snapshot()["phases"] == []


class TestStreamEmitsPhaseProfile:
    def test_sqlite_transfer_reports_every_phase(self, tmp_path):
        import sqlite3

        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        src_db = tmp_path / "src.db"
        con = sqlite3.connect(src_db)
        con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
        con.executemany(
            "INSERT INTO orders VALUES (?,?)",
            [(i, "shipped" if i % 3 else "pending") for i in range(1, 2001)],
        )
        con.commit()
        con.close()

        src = EndpointConfig(
            kind="database", format="sqlite", database=str(src_db), table="orders"
        )
        dst = EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(tmp_path / "dst.db"),
            table="orders_copy",
        )
        mappings = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "status")]
        written, _ddl, summary, _cols = stream_database_transfer(
            src, dst, mappings, {"id": "integer", "status": "string"}, job_id="phase-test"
        )

        assert written == 2000
        profile = summary.get("phase_profile")
        assert profile, "transfer must report where its time went"

        by_phase = {p["phase"]: p for p in profile["phases"]}
        assert PHASE_TRANSFORM_WRITE in by_phase
        assert PHASE_READ in by_phase

        # The source read is real work — reporting 0 rows read for a 2000-row
        # transfer was the misleading version of this.
        assert by_phase[PHASE_READ]["rows"] >= 2000
        assert profile["dominant_phase"]
        assert profile["busy_seconds"] > 0
