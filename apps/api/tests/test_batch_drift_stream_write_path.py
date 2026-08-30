"""DB stream write path uses the same BatchDriftDetector as file_stream."""

from __future__ import annotations

from pathlib import Path

from services.data_quality import BatchDriftDetector


def test_stream_write_path_constructs_and_checks_detector():
    src = Path(__file__).resolve().parents[1] / "src" / "transfer" / "stream.py"
    text = src.read_text(encoding="utf-8")
    assert "BatchDriftDetector()" in text
    assert "drift_detector.check(audit.stats or {})" in text
    assert 'validation_mode == "maximum"' in text


def test_stream_detector_same_algorithm_as_file_path():
    detector = BatchDriftDetector(numeric_threshold=0.05)
    baseline = {"columns": {"amount": {"mean": 10.0, "stdev": 1.0}}, "total_rows": 100}
    current = {"columns": {"amount": {"mean": 80.0, "stdev": 1.0}}, "total_rows": 100}
    detector.update(baseline)
    warnings = detector.check(current)
    assert any("mean drift" in w or "drift" in w for w in warnings)
