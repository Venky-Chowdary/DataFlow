"""M10 — pre-write verification phases must not bill their time to extract.

Jobs sit 2–5 min in 'Validating mapping and schema' / 'Connecting to
destination' while ``phases`` credited everything to extract, because
phase_from_engine_label mapped validate/verify/quality_check/resuming to the
default bucket. They belong to preflight (pre-write verification).
"""

from __future__ import annotations

from services.job_phases import advance_phase, phase_from_engine_label


def test_verify_phase_labels_map_to_preflight():
    for label in ("validate", "verify", "quality_check", "resuming"):
        assert phase_from_engine_label(label) == "preflight", label


def test_known_labels_unchanged():
    assert phase_from_engine_label("reading") == "extract"
    assert phase_from_engine_label("writing") == "load"
    assert phase_from_engine_label("reconcile") == "reconcile"
    assert phase_from_engine_label("preflight") == "preflight"


def test_validate_time_is_credited_to_preflight():
    phases = advance_phase(None, "preflight", message="Validating mapping and schema…")
    phases = advance_phase(phases, "load")
    preflight = next(p for p in phases if p["name"] == "preflight")
    assert preflight["status"] == "done"
    assert "elapsed_ms" in preflight
