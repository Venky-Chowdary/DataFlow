"""CR-D123 — the keyed quiet-poll verifier caught an exception it never imported.

``_covered_keyed_noop_verify`` handles ``PopulationTooLarge`` (an over-budget
re-read) by returning a fail-closed "unproven" report. The name was never
imported into ``reconcile_step``, so evaluating that ``except`` clause on ANY
read error raised ``NameError`` instead — the quiet poll crashed rather than
reporting itself unproven.
"""

from __future__ import annotations

import pytest

import services.row_conservation as row_conservation
import src.transfer.reconcile_step as rs
from services.verification_ladder import PopulationTooLarge
from src.transfer.models import EndpointConfig


def _verify(monkeypatch, exc: Exception) -> dict:
    def _raise(*_a, **_k):
        raise exc

    monkeypatch.setattr(rs, "_read_verify_rows", _raise)
    monkeypatch.setattr(
        row_conservation, "conservation_kind", lambda *_a, **_k: row_conservation.KIND_KEYED
    )
    return rs._covered_keyed_noop_verify(
        endpoint=EndpointConfig(kind="database", format="sqlite", database="d.db", table="t"),
        source_endpoint=EndpointConfig(kind="database", format="sqlite", database="s.db", table="t"),
        dest_summary={},
        mappings=[{"source": "id", "target": "id"}],
        sync_mode="incremental_upsert",
        rows_before=5,
        target_rows=5,
        pk_column="id",
        db_type="sqlite",
    )


def test_over_budget_reread_is_unproven_not_a_name_error(monkeypatch):
    report = _verify(monkeypatch, PopulationTooLarge(10, 5))
    assert report["passed"] is False
    assert report["unproven"] is True
    assert "exceeded budget" in report["message"]


@pytest.mark.parametrize("exc", [RuntimeError("boom"), OSError("gone")])
def test_any_read_error_fails_closed(monkeypatch, exc):
    report = _verify(monkeypatch, exc)
    assert report["passed"] is False
