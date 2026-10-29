"""D24: rank_connector_tables ranks the whole inventory or says exactly what it did not.

The catalog estimate covers every table in one query, but the shortlist was cut
to the first 40 names *before* the estimates ranked them, so a big table past
the 40th name was never counted. Engines with no estimates count the first 40
names only. Either way the answer led with "The largest table is ..." and
vouched "exact".
"""
from __future__ import annotations

import pytest

import src.ai.copilot.aggregate_tools as agg
import src.ai.copilot.schema_tools as schema_tools
from src.ai.copilot.aggregate_tools import rank_connector_tables
from src.ai.copilot.pilot_agent import _render_table_ranking


class _Conn:
    def get(self, key, default=None):
        return {"id": "c1", "name": "Wide", "type": "postgresql"}.get(key, default)


class _Listed:
    success = True
    error = ""

    def __init__(self, names):
        self.output = {"objects": names, "total": len(names), "type": "postgresql"}


class _Counted:
    success = True
    error = ""

    def __init__(self, value):
        self.output = {"value": value}


@pytest.fixture
def wide(monkeypatch):
    # Alphabetical name order puts the biggest table last: t00 .. t59.
    names = [f"t{i:02d}" for i in range(60)]
    monkeypatch.setattr(agg, "_safe_connector", lambda *a, **k: (_Conn(), None))
    monkeypatch.setattr(schema_tools, "list_connector_objects", lambda **k: _Listed(names))
    monkeypatch.setattr(
        agg,
        "aggregate_connector_data",
        lambda **k: _Counted(int(k["table"][1:]) * 100),
    )
    return names


def test_estimates_rank_every_table_not_the_first_forty_names(monkeypatch, wide):
    monkeypatch.setattr(
        agg, "_estimate_table_row_counts", lambda conn, names: {n: int(n[1:]) * 100 for n in names}
    )
    res = rank_connector_tables(connector_id="c1", limit=3)
    assert res.success, res.error
    assert [r["table"] for r in res.output["ranked"]] == ["t59", "t58", "t57"]


def test_unestimated_engine_counts_first_forty_and_says_so(monkeypatch, wide):
    monkeypatch.setattr(agg, "_estimate_table_row_counts", lambda conn, names: {})
    res = rank_connector_tables(connector_id="c1", limit=3)
    assert res.success, res.error
    assert res.output["exact"] is False
    assert res.output["considered"] == 40 and res.output["total_objects"] == 60
    text = _render_table_ranking(res.output)
    assert "largest table on **Wide**" not in text
    assert "Counted **40** of 60" in text
    assert "not a ranking of all 60" in text


def test_full_population_still_reads_as_exact(monkeypatch):
    names = ["a", "b", "c"]
    monkeypatch.setattr(agg, "_safe_connector", lambda *a, **k: (_Conn(), None))
    monkeypatch.setattr(schema_tools, "list_connector_objects", lambda **k: _Listed(names))
    monkeypatch.setattr(agg, "_estimate_table_row_counts", lambda conn, names: {})
    monkeypatch.setattr(agg, "aggregate_connector_data", lambda **k: _Counted(7))
    res = rank_connector_tables(connector_id="c1", limit=3)
    assert res.output["exact"] is True
    text = _render_table_ranking(res.output)
    assert "largest table on **Wide**" in text and "exact server-side `COUNT(*)`" in text


def test_estimate_shortlist_that_drops_tables_is_labelled(monkeypatch, wide):
    monkeypatch.setattr(agg, "_estimate_table_row_counts", lambda conn, names: {n: 1 for n in names})
    res = rank_connector_tables(connector_id="c1", limit=3)
    assert res.output["exact"] is False
    text = _render_table_ranking(res.output)
    assert "catalog row estimates" in text and "not a ranking of all 60" in text
