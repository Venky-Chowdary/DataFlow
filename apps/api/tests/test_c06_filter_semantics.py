"""C06 — Pilot filter truth: ``or`` is a disjunction, not another AND (D23),
and ``rank_connector_tables`` only claims ``exact`` over the full requested
population (D24).

D23: ``qty = 1 or qty = 2`` ran as ``qty = 1 AND qty = 2`` and reported 0
with ``exact=true`` while the true count was 10. ``cust is null or
amt > 500`` was equally wrong. A literal with an apostrophe
(``name contains O'Brien``) truncated the value at the quote.

All fail on c6c16128; pass post-fix.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store  # noqa: E402
from src.ai.copilot import tools as tools_mod  # noqa: E402
from src.ai.copilot.aggregate_tools import (  # noqa: E402
    aggregate_connector_data,
    rank_connector_tables,
)
from src.ai.copilot.predicates import (  # noqa: E402
    describe,
    ground_filters,
    parse_filters,
    to_mongo_match,
    to_sql,
)


def _isolated_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None


def _seed_qa_db(tmp_path: Path) -> Path:
    """qty: 6×1 + 4×2 + 5×3 -> 'qty = 1 or qty = 2' truth is 10."""
    db_path = tmp_path / "c06_qa.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE qa ("
        "id INTEGER PRIMARY KEY, qty INTEGER, cust TEXT, "
        "amt REAL, name TEXT, status TEXT)"
    )
    rows = []
    rid = 0
    for _ in range(6):
        rid += 1
        rows.append((rid, 1, "acme", 10.0, "Ann", "a"))
    for _ in range(4):
        rid += 1
        rows.append((rid, 2, "globex", 20.0, "Bob", "a"))
    for _ in range(5):
        rid += 1
        rows.append((rid, 3, "initech", 600.0, "Cal", "b"))
    rows.append((99, 9, None, 5.0, "O'Brien", "a"))
    rows.append((100, 9, None, 700.0, "Dan", "b"))
    conn.executemany(
        "INSERT INTO qa (id, qty, cust, amt, name, status) VALUES (?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()
    return db_path


def _seed_connector(tmp_path: Path) -> connector_store.SavedConnector:
    db_path = _seed_qa_db(tmp_path)
    uri = f"sqlite:///{db_path.resolve().as_posix()}"
    return connector_store.create_connector({
        "name": "C06SQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": uri,
        "workspace_id": "",
    })


# ---------------------------------------------------------------------------
# D23 — spoken ``or`` must reach the engine as OR
# ---------------------------------------------------------------------------

def test_aggregate_or_same_column_counts_both_values(monkeypatch, tmp_path):
    """Truth: 10 rows have qty in {1,2}. AND-ing the disjuncts answers 0."""
    _isolated_store(monkeypatch, tmp_path)
    saved = _seed_connector(tmp_path)
    res = aggregate_connector_data(
        connector_id=str(saved.id), table="qa", metric="count",
        where="qty = 1 or qty = 2",
    )
    assert res.success, res.error
    assert res.output["value"] == 10
    assert res.output["exact"] is True


def test_aggregate_or_null_and_comparison(monkeypatch, tmp_path):
    """cust IS NULL (2 rows) OR amt > 500 (5 rows: 600×5, 700×1) = 6 + 0 overlap."""
    _isolated_store(monkeypatch, tmp_path)
    saved = _seed_connector(tmp_path)
    res = aggregate_connector_data(
        connector_id=str(saved.id), table="qa", metric="count",
        where="cust is null or amt > 500",
    )
    assert res.success, res.error
    # NULL cust rows: id 99,100; amt>500: the five 600s + the 700 = 6.
    # id 100 satisfies both -> counted once.
    assert res.output["value"] == 7


def test_aggregate_and_or_precedence(monkeypatch, tmp_path):
    """`status = a and qty > 3 or amt < 5` is `(a AND qty>3) OR amt<5` = 1."""
    _isolated_store(monkeypatch, tmp_path)
    saved = _seed_connector(tmp_path)
    res = aggregate_connector_data(
        connector_id=str(saved.id), table="qa", metric="count",
        where="status = a and qty > 3 or amt < 5",
    )
    assert res.success, res.error
    # status=a & qty>3: id 99 (qty 9, cust NULL). amt<5: nobody (lowest is 5.0).
    assert res.output["value"] == 1


def test_aggregate_filter_apostrophe_value(monkeypatch, tmp_path):
    """The apostrophe is data — 'name contains O'Brien' finds the row."""
    _isolated_store(monkeypatch, tmp_path)
    saved = _seed_connector(tmp_path)
    res = aggregate_connector_data(
        connector_id=str(saved.id), table="qa", metric="count",
        where="name contains O'Brien",
    )
    assert res.success, res.error
    assert res.output["value"] == 1


def test_parse_filters_recovers_or_joiner():
    parsed, leftover = parse_filters("qty = 1 or qty = 2")
    assert [p.op for p in parsed] == ["eq", "eq"]
    assert parsed[0].join == "and"
    assert parsed[1].join == "or"


def test_parse_filters_keeps_and_joiner():
    parsed, _ = parse_filters("qty = 1 and qty < 3")
    assert len(parsed) == 2
    assert all(p.join == "and" for p in parsed)


def test_to_sql_renders_or_groups():
    parsed, _ = parse_filters("qty = 1 or qty = 2")
    cols = [{"name": "qty", "type": "INTEGER"}]
    preds = ground_filters(
        parsed, cols, lambda c, names: c, lambda cs, r: "INTEGER"
    )
    sql, params = to_sql(preds, lambda ident, dialect: f'"{ident}"', "sqlite")
    assert " OR " in sql
    assert " AND " not in sql
    assert params == {"f0": 1, "f1": 2}


def test_to_mongo_match_uses_or():
    parsed, _ = parse_filters("qty = 1 or qty = 2")
    cols = [{"name": "qty", "type": "INTEGER"}]
    preds = ground_filters(
        parsed, cols, lambda c, names: c, lambda cs, r: "INTEGER"
    )
    match = to_mongo_match(preds)
    assert "$or" in match
    assert match["$or"] == [{"qty": {"$eq": 1}}, {"qty": {"$eq": 2}}]


def test_describe_echoes_or():
    parsed, _ = parse_filters("qty = 1 or qty = 2")
    cols = [{"name": "qty", "type": "INTEGER"}]
    preds = ground_filters(
        parsed, cols, lambda c, names: c, lambda cs, r: "INTEGER"
    )
    assert " or " in describe(preds)


# ---------------------------------------------------------------------------
# D24 — rank_connector_tables: exact only over the full population
# ---------------------------------------------------------------------------

def test_rank_shortlist_is_not_exact(monkeypatch):
    """A catalog-estimate shortlist that drops objects is not an exact rank."""
    import src.ai.copilot.aggregate_tools as agg

    class _Conn:
        def get(self, key, default=None):
            return {"id": "c1", "name": "C06", "type": "postgresql"}.get(key, default)

    monkeypatch.setattr(agg, "_safe_connector", lambda *a, **k: (_Conn(), None))

    class _Listed:
        success = True
        output = {
            "objects": [f"t{i}" for i in range(20)],
            "total": 20,
            "type": "postgresql",
        }
        error = ""

    import src.ai.copilot.schema_tools as schema_tools

    monkeypatch.setattr(
        schema_tools, "list_connector_objects", lambda **k: _Listed()
    )
    # Stale estimates: every table claims 1 row, so the shortlist keeps only
    # limit+margin candidates and never counts the rest.
    monkeypatch.setattr(
        agg, "_estimate_table_row_counts", lambda conn, names: {n: 1 for n in names}
    )

    class _Counted:
        success = True
        output = {"value": 1}
        error = ""

    monkeypatch.setattr(
        agg, "aggregate_connector_data", lambda **k: _Counted()
    )

    res = rank_connector_tables(connector_id="c1", limit=5)
    assert res.success, res.error
    assert res.output["shortlisted_by"] == "catalog_estimate"
    assert res.output["considered"] < 20
    assert res.output["exact"] is False


def test_rank_full_population_is_exact(monkeypatch):
    """When every object is counted, exact=true is honest."""
    import src.ai.copilot.aggregate_tools as agg

    class _Conn:
        def get(self, key, default=None):
            return {"id": "c1", "name": "C06", "type": "sqlite"}.get(key, default)

    monkeypatch.setattr(agg, "_safe_connector", lambda *a, **k: (_Conn(), None))

    class _Listed:
        success = True
        output = {"objects": ["t1", "t2", "t3"], "total": 3, "type": "sqlite"}
        error = ""

    import src.ai.copilot.schema_tools as schema_tools

    monkeypatch.setattr(schema_tools, "list_connector_objects", lambda **k: _Listed())
    monkeypatch.setattr(agg, "_estimate_table_row_counts", lambda conn, names: {})

    class _Counted:
        success = True
        output = {"value": 10}
        error = ""

    monkeypatch.setattr(agg, "aggregate_connector_data", lambda **k: _Counted())

    res = rank_connector_tables(connector_id="c1", limit=5)
    assert res.success, res.error
    assert res.output["considered"] == 3
    assert res.output["exact"] is True
