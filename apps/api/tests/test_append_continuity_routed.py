"""Append continuity for a routed (shape router) run follows the route tables.

A router recipe writes ``{dest}__{route}`` (or each route's ``targets``
table), never the request's destination table itself. Continuity
fingerprinted that logical table, which does not exist. So every routed run
recorded an "unreadable" baseline, and the next append was refused with
"the destination could not be fingerprinted after the last run" (PR 424 CI,
api-tests 6/6: test_shape_router_pg_live rt4/rt5/rt6/p1a/p2, once the live
PostgreSQL tests stopped skipping).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from services import append_continuity as ac
from src.transfer.models import EndpointConfig


@dataclass
class _Req:
    destination: Any
    shape_recipe: Any = None
    sync_mode: str = "full_refresh_append"
    stream_contracts: Any = None
    extra: dict = field(default_factory=dict)


@dataclass(frozen=True)
class _Ok:
    success: bool = True


ROUTER = {
    "steps": [{"op": "router", "options": {
        "routes": [
            {"name": "high", "condition": "to_number([amt]) >= 100"},
            {"name": "other", "default": True},
        ],
        "targets": {"high": "rt_high"},
    }}],
}


@pytest.fixture
def db(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr(ac, "_collection", lambda: None)
    path = tmp_path / "dest.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE rt_high (id INTEGER, amt TEXT)")
        conn.execute("CREATE TABLE rt_out__other (id INTEGER, amt TEXT)")
        conn.execute("INSERT INTO rt_high VALUES (1, '150')")
        conn.execute("INSERT INTO rt_out__other VALUES (2, '10')")
    return path


def _req(db: Path, recipe: Any = ROUTER) -> _Req:
    return _Req(EndpointConfig(kind="database", format="sqlite", database=str(db), table="rt_out"), recipe)


def test_routed_targets_are_the_route_tables(db: Path) -> None:
    tables = [t.table for t in ac.resolve_targets(_req(db))]
    assert tables == ["rt_high", "rt_out__other"]
    assert [t.table for t in ac.resolve_targets(_req(db, recipe=None))] == ["rt_out"]


def test_a_routed_run_baselines_each_route_table_and_the_next_append_passes(db: Path) -> None:
    req = _req(db)
    ac.record_after_run(req, "job1", _Ok())
    for target in ac.resolve_targets(req):
        base = ac.load_baseline(target.key)
        assert base and not base.get("unreadable"), target.table
    assert ac.load_baseline(ac.resolve_target(req.destination).key) is None, "no baseline for the logical table"
    assert ac.check_before_write(req) is None


def test_a_tampered_route_table_is_refused_naming_it(db: Path) -> None:
    req = _req(db)
    ac.record_after_run(req, "job1", _Ok())
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE rt_high SET amt = '999' WHERE id = 1")
    violation = ac.check_before_write(req)
    assert violation is not None and "rt_high" in violation.message
