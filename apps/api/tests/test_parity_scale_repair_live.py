"""Parity at Scale phase 3: repair SQL writes what the run would have written (review defects 9 and 13).

* A column the run transformed gets the source value as the run wrote it (its
  transform replayed), not the raw source value; running the statements and
  re-verifying then passes.
* Extra rows are left alone when the run wrote into a table that already
  held rows (they may be outside this run); quarantined rows are not inserted.
* Comment lines never carry a newline from a name, so a name cannot start a
  statement of its own.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import N, _endpoint, _env, _sql  # noqa: E402,F401

pytestmark = pytest.mark.skipif(not parity_engines.available("postgresql"),
                                reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


@pytest.fixture()
def upper_tables():
    """A source and the destination a run with ``upper`` on ``name`` wrote."""
    src, dst = f"psd1_r_s_{uuid.uuid4().hex[:8]}", f"psd1_r_d_{uuid.uuid4().hex[:8]}"
    ddl = "id bigint PRIMARY KEY, name varchar(40), amt numeric(12,2)"
    _sql(f'CREATE TABLE "{src}" ({ddl})',
         f"INSERT INTO \"{src}\" SELECT g, 'name ' || g, g * 1.25 FROM generate_series(1, {N}) g",
         f'CREATE TABLE "{dst}" ({ddl})', f'INSERT INTO "{dst}" SELECT id, upper(name), amt FROM "{src}"')
    yield src, dst
    _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def _job(src: str, dst: str, *, request: dict[str, Any] | None = None, recon: dict[str, Any] | None = None,
         summary: dict[str, Any] | None = None) -> str:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().create_transfer_job({
        "status": "completed",
        "transfer_request": {
            "source": _endpoint(src), "destination": _endpoint(dst),
            "mappings": [{"source": "id", "target": "id"},
                         {"source": "name", "target": "name", "transform": "upper"},
                         {"source": "amt", "target": "amt"}],
            "sync_mode": "full_refresh_overwrite", **(request or {}),
        },
        "destination_summary": {"table": dst, "primary_key_columns": ["id"], **(summary or {})},
        "reconciliation": {"passed": True, "source_rows": N, "target_rows": N, "phase": "post_write",
                           "coverage_limit": {"rows": N, "limit": 100},
                           "verification_verdict_qualifier": "coverage_limited", **(recon or {})},
    })


def _verify(job_id: str, **kwargs: Any) -> dict[str, Any]:
    from services.parity_scale.verification import start_for_job

    return start_for_job(job_id, background=False, **kwargs)


def _repair(job_id: str, table: str) -> dict[str, Any]:
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import explorer

    return explorer.repair_sql(get_mongodb_service().get_job(job_id), table)


def _statements(sql: str) -> list[str]:
    return [line for line in sql.splitlines() if line and not line.startswith("--")]


def test_transformed_columns_get_the_value_as_written_and_the_repair_verifies(upper_tables) -> None:
    src, dst = upper_tables
    _sql(f'DELETE FROM "{dst}" WHERE id = 20', f"UPDATE \"{dst}\" SET name = 'WRONG' WHERE id = 30")
    job_id = _job(src, dst)
    assert _verify(job_id)["state"] == "failed"
    repair = _repair(job_id, dst)
    assert "'NAME 20'" in repair["sql"] and "'NAME 30'" in repair["sql"]
    assert "'name 20'" not in repair["sql"] and "'name 30'" not in repair["sql"]
    _sql(*_statements(repair["sql"]))  # the reviewer runs them
    assert _verify(job_id, only_table=dst, force=True)["state"] == "passed"


def test_extras_outside_the_run_and_quarantined_rows_are_left_out(upper_tables) -> None:
    src, dst = upper_tables
    _sql(f"INSERT INTO \"{dst}\" VALUES (-1, 'OLDER', 1)", f'DELETE FROM "{dst}" WHERE id IN (7, 8)')
    job_id = _job(src, dst, request={"sync_mode": "full_refresh_append"},
                  recon={"target_rows_before": 1, "target_rows": N - 1, "rejected_rows": 1},
                  summary={"rejected_details": [{"row": {"id": 7}, "error": "failed validation"}]})
    assert _verify(job_id)["state"] == "failed"
    sql = _repair(job_id, dst)["sql"]
    assert "DELETE" not in sql
    assert "1 extra row left alone" in sql
    inserts = [s for s in _statements(sql) if s.startswith("INSERT")]
    assert len(inserts) == 1 and "VALUES (8," in inserts[0]
    assert "1 missing row the run quarantined left out" in sql


def test_comment_text_cannot_end_its_line() -> None:
    from services.parity_scale.explorer import _comment

    assert _comment("t\nDROP TABLE users; --") == "t DROP TABLE users; --"
    assert "\n" not in _comment("a\r\n b") and " " not in _comment("a b")


def test_names_with_a_newline_stay_inside_their_comments(monkeypatch) -> None:
    from types import SimpleNamespace

    from services.parity_scale import explorer

    col = SimpleNamespace(target="note\nDROP TABLE users; --", transform="")
    plan = SimpleNamespace(dest_engine="postgresql", key=[("id", "id")], compared_columns=[col])

    class Side:
        table_ref = '"t"'

        def close(self) -> None:
            return None

    table = "orders\nDROP TABLE users; --"
    monkeypatch.setattr(explorer, "_evidence", lambda job, name: {"differences": {"missing_in_destination": 1}})
    monkeypatch.setattr(explorer, "_sides", lambda job: (plan, Side(), Side()))
    monkeypatch.setattr(explorer, "_protected", lambda job: {col.target.lower()})
    monkeypatch.setattr(explorer, "_run_scope", lambda job, keys: (False, set()))
    sql = explorer.repair_sql({}, table)["sql"]
    assert all(line.startswith("--") for line in sql.splitlines() if line), sql
