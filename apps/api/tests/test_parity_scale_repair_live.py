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
    assert not any(s.startswith("DELETE") for s in _statements(sql))
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


def test_a_crosswalked_column_is_compared_and_repaired_through_the_writers_pipeline() -> None:
    """Review 2, item 9: the crosswalk (and the run's locale) are replayed by the writer's own function."""
    src, dst = f"psd1_x_s_{uuid.uuid4().hex[:8]}", f"psd1_x_d_{uuid.uuid4().hex[:8]}"
    ddl = "id bigint PRIMARY KEY, name varchar(40), amt numeric(12,2)"
    _sql(f'CREATE TABLE "{src}" ({ddl})',
         f"INSERT INTO \"{src}\" SELECT g, 'n' || g, g * 1.25 FROM generate_series(1, {N}) g",
         f'CREATE TABLE "{dst}" ({ddl})',
         f"INSERT INTO \"{dst}\" SELECT id, 'MAPPED-' || substr(name, 2), amt FROM \"{src}\"",
         f"UPDATE \"{dst}\" SET name = 'WRONG' WHERE id = 1500", f'DELETE FROM "{dst}" WHERE id = 20')
    try:
        from services.mongodb_service import get_mongodb_service

        crosswalk = {f"n{i}": f"MAPPED-{i}" for i in range(1, N + 1)}
        job_id = get_mongodb_service().create_transfer_job({
            "status": "completed",
            "transfer_request": {
                "source": _endpoint(src), "destination": _endpoint(dst), "sync_mode": "full_refresh_overwrite",
                "mappings": [{"source": "id", "target": "id"},
                             {"source": "name", "target": "name", "code_crosswalk": crosswalk},
                             {"source": "amt", "target": "amt"}],
            },
            "destination_summary": {"table": dst, "primary_key_columns": ["id"]},
            "reconciliation": {"passed": True, "source_rows": N, "target_rows": N - 1, "phase": "post_write",
                               "coverage_limit": {"rows": N, "limit": 100},
                               "verification_verdict_qualifier": "coverage_limited"},
        })
        block = _verify(job_id)
        assert block["state"] == "failed", block
        evidence = get_mongodb_service().get_job(job_id)["parity_scale"]["tables"][0]["evidence"]
        assert evidence["differences"]["changed_rows"] == 1  # only the row that really differs
        repair = _repair(job_id, dst)
        assert "\"name\" = 'MAPPED-1500'" in repair["sql"] and "'MAPPED-20'" in repair["sql"]
        assert "'n1500'" not in repair["sql"] and "'n20'" not in repair["sql"]
        _sql(*_statements(repair["sql"]))
        assert _verify(job_id, only_table=dst, force=True)["state"] == "passed"
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def test_the_runs_locales_are_replayed_by_the_judge_the_explorer_and_the_repair() -> None:
    """Review 3, item 5: the judge replayed transforms and crosswalks without the run's locales:
    every DMY ``to_date`` and EU ``to_number`` row read as changed (unexplained), and the repair
    SQL rewrote correct rows."""
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import explorer

    src, dst = f"psd1_l_s_{uuid.uuid4().hex[:8]}", f"psd1_l_d_{uuid.uuid4().hex[:8]}"
    _sql(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, d varchar(20), n varchar(20), c varchar(20))',
         # DMY dates, EU numbers, and a crosswalked DMY date column
         f"INSERT INTO \"{src}\" SELECT g, to_char(date '2024-01-01' + g, 'DD/MM/YYYY'), "
         f"replace(to_char(g * 1.5, 'FM99990.00'), '.', ','), to_char(date '2024-01-01' + g, 'DD/MM/YYYY') "
         f"FROM generate_series(1, {N}) g",
         f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, d date, n numeric(12,2), c varchar(20))',
         f"INSERT INTO \"{dst}\" SELECT g, date '2024-01-01' + g, g * 1.5, 'D' || g "
         f"FROM generate_series(1, {N}) g",
         f"UPDATE \"{dst}\" SET n = 0 WHERE id = 1500")
    try:
        from datetime import date, timedelta

        # to_date (DMY) first, then the crosswalk of the date: the writer's order.
        crosswalk = {(date(2024, 1, 1) + timedelta(days=g)).isoformat(): f"D{g}" for g in range(1, N + 1)}
        job_id = get_mongodb_service().create_transfer_job({
            "status": "completed",
            "transfer_request": {
                "source": _endpoint(src), "destination": _endpoint(dst), "sync_mode": "full_refresh_overwrite",
                "number_locale": "EU", "date_locale": "DMY",
                "mappings": [{"source": "id", "target": "id"},
                             {"source": "d", "target": "d", "transform": "to_date"},
                             {"source": "n", "target": "n", "transform": "to_number"},
                             {"source": "c", "target": "c", "transform": "to_date", "code_crosswalk": crosswalk}],
            },
            "destination_summary": {"table": dst, "primary_key_columns": ["id"]},
            "reconciliation": {"passed": True, "source_rows": N, "target_rows": N, "phase": "post_write",
                               "coverage_limit": {"rows": N, "limit": 100},
                               "verification_verdict_qualifier": "coverage_limited"},
        })
        block = _verify(job_id)
        evidence = get_mongodb_service().get_job(job_id)["parity_scale"]["tables"][0]["evidence"]
        diff = evidence["differences"]
        # Before: thousands of DMY / EU rows read as changed; now only the row that differs.
        assert diff["changed_rows"] == 1, (diff["changed_rows"], diff["changed_cells_by_column"])
        assert diff["changed_cells_by_column"] == {"n": 1}
        assert block["state"] == "failed"
        values = explorer.values(get_mongodb_service().get_job(job_id), dst)
        cells = {c["column"]: c for c in values["rows"][0]["cells"]}
        assert [c for c, cell in cells.items() if cell["differs"]] == ["n"]
        repair = _repair(job_id, dst)
        updates = [s for s in _statements(repair["sql"]) if s.startswith("UPDATE")]
        assert len(updates) == 1 and "2250" in updates[0] and "1500" in updates[0], updates
        _sql(*_statements(repair["sql"]))
        assert _verify(job_id, only_table=dst, force=True)["state"] == "passed"
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def test_repair_statements_never_overwrite_a_row_changed_since_they_were_generated(upper_tables) -> None:
    """Review 3, item 9: UPDATE carried no old-value guard and DELETE no re-check of the source."""
    src, dst = upper_tables
    _sql(f"UPDATE \"{dst}\" SET name = 'WRONG' WHERE id IN (30, 31)",
         f"INSERT INTO \"{dst}\" VALUES (-5, 'EXTRA', 1), (-6, 'EXTRA', 2)")
    job_id = _job(src, dst)
    assert _verify(job_id)["state"] == "failed"
    # Since the comparison: the source now holds key -6 (so it is not an extra row any more) ...
    _sql(f"INSERT INTO \"{src}\" VALUES (-6, 'extra', 2)")
    sql = _repair(job_id, dst)["sql"]
    statements = _statements(sql)
    deletes = [s for s in statements if s.startswith("DELETE")]
    assert len(deletes) == 1 and '"id" = -5' in deletes[0] and "\"name\" = 'EXTRA'" in deletes[0]
    assert "1 extra row left alone: the source holds that key now" in sql
    updates = [s for s in statements if s.startswith("UPDATE")]
    assert all("\"name\" = 'WRONG'" in u.split(" WHERE ", 1)[1] for u in updates), updates
    # ... and someone fixed row 31 (differently) in the destination before the reviewer ran them.
    _sql(f"UPDATE \"{dst}\" SET name = 'FIXED BY HAND' WHERE id = 31", f'DELETE FROM "{src}" WHERE id = -6')
    _sql(*statements)
    conn = parity_engines.connect("postgresql")
    with conn.cursor() as cur:
        cur.execute(f'SELECT id, name FROM "{dst}" WHERE id IN (30, 31, -5, -6) ORDER BY id')
        rows = cur.fetchall()
    conn.close()
    assert rows == [(-6, "EXTRA"), (30, "NAME 30"), (31, "FIXED BY HAND")]


def _mysql_usable() -> bool:
    if not parity_engines.available("mysql"):
        return False
    try:
        parity_engines.connect("mysql").close()
        return True
    except Exception:  # noqa: BLE001 — a reachable port with other credentials is not usable here
        return False


@pytest.mark.skipif(not _mysql_usable(), reason="MySQL not usable (DATAFLOW_TEST_MYSQL_*)")
@pytest.mark.parametrize("sql_mode", ["", "NO_BACKSLASH_ESCAPES"])
def test_mysql_literals_mean_the_same_under_every_sql_mode(sql_mode: str) -> None:
    """Review 3, item 9: a doubled backslash is two backslashes under NO_BACKSLASH_ESCAPES."""
    from services.parity_scale.explorer import literal

    values = ["C:\\temp\\new", "it's", "both \\' here", "plain", "naïve ünïcode \\n"]
    conn = parity_engines.connect("mysql")
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION sql_mode = %s", (sql_mode,))
            for value in values:
                cur.execute(f"SELECT {literal('mysql', value)}")
                got = cur.fetchone()[0]
                assert (got.decode("utf-8") if isinstance(got, bytes) else got) == value, (sql_mode, value)
    finally:
        conn.close()
