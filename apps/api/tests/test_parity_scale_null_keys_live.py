"""Parity at Scale: rows whose key is NULL, and the population check (review 3, item 1).

A NULL key compares as unknown, so it is in no key range: a NULL boundary
(``>= NULL`` / ``< NULL``) emptied whole ranges on both sides, and NULL-key
rows fell out of every range once a table had more than one unit. Both sides
then agreed on the rows that were left and the scan read "proven".

Now boundaries never hold a NULL, the NULL-key rows are their own unit
(compared as whole rows), and before any match the units must add up, per
side, to the table's row count.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import _endpoint, _env, _job, _report, _sql, _verify  # noqa: E402,F401

pytestmark = pytest.mark.skipif(not parity_engines.available("postgresql"),
                                reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")

FILL = ("SELECT g, 'n' || g, g * 1.25, timestamp '2024-01-01' + g * interval '1 second' "
        "FROM generate_series(1, 3000) g")


@pytest.fixture()
def no_pk_source():
    """A 3000-row source without a primary key (the key comes from the mapping) plus one NULL-id row."""
    src, dst = f"psd1_nk_s_{uuid.uuid4().hex[:8]}", f"psd1_nk_d_{uuid.uuid4().hex[:8]}"
    _sql(f'CREATE TABLE "{src}" (id bigint, name varchar(40), amt numeric(12,2), ts timestamp(6))',
         f'INSERT INTO "{src}" {FILL}',
         f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, name varchar(40), amt numeric(12,2), ts timestamp(6))',
         f'INSERT INTO "{dst}" SELECT * FROM "{src}"',
         f"INSERT INTO \"{src}\" VALUES (NULL, 'ghost', 1, timestamp '2024-01-01')")
    yield src, dst
    _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def _cfg(**kw: Any):
    from services.parity_scale.driver import ScanConfig

    return ScanConfig(**{"keys_stored_cap": 2000, "segment_rows": 300, "chunk_rows": 300,
                         "stream_chunk_rows": 300, **kw})


@pytest.mark.parametrize("mode", ["R", "H", "auto"])
def test_a_null_key_row_is_compared_in_its_own_unit_and_every_row_is_counted(no_pk_source, mode) -> None:
    job_id = _job(*no_pk_source)
    block = _verify(job_id, config=_cfg(mode=mode))
    ev = block["tables"][0]["evidence"]
    # Before: a NULL boundary emptied the last ranges — rows 2700/2700 and "proven".
    assert block["state"] != "passed"
    assert _report(job_id)["verdict"] != "proven"
    assert ev["population"]["rows_source"] == 3001
    assert ev["population"]["rows_destination"] == 3000
    assert ev["population"]["population_check"]["ok"] is True
    assert ev["differences"]["missing_in_destination"] == 1
    nk = ev["differences"]["rows_without_key"]
    assert (nk["rows_source"], nk["rows_destination"], nk["missing_in_destination"]) == (1, 0, 1)
    assert nk["missing_content_examples"][0][1] == "ghost"


def test_a_null_key_row_no_longer_hides_wrong_values_in_the_last_ranges(no_pk_source) -> None:
    src, dst = no_pk_source
    _sql(f"UPDATE \"{dst}\" SET name = 'WRONG', amt = 0 WHERE id BETWEEN 2702 AND 2999")
    job_id = _job(src, dst)
    block = _verify(job_id, config=_cfg())
    ev = block["tables"][0]["evidence"]
    # Before: still "proven" with 298 wrong rows in the emptied range.
    assert ev["differences"]["changed_rows"] == 298
    assert _report(job_id)["verdict"] != "proven"


def test_null_key_rows_on_both_sides_are_compared_as_whole_rows_in_a_chunked_stream() -> None:
    """The streamed pass, chunked into key ranges, dropped NULL-key rows from every range."""
    from services.mongodb_service import get_mongodb_service

    src, dst = f"psd1_nk_s_{uuid.uuid4().hex[:8]}", f"psd1_nk_d_{uuid.uuid4().hex[:8]}"
    ddl = "id bigint, name varchar(40), amt numeric(12,2), ts timestamp(6)"
    _sql(f'CREATE TABLE "{src}" ({ddl})', f'INSERT INTO "{src}" {FILL}',
         f'CREATE TABLE "{dst}" ({ddl})', f'INSERT INTO "{dst}" SELECT id, upper(name), amt, ts FROM "{src}"',
         f"INSERT INTO \"{src}\" VALUES (NULL, 'ghost', 1, timestamp '2024-01-01')",
         f"INSERT INTO \"{dst}\" VALUES (NULL, 'NOT THE GHOST', 1, timestamp '2024-01-01')")
    try:
        job_id = _job(src, dst)
        svc = get_mongodb_service()
        request = svc.get_job(job_id)["transfer_request"]
        request["mappings"][1]["transform"] = "upper"  # name is compared by the streamed pass
        svc.update_job_fields(job_id, {"transfer_request": request})
        block = _verify(job_id, config=_cfg(mode="R"))
        ev = block["tables"][0]["evidence"]
        assert ev["coverage"]["stream"]["columns"] == ["name"]
        assert ev["population"]["rows_source"] == ev["population"]["rows_destination"] == 3001
        nk = ev["differences"]["rows_without_key"]
        assert (nk["missing_in_destination"], nk["extra_in_destination"]) == (1, 1)
        assert block["state"] != "passed"
        # The same NULL-key row on both sides (after the transform) is a match.
        _sql(f"UPDATE \"{dst}\" SET name = 'GHOST' WHERE id IS NULL")
        job_id = _job(src, dst)
        svc.update_job_fields(job_id, {"transfer_request": request})
        block = _verify(job_id, config=_cfg(mode="R"))
        assert block["state"] == "passed", block["tables"][0]["evidence"]["differences"]
        assert _report(job_id)["verdict"] == "proven"
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def test_a_row_in_no_unit_fails_the_population_check(monkeypatch) -> None:
    """Any hole in the key ranges (here: one key left out on both sides) is caught by COUNT(*)."""
    from services.parity_scale.dialects.sqlbase import SqlSide

    src, dst = f"psd1_nk_s_{uuid.uuid4().hex[:8]}", f"psd1_nk_d_{uuid.uuid4().hex[:8]}"
    ddl = "id bigint PRIMARY KEY, name varchar(40), amt numeric(12,2), ts timestamp(6)"
    _sql(f'CREATE TABLE "{src}" ({ddl})', f'INSERT INTO "{src}" {FILL}',
         f'CREATE TABLE "{dst}" ({ddl})', f'INSERT INTO "{dst}" SELECT * FROM "{src}"',
         f"UPDATE \"{dst}\" SET name = 'WRONG' WHERE id = 1234")
    real = SqlSide.where

    def holed(self, lo, hi, **kw):
        sql, params = real(self, lo, hi, **kw)
        return f"{sql} AND {self.quote('id')} <> 1234", params

    monkeypatch.setattr(SqlSide, "where", holed)
    try:
        job_id = _job(src, dst)
        block = _verify(job_id, config=_cfg())
        ev = block["tables"][0]["evidence"]
        check = ev["population"]["population_check"]
        assert check["ok"] is False
        assert check["source"] == {"rows": 3000, "in_units": 2999}
        assert ev["outcome"] == "not_fully_compared"
        assert any("row count check failed" in n["reason"] for n in ev["coverage"]["not_compared"])
        assert block["state"] != "passed"
        assert _report(job_id)["verdict"] != "proven"
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')
