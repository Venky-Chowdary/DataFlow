"""QA round 11 (build 9c75ada4): mirror partial write, and salvage accounting.

QA M5 "mirror without a PK": refused, but "From the first run: 1000 rows were
written before the job failed on the missing primary key". Reproduction on
the round-11 code, QA's exact flow: MCP ``start_transfer`` -> confirm, mirror
soft (the default), a keyless source table, first run into a destination that
does not exist yet. The run must be refused before any write: no destination
table, no rows.

Salvage accounting (from the root-cause analysis of the "aborted" family):
the PostgreSQL writer's row-by-row salvage counted a row as written once its
savepoint was released; when a later ``ROLLBACK TO SAVEPOINT`` failed it rolled
the whole transaction back (discarding those rows) and kept counting them. The
reported ``rows_written`` must equal what the destination holds.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r

pytestmark = pytest.mark.timeout(600)

NOTES = {1: "Zoë ✓", 2: None, 3: "", 4: "   ", 5: "日本語"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize("rows", [5, 1000])
def test_mirror_of_a_keyless_table_writes_nothing_on_the_first_run(client, rows):
    token = uuid.uuid4().hex[:8]
    src, dst = f"r11mk_s_{token}", f"r11mk_d_{token}"
    r.pg(f'CREATE TABLE public."{src}" (id integer, note text)')  # no primary key
    r.pg(f'INSERT INTO public."{src}" SELECT g, \'n\' || g FROM generate_series(1, {int(rows)}) g')
    for i, note in NOTES.items():
        r.pg(f'UPDATE public."{src}" SET note = %s WHERE id = %s', (note, i))
    source = r.connector("source", r.pg_cfg())
    dest = r.connector("destination", r.pg_cfg())
    args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest,
                dest_table=dst, sync_mode="full_refresh_mirror")
    try:
        for _attempt in (1, 2):  # run 2 against the same (still absent) table
            ok, staged = r.stage(client, args)
            if ok and not h.blockers_of(staged) and isinstance(staged, dict) and staged.get("ack_id"):
                job = r.confirm_job(client, staged, expect=None)
                assert job.get("status") != "completed", job
            else:
                text = str(staged) if not ok else " ".join(h.blockers_of(staged))
                assert "key" in text.lower(), text
            assert not r.table_exists(dst), "a refused mirror created the destination"
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


class _LostSavepointCursor:
    """psycopg2 cursor whose second ``ROLLBACK TO SAVEPOINT df_row_sp`` fails.

    Stands in for a savepoint the server no longer has (a dropped connection
    mid-salvage, a pooler that ended the transaction): the writer then has to
    roll back the whole transaction, and must stop counting the rows it held.
    """

    def __init__(self, inner, state):
        self._inner, self._state = inner, state

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else ""
        if text.strip().upper() == "ROLLBACK TO SAVEPOINT DF_ROW_SP":
            self._state["rollbacks"] += 1
            if self._state["rollbacks"] == 2:
                raise RuntimeError("savepoint df_row_sp does not exist")
        return self._inner.execute(query, params)

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._inner.close()
        return False


def test_salvage_reports_only_rows_the_destination_holds(monkeypatch):
    psycopg2 = pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows

    cfg = r.pg_cfg()
    dst = f"r11sv_{uuid.uuid4().hex[:8]}"
    # The destination's CHECK refuses rows 2 and 4 at INSERT time, so the
    # chunk fails and the writer salvages row by row.
    r.pg(f'CREATE TABLE public."{dst}" (id integer PRIMARY KEY, qty integer '
         "CHECK (qty IS NULL OR qty < 100), note text)")
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], user=cfg["username"],
                            password=cfg["password"], dbname=cfg["database"])
    state = {"rollbacks": 0}
    real_cursor = conn.cursor

    class _Conn:
        def __getattr__(self, name):
            return getattr(conn, name)

        def __setattr__(self, name, value):
            setattr(conn, name, value)

        def cursor(self, *a, **k):
            return _LostSavepointCursor(real_cursor(*a, **k), state)

    rows = [["1", "7", "Zoë"], ["2", "500", None], ["3", None, ""], ["4", "900", "   "], ["5", "8", "日本語"]]
    try:
        result = write_mapped_rows(
            host=cfg["host"], port=cfg["port"], database=cfg["database"],
            username=cfg["username"], password=cfg["password"], schema="public",
            connection_string="", ssl=False, table_name=dst,
            headers=["id", "qty", "note"], data_rows=rows,
            mappings=[{"source": c, "target": c} for c in ("id", "qty", "note")],
            column_types={"id": "INTEGER", "qty": "INTEGER", "note": "TEXT"},
            create_table=False, error_policy="quarantine",
            connection=_Conn(), close_connection=False,
        )
        assert state["rollbacks"] >= 2, state
        held = r.pg(f'SELECT count(*) FROM public."{dst}"')[0][0]
        assert result.rows_written == held, (result.rows_written, held, result.error)
        assert not result.ok, "the writer reported a clean salvage over a rolled-back transaction"
    finally:
        conn.close()
        h.drop_table("postgresql", dst)
