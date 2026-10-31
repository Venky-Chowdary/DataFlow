"""QA round 11 (build 9c75ada4) M3: one stored key blocks an incremental append.

QA: incremental_append (cursor ``updated_at``) after +10 inserts and 5 updates.
The writer refused the batch at Run because the 5 updated keys already exist
in the destination, so the 10 new rows did not load either. Validate had
passed: its collision probe judged only the preview rows, which did not
contain the updated keys. The refusal named "upsert/merge or overwrite", not
the sync modes that land an updated row (incremental_deduped / upsert) or the
``dedupe_skip`` duplicate policy.

Contract now: Validate probes every key the incremental read will deliver
(the delta past the watermark, not a preview), so the refusal is predicted
before Run and nothing is written; the message names incremental_deduped /
upsert and dedupe_skip. Execute's own preflight runs the same whole-delta
probe, so the run is refused before its first batch — never after an earlier
batch committed.

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, PG -> PG,
with NULL, '', whitespace and unicode cells; run 2 goes into the table run 1
created.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r

pytestmark = pytest.mark.timeout(900)

NOTES = {1: "Zoë ✓", 2: None, 3: "", 4: "   ", 5: "日本語"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_incremental_append_refusal_is_predicted_by_validate_and_writes_nothing(client):
    token = uuid.uuid4().hex[:8]
    src, dst = f"r11ap_s_{token}", f"r11ap_d_{token}"
    r.pg(f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, note text, rev integer NOT NULL)')
    r.pg(f'INSERT INTO public."{src}" SELECT g, \'n\' || g, 1 FROM generate_series(1, 2000) g')
    for i, note in NOTES.items():
        r.pg(f'UPDATE public."{src}" SET note = %s WHERE id = %s', (note, i))
    source = r.connector("source", r.pg_cfg())
    dest = r.connector("destination", r.pg_cfg())
    args = dict(source_connector_id=source, source_table=src, dest_connector_id=dest,
                dest_table=dst, sync_mode="incremental_append", cursor_column="rev",
                cursor_semantics="monotonic_sequence", primary_key="id")
    try:
        r.run(client, args)
        assert r.pg(f'SELECT count(*) FROM public."{dst}"') == [(2000,)]
        # The destination enforces the key (carried from the source).
        assert r.pg(
            "SELECT 1 FROM pg_index WHERE indrelid = %s::regclass AND indisprimary",
            (f"public.{dst}",),
        )

        # +10 inserts, then 5 updates of keys far past any preview page; the
        # updated rows carry the newest cursor values.
        r.pg(f'INSERT INTO public."{src}" SELECT g, \'new\' || g, 2 FROM generate_series(2001, 2010) g')
        r.pg(f'UPDATE public."{src}" SET note = \'ⓤpd\', rev = 3 WHERE id IN (1500, 1600, 1700, 1800, 1900)')

        ok, staged = r.stage(client, args)
        text = str(staged) if not ok else " ".join(h.blockers_of(staged))
        assert (not ok) or h.blockers_of(staged), "Validate passed an append Run will refuse"
        assert "incremental_deduped" in text and "upsert" in text and "dedupe_skip" in text, text
        # Nothing moved: the 10 new rows are not half-loaded.
        assert r.pg(f'SELECT count(*) FROM public."{dst}"') == [(2000,)]

        # The remedy the refusal names lands the update and the new rows.
        job = r.run(client, {**args, "sync_mode": "incremental_deduped"})
        assert job.get("status") == "completed"
        got = dict(r.pg(f'SELECT id, note FROM public."{dst}" WHERE id IN (1, 2, 3, 4, 5, 1500, 2010)'))
        assert got == {**NOTES, 1500: "ⓤpd", 2010: "new2010"}
        assert r.pg(f'SELECT count(*) FROM public."{dst}"') == [(2010,)]
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)
