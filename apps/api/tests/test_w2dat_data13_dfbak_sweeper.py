"""DATA-13: ``__dfbak`` backups a killed run left behind are swept, safely.

Before the fix every SIGKILLed strict PostgreSQL upsert left
``<table>__dfbak_<job>`` forever, and a second attempt of the same job
dropped the only pre-run image and recopied the half-written table.
"""

from __future__ import annotations

import time

from tests import w2dat_live as live


def test_sweep_decision_never_drops_a_live_or_unknown_job_backup():
    from services.dfbak_sweeper import sweep_decision

    day = 86400.0
    assert sweep_decision("completed", 1, 7 * day)
    assert sweep_decision("failed", 8 * day, 7 * day)
    assert not sweep_decision("failed", 1 * day, 7 * day)
    for status in ("running", "pending", "", "queued"):
        assert not sweep_decision(status, 365 * day, 7 * day), status


def test_sweep_drops_only_obsolete_stamped_backups():
    live.need_pg()
    from services.dfbak_sweeper import backup_comment, sweep_pg_backups

    u = live.uid()
    old = time.time() - 30 * 86400
    tables = {
        f"w2dat_t{u}__dfbak_done": backup_comment("jobdone", old),
        f"w2dat_t{u}__dfbak_fold": backup_comment("jobfailed", old),
        f"w2dat_t{u}__dfbak_fnew": backup_comment("jobfailed2", time.time()),
        f"w2dat_t{u}__dfbak_run": backup_comment("jobrunning", old),
        f"w2dat_t{u}__dfbak_mine": backup_comment("current", old),
        f"w2dat_t{u}__dfbak_unst": None,
    }
    status = {"jobdone": "completed", "jobfailed": "failed", "jobfailed2": "failed",
              "jobrunning": "running", "current": "completed"}
    try:
        for name, comment in tables.items():
            live.pg(f'CREATE TABLE public."{name}" (id int)')
            if comment:
                live.pg(f'COMMENT ON TABLE public."{name}" IS %s', (comment,))
        conn = live.pg_conn()
        try:
            with conn.cursor() as cur:
                dropped = sweep_pg_backups(cur, "public", keep_job="current",
                                           job_status=lambda j: status.get(j, ""))
        finally:
            conn.close()
        mine = sorted(d for d in dropped if d.startswith(f"w2dat_t{u}"))
        assert mine == sorted([f"w2dat_t{u}__dfbak_done", f"w2dat_t{u}__dfbak_fold"])
        left = {r[0] for r in live.pg(
            "SELECT tablename FROM pg_tables WHERE tablename LIKE %s", (f"w2dat_t{u}%",))}
        assert left == set(tables) - set(mine)
    finally:
        live.drop_pg(*tables)


def test_a_second_attempt_keeps_the_first_pre_run_image():
    live.need_pg()
    from connectors.table_manager import create_pg_table_backup

    u = live.uid()
    t = f"w2dat_bk_{u}"
    job = f"{u}feedbeef0000aaaa"
    try:
        live.pg(f'CREATE TABLE public."{t}" (id int primary key, v text)')
        live.pg(f'INSERT INTO public."{t}" VALUES (1, \'pre-run\')')
        pair = create_pg_table_backup(dict(live.PG), schema="public", table=t, job_id=job)
        assert pair is not None
        # The killed attempt half-wrote the live table.
        live.pg(f'UPDATE public."{t}" SET v = \'half-written\'')
        again = create_pg_table_backup(dict(live.PG), schema="public", table=t, job_id=job)
        assert again == pair
        assert live.pg(f'SELECT v FROM public."{pair["backup"]}"') == [("pre-run",)]
    finally:
        live.drop_pg(t, f"{t}__dfbak_{job[:8]}")
