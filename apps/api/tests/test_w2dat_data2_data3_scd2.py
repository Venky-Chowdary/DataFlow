"""DATA-2 / DATA-3: SCD2 applies atomically and refuses an emptied source.

DATA-2: with a failure injected at id 2500 of 3,000 keys, keys 1-2000 were
closed and re-versioned while 2001-3000 stayed v1, and the run said nothing
was written. DATA-3: an emptied source closed every current version, then the
run reported failed and did not undo it.
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live

N = 3000
BAD = 2500


def _seed(src: str, version: str, bad: int | None = None) -> None:
    live.pg(f'TRUNCATE public."{src}"')
    live.pg(
        f'INSERT INTO public."{src}" SELECT g, %s || g, CASE WHEN g = %s THEN 999999 ELSE g END '
        "FROM generate_series(1, %s) g",
        (f"{version}-", bad or -1, N),
    )


def _history(dst: str) -> list[tuple]:
    return live.pg(
        f'SELECT id, v, is_current FROM public."{dst}" ORDER BY id, v'
    )


def test_scd2_failure_mid_run_leaves_history_untouched():
    live.need_pg()
    u = live.uid()
    src, dst = f"w2dat_s2_{u}", f"w2dat_s2d_{u}"
    fn = f"w2dat_s2fn_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, v text, n int)')
        _seed(src, "v1")
        first = live.run(live.ep_pg(src), live.ep_pg(dst), "scd2", key="id", cols=["id", "v", "n"])
        assert first.success, first.error
        before = _history(dst)
        assert len(before) == N
        live.pg(
            f"CREATE FUNCTION {fn}() RETURNS trigger AS $$ BEGIN IF NEW.n = 999999 THEN "
            "RAISE EXCEPTION 'w2dat injected failure'; END IF; RETURN NEW; END $$ LANGUAGE plpgsql"
        )
        live.pg(
            f'CREATE TRIGGER {fn}_t BEFORE INSERT OR UPDATE ON public."{dst}" '
            f"FOR EACH ROW EXECUTE FUNCTION {fn}()"
        )
        _seed(src, "v2", bad=BAD)
        second = live.run(live.ep_pg(src), live.ep_pg(dst), "scd2", key="id", cols=["id", "v", "n"])
        assert not second.success
        after = _history(dst)
        assert after == before, f"{sum(1 for r in after if r[1].startswith('v2'))} v2 versions landed"
        assert second.records_transferred == 0
        assert "rolled back" in (second.error or "") and "no version was" in (second.error or "")
    finally:
        live.drop_pg(src, dst)
        live.pg(f"DROP FUNCTION IF EXISTS {fn}() CASCADE")
