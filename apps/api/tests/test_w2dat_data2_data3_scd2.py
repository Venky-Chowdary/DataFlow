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


@pytest.mark.parametrize("ack", [False, True])
def test_scd2_emptied_source_needs_an_acknowledgement(ack):
    live.need_pg()
    u = live.uid()
    src, dst = f"w2dat_s3_{u}", f"w2dat_s3d_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, v text)')
        live.pg(f'INSERT INTO public."{src}" SELECT g, \'v\' || g FROM generate_series(1, 5) g')
        first = live.run(live.ep_pg(src), live.ep_pg(dst), "scd2", key="id", cols=["id", "v"])
        assert first.success, first.error
        live.pg(f'DELETE FROM public."{src}"')
        second = live.run(
            live.ep_pg(src), live.ep_pg(dst), "scd2", key="id", cols=["id", "v"],
            extra_req={"empty_source_overwrite_acknowledged": ack},
        )
        current = live.pg(f'SELECT count(*) FROM public."{dst}" WHERE is_current')[0][0]
        if ack:
            assert second.success, second.error
            assert current == 0
        else:
            assert not second.success
            assert current == 5
            assert "0 rows" in (second.error or "") and "acknowledg" in (second.error or "").lower()
    finally:
        live.drop_pg(src, dst)


@pytest.mark.parametrize("kind", ["pg", "sqlite", "mysql"])
def test_mirror_of_an_emptied_source(kind, tmp_path):
    """DATA-6: mirror of an emptied source, acknowledged, removes every live row."""
    live.need_pg()
    u = live.uid()
    src, dst = f"w2dat_m6_{u}", f"w2dat_m6d_{u}"
    if kind == "mysql":
        live.need_mysql()
        dest = live.ep_my(dst)
    elif kind == "sqlite":
        dest = live.ep_sqlite(tmp_path / "m6.db", dst)
    else:
        dest = live.ep_pg(dst)

    def live_ids() -> list[int]:
        if kind == "pg":
            rows = live.pg(f'SELECT * FROM public."{dst}"')
            names = [r[0] for r in live.pg(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s "
                "ORDER BY ordinal_position", (dst,))]
        elif kind == "mysql":
            conn = live.my_conn()
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM `{dst}`")
                names = [d[0] for d in cur.description]
                rows = cur.fetchall()
            conn.close()
        else:
            import sqlite3

            with sqlite3.connect(tmp_path / "m6.db") as conn:
                cur = conn.execute(f'SELECT * FROM "{dst}"')
                names = [d[0] for d in cur.description]
                rows = cur.fetchall()
        out = []
        for r in rows:
            d = dict(zip(names, r))
            if d.get("_deleted") in (True, 1, "1", "true"):
                continue
            out.append(int(d["id"]))
        return sorted(out)

    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, v text)')
        live.pg(f'INSERT INTO public."{src}" SELECT g, \'v\' || g FROM generate_series(1, 5) g')
        first = live.run(live.ep_pg(src), dest, "mirror", key="id", cols=["id", "v"])
        assert first.success, first.error
        live.pg(f'DELETE FROM public."{src}" WHERE id IN (4, 5)')
        second = live.run(live.ep_pg(src), dest, "mirror", key="id", cols=["id", "v"])
        assert second.success, second.error
        assert live_ids() == [1, 2, 3]
        live.pg(f'DELETE FROM public."{src}"')
        refused = live.run(live.ep_pg(src), dest, "mirror", key="id", cols=["id", "v"])
        assert not refused.success
        assert live_ids() == [1, 2, 3]
        third = live.run(
            live.ep_pg(src), dest, "mirror", key="id", cols=["id", "v"],
            extra_req={"empty_source_overwrite_acknowledged": True},
        )
        assert live_ids() == [], third.error
        assert third.success, third.error
    finally:
        live.drop_pg(src, dst)
        if kind == "mysql":
            live.drop_my(dst)
