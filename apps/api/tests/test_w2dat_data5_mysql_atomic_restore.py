"""DATA-5: a failed MySQL upsert/mirror/overwrite leaves the destination as it was.

Before the fix an FK violation at id 2500 of 3,000 left 2,048 rows rewritten as
v2 on upsert and mirror, and the messages contradicted themselves ("2048
committed" next to "3000 rows already written"; "partial write" on an
overwrite whose table had been restored).
"""

from __future__ import annotations

import pytest

from tests import w2dat_live as live

N = 3000
BAD = 2500


def _seed(src: str, version: str, bad: bool) -> None:
    live.pg(f'TRUNCATE public."{src}"')
    live.pg(
        f'INSERT INTO public."{src}" SELECT g, %s || g, CASE WHEN %s AND g = %s THEN 999999 ELSE g END '
        "FROM generate_series(1, %s) g",
        (f"{version}-", bad, BAD, N),
    )


@pytest.mark.parametrize("mode", ["upsert", "mirror", "full_refresh_overwrite"])
def test_mysql_failure_mid_write_restores_and_says_so(mode):
    live.need_pg()
    live.need_mysql()
    u = live.uid()
    src, dst, par = f"w2dat_d5_{u}", f"w2dat_d5d_{u}", f"w2dat_d5p_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, v text, n int)')
        _seed(src, "v1", bad=False)
        first = live.run(live.ep_pg(src), live.ep_my(dst), mode, key="id", cols=["id", "v", "n"])
        assert first.success, first.error
        ctype = live.my(
            "SELECT COLUMN_TYPE FROM information_schema.columns WHERE table_schema = DATABASE() "
            "AND table_name = %s AND column_name = 'n'", (dst,))[0][0]
        live.my(f"CREATE TABLE `{par}` (id {ctype} PRIMARY KEY)")
        live.my("SET SESSION cte_max_recursion_depth = 100000")
        conn = live.my_conn()
        with conn.cursor() as cur:
            cur.execute("SET SESSION cte_max_recursion_depth = 100000")
            cur.execute(f"INSERT INTO `{par}` WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL "
                        f"SELECT i + 1 FROM s WHERE i < {N}) SELECT i FROM s")
        conn.close()
        live.my(f"ALTER TABLE `{dst}` ADD CONSTRAINT w2dat_fk_{u} FOREIGN KEY (n) REFERENCES `{par}` (id)")
        before = sorted(live.my(f"SELECT id, v, n FROM `{dst}`"))
        _seed(src, "v2", bad=True)
        second = live.run(live.ep_pg(src), live.ep_my(dst), mode, key="id", cols=["id", "v", "n"])
        assert not second.success
        after = sorted(live.my(f"SELECT id, v, n FROM `{dst}`"))
        changed = sum(1 for a, b in zip(before, after) if a != b) + abs(len(after) - len(before))
        assert after == before, f"{changed} row(s) changed; error={second.error}"
        err = second.error or ""
        assert "committed" not in err and "already written" not in err, err
        assert "restored" in err, err
        assert second.records_transferred == 0
    finally:
        live.drop_my(dst)
        live.drop_my(par)
        live.drop_pg(src)
        for (name,) in live.my("SELECT table_name FROM information_schema.tables WHERE "
                               "table_schema = DATABASE() AND table_name LIKE %s", (f"{dst}%",)):
            live.drop_my(name)
