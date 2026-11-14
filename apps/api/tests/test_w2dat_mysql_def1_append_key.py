"""MYSQL-DEF-1: append on MySQL with local_infile=OFF does not enforce the key.

With LOAD DATA unavailable (MySQL 8's default local_infile=OFF) the row path
created a full_refresh_append destination with an enforced key on the declared
id, so the second append was refused ("Destination already stores these
keys"). The fast path, and the PostgreSQL/SQLite paths, create no such key.
"""

from __future__ import annotations

from tests import w2dat_live as live


def test_append_sync_mode_leaves_the_key_unenforced_unless_mysql_requires_one(monkeypatch):
    from services import duplicate_policy as dp

    for mode in ("full_refresh_append", "append"):
        decided = dp.resolve_duplicate_policy(sync_mode=mode, business_key=True, write_mode="insert")
        assert dp.create_allows_repeated_keys(decided, "postgresql", {}), mode
        monkeypatch.setattr(dp, "_mysql_requires_primary_key", lambda cfg: False)
        assert dp.create_allows_repeated_keys(decided, "mysql", {}), mode
        # sql_require_primary_key=ON: the declared key is the only key a CREATE can have.
        monkeypatch.setattr(dp, "_mysql_requires_primary_key", lambda cfg: True)
        assert not dp.create_allows_repeated_keys(decided, "mysql", {}), mode
    # incremental_append merges its watermark overlap on the key the next run (d334).
    for mode, write_mode in (("full_refresh_overwrite", "insert"), ("upsert", "upsert"),
                             ("incremental_append", "insert")):
        decided = dp.resolve_duplicate_policy(sync_mode=mode, business_key=True, write_mode=write_mode)
        assert not dp.create_allows_repeated_keys(decided, "postgresql", {}), mode


def test_second_append_into_mysql_lands_with_local_infile_off():
    live.need_pg()
    live.need_mysql()
    u = live.uid()
    src, dst = f"w2dat_md1_{u}", f"w2dat_md1d_{u}"
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int, v text)')
        live.pg(f'INSERT INTO public."{src}" VALUES (1, \'a\'), (2, \'b\')')
        for _ in range(2):
            res = live.run(live.ep_pg(src), live.ep_my(dst), "full_refresh_append",
                           key="id", cols=["id", "v"])
            assert res.success, res.error
        assert sorted(live.my(f"SELECT id, v FROM `{dst}`")) == [(1, "a"), (1, "a"), (2, "b"), (2, "b")]
    finally:
        live.drop_pg(src)
        live.drop_my(dst)
