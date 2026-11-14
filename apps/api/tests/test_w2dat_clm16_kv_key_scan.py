"""CLM-16: a Redis append onto stored keys is refused at Validate.

Before the fix the SQL destinations refused such an append at Validate, but
Redis passed Validate and failed at run time on the first stored key.
"""

from __future__ import annotations

from tests import w2dat_live as live


def test_redis_append_onto_existing_keys_is_refused_before_writing():
    live.need_pg()
    live.need_redis()
    import redis

    u = live.uid()
    src, prefix = f"w2dat_c16_{u}", f"w2dat_c16r_{u}"
    client = redis.Redis(host="127.0.0.1", port=6379, db=0)
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, v text)')
        live.pg(f'INSERT INTO public."{src}" VALUES (1, \'a\'), (2, \'b\')')
        first = live.run(live.ep_pg(src), live.ep_redis(prefix), "full_refresh_append", key="id", cols=["id", "v"])
        assert first.success, first.error
        before = sorted(client.scan_iter(match=f"{prefix}:*"))
        live.pg(f'INSERT INTO public."{src}" VALUES (3, \'c\')')
        live.pg(f'UPDATE public."{src}" SET v = \'a2\' WHERE id = 1')
        second = live.run(live.ep_pg(src), live.ep_redis(prefix), "full_refresh_append", key="id", cols=["id", "v"])
        assert not second.success
        err = (second.error or "").lower()
        assert "already" in err and ("stores" in err or "exist" in err), second.error
        assert second.records_transferred == 0
        assert sorted(client.scan_iter(match=f"{prefix}:*")) == before
    finally:
        live.redis_drop(prefix)
        live.drop_pg(src)


def test_balanced_append_keeps_stored_keys_and_lands_the_rest():
    """Balanced holds the stored keys out (SET NX keeps their values) instead of refusing."""
    live.need_pg()
    live.need_redis()
    import json

    import redis

    u = live.uid()
    src, prefix = f"w2dat_c16b_{u}", f"w2dat_c16br_{u}"
    client = redis.Redis(host="127.0.0.1", port=6379, db=0)
    try:
        live.pg(f'CREATE TABLE public."{src}" (id int primary key, v text)')
        live.pg(f'INSERT INTO public."{src}" VALUES (1, \'a\'), (2, \'b\')')
        run = lambda: live.run(live.ep_pg(src), live.ep_redis(prefix), "full_refresh_append",  # noqa: E731
                               key="id", cols=["id", "v"], validation_mode="balanced")
        assert run().success
        live.pg(f'INSERT INTO public."{src}" VALUES (3, \'c\')')
        live.pg(f'UPDATE public."{src}" SET v = \'a2\' WHERE id = 1')
        second = run()
        assert second.success, second.error
        docs = {json.loads(client.get(k))["id"]: json.loads(client.get(k))["v"]
                for k in client.scan_iter(match=f"{prefix}:*")}
        assert docs == {1: "a", 2: "b", 3: "c"}
    finally:
        live.redis_drop(prefix)
        live.drop_pg(src)
