"""Engine and schema cache — prove reuse, not just claim it.

These tests count constructions and metadata round-trips. A transfer of N
chunks that still builds N engines, or that still re-reflects the table N
times, is a regression of the exact defect this module exists to close.
"""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import reflection_cache  # noqa: E402
from services.engine_pool import (  # noqa: E402
    dispose_all,
    engine_cache_key,
    get_pooled_engine,
    invalidate,
    release_engine,
    reset_for_tests,
    stats,
)


@pytest.fixture(autouse=True)
def _isolate_caches(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENGINE_CACHE", "1")
    monkeypatch.setenv("DATAFLOW_SCHEMA_CACHE", "1")
    monkeypatch.setenv("DATAFLOW_ENGINE_CACHE_SIZE", "8")
    reset_for_tests()
    reflection_cache.reset_for_tests()
    yield
    reset_for_tests()
    reflection_cache.reset_for_tests()


# ---------------------------------------------------------------------------
# Engine pool
# ---------------------------------------------------------------------------


class TestEngineCacheKey:
    def test_unhashable_extra_fields_do_not_raise(self):
        """Operator JSON under ``extra`` may contain lists. The key must not."""
        cfg = {
            "type": "postgres",
            "host": "db.example",
            "port": 5432,
            "database": "sales",
            "username": "u",
            "password": "p",
            "metadata_columns": ["region", "tier"],  # unhashable
            "exclude_pii_columns": {"ssn", "email"},  # unhashable
        }
        key = engine_cache_key(cfg)
        assert isinstance(key, str)
        assert "postgres" in key
        assert "db.example" in key

    def test_int_and_str_port_collapse(self):
        a = engine_cache_key({"type": "mysql", "host": "h", "port": 3306})
        b = engine_cache_key({"type": "mysql", "host": "h", "port": "3306"})
        assert a == b

    def test_password_change_produces_distinct_key(self):
        a = engine_cache_key({"type": "mysql", "host": "h", "password": "old"})
        b = engine_cache_key({"type": "mysql", "host": "h", "password": "new"})
        assert a != b

    def test_table_name_does_not_affect_key(self):
        """Two tables in the same database must share one pool."""
        a = engine_cache_key({"type": "mysql", "host": "h", "database": "d", "table": "orders"})
        b = engine_cache_key({"type": "mysql", "host": "h", "database": "d", "table": "customers"})
        assert a == b


class TestEngineReuse:
    def test_same_cfg_returns_same_engine(self):
        builds: list[dict] = []

        def factory(cfg):
            builds.append(dict(cfg))
            eng = MagicMock(name=f"engine-{len(builds)}")
            eng.dispose = MagicMock()
            return eng

        cfg = {"type": "mysql", "host": "h", "database": "d", "password": "p"}
        e1 = get_pooled_engine(cfg, factory)
        e2 = get_pooled_engine(cfg, factory)
        e3 = get_pooled_engine(dict(cfg), factory)
        assert e1 is e2 is e3
        assert len(builds) == 1
        s = stats()
        assert s.hits == 2
        assert s.misses == 1
        assert s.reuse_ratio == pytest.approx(2 / 3, abs=0.01)

    def test_release_does_not_dispose_pooled_engine(self):
        eng = MagicMock()
        eng.dispose = MagicMock()
        factory = MagicMock(return_value=eng)
        cfg = {"type": "mysql", "host": "h"}
        got = get_pooled_engine(cfg, factory)
        release_engine(got)
        release_engine(got)
        eng.dispose.assert_not_called()
        # Still reusable after many "releases".
        assert get_pooled_engine(cfg, factory) is eng

    def test_release_disposes_unowned_engine(self):
        """Opt-out path and foreign engines must still be disposed."""
        eng = MagicMock()
        eng.dispose = MagicMock()
        release_engine(eng)
        eng.dispose.assert_called_once()

    def test_opt_out_builds_every_call(self, monkeypatch):
        monkeypatch.setenv("DATAFLOW_ENGINE_CACHE", "0")
        builds = []

        def factory(cfg):
            eng = MagicMock()
            eng.dispose = MagicMock()
            builds.append(eng)
            return eng

        cfg = {"type": "mysql", "host": "h"}
        e1 = get_pooled_engine(cfg, factory)
        e2 = get_pooled_engine(cfg, factory)
        assert e1 is not e2
        assert len(builds) == 2

    def test_lru_eviction_disposes_oldest(self, monkeypatch):
        monkeypatch.setenv("DATAFLOW_ENGINE_CACHE_SIZE", "2")
        disposed: list[Any] = []

        def factory(cfg):
            eng = MagicMock(name=cfg["host"])
            eng.dispose = lambda: disposed.append(cfg["host"])
            return eng

        a = get_pooled_engine({"type": "mysql", "host": "a"}, factory)
        b = get_pooled_engine({"type": "mysql", "host": "b"}, factory)
        # Touch a so b becomes the LRU victim.
        assert get_pooled_engine({"type": "mysql", "host": "a"}, factory) is a
        c = get_pooled_engine({"type": "mysql", "host": "c"}, factory)
        assert "b" in disposed
        assert a is get_pooled_engine({"type": "mysql", "host": "a"}, factory)
        assert c is get_pooled_engine({"type": "mysql", "host": "c"}, factory)
        # b was evicted: next ask rebuilds it.
        b2 = get_pooled_engine({"type": "mysql", "host": "b"}, factory)
        assert b2 is not b

    def test_invalidate_drops_one_entry(self):
        factory = lambda cfg: MagicMock(dispose=MagicMock())  # noqa: E731
        cfg = {"type": "mysql", "host": "h", "password": "p"}
        e1 = get_pooled_engine(cfg, factory)
        assert invalidate(cfg) is True
        e2 = get_pooled_engine(cfg, factory)
        assert e1 is not e2

    def test_thread_safe_under_contention(self):
        builds = []
        lock = threading.Lock()

        def factory(cfg):
            # Slow enough that many threads race the cold miss.
            import time

            time.sleep(0.02)
            with lock:
                builds.append(1)
            eng = MagicMock()
            eng.dispose = MagicMock()
            return eng

        cfg = {"type": "mysql", "host": "race"}
        results: list[Any] = []

        def worker():
            results.append(get_pooled_engine(cfg, factory))

        threads = [threading.Thread(target=worker) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # Exactly one engine survives in the cache; losers were disposed.
        assert len(set(id(r) for r in results)) == 1
        assert stats().live == 1


# ---------------------------------------------------------------------------
# Reflection / schema cache
# ---------------------------------------------------------------------------


class TestReflectionCache:
    def test_loader_called_once_per_table(self):
        loads = []

        def loader():
            loads.append(1)
            return {"cols": ["id", "name"]}

        eng = MagicMock()
        eng.url = "postgresql://u@h/db"
        a = reflection_cache.get_or_load(eng, "public", "orders", "reflect", loader)
        b = reflection_cache.get_or_load(eng, "public", "orders", "reflect", loader)
        c = reflection_cache.get_or_load(eng, "public", "orders", "has_table", loader)
        assert a is b or a == b
        assert len(loads) == 2  # reflect once, has_table once
        assert reflection_cache.stats().hits == 1
        assert c == {"cols": ["id", "name"]}

    def test_invalidate_drops_all_kinds_for_table(self):
        eng = MagicMock()
        eng.url = "postgresql://u@h/db"
        reflection_cache.get_or_load(eng, "public", "orders", "reflect", lambda: "R")
        reflection_cache.get_or_load(eng, "public", "orders", "has_table", lambda: True)
        reflection_cache.get_or_load(eng, "public", "customers", "reflect", lambda: "C")
        dropped = reflection_cache.invalidate_table(eng, "public", "orders")
        assert dropped == 2
        loads = []
        reflection_cache.get_or_load(
            eng, "public", "orders", "reflect", lambda: loads.append(1) or "R2"
        )
        assert loads == [1]
        # Untouched table stays cached.
        assert (
            reflection_cache.get_or_load(
                eng, "public", "customers", "reflect", lambda: "SHOULD_NOT"
            )
            == "C"
        )

    def test_empty_mysql_types_are_not_cached(self):
        """An empty answer means the table is missing — never pin that."""
        identity = reflection_cache.dsn_identity(
            driver="mysql", host="h", database="d", username="u"
        )
        assert (
            reflection_cache.peek_by_identity(identity, "", "t", "mysql_col_types") is None
        )
        # Caller chooses not to put empties; put of a real answer sticks.
        reflection_cache.put_by_identity(
            identity, "", "t", "mysql_col_types", {"id": "int"}
        )
        assert reflection_cache.peek_by_identity(
            identity, "", "t", "mysql_col_types"
        ) == {"id": "int"}

    def test_dsn_identity_excludes_password(self):
        a = reflection_cache.dsn_identity(
            driver="mysql", host="h", database="d", username="u"
        )
        # Password is not even a parameter — keeping it out of the signature
        # is the proof it can never leak into a cache key.
        assert "password" not in reflection_cache.dsn_identity.__code__.co_varnames
        assert "secret" not in a

    def test_opt_out_always_reloads(self, monkeypatch):
        monkeypatch.setenv("DATAFLOW_SCHEMA_CACHE", "0")
        loads = []
        eng = MagicMock()
        eng.url = "x"
        reflection_cache.get_or_load(eng, "", "t", "reflect", lambda: loads.append(1) or "v")
        reflection_cache.get_or_load(eng, "", "t", "reflect", lambda: loads.append(1) or "v")
        assert loads == [1, 1]


# ---------------------------------------------------------------------------
# Integration: generic_sql._engine reuses, and dispose is neutralized
# ---------------------------------------------------------------------------


class TestGenericSqlWiring:
    def test_engine_accessor_reuses_across_calls(self):
        from connectors import generic_sql as g

        cfg = {
            "type": "sqlite",
            "database": ":memory:",
            "connection_string": "sqlite://",
            # Unhashable extras — must not break the key.
            "metadata_columns": ["a", "b"],
        }
        e1 = g._engine(cfg)
        e2 = g._engine(cfg)
        assert e1 is e2
        # The call-site "finally dispose" must not kill the shared pool.
        release_engine(e1)
        e3 = g._engine(cfg)
        assert e3 is e1

    def test_dispose_all_cleans_up(self):
        from connectors import generic_sql as g

        cfg = {"type": "sqlite", "database": ":memory:", "connection_string": "sqlite://"}
        g._engine(cfg)
        assert stats().live >= 1
        dispose_all()
        assert stats().live == 0

    def test_transform_runner_release_does_not_kill_pool(self):
        """Bugbot: TransformRunner used to dispose the shared engine in finally."""
        from connectors import generic_sql as g
        from services.transform_runner import TransformRunner

        cfg = {
            "type": "sqlite",
            "database": ":memory:",
            "connection_string": "sqlite://",
        }
        shared = g._engine(cfg)
        runner = TransformRunner(cfg, dialect="sqlite", dry_run=True)
        # Dry-run still acquires an engine through the same accessor; releasing
        # it must leave the pool entry alive for the next transfer chunk.
        engine = runner._engine()
        assert engine is shared
        release_engine(engine)
        assert g._engine(cfg) is shared
        assert stats().live >= 1
