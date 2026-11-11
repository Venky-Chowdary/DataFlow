"""Re-review 4, 9b (Low): a parity widen forgets the column types this process cached.

The widen ran an ALTER but never told ``services.reflection_cache``, which the MySQL writer reads
for physical column types: an in-process write within the cache's TTL could bind against the old,
narrow type. Every DDL path drops the table's cached answers; the parity widen now does too.
(A re-sync or re-run job also forgets them when it starts, ``_forget_cached_table_shapes``.)
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from services import reflection_cache


@pytest.fixture()
def widen(monkeypatch):
    import connectors.schema_drift as schema_drift
    import services.dest_precount as dest_precount
    import services.parity_evidence as parity_evidence
    import src.transfer.adapters as adapters
    from types import SimpleNamespace

    class _Raw:
        def cursor(self):
            return object()

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    @contextmanager
    def engine(_dialect, _cfg):
        yield SimpleNamespace(raw_connection=lambda: _Raw())

    dest = SimpleNamespace(format="mysql", table="orders")
    monkeypatch.setattr(parity_evidence, "_endpoints", lambda job: (None, None, dest))
    monkeypatch.setattr(adapters, "resolve_connector_config", lambda endpoint: {"type": "mysql", "database": "d"})
    monkeypatch.setattr(dest_precount, "warehouse_sql_engine", engine)
    monkeypatch.setattr(schema_drift, "widen_existing_columns_native",
                        lambda *a, dry_run, **k: ["ALTER TABLE `orders` MODIFY COLUMN `code` VARCHAR(20)"])
    reflection_cache.reset_for_tests()
    yield
    reflection_cache.reset_for_tests()


def _cache_types() -> None:
    reflection_cache.get_or_load_by_identity("mysql://h/d", "d", "orders", "column_types", lambda: {"code": "varchar(3)"})


@pytest.mark.parametrize("dry_run, forgotten", [(False, True), (True, False)])
def test_an_executed_widen_drops_the_tables_cached_types(widen, dry_run, forgotten):
    from services.parity_fixes import widen_statements

    _cache_types()
    widen_statements({}, "orders", "code", "VARCHAR(20)", dry_run=dry_run)
    cached = reflection_cache.peek_by_identity("mysql://h/d", "d", "orders", "column_types")
    assert (cached is None) is forgotten
