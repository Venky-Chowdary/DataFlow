"""An Oracle read right after CREATE TABLE is re-run on ORA-01466, never duplicated.

The query runner reads in a read-only transaction (audit M-5). Oracle maps
SCNs to time in steps of a few seconds, so a table created seconds earlier
answers ORA-01466 "unable to read data - table definition has changed" in
that transaction although nothing is wrong. On Oracle Database Free (the
oracle-live CI job) the Pilot sample of a just-created table failed that way
(test_d155_oracle_tz_source_live, the R11 preview sampler).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


class _OraError(Exception):
    pass


def _definition_changed() -> Exception:
    try:
        raise _OraError("ORA-01466: unable to read data - table definition has changed")
    except _OraError as cause:
        try:
            raise RuntimeError("Query failed") from cause
        except RuntimeError as wrapped:
            return wrapped


@pytest.fixture
def router(monkeypatch):
    import src.routers.query_router as qr

    monkeypatch.setattr(qr.time, "sleep", lambda s: None)
    return qr


def _connector(kind: str = "oracle") -> SimpleNamespace:
    return SimpleNamespace(type=kind)


def test_ora_01466_before_any_row_is_re_run_until_it_reads(router, monkeypatch) -> None:
    calls: list[int] = []

    def once(connector, body, **kwargs):
        calls.append(1)
        if len(calls) < 3:
            raise _definition_changed()
        return ["row"], ["c"], {}, False

    monkeypatch.setattr(router, "_run_sql_query_once", once)
    assert router._run_sql_query(_connector(), SimpleNamespace(query="SELECT 1 FROM T")) == (
        ["row"], ["c"], {}, False,
    )
    assert len(calls) == 3


def test_the_retry_is_bounded_and_the_error_surfaces(router, monkeypatch) -> None:
    calls: list[int] = []

    def once(connector, body, **kwargs):
        calls.append(1)
        raise _definition_changed()

    monkeypatch.setattr(router, "_run_sql_query_once", once)
    with pytest.raises(RuntimeError, match="Query failed"):
        router._run_sql_query(_connector(), SimpleNamespace(query="SELECT 1 FROM T"))
    assert len(calls) == 1 + router._ORA_DEFINITION_RETRIES


def test_nothing_is_re_run_once_a_row_was_streamed(router, monkeypatch) -> None:
    calls: list[int] = []
    got: list[dict] = []

    def once(connector, body, *, on_row=None, **kwargs):
        calls.append(1)
        on_row(["c"], {"c": 1})
        raise _definition_changed()

    monkeypatch.setattr(router, "_run_sql_query_once", once)
    with pytest.raises(RuntimeError):
        router._run_sql_query(
            _connector(), SimpleNamespace(query="SELECT 1 FROM T"),
            on_row=lambda cols, rec: got.append(rec),
        )
    assert len(calls) == 1 and got == [{"c": 1}]


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "sqlserver"])
def test_other_engines_and_other_errors_are_not_retried(router, monkeypatch, kind) -> None:
    calls: list[int] = []

    def once(connector, body, **kwargs):
        calls.append(1)
        raise _definition_changed()

    monkeypatch.setattr(router, "_run_sql_query_once", once)
    with pytest.raises(RuntimeError):
        router._run_sql_query(_connector(kind), SimpleNamespace(query="SELECT 1"))
    assert len(calls) == 1

    calls.clear()

    def other(connector, body, **kwargs):
        calls.append(1)
        raise RuntimeError("ORA-00942: table or view does not exist")

    monkeypatch.setattr(router, "_run_sql_query_once", other)
    with pytest.raises(RuntimeError):
        router._run_sql_query(_connector("oracle"), SimpleNamespace(query="SELECT 1 FROM T"))
    assert len(calls) == 1
