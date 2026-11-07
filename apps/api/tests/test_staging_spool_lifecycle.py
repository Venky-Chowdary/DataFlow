"""Internal staging tables must not accumulate inside a customer's schema.

Two engines write scratch tables into the destination: the mirror key staging
table (``_df_mirrorkeys_*``) and the SCD2/mirror source spool
(``_dataflow_stg_*``). Operator listings hide both prefixes, so an orphan left
by a run killed mid-flight was invisible *and* never cleaned — and enough of
them is why a real table can sort out of a bounded object listing. Both now use
one owner, ``services.staging_reaper``; these tests hold the rules that make a
sweep safe, and hold the spool's own cleanup on the success and failure paths.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

from services.staging_reaper import (  # noqa: E402
    STAGING_TTL_SECONDS,
    reap_orphan_staging,
    staging_age_seconds,
    staging_table_name,
)
from src.transfer.models import EndpointConfig  # noqa: E402
from src.transfer.stream_scd2 import SCD2_STAGING_PREFIX, _staging_endpoint  # noqa: E402


def _endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(format="postgresql", table=table, collection=table)


class _FakeResult:
    def __init__(self, rows: list[tuple[str, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[str, ...]]:
        return self._rows


class _FakeConn:
    """Records SQL; the catalog query answers with the names it was given."""

    def __init__(self, names: list[str], *, catalog_error: bool = False) -> None:
        self._names = names
        self._catalog_error = catalog_error
        self.statements: list[str] = []

    def execute(self, statement: Any, params: Any = None) -> _FakeResult:  # noqa: ARG002
        sql = str(statement)
        self.statements.append(sql)
        if "information_schema.tables" in sql:
            if self._catalog_error:
                raise RuntimeError("permission denied for information_schema")
            return _FakeResult([(n,) for n in self._names])
        return _FakeResult([])

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_a_spool_name_carries_its_job_and_its_age() -> None:
    endpoint = _staging_endpoint(_endpoint("orders"), "job-4711-abc")
    assert endpoint.table.startswith(f"{SCD2_STAGING_PREFIX}job4711abc_")
    assert endpoint.collection == endpoint.table
    # MySQL rejects an identifier over 64 chars, so the stamp must stay inside it.
    assert len(endpoint.table) <= 64
    age = staging_age_seconds(SCD2_STAGING_PREFIX, endpoint.table)
    assert age is not None and age < STAGING_TTL_SECONDS


def test_two_runs_of_one_job_do_not_share_a_spool() -> None:
    """A retry starting while the first attempt drains must not drop its spool."""
    first = _staging_endpoint(_endpoint("orders"), "job-1").table
    second = _staging_endpoint(_endpoint("orders"), "job-1").table
    assert first != second


@pytest.mark.parametrize("prefix", [SCD2_STAGING_PREFIX, "_df_mirrorkeys_"])
def test_the_sweep_drops_only_what_no_live_run_can_own(prefix: str) -> None:
    live = staging_table_name(prefix, "job1")
    mine = staging_table_name(prefix, "job2")
    conn = _FakeConn([live, mine, f"{prefix}1600000000_abc", f"{prefix}deadbeefcafe"])
    dropped = reap_orphan_staging(conn, prefix, "public", "postgresql", keep=mine)
    assert dropped == [f"{prefix}1600000000_abc", f"{prefix}deadbeefcafe"]
    executed = " ".join(conn.statements)
    assert live not in executed  # in TTL: another run may still be filling it
    assert mine not in executed  # the caller drops its own
    # An unstamped name has no knowable age, so it is only touched behind a
    # bounded lock wait — one an older build still holds is skipped, not waited on.
    assert any("lock_timeout" in s for s in conn.statements)


def test_a_legacy_all_digit_suffix_is_not_read_as_a_clock() -> None:
    # ``_dataflow_stg_255577532241`` read as a stamp dated the orphan in the
    # year 10069, so it could never age out.
    assert staging_age_seconds(SCD2_STAGING_PREFIX, "_dataflow_stg_255577532241") is None
    stale = staging_age_seconds(SCD2_STAGING_PREFIX, "_dataflow_stg_job_1600000000_ab")
    assert stale is not None and stale > STAGING_TTL_SECONDS


def test_no_catalog_access_is_not_a_transfer_failure() -> None:
    conn = _FakeConn(["whatever"], catalog_error=True)
    assert reap_orphan_staging(conn, SCD2_STAGING_PREFIX, "", "mysql") == []


def test_the_spool_is_dropped_on_the_failure_path_too() -> None:
    """A stage that raises still leaves no spool, and keeps its own error."""
    import connectors.generic_sql as real_sql
    import src.transfer.stream as stream_mod
    import src.transfer.stream_scd2 as mod

    dropped: list[str] = []
    staged: dict[str, str] = {}

    def fake_drop(cfg: Any, table: str, schema: Any = None) -> None:  # noqa: ARG001
        dropped.append(table)

    def fake_stream(*args: Any, **kwargs: Any) -> tuple:  # noqa: ARG001
        staged["table"] = args[1].table
        raise RuntimeError("source went away mid-stage")

    def fake_reap(*args: Any, **kwargs: Any) -> list[str]:  # noqa: ARG001
        return []

    orig = (real_sql.drop_table, stream_mod.stream_database_transfer, mod._reap_orphan_spools)
    real_sql.drop_table = fake_drop  # type: ignore[assignment]
    stream_mod.stream_database_transfer = fake_stream  # type: ignore[assignment]
    mod._reap_orphan_spools = fake_reap  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="source went away"):
            mod.stream_scd2_mirror_transfer(
                _endpoint("src"),
                _endpoint("dst"),
                [{"source": "id", "target": "id"}],
                {"id": "integer"},
                sync_mode="scd2",
                stream_contracts=[
                    {"selected": True, "sync_mode": "scd2", "primary_key": "id"}
                ],
                job_id="job-9",
            )
    finally:
        (
            real_sql.drop_table,
            stream_mod.stream_database_transfer,
            mod._reap_orphan_spools,
        ) = orig

    # Dropped before the stage and again in the finally, even though the stage
    # raised: the spool never survives its run.
    assert dropped.count(staged["table"]) == 2


def test_a_streamed_read_does_not_leave_the_connection_in_cursor_mode() -> None:
    """The reason mirror spools survived their own DROP.

    ``Connection.execution_options()`` mutates the connection, so every later
    statement on it inherited ``stream_results``. PostgreSQL then compiled the
    cleanup as ``DECLARE ... CURSOR WITHOUT HOLD FOR DROP TABLE ...`` and raised
    a syntax error, so the key-staging table stayed in the customer's schema.
    """
    import sqlalchemy as sa

    from services.reconciliation import sa_streaming_result

    engine = sa.create_engine("sqlite://")
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE t (id INTEGER)"))
            conn.execute(sa.text("INSERT INTO t (id) VALUES (1), (2)"))
            names, rows = sa_streaming_result(conn, sa.text("SELECT id FROM t"))
            assert names == ["id"]
            assert [r[0] for r in rows] == [1, 2]
            assert not conn.get_execution_options().get("stream_results")
            # The cleanup a mirror run does next must reach the server as DDL.
            conn.execute(sa.text("DROP TABLE t"))
    finally:
        engine.dispose()
