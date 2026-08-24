"""A table's existence must not depend on where its name sorts in a listing.

Studio carries `public.vt_src` into introspect. The bounded object listing is
what used to normalise that to `vt_src`, so once a schema held more than the
listing cap, the catalog was asked for a table literally named
`public.vt_src`, answered no rows, and an existing, readable source table was
reported to the operator as "not found" — with Continue disabled, the route
could not be started at all.
"""

from __future__ import annotations

import pytest

from services.schema_introspect import split_object_namespace
from src.transfer.endpoint_intelligence import _operator_visible_objects


@pytest.mark.parametrize(
    ("db_type", "name", "expected"),
    [
        ("postgresql", "public.vt_src", ("public", "db", "vt_src")),
        ("postgresql", "vt_src", ("cfg_schema", "db", "vt_src")),
        ("postgresql", 'dataflow.public."vt src"', ("public", "dataflow", "vt src")),
        ("redshift", "analytics.orders", ("analytics", "db", "orders")),
        ("snowflake", "SALES.PUBLIC.ORDERS", ("PUBLIC", "SALES", "ORDERS")),
        ("sqlserver", "[dbo].[Orders]", ("dbo", "db", "Orders")),
        # MySQL has no schema layer: the qualifier names the database.
        ("mysql", "dataflow.orders", ("cfg_schema", "dataflow", "orders")),
        ("mysql", "`dataflow`.`orders`", ("cfg_schema", "dataflow", "orders")),
    ],
)
def test_qualified_names_resolve_to_namespace_plus_bare_object(
    db_type: str, name: str, expected: tuple[str, str, str]
) -> None:
    assert (
        split_object_namespace(db_type, name, schema="cfg_schema", database="db")
        == expected
    )


def test_a_document_store_collection_may_contain_a_dot() -> None:
    """MongoDB names are not namespaced by dots — splitting one loses the object."""
    assert split_object_namespace(
        "mongodb", "events.raw", schema="", database="app"
    ) == ("", "app", "events.raw")


def test_internal_scratch_objects_stay_out_of_the_operator_listing() -> None:
    """Our own mirror/SCD2 scratch tables must not consume a bounded page."""
    assert _operator_visible_objects(
        [
            "orders",
            "_df_mirrorkeys_1787565582_695e02c4",
            "_dataflow_stg_abc",
            "(no tables)",
            "users",
        ],
        "table",
    ) == [{"name": "orders", "type": "table"}, {"name": "users", "type": "table"}]


def test_orphaned_mirror_staging_is_reaped_only_once_it_cannot_be_live() -> None:
    """Age comes from the name, so a sweep never drops a concurrent run's table."""
    from services.mirror_engine import _staging_age_seconds, _staging_table_name
    from services.staging_reaper import STAGING_TTL_SECONDS as _MIRROR_STAGING_TTL_SECONDS

    fresh = _staging_table_name()
    age = _staging_age_seconds(fresh)
    assert age is not None and age < _MIRROR_STAGING_TTL_SECONDS
    stale = _staging_age_seconds("_df_mirrorkeys_1600000000_abc")
    assert stale is not None and stale > _MIRROR_STAGING_TTL_SECONDS
    # Pre-stamp names have no knowable age: they are reaped under a bounded
    # lock wait instead, so one an older build still holds is skipped.
    assert _staging_age_seconds("_df_mirrorkeys_deadbeefcafe") is None
    # A legacy random suffix of only digits is not a clock. Read as one, it
    # dated an orphan in the year 10069 and it could never age out.
    assert _staging_age_seconds("_df_mirrorkeys_255577532241") is None


class _FakeConn:
    """Records SQL; the catalog query answers with the names it was given."""

    def __init__(self, names: list[str]) -> None:
        self._names = names
        self.statements: list[str] = []

    def execute(self, statement, params=None):  # noqa: ANN001 - sqlalchemy TextClause
        sql = str(statement)
        self.statements.append(sql)
        if "information_schema.tables" in sql:
            return _FakeResult([(n,) for n in self._names])
        return _FakeResult([])

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class _FakeResult:
    def __init__(self, rows: list[tuple[str, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[str, ...]]:
        return self._rows

    def fetchone(self):  # noqa: ANN201
        return self._rows[0] if self._rows else None


def test_the_sweep_drops_only_staging_no_live_run_can_own() -> None:
    from services.mirror_engine import _reap_orphan_staging, _staging_table_name

    live = _staging_table_name()
    dropped = _reap_orphan_staging(
        conn := _FakeConn(
            [live, "_df_mirrorkeys_1600000000_abc", "_df_mirrorkeys_deadbeefcafe"]
        ),
        "public",
        "postgresql",
    )
    assert dropped == ["_df_mirrorkeys_1600000000_abc", "_df_mirrorkeys_deadbeefcafe"]
    assert live not in " ".join(conn.statements)
    # The unstamped one is only touched behind a bounded lock wait.
    assert any("lock_timeout" in s for s in conn.statements)
