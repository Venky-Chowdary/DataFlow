"""A fast-path CREATE never puts a key on an unbounded LOB column.

PR 424 CI, cdc-sqlserver (pyodbc), file_sqlite_to_database_sqlserver: the
SQLite source declares ``id TEXT PRIMARY KEY``. The SQLite→SQL Server
fast_executemany path created ``id NVARCHAR(MAX)``, and the planner's
PRIMARY KEY then failed with SQL Server error 1919 ("Column 'id' ... is of a
type that is invalid for use as a key column in an index"). The row path
sizes an unbounded text key to the widest full-value carrier
(``upsert_identity.unbounded_key_widths``). So a fast path whose key column
would be a LOB declines and hands the load to the row path.
"""

from __future__ import annotations

import pytest

from services import copy_fast_path as fp
from services.copy_fast_path import FastPathUnavailable


@pytest.mark.parametrize(
    ("ddl", "unbounded"),
    [
        ("NVARCHAR(MAX)", True),
        ("varchar( max )", True),
        ("VARBINARY(MAX)", True),
        ("NTEXT", True),
        ("TEXT", True),
        ("XML", True),
        ("CLOB", True),
        ("NVARCHAR(450)", False),
        ("VARCHAR(255)", False),
        ("INT", False),
        ("DATETIME2(7)", False),
    ],
)
def test_lob_key_ddl_is_recognised(ddl: str, unbounded: bool) -> None:
    assert fp.key_ddl_is_unbounded(ddl) is unbounded


@pytest.fixture
def planned_pk(monkeypatch):
    from services import schema_fidelity

    monkeypatch.setattr(
        schema_fidelity,
        "resolve_create_fidelity_plan",
        lambda **_k: schema_fidelity.CreateFidelityPlan(
            report=schema_fidelity.SchemaFidelityReport(), primary_key=["id"]
        ),
    )
    token, _scope = fp.begin_fast_path_create_scope(object(), None, "payments")
    yield
    fp.reset_fast_path_create_scope(token)


@pytest.mark.parametrize("dialect", ["sqlserver", "mssql", "oracle"])
def test_a_planned_key_on_a_lob_declines_the_fast_path(planned_pk, dialect: str) -> None:
    with pytest.raises(FastPathUnavailable, match=r"PRIMARY KEY \(id\).*row path"):
        fp.plan_fast_path_create(
            dest_dialect=dialect,
            pairs=[("id", "id"), ("amount", "amount")],
            ddls=["NVARCHAR(MAX)", "NVARCHAR(MAX)"],
            primary_key=None,
            dest_table="payments",
        )


def test_a_bounded_key_keeps_the_fast_path(planned_pk) -> None:
    create = fp.plan_fast_path_create(
        dest_dialect="sqlserver",
        pairs=[("id", "id"), ("amount", "amount")],
        ddls=["NVARCHAR(450)", "NVARCHAR(MAX)"],
        primary_key=None,
        dest_table="payments",
    )
    assert create.primary_key == ("id",)
