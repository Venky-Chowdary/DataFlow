"""D200 / D107 / D119 — an approved source widen reaches the table run 1 created.

Run 1 creates the destination from a create-new mapping; a schedule or rerun
replays that stored mapping. After the source widened (VARCHAR(20) ->
VARCHAR(200), NUMERIC(8,2) -> NUMERIC(12,4)) an acknowledged run 2 was
refused as "collapse fidelity on write" and the destination was never
widened: the stored ``target_type`` (DataFlow's own run-1 projection, with no
recorded provenance) was read as an operator ceiling, so the armed widen
skipped it. A fresh plan widened correctly, which is why only scheduled and
rerun routes failed.

Live PostgreSQL source -> PostgreSQL / SQL Server, incremental upsert,
preflight ON, strict.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from connectors.writer_common import stamp_is_operator_ceiling
from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def test_dataflow_projection_is_not_an_operator_ceiling():
    stored = {"source": "name", "target": "name", "target_type": "VARCHAR(20)",
              "create_new": True, "assignment_strategy": "identity_passthrough"}
    assert stamp_is_operator_ceiling(stored) is False
    # Approve / Accept risk in Map stamps user_override on the kernel-invented
    # row (fixture shape: user_override + create_new +
    # target_type_provenance="kernel_invent"); that approves the plan, it does
    # not make DataFlow's type an operator ceiling.
    approved = {**stored, "user_override": True, "target_type_provenance": "kernel_invent"}
    assert stamp_is_operator_ceiling(approved) is False
    assert stamp_is_operator_ceiling(
        {"target": "name", "target_type": "VARCHAR(20)", "target_type_provenance": "kernel_invent",
         "user_override": True}
    ) is False
    # An operator's own stamp on an existing column is a ceiling.
    assert stamp_is_operator_ceiling(
        {"target": "name", "target_type": "VARCHAR(20)", "user_override": True}
    ) is True


def _stored(name_type: str, amt_type: str) -> list[dict]:
    cols = [("id", "INTEGER"), ("name", name_type), ("amt", amt_type),
            ("qty", "INTEGER"), ("updated_at", "TIMESTAMP")]
    return [
        {"source": c, "target": c, "source_type": t, "target_type": t,
         "create_new": True, "assignment_strategy": "identity_passthrough",
         "target_type_provenance": "kernel_invent", "user_override": True,
         "confidence": 0.95}
        for c, t in cols
    ]


def _run(fake, table, out, dest, **kw):
    return h.execute(
        fake, h.endpoint("postgresql", table), h.endpoint(dest, out),
        sync_mode="incremental_upsert",
        stream_contracts=[{
            "name": table, "sync_mode": "incremental_upsert", "primary_key": "id",
            "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
            "selected": True,
        }],
        mappings=_stored("VARCHAR(20)", "NUMERIC(8,2)"),
        **kw,
    )


def _dest_rows(dest, out):
    length = "length" if dest == "postgresql" else "LEN"
    return h.run_sql(dest, f"SELECT id, name, amt FROM {h.quote(dest, out)} ORDER BY id")  # nosec B608


@pytest.mark.parametrize("dest", ["postgresql", "sqlserver"])
def test_acknowledged_widen_alters_the_created_table_and_loads_exact(isolated, dest):  # noqa: F811
    table = f"d200_{h.token()}"
    out = f"{table}_d"
    rows = [(1, "Ada", Decimal("1.50"), 1, "2024-01-01"), (2, "", Decimal("0.00"), 2, "2024-01-02"),
            (3, None, Decimal("7.25"), 3, "2024-01-03"), (4, "東京 🚀", Decimal("2.00"), 4, "2024-01-04")]
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(20), "
                  "amt NUMERIC(8,2), qty INT, updated_at TIMESTAMP)")
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s,%s,%s,%s,%s)", params=rows)  # nosec B608
        assert _run(isolated, table, out, dest).success
        long_name = "Zoë " + "y" * 146
        h.run_sql(
            "postgresql",
            f"ALTER TABLE {table} ALTER COLUMN name TYPE VARCHAR(200)",
            f"ALTER TABLE {table} ALTER COLUMN amt TYPE NUMERIC(12,4)",
            f"UPDATE {table} SET name = '{long_name}', amt = 12345678.1234, "  # nosec B608
            "updated_at = '2024-02-01' WHERE id = 2",
        )
        refused = _run(isolated, table, out, dest)
        assert not refused.success
        assert _dest_rows(dest, out)[1][1] == ""  # untouched
        approved = _run(isolated, table, out, dest, schema_drift_acknowledged=True)
        assert approved.success, approved.error
        got = _dest_rows(dest, out)
        assert got[1] == (2, long_name, Decimal("12345678.1234")), got[1]
        assert [r[1] for r in got] == ["Ada", long_name, None, "東京 🚀"]
    finally:
        h.drop("postgresql", table)
        h.drop(dest, out)
