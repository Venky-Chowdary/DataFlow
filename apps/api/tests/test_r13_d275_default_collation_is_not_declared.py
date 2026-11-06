"""D275 (legacy tables) — a destination collation at the database default was not chosen.

Tables DataFlow created before it stamped collations carry the database
default (SQL Server ``SQL_Latin1_General_CP1_CI_AS``, MySQL
``@@collation_database``), exactly as any CREATE without a COLLATE clause
does. Reading that back as a declared collation made mirror / SCD2 run 2 into
DataFlow's own table a fidelity collapse ("… COLLATE UTF8MB4_0900_AI_CI ->
NVARCHAR(400) COLLATE SQL_LATIN1_GENERAL_CP1_CI_AS") although every value
had landed. Destination introspection now keeps the carrier bare at the
default and discloses the default in the plan; a collation someone chose
(differs from the default) stays on the carrier and is still graded. Sources
keep their declared collation, default or not.

Live MySQL 8 / SQL Server 2022.
"""

from __future__ import annotations

import pytest

from services.default_collation import database_default_collation, declared_collation_carrier
from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)

_DEFAULT = "SQL_Latin1_General_CP1_CI_AS"


def test_default_collation_stays_off_the_carrier_and_is_disclosed():
    carrier, note = declared_collation_carrier("NVARCHAR(400)", _DEFAULT, _DEFAULT.lower(), column="name")
    assert carrier == "NVARCHAR(400)" and "'name'" in note and _DEFAULT in note
    carrier, note = declared_collation_carrier("NVARCHAR(400)", "Latin1_General_CS_AS", _DEFAULT)
    assert carrier == "NVARCHAR(400) COLLATE Latin1_General_CS_AS" and note == ""
    # MySQL: the collation name was the charset evidence; keep it.
    carrier, note = declared_collation_carrier(
        "LONGTEXT", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci", column="note", charset="utf8mb4"
    )
    assert carrier == "LONGTEXT CHARACTER SET utf8mb4" and note
    assert declared_collation_carrier("NVARCHAR(400)", "", _DEFAULT) == ("NVARCHAR(400)", "")
    # Unknown default -> today's grading (clause kept).
    assert declared_collation_carrier("NVARCHAR(400)", _DEFAULT, "") == (f"NVARCHAR(400) COLLATE {_DEFAULT}", "")
    assert database_default_collation(lambda q: 1 / 0, "sqlserver") == ""
    assert database_default_collation(lambda q: ("utf8mb4_0900_ai_ci",), "mysql") == "utf8mb4_0900_ai_ci"
    assert database_default_collation(lambda q: "x", "postgresql") == ""


def _dest_types(kind: str, table: str, columns: list[str]) -> dict[str, str]:
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.adapters_introspect import _introspect_table_schema_rich

    cfg = resolve_connector_config(h.endpoint(kind, table))
    types, _nulls, _keys = _introspect_table_schema_rich(kind, cfg, table, columns, physical_carriers=True)
    return types


def test_sqlserver_destination_read_drops_only_the_database_default():
    table = f"d275c_{h.token()}"
    try:
        h.run_sql("sqlserver", f"CREATE TABLE [{table}] (id BIGINT PRIMARY KEY, "
                  "name NVARCHAR(400), chosen NVARCHAR(400) COLLATE Latin1_General_CS_AS)")
        types = _dest_types("sqlserver", table, ["id", "name", "chosen"])
        assert types["name"] == "NVARCHAR(400)", types
        assert types["chosen"] == "NVARCHAR(400) COLLATE Latin1_General_CS_AS", types
    finally:
        h.drop("sqlserver", table)


def test_source_read_keeps_the_collation_it_compares_with():
    """The MySQL source at its own database default still declares ai_ci."""
    table = f"d275s_{h.token()}"
    try:
        h.run_sql("mysql", f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(100)) CHARACTER SET utf8mb4")
        from src.transfer.adapters import resolve_connector_config
        from src.transfer.adapters_introspect import _introspect_table_schema_rich

        cfg = resolve_connector_config(h.endpoint("mysql", table))
        types, _n, _k = _introspect_table_schema_rich("mysql", cfg, table, ["id", "name"])
        assert "COLLATE utf8mb4_0900_ai_ci" in types["name"], types
    finally:
        h.drop("mysql", table)


_ROWS = [(i, f"Zoë {i} 東京", None if i % 3 == 0 else "") for i in range(1, 8)]


def _mirror(fake, table, out):
    return h.execute(
        fake, h.endpoint("mysql", table), h.endpoint("sqlserver", out), sync_mode="mirror",
        stream_contracts=[{"name": table, "sync_mode": "mirror", "primary_key": "id",
                           "delete_mode": "hard", "selected": True}],
    )


@pytest.mark.parametrize(
    ("clause", "accepted"),
    [("", True), (" COLLATE Latin1_General_CS_AS", False)],
    ids=["dataflow_legacy_default", "operator_chosen"],
)
def test_mirror_run_two_into_a_legacy_table(isolated, clause, accepted):  # noqa: F811
    table = f"d275l_{h.token()}"
    out = f"{table}_o"
    try:
        h.run_sql("mysql", f"CREATE TABLE `{table}` (id INT PRIMARY KEY, "
                  "name VARCHAR(400) COLLATE utf8mb4_0900_ai_ci, note VARCHAR(100) COLLATE utf8mb4_0900_ai_ci) "
                  "CHARACTER SET utf8mb4")
        h.run_sql("mysql", f"INSERT INTO `{table}` VALUES (%s,%s,%s)", params=_ROWS)  # nosec B608
        # The table "run 1" left behind: created without a COLLATE clause (DataFlow
        # before this fix), or with one the operator chose.
        h.run_sql("sqlserver", f"CREATE TABLE [{out}] (id BIGINT PRIMARY KEY, "
                  f"name NVARCHAR(400){clause}, note NVARCHAR(100){clause})",
                  f"INSERT INTO [{out}] VALUES (1, N'Zoë 1 東京', N''), (9, N'gone', NULL)")
        result = _mirror(isolated, table, out)
        if accepted:
            assert result.success, result.error
            got = h.run_sql("sqlserver", f"SELECT id, name, note FROM [{out}] ORDER BY id")  # nosec B608
            assert [tuple(r) for r in got] == _ROWS
        else:
            assert not result.success
            assert "LATIN1_GENERAL_CS_AS" in (result.error or "").upper(), result.error
    finally:
        h.drop("mysql", table)
        h.drop("sqlserver", out)


def test_plan_discloses_the_default_instead_of_refusing():
    from services.default_collation import default_collation_note
    from services.mapping_pipeline import run_mapping_pipeline
    from src.ai.copilot.transfer_tools import _schema_rows

    note = default_collation_note("name", _DEFAULT)
    rows = _schema_rows([{"name": "name", "inferred_type": "NVARCHAR(400)", "collation": _DEFAULT,
                          "collation_note": note}])
    assert rows[0]["collation_note"] == note
    out = run_mapping_pipeline(
        ["id", "name"], ["id", "name"],
        source_schemas=[{"name": "id", "inferred_type": "INT", "samples": ["1"]},
                        {"name": "name", "inferred_type": "VARCHAR(400) COLLATE utf8mb4_0900_ai_ci",
                         "samples": ["Zoë"]}],
        target_schemas=[{"name": "id", "inferred_type": "BIGINT"}, *rows],
        destination_db_type="sqlserver", source_db_type="mysql",
        destination_table_exists=True, use_llm=False,
    )
    name = next(m for m in out["mappings"] if m["source"] == "name")
    assert name["fidelity"] == "preserve" and not name["requires_review"], name
    assert name["destination_note"] == note and note in name["reasoning"]
