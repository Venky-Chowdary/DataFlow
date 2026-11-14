"""G11 review: the Map≡ALTER stamp lookup finds mixed-case and long targets.

The SQL writers look up each target column's Map row (its explicit
``target_type`` ceiling and its source type) by a re-sanitized name. The key
was lowercased and cut at 63 (``sanitize_identifier(..., preserve_case=False)``)
while the target columns keep case and, since G11, the engine's real length
(Redshift 127, SQL Server 128, Snowflake 255). ``CustomerName`` and a
100-character Redshift column therefore found no Map row: the explicit
ceiling was not applied, so a wider source carrier was proposed past it.
On live PostgreSQL a second miss (no widen candidate) hid the first; the
lookup is now keyed the way the writers name columns.
"""

from __future__ import annotations

import pytest

LONG = "r" * 100


@pytest.mark.parametrize("target", ["CustomerName", LONG, "customer_name"])
def test_explicit_map_ceiling_is_found_for_the_writers_column_name(target):
    from connectors.writer_common import desired_types_honoring_map_stamps

    desired, refusals = desired_types_honoring_map_stamps(
        target_cols=[target],
        current_target_types=["VARCHAR(5)"],
        mappings=[{"source": "s", "target": target, "target_type": "VARCHAR(5)"}],
        candidate_by_col={target: "VARCHAR(20)"},
    )
    assert desired == ["VARCHAR(5)"], desired
    assert refusals and refusals[0]["column"] == target, refusals


def test_writer_lookup_key_matches_the_writers_spelling():
    from connectors.destination_naming import column_key, writer_identifier

    for dest, name in (("postgresql", "CustomerName"), ("redshift", LONG),
                       ("mysql", "Order Total"), ("snowflake", "x" * 200)):
        stored = writer_identifier(dest, preserve_case=True)(name)
        assert column_key(name) == column_key(stored), (dest, name, stored)


@pytest.mark.parametrize("stamped", [True, False])
def test_live_postgres_mixed_case_column_honours_the_map_ceiling(stamped):
    """Live: a stamped ceiling holds (quarantine); an unstamped column widens."""
    import uuid

    from connectors.postgresql_writer import write_mapped_rows
    from tests import r4_route_harness as h

    cfg = h.dest_config("postgresql")
    table = f"g11c_{uuid.uuid4().hex[:8]}"
    h.execute("postgresql", f'CREATE TABLE {table} (id int, "Amount" varchar(5))')
    try:
        stamp = {"target_type": "VARCHAR(5)"} if stamped else {}
        res = write_mapped_rows(
            host=cfg["host"], port=cfg["port"], database=cfg["database"],
            username=cfg["username"], password=cfg["password"], schema="public",
            connection_string="", ssl=False, table_name=table,
            headers=["id", "amt"], data_rows=[["1", "12345678"]],
            mappings=[{"source": "id", "target": "id"},
                      {"source": "amt", "target": "Amount", **stamp}],
            column_types={"id": "INTEGER", "amt": "VARCHAR(20)"},
            create_table=False, backfill_new_fields=True, error_policy="quarantine",
        )
        assert res.ok, res.error
        width = h.query("postgresql", "SELECT character_maximum_length FROM "  # nosec B608
                        f"information_schema.columns WHERE table_name = '{table}' "
                        "AND column_name = 'Amount'")[0][0]
        if stamped:
            assert (width, res.rows_written, res.rejected_rows) == (5, 0, 1)
        else:
            assert (width, res.rows_written) == (20, 1)
    finally:
        h.drop_table("postgresql", table)
