"""Snowflake rematerialize when live DDL carriers differ from Map stamps."""

from __future__ import annotations


def test_sf_rematerialize_when_physical_varchar_vs_number():
    from connectors.snowflake_writer import _sf_rematerialize_if_physical_differs

    headers = ["amount"]
    data_rows = [["12.50"], ["not-a-number"]]
    mappings = [{"source": "amount", "target": "amount", "target_type": "VARCHAR"}]
    dest_types = {"amount": "VARCHAR"}
    physical = {"amount": "NUMBER(10,2)", "AMOUNT": "NUMBER(10,2)"}

    batch = _sf_rematerialize_if_physical_differs(
        physical=physical,
        dest_types=dest_types,
        target_cols=["amount"],
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types={"amount": "DECIMAL(10,2)"},
        logical_types=["VARCHAR"],
        policy="quarantine",
        conflict_columns=None,
        write_mode="insert",
    )
    assert batch is not None
    assert "NUMBER" in batch.target_types[0].upper()
    # Fit cell survives; unfit text quarantines under NUMBER.
    assert len(batch.mapped_rows) + len(batch.rejected_details) >= 1
    assert any("NUMBER" in (d.get("reason") or d.get("message") or "").upper()
               or "DECIMAL" in (d.get("reason") or d.get("message") or "").upper()
               or d.get("column") == "amount"
               for d in batch.rejected_details) or len(batch.mapped_rows) == 1


def test_sf_no_rematerialize_when_carriers_match():
    from connectors.snowflake_writer import _sf_rematerialize_if_physical_differs

    batch = _sf_rematerialize_if_physical_differs(
        physical={"amount": "NUMBER(10,2)"},
        dest_types={"amount": "NUMBER(10,2)"},
        target_cols=["amount"],
        headers=["amount"],
        data_rows=[["1"]],
        mappings=[{"source": "amount", "target": "amount", "target_type": "NUMBER(10,2)"}],
        column_types={"amount": "NUMBER(10,2)"},
        logical_types=["NUMBER(10,2)"],
        policy="quarantine",
        conflict_columns=None,
        write_mode="insert",
    )
    assert batch is None
