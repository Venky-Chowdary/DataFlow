"""Iceberg rematerialize when live physical carriers differ from Map stamps."""

from __future__ import annotations

import pytest


def test_iceberg_rematerialize_when_physical_int_vs_map_varchar():
    from connectors.iceberg_writer import _iceberg_rematerialize_if_physical_differs

    headers = ["qty"]
    data_rows = [["12"], ["not-an-int"]]
    mappings = [{"source": "qty", "target": "qty", "target_type": "VARCHAR"}]
    dest_types = {"qty": "VARCHAR"}
    physical = {"qty": "INT", "QTY": "INT"}

    batch = _iceberg_rematerialize_if_physical_differs(
        physical=physical,
        dest_types=dest_types,
        target_cols=["qty"],
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types={"qty": "VARCHAR"},
        logical_types=["VARCHAR"],
        policy="quarantine",
    )
    assert batch is not None
    mapped_rows, _errs, rejected, live = batch
    assert "INT" in str(live.get("qty") or "").upper()
    # Fit numeric survives; unfit text quarantines under INT.
    assert len(mapped_rows) + len(rejected) >= 1
    assert any(
        (d.get("column") or "").lower() == "qty" for d in rejected
    ) or len(mapped_rows) == 1


def test_iceberg_no_rematerialize_when_carriers_match():
    from connectors.iceberg_writer import _iceberg_rematerialize_if_physical_differs

    batch = _iceberg_rematerialize_if_physical_differs(
        physical={"qty": "INT"},
        dest_types={"qty": "INT"},
        target_cols=["qty"],
        headers=["qty"],
        data_rows=[["1"]],
        mappings=[{"source": "qty", "target": "qty", "target_type": "INT"}],
        column_types={"qty": "INT"},
        logical_types=["INT"],
        policy="quarantine",
    )
    assert batch is None


def test_iceberg_rematerialize_overlays_existing_with_additive_map_cols():
    """Additive Map columns keep Map stamps; live existing cols rematerialize."""
    from connectors.iceberg_writer import _iceberg_rematerialize_if_physical_differs

    batch = _iceberg_rematerialize_if_physical_differs(
        physical={"qty": "INT"},
        dest_types={"qty": "VARCHAR", "note": "VARCHAR"},
        target_cols=["qty", "note"],
        headers=["qty", "note"],
        data_rows=[["12", "hello"], ["x", "world"]],
        mappings=[
            {"source": "qty", "target": "qty", "target_type": "VARCHAR"},
            {"source": "note", "target": "note", "target_type": "VARCHAR"},
        ],
        column_types={"qty": "VARCHAR", "note": "VARCHAR"},
        logical_types=["VARCHAR", "VARCHAR"],
        policy="quarantine",
    )
    assert batch is not None
    mapped_rows, _errs, rejected, live = batch
    assert "INT" in str(live.get("qty") or "").upper()
    assert str(live.get("note") or "").upper().startswith("VARCHAR")
    assert len(mapped_rows) + len(rejected) >= 1


def test_physical_carriers_from_arrow_decimal():
    pa = pytest.importorskip("pyarrow")
    from connectors.iceberg_writer import _physical_carriers_from_arrow

    schema = pa.schema(
        [
            ("amount", pa.decimal128(10, 2)),
            ("flag", pa.bool_()),
        ]
    )
    physical = _physical_carriers_from_arrow(schema, pa)
    assert "DECIMAL(10,2)" in str(physical.get("amount") or "").upper()
    assert "BOOL" in str(physical.get("flag") or "").upper()
