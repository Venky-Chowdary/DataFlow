"""Physical dest type probe must not crash on name-only column lists."""

from __future__ import annotations


def test_physical_column_types_skips_string_columns(monkeypatch) -> None:
    from services import dest_physical_types as dpt

    monkeypatch.setattr(
        "services.schema_introspect.introspect_schema",
        lambda *a, **k: {
            "ok": True,
            "columns": ["id", "amount", {"name": "code", "inferred_type": "VARCHAR(64)"}],
        },
    )
    types = dpt.physical_column_types(
        "oracle",
        {"host": "localhost", "port": 1521, "database": "XEPDB1"},
        table="ORDERS",
        columns=["id", "code"],
    )
    assert types == {"code": "VARCHAR(64)"}
    assert "id" not in types


def test_physical_column_types_empty_when_catalog_unreadable(monkeypatch) -> None:
    from services import dest_physical_types as dpt

    monkeypatch.setattr(
        "services.schema_introspect.introspect_schema",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")),
    )
    assert dpt.physical_column_types("oracle", {}, table="T") == {}
