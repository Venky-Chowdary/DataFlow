"""Phase C5 — same source type, different invent by execution context."""

from __future__ import annotations

import pytest

from services.decision_kernel import (
    InventContext,
    InventRefused,
    invent_context_from_sync_mode,
    invent_dest_type,
)


def test_create_new_widens_integer_to_64bit():
    out = invent_dest_type(
        "INTEGER",
        dest_db="postgresql",
        context=InventContext.CREATE_NEW,
    )
    assert out.upper() == "BIGINT"


def test_bind_existing_refuses_without_stamp():
    with pytest.raises(InventRefused):
        invent_dest_type(
            "BIGINT",
            dest_db="postgresql",
            context=InventContext.BIND_EXISTING,
            existing_dest_type="",
        )


def test_bind_existing_keeps_proven_stamp_not_source_invent():
    out = invent_dest_type(
        "BIGINT",
        dest_db="postgresql",
        context=InventContext.BIND_EXISTING,
        existing_dest_type="INTEGER",
    )
    # Bind must not silently widen the live column — proven stamp wins.
    assert "INT" in out.upper()


def test_cdc_sparse_refuses_source_only_invent():
    with pytest.raises(InventRefused) as ei:
        invent_dest_type(
            "TEXT",
            dest_db="postgresql",
            context=InventContext.CDC_SPARSE,
        )
    assert ei.value.context is InventContext.CDC_SPARSE


def test_sync_mode_derives_context():
    assert (
        invent_context_from_sync_mode("full_refresh_append", table_exists=True)
        is InventContext.APPEND
    )
    assert (
        invent_context_from_sync_mode("full_refresh_overwrite", create_new=True)
        is InventContext.CREATE_NEW
    )
    assert invent_context_from_sync_mode("cdc", cdc=True) is InventContext.CDC_SPARSE


def test_same_conversion_different_ddl_by_context():
    create = invent_dest_type(
        "INTEGER", dest_db="mysql", context=InventContext.CREATE_NEW
    )
    bound = invent_dest_type(
        "INTEGER",
        dest_db="mysql",
        context=InventContext.BIND_EXISTING,
        existing_dest_type="INT",
    )
    # Create-new invents 64-bit; bind keeps the live INT stamp.
    assert create.upper() in {"BIGINT", "INT64", "LONG"}
    assert bound.upper() in {"INT", "INTEGER"}


def test_writer_invent_imports_use_decision_kernel_surface():
    """C2: CREATE invent helpers on writers must import kernel facade, not type_system."""
    from pathlib import Path
    import re

    root = Path(__file__).resolve().parents[1] / "connectors"
    # Top-level invent surface — not specialty helpers (parse_enum, LOGICAL_*).
    invent_names = ("materialize_dest_ddl", "ddl_type", "create_new_mapping_target_type")
    offenders: list[str] = []
    for name in (
        "bigquery_writer.py",
        "mysql_writer.py",
        "snowflake_writer.py",
        "sqlite_writer.py",
        "iceberg_writer.py",
    ):
        text = (root / name).read_text(encoding="utf-8")
        for m in re.finditer(
            r"from\s+services\.type_system\s+import\s+\(([^)]+)\)|"
            r"from\s+services\.type_system\s+import\s+([^\n]+)",
            text,
        ):
            imported = (m.group(1) or m.group(2) or "").replace("\n", " ")
            for invent in invent_names:
                if re.search(rf"\b{invent}\b", imported):
                    offenders.append(f"{name}:{invent}")
    assert not offenders, (
        "Writers must import invent/DDL via services.decision_kernel — " + ", ".join(offenders)
    )
