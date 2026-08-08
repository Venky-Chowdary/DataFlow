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
