"""AUDIT-INTEGRITY — duplicate identity matrix across sync modes and key shapes.

Source duplicate identity blocks on every key-addressed or overwrite mode; append
without a covering destination key warns. Genuinely unique scalar and composite
keys never block, and SCD2 checks the business key, not the history columns.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.data_integrity import _check_duplicate_keys, run_integrity_audit

BLOCKING_MODES = ["upsert", "overwrite", "replace", "full_refresh_overwrite", "scd2"]
APPEND_MODES = ["append", "full_refresh_append"]
ALL_MODES = APPEND_MODES + BLOCKING_MODES


def _check(rows: list[dict[str, Any]], cols: list[str], **kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        mappings=[{"source": c, "target": c} for c in sorted({k for r in rows for k in r})],
        rows=rows,
        validation_mode="strict",
        dest_kind="postgresql",
        primary_key=cols[0],
        primary_key_columns=cols,
        destination_pk_columns=[],
        destination_unique_keys=[],
    )
    base.update(kw)
    return _check_duplicate_keys(**base)


@pytest.mark.parametrize("sync_mode", ALL_MODES)
def test_unique_scalar_key_never_blocks(sync_mode: str) -> None:
    rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "a"}, {"id": 3, "v": "b"}]
    result = _check(rows, ["id"], sync_mode=sync_mode)
    assert result["blocks_transfer"] is False, result
    assert result["passed"] is True


@pytest.mark.parametrize("sync_mode", ALL_MODES)
def test_unique_composite_key_with_repeating_components_never_blocks(sync_mode: str) -> None:
    rows = [
        {"region": "eu", "id": 1},
        {"region": "us", "id": 1},
        {"region": "eu", "id": 2},
        {"region": "us", "id": 2},
    ]
    result = _check(rows, ["region", "id"], sync_mode=sync_mode)
    assert result["blocks_transfer"] is False, result


@pytest.mark.parametrize("sync_mode", BLOCKING_MODES)
def test_duplicate_scalar_key_blocks_keyed_and_overwrite_modes(sync_mode: str) -> None:
    rows = [{"id": 1}, {"id": 1}, {"id": 2}]
    result = _check(rows, ["id"], sync_mode=sync_mode)
    assert result["blocks_transfer"] is True, result
    assert result["passed"] is False


@pytest.mark.parametrize("sync_mode", BLOCKING_MODES)
def test_duplicate_composite_tuple_blocks_keyed_and_overwrite_modes(sync_mode: str) -> None:
    rows = [{"region": "eu", "id": 1}, {"region": "eu", "id": 1}, {"region": "us", "id": 1}]
    result = _check(rows, ["region", "id"], sync_mode=sync_mode)
    assert result["blocks_transfer"] is True, result
    assert any("duplicate" in str(i).lower() for i in result["issues"])


@pytest.mark.parametrize("sync_mode", APPEND_MODES)
def test_duplicate_key_on_append_heap_warns(sync_mode: str) -> None:
    result = _check([{"id": 1}, {"id": 1}], ["id"], sync_mode=sync_mode)
    assert result["blocks_transfer"] is False, result


@pytest.mark.parametrize("sync_mode", APPEND_MODES)
def test_duplicate_key_on_append_with_dest_pk_blocks(sync_mode: str) -> None:
    result = _check(
        [{"id": 1}, {"id": 1}], ["id"], sync_mode=sync_mode, destination_pk_columns=["id"]
    )
    assert result["blocks_transfer"] is True, result


@pytest.mark.parametrize("sync_mode", ["overwrite", "replace", "full_refresh_overwrite"])
def test_overwrite_probe_duplicates_block_and_unique_probe_passes(sync_mode: str) -> None:
    rows = [{"id": 1}, {"id": 2}]
    dup = _check(
        rows,
        ["id"],
        sync_mode=sync_mode,
        source_duplicate_findings=[{"value": 7, "count": 2}],
        source_duplicate_probe_status="ran",
        source_duplicate_probe_expected=True,
    )
    assert dup["blocks_transfer"] is True, dup
    clean = _check(
        rows,
        ["id"],
        sync_mode=sync_mode,
        source_duplicate_findings=[],
        source_duplicate_probe_status="ran",
        source_duplicate_probe_expected=True,
    )
    assert clean["blocks_transfer"] is False, clean


@pytest.mark.parametrize("sync_mode", ["overwrite", "full_refresh_overwrite"])
def test_overwrite_probe_unavailable_fails_closed(sync_mode: str) -> None:
    result = _check(
        [{"id": 1}, {"id": 2}],
        ["id"],
        sync_mode=sync_mode,
        source_duplicate_findings=[],
        source_duplicate_probe_status="error",
        source_duplicate_probe_message="simulated",
        source_duplicate_probe_expected=True,
    )
    assert result["blocks_transfer"] is True, result


def test_overwrite_without_any_identity_key_does_not_block() -> None:
    result = _check_duplicate_keys(
        [{"source": "v", "target": "v"}],
        [{"v": 1}, {"v": 1}],
        sync_mode="overwrite",
        dest_kind="postgresql",
        primary_key=None,
    )
    assert result["blocks_transfer"] is False


def test_scd2_history_key_checks_business_key_only() -> None:
    """An SCD2 destination key (id, valid_from) must not reach the source probe."""
    unique = _check(
        [{"id": 1, "v": "a"}, {"id": 2, "v": "a"}],
        ["id", "valid_from"],
        sync_mode="scd2",
        destination_pk_columns=["id", "valid_from"],
    )
    assert unique["blocks_transfer"] is False, unique
    dup = _check(
        [{"id": 1, "v": "a"}, {"id": 1, "v": "b"}],
        ["id", "valid_from"],
        sync_mode="scd2",
        destination_pk_columns=["id", "valid_from"],
    )
    assert dup["blocks_transfer"] is True, dup


@pytest.mark.parametrize("sync_mode", ["overwrite", "replace", "full_refresh_overwrite"])
def test_audit_report_blocks_inferred_duplicate_pk_on_overwrite(sync_mode: str) -> None:
    report = run_integrity_audit(
        source_columns=["order_id"],
        mappings=[{"source": "order_id", "target": "order_id", "confidence": 1.0}],
        sample_rows=[{"order_id": "1"}, {"order_id": "1"}],
        destination_db_type="postgresql",
        validation_mode="strict",
        sync_mode=sync_mode,
    )
    dup = next(c for c in report["checks"] if c["check"] == "duplicate_keys")
    assert dup["blocks_transfer"] is True
    assert report["passed"] is False


@pytest.mark.parametrize("sync_mode", ["overwrite", "upsert"])
def test_audit_report_unique_pk_on_overwrite_passes(sync_mode: str) -> None:
    report = run_integrity_audit(
        source_columns=["order_id"],
        mappings=[{"source": "order_id", "target": "order_id", "confidence": 1.0}],
        sample_rows=[{"order_id": "1"}, {"order_id": "2"}],
        destination_db_type="postgresql",
        validation_mode="strict",
        sync_mode=sync_mode,
    )
    dup = next(c for c in report["checks"] if c["check"] == "duplicate_keys")
    assert dup["blocks_transfer"] is False, dup
