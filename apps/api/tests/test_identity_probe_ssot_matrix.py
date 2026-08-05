"""Identity / source-probe SSOT matrix — sync modes × uniqueness × probe status.

Proves the shared algorithm (not a single MySQL→Postgres paste):
- append/create-new without dest UNIQUE → probe dups warn
- append + dest PK → probe dups block
- upsert/CDC/overwrite → probe unavailable fail-closed
- insert alias normalizes to append
- unsupported/error probe never stamps false full_selected
"""

from __future__ import annotations

from typing import Any

import pytest

from services.data_integrity import _check_duplicate_keys, run_integrity_audit
from services.source_duplicate_probe import (
    SourceDuplicateProbeResult,
    probe_source_duplicate_keys_result,
)


FINDINGS = [{"value": "dup-a", "count": 3}, {"value": "dup-b", "count": 2}]
ROWS = [{"id": "unique-1"}, {"id": "unique-2"}]
MAPPINGS = [{"source": "id", "target": "id", "confidence": 0.99}]


def _dup_check(**kwargs: Any) -> dict[str, Any]:
    base = dict(
        mappings=MAPPINGS,
        rows=ROWS,
        validation_mode="strict",
        dest_kind="postgresql",
        primary_key="id",
        source_duplicate_findings=FINDINGS,
        destination_pk_columns=[],
        destination_unique_keys=[],
    )
    base.update(kwargs)
    return _check_duplicate_keys(**base)


@pytest.mark.parametrize(
    "sync_mode",
    ["full_refresh_append", "append", "insert", "full_append", "incremental_append"],
)
def test_append_aliases_warn_on_probe_dups_without_dest_pk(sync_mode: str) -> None:
    result = _dup_check(sync_mode=sync_mode)
    assert result["blocks_transfer"] is False
    assert result["passed"] is True
    blob = " ".join(str(w) for w in (result.get("warnings") or []))
    assert "duplicate" in blob.lower() or "probe" in (result.get("note") or "").lower()


@pytest.mark.parametrize(
    "sync_mode",
    ["upsert", "cdc", "cdc_incremental", "mirror", "full_refresh_mirror", "scd2"],
)
def test_uniqueness_required_modes_block_on_probe_dups(sync_mode: str) -> None:
    result = _dup_check(sync_mode=sync_mode)
    assert result["blocks_transfer"] is True
    assert result["passed"] is False
    joined = " ".join(result.get("issues") or [])
    assert "source probe" in joined.lower()


@pytest.mark.parametrize("sync_mode", ["full_refresh_overwrite", "overwrite", "replace"])
def test_overwrite_modes_block_on_probe_dups(sync_mode: str) -> None:
    result = _dup_check(sync_mode=sync_mode)
    assert result["blocks_transfer"] is True


def test_append_with_dest_pk_blocks_on_probe_dups() -> None:
    result = _dup_check(
        sync_mode="full_refresh_append",
        destination_pk_columns=["id"],
    )
    assert result["blocks_transfer"] is True


@pytest.mark.parametrize(
    "probe_status",
    ["error", "skipped_unsupported", "skipped_no_source"],
)
def test_probe_unavailable_fail_closed_for_upsert(probe_status: str) -> None:
    result = _dup_check(
        sync_mode="upsert",
        source_duplicate_findings=[],
        source_duplicate_probe_status=probe_status,
        source_duplicate_probe_message=f"simulated {probe_status}",
        source_duplicate_probe_expected=True,
    )
    assert result["blocks_transfer"] is True
    joined = " ".join(result.get("issues") or [])
    assert "probe unavailable" in joined.lower()


def test_probe_unavailable_warns_for_append_create_new() -> None:
    result = _dup_check(
        sync_mode="full_refresh_append",
        source_duplicate_findings=[],
        source_duplicate_probe_status="error",
        source_duplicate_probe_message="connection refused",
        source_duplicate_probe_expected=True,
    )
    assert result["blocks_transfer"] is False
    assert result["passed"] is True
    warn = " ".join(str(w) for w in (result.get("warnings") or []))
    assert "probe unavailable" in warn.lower()


def test_schemaless_mongo_dest_blocks_on_probe_dups() -> None:
    result = _dup_check(sync_mode="full_refresh_append", dest_kind="mongodb")
    assert result["blocks_transfer"] is True


def test_integrity_audit_never_stamps_full_selected_on_error_status() -> None:
    report = run_integrity_audit(
        source_columns=["id"],
        mappings=MAPPINGS,
        sample_rows=ROWS,
        validation_mode="strict",
        destination_db_type="postgresql",
        sync_mode="full_refresh_append",
        contract_primary_key="id",
        source_duplicate_findings=[],
        source_duplicate_probe_ran=False,
        source_duplicate_probe_pk="id",
        source_duplicate_probe_status="error",
        source_duplicate_probe_message="dialect timeout",
        source_duplicate_probe_expected=True,
    )
    probe = report["source_uniqueness_probe"]
    assert probe["ran"] is False
    assert probe["coverage"] == "sample"
    assert probe["status"] == "error"


def test_probe_result_unsupported_redis_is_not_ran() -> None:
    result = probe_source_duplicate_keys_result(
        source_config={"type": "redis", "host": "localhost"},
        source_table="ignored",
        primary_key="id",
    )
    assert isinstance(result, SourceDuplicateProbeResult)
    assert result.status == "skipped_unsupported"
    assert result.ran is False
    assert result.findings == []


def test_probe_result_missing_source_is_honest() -> None:
    result = probe_source_duplicate_keys_result(
        primary_key="id",
        source_table="users",
    )
    assert result.status == "skipped_no_source"
    assert result.ran is False


def test_composite_probe_findings_block_upsert() -> None:
    """Composite tuple findings are authoritative for uniqueness-required modes."""
    result = _dup_check(
        sync_mode="upsert",
        primary_key="org_id",
        source_duplicate_findings=[
            {
                "value": "(o1, a)",
                "values": {"org_id": "o1", "code": "a"},
                "columns": ["org_id", "code"],
                "count": 2,
            }
        ],
        destination_pk_columns=["org_id", "code"],
        mappings=[
            {"source": "org_id", "target": "org_id", "confidence": 0.99},
            {"source": "code", "target": "code", "confidence": 0.99},
        ],
    )
    assert result["blocks_transfer"] is True


def test_append_with_composite_dest_pk_blocks_on_probe_tuple_dups() -> None:
    """Append into composite UNIQUE/PK must not green Validate when probe finds tuple dups."""
    result = _dup_check(
        sync_mode="full_refresh_append",
        primary_key="org_id",
        source_duplicate_findings=[
            {
                "value": "(o1, a)",
                "values": {"org_id": "o1", "code": "a"},
                "columns": ["org_id", "code"],
                "count": 2,
            }
        ],
        destination_pk_columns=["org_id", "code"],
        mappings=[
            {"source": "org_id", "target": "org_id", "confidence": 0.99},
            {"source": "code", "target": "code", "confidence": 0.99},
        ],
    )
    assert result["blocks_transfer"] is True


def test_resolve_primary_key_source_columns_composite_dest() -> None:
    from services.primary_key import resolve_primary_key_source_columns

    cols = resolve_primary_key_source_columns(
        mappings=[
            {"source": "org_id", "target": "org_id"},
            {"source": "code", "target": "code"},
        ],
        source_columns=["org_id", "code", "name"],
        dest_kind="postgresql",
        destination_pk_columns=["org_id", "code"],
    )
    assert cols == ["org_id", "code"]


def test_resolve_primary_key_source_columns_from_stream_contracts() -> None:
    """Create-new without dest PK still probes full Studio composite contract."""
    from services.primary_key import resolve_primary_key_source_columns

    cols = resolve_primary_key_source_columns(
        mappings=[
            {"source": "org_id", "target": "org_id"},
            {"source": "code", "target": "code"},
        ],
        source_columns=["org_id", "code", "name"],
        dest_kind="postgresql",
        destination_pk_columns=[],
        contract_primary_key="org_id",  # truncated legacy first-col must not win
        stream_contracts=[
            {"name": "items", "selected": True, "primary_key": ["org_id", "code"]}
        ],
        stream_name="items",
    )
    assert cols == ["org_id", "code"]


def test_resolve_primary_key_source_columns_ignores_unmatched_stream() -> None:
    """Mismatched stream_name must not steal contracts[0] identity."""
    from services.primary_key import resolve_primary_key_source_columns

    cols = resolve_primary_key_source_columns(
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "order_id", "target": "order_id"},
        ],
        source_columns=["id", "order_id"],
        dest_kind="postgresql",
        destination_pk_columns=["id"],
        stream_contracts=[
            {"name": "orders", "selected": True, "primary_key": ["order_id"]},
            {"name": "users", "selected": True, "primary_key": ["id"]},
        ],
        stream_name="users_v2",  # no exact match
    )
    # Fall through to destination PK / resolver — not orders.order_id.
    assert cols == ["id"]
