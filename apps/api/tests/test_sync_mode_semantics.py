"""Unit proofs: append never drops; overwrite always drops; aliases + contracts."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from services.sync_cursor import (  # noqa: E402
    SyncContract,
    is_overwrite_sync,
    normalize_sync_mode,
    resolve_effective_sync_mode,
    should_drop_destination_for_sync,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("full_refresh_append", "full_refresh_append"),
        ("Full Append", "full_refresh_append"),
        ("append", "full_refresh_append"),
        ("insert", "full_refresh_append"),
        ("full_append", "full_refresh_append"),
        ("full_refresh_overwrite", "full_refresh_overwrite"),
        ("overwrite", "full_refresh_overwrite"),
        ("replace", "full_refresh_overwrite"),
        ("truncate", "full_refresh_overwrite"),
        ("", "full_refresh_append"),
        (None, "full_refresh_append"),
    ],
)
def test_normalize_sync_mode_aliases(raw, expected):
    assert normalize_sync_mode(raw) == expected


@pytest.mark.parametrize(
    "mode",
    [
        "full_refresh_append",
        "append",
        "insert",
        "full_append",
        "incremental_append",
    ],
)
def test_append_modes_do_not_drop(mode):
    assert not should_drop_destination_for_sync(request_sync_mode=mode)
    assert not is_overwrite_sync(mode)


@pytest.mark.parametrize(
    "mode",
    [
        "full_refresh_overwrite",
        "overwrite",
        "replace",
        "truncate",
        "full_overwrite",
    ],
)
def test_overwrite_modes_do_drop(mode):
    assert is_overwrite_sync(mode)
    assert should_drop_destination_for_sync(request_sync_mode=mode)


def test_empty_contract_inherits_request_append_no_drop():
    """Missing contract sync_mode must not silently upgrade append → overwrite."""
    contract = SyncContract.from_dict({"name": "csv", "selected": True})
    assert contract.sync_mode == ""
    effective = resolve_effective_sync_mode("full_refresh_append", contract.sync_mode)
    assert effective == "full_refresh_append"
    assert not should_drop_destination_for_sync(
        request_sync_mode="full_refresh_append",
        contract_sync_mode=contract.sync_mode,
    )


def test_explicit_contract_overwrite_wins_over_request_append():
    effective = resolve_effective_sync_mode("full_refresh_append", "full_refresh_overwrite")
    assert effective == "full_refresh_overwrite"
    assert should_drop_destination_for_sync(
        request_sync_mode="full_refresh_append",
        contract_sync_mode="full_refresh_overwrite",
    )


def test_explicit_contract_append_wins_over_request_overwrite():
    effective = resolve_effective_sync_mode("full_refresh_overwrite", "full_refresh_append")
    assert effective == "full_refresh_append"
    assert not should_drop_destination_for_sync(
        request_sync_mode="full_refresh_overwrite",
        contract_sync_mode="full_refresh_append",
    )


def test_default_request_mode_is_non_destructive():
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="sqlite", table="t"),
    )
    assert req.sync_mode == "full_refresh_append"
    assert not should_drop_destination_for_sync(request_sync_mode=req.sync_mode)
