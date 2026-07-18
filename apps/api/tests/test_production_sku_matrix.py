"""Production SKU transfer matrix.

Runs the 5 × 5 source × destination pairs we explicitly support and verifies
each route with two reference rows, full reconciliation, and a pipeline
explanation. Routes whose source or destination is not reachable on the
machine are skipped so the same test runs in CI and locally.
"""

from __future__ import annotations

import contextlib
import uuid
from pathlib import Path

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from src.transfer.registry import PRODUCTION_SKU

try:
    from test_execute_tracked_universal_matrix import (
        MAPPINGS,
        _build_destination,
        _build_source,
        _endpoint_reachable,
        _seed_source,
        _uses_snowflake,
    )
except Exception:  # pragma: no cover - fallback if import path differs
    from tests.test_execute_tracked_universal_matrix import (
        MAPPINGS,
        _build_destination,
        _build_source,
        _endpoint_reachable,
        _seed_source,
        _uses_snowflake,
    )


@pytest.mark.parametrize(
    "route",
    PRODUCTION_SKU,
    ids=lambda r: f"{r[0]}_{r[1]}_to_{r[2]}_{r[3]}",
)
def test_production_sku_transfer(route: tuple[str, str, str, str], tmp_path: Path) -> None:
    src_kind, src_fmt, dst_kind, dst_fmt = route
    suffix = uuid.uuid4().hex[:12]

    source, source_content, source_filename = _build_source(src_kind, src_fmt, tmp_path, suffix)
    destination = _build_destination(dst_kind, dst_fmt, tmp_path, suffix)

    if not _endpoint_reachable(source):
        pytest.skip(f"source {src_kind}/{src_fmt} not reachable")
    if not _endpoint_reachable(destination):
        pytest.skip(f"destination {dst_kind}/{dst_fmt} not reachable")

    request = TransferRequest(
        source=source,
        destination=destination,
        source_content=source_content,
        source_filename=source_filename,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=MAPPINGS,
    )

    snowflake = pytest.importorskip("fakesnow") if _uses_snowflake(source, destination) else None
    ctx = snowflake.patch() if snowflake else contextlib.nullcontext()

    engine = UniversalTransferEngine()
    with ctx:
        if source.kind == "database":
            _seed_source(source)
        result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{route}: {result.error}"
    assert result.records_transferred == 2, (
        f"{route}: expected 2 records, got {result.records_transferred}"
    )
    assert result.explanation, f"{route}: missing pipeline explanation"

    if destination.kind == "database":
        assert result.reconciliation.get("passed") is True, (
            f"{route}: reconciliation failed: {result.reconciliation}"
        )
    else:
        assert result.destination_summary.get("filename"), (
            f"{route}: no exported filename in destination_summary"
        )
