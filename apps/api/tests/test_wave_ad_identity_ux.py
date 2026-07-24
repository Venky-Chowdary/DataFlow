"""Unit tests for sample-unique key suggestion helper (TS parity via Python stub not needed).

Honest sample uniqueness helpers live in apps/web; this file covers sync_requires_unique_identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))


def test_sync_requires_unique_identity_modes():
    from services.primary_key import sync_requires_unique_identity

    assert sync_requires_unique_identity("cdc")
    assert sync_requires_unique_identity("incremental_deduped")
    assert sync_requires_unique_identity("mirror")
    assert sync_requires_unique_identity("scd2")
    assert not sync_requires_unique_identity("full_refresh_append")
    assert not sync_requires_unique_identity("full_refresh_overwrite")
    assert not sync_requires_unique_identity("incremental_append")
    assert not sync_requires_unique_identity("")


def test_ddl_skips_duplicate_pk_for_append():
    from services.ddl_compatibility import evaluate_ddl_compatibility

    mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
    ]
    rows = [
        {"id": "a", "name": "1"},
        {"id": "a", "name": "2"},
    ]
    ok, issues = evaluate_ddl_compatibility(
        mappings=mappings,
        source_schema={"id": "TEXT", "name": "TEXT"},
        target_schema={},
        sample_rows=rows,
        table_exists=False,
        dest_connected=True,
        dest_db_type="postgresql",
        allow_create=True,
        sync_mode="full_refresh_append",
    )
    assert ok or not any("duplicate" in i.lower() for i in issues)
    assert not any("Primary key candidate" in i for i in issues)


def test_ddl_blocks_duplicate_pk_for_cdc():
    from services.ddl_compatibility import evaluate_ddl_compatibility

    mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
    ]
    rows = [
        {"id": "a", "name": "1"},
        {"id": "a", "name": "2"},
    ]
    _ok, issues = evaluate_ddl_compatibility(
        mappings=mappings,
        source_schema={"id": "TEXT", "name": "TEXT"},
        target_schema={},
        sample_rows=rows,
        table_exists=False,
        dest_connected=True,
        dest_db_type="postgresql",
        allow_create=True,
        sync_mode="cdc",
    )
    assert any("Primary key candidate" in i for i in issues)


def test_integrity_skips_dupes_for_append():
    from services.data_integrity import run_integrity_audit

    report = run_integrity_audit(
        source_columns=["id", "name"],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.95},
            {"source": "name", "target": "name", "confidence": 0.95},
        ],
        sample_rows=[{"id": "a", "name": "1"}, {"id": "a", "name": "2"}],
        destination_db_type="postgresql",
        validation_mode="strict",
        sync_mode="full_refresh_append",
    )
    dup = next((c for c in report.get("checks", []) if c.get("check") == "duplicate_keys"), None)
    assert dup is not None
    assert dup.get("passed") is True
    assert not dup.get("blocks_transfer")
