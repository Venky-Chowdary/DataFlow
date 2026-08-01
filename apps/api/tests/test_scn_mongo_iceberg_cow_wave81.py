"""Wave 81: Oracle SCN / Mongo resume family isolation + Iceberg CoW honesty.

Research anchors
----------------
- Debezium Oracle uses SCN watermarks; SQL Server CT uses integer versions —
  bare integers must not share a compare family (silent invent).
- MongoDB change-stream resume tokens are opaque (``_data``) — prefix isolate.
- Iceberg V3 MoR / deletion vectors are competitor-class for CDC lakes; DataFlow
  filesystem path is Copy-on-Write today — advertise honestly (not MoR).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_oracle_scn_isolated_from_mssql_ct_version():
    from connectors.writer_common import (
        compare_lsn,
        extract_cdc_lsn,
        lsn_family,
        lsn_is_newer,
    )

    scn = extract_cdc_lsn({"scn": 100})
    ct = extract_cdc_lsn({"version": 100})
    assert scn == "scn:100"
    assert ct == f"{100:020d}"
    assert lsn_family(scn) == "oracle_scn"
    assert lsn_family(ct) == "numeric_version"
    # Same numeric magnitude must not invent newer across dialects.
    assert compare_lsn(scn, ct) == 0
    assert not lsn_is_newer(scn, ct)
    assert not lsn_is_newer(ct, scn)

    assert compare_lsn("scn:200", "scn:100") == 1
    assert lsn_is_newer("scn:200", "scn:100")


def test_mongo_resume_token_family():
    from connectors.writer_common import extract_cdc_lsn, lsn_family, compare_lsn

    tok = extract_cdc_lsn({"_data": "8264ABCDEF"})
    assert tok == "mongo:8264ABCDEF"
    assert lsn_family(tok) == "mongo_resume"
    assert compare_lsn(tok, "opaque-other") == 0
    assert compare_lsn(tok, extract_cdc_lsn({"_data": "8264ABCDEF"})) == 0


def test_incomparable_family_surfaces_in_effectively_once():
    from services.cdc_effectively_once import should_apply_pk_delete, should_apply_pk_row

    upsert = should_apply_pk_row(existing_lsn="0/100", incoming_lsn="scn:999")
    assert upsert.applied is False
    assert upsert.reason == "incomparable_lsn_family"

    delete = should_apply_pk_delete(existing_lsn="0/100", incoming_lsn="mongo:abc")
    assert delete.applied is False
    assert delete.reason == "incomparable_lsn_family"


def test_iceberg_capability_honest_copy_on_write():
    from services.connector_capability_registry import get_connector_capability

    caps = get_connector_capability("iceberg")
    assert caps.get("write_strategy") == "copy-on-write"
    assert caps.get("supports_merge_on_read") is False
    assert caps.get("supports_lsn_guard") is True
    issues = " ".join(caps.get("common_issues") or []).lower()
    assert "copy-on-write" in issues or "merge-on-read" in issues


def test_iceberg_snapshot_stamps_write_strategy(tmp_path):
    from connectors.iceberg_writer import write_mapped_rows, _load_metadata
    from pathlib import Path

    warehouse = str(tmp_path / "wh")
    mappings = [
        {"source": "id", "target": "id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    result = write_mapped_rows(
        connection_string=warehouse,
        table_name="orders",
        headers=["id", "v"],
        data_rows=[["1", "a"]],
        mappings=mappings,
        write_mode="append",
    )
    assert result.ok
    meta_dir = Path(warehouse) / "orders" / "metadata"
    versions = sorted(meta_dir.glob("v*.metadata.json"))
    meta = _load_metadata(versions[-1])
    assert (meta.get("properties") or {}).get("dataflow.write_strategy") == (
        "copy-on-write"
    )
    snap = (meta.get("snapshots") or [])[-1]
    assert (snap.get("summary") or {}).get("dataflow.write_strategy") == (
        "copy-on-write"
    )
