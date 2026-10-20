"""X-127: MSSQL/Oracle sources were dropped to write_pass_dest_readback.

``should_reread_source`` admitted them (both are in
``SNAPSHOT_SCAN_SOURCES``) but ``REREAD_SCAN_SOURCES`` lacked their engine
keys, so the re-read loop's ``elif src_read_type in REREAD_SCAN_SOURCES``
failed and the run stayed on the write pass — an MSSQL source could never
earn full_checksum/migration_proven where a DuckDB/MySQL source could.
"""
from __future__ import annotations

from services.source_reread import REREAD_SCAN_SOURCES, should_reread_source


def test_mssql_family_in_reread_sources():
    for eng in ("sqlserver", "mssql", "azure_sql"):
        assert eng in REREAD_SCAN_SOURCES


def test_oracle_family_in_reread_sources():
    for eng in ("oracle", "oracledb"):
        assert eng in REREAD_SCAN_SOURCES


def test_reread_membership_matches_policy():
    """Every engine should_reread_source admits via SNAPSHOT_SCAN_SOURCES
    must also be pageable by the re-read loop (one-bug-matrix)."""
    from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES

    for eng in sorted(SNAPSHOT_SCAN_SOURCES):
        if should_reread_source(src_type=eng, dest_type="postgresql"):
            assert eng in REREAD_SCAN_SOURCES or eng == "databricks", eng


def test_policy_admits_mssql_source():
    assert should_reread_source(src_type="sqlserver", dest_type="postgresql")
    assert should_reread_source(src_type="mssql", dest_type="duckdb")
