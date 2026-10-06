"""Independent source re-read — when, how, and Gate-8 provenance.

Named hole: Snowflake→Postgres 150k streaming used write-pass fingerprints,
then (if the operator opted into re-read) OFFSET-paged the second scan and
stamped the digest as writer_ack. Neither path can earn migration_proven.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.reconcile_coverage import SOURCE_DIGEST_SOURCE_REREAD
from services.source_reread import (
    alignment_when_write_pass_empty,
    engine_family,
    reread_pagination_plan,
    should_reread_source,
)
from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import run_reconciliation


def test_reread_without_a_write_pass_digest_is_not_a_mismatch():
    skipped = alignment_when_write_pass_empty(2, inline_fingerprints_used=False)
    assert skipped["identity_hash_aligned"] is None
    assert skipped["reason"] == "write_pass_not_fingerprinted"
    assert skipped["reread_rows"] == 2
    missed = alignment_when_write_pass_empty(2, inline_fingerprints_used=True)
    assert missed["identity_hash_aligned"] is False
    assert missed["reason"] == "write_pass_empty"


def test_engine_family_collapses_postgres_aliases():
    assert engine_family("postgres") == engine_family("postgresql") == "postgresql"
    assert engine_family("redshift") == "postgresql"
    assert engine_family("snowflake") == "snowflake"
    assert engine_family("mysql") != engine_family("postgresql")


def test_auto_reread_full_refresh_includes_same_engine(monkeypatch):
    """HVR bulk compare re-reads source and target, including same-engine refresh."""
    monkeypatch.delenv("DATAFLOW_RECONCILE_SOURCE_REREAD", raising=False)
    monkeypatch.delenv("DATAWRAP_RECONCILE_SOURCE_REREAD", raising=False)
    monkeypatch.delenv("RECONCILE_SOURCE_REREAD", raising=False)
    assert should_reread_source(src_type="snowflake", dest_type="postgresql") is True
    assert should_reread_source(src_type="mysql", dest_type="postgresql") is True
    assert should_reread_source(src_type="postgresql", dest_type="postgresql") is True
    assert should_reread_source(src_type="postgres", dest_type="postgres") is True
    assert should_reread_source(src_type="sqlite", dest_type="sqlite") is True
    assert should_reread_source(src_type="mssql", dest_type="postgresql") is True
    assert should_reread_source(src_type="s3", dest_type="postgresql") is False
    assert should_reread_source(src_type="snowflake", dest_type="kafka") is False
    assert should_reread_source(
        src_type="postgresql", dest_type="postgresql", incremental=True
    ) is False
    assert should_reread_source(
        src_type="snowflake", dest_type="postgresql", incremental=True
    ) is False


def test_env_off_cannot_suppress_partial_write_pass(monkeypatch):
    monkeypatch.setenv("DATAFLOW_RECONCILE_SOURCE_REREAD", "0")
    assert should_reread_source(src_type="snowflake", dest_type="postgresql") is False
    assert should_reread_source(
        src_type="sqlite",
        dest_type="sqlite",
        partial_write_pass=True,
    ) is True


def test_env_on_forces_same_engine_reread(monkeypatch):
    monkeypatch.setenv("DATAFLOW_RECONCILE_SOURCE_REREAD", "1")
    assert should_reread_source(src_type="sqlite", dest_type="sqlite") is True


def test_reread_plan_never_offsets_snapshot_scan_sources():
    for src in (
        "snowflake",
        "mysql",
        "postgresql",
        "mongodb",
        "sqlite",
        "oracle",
        "sqlserver",
        "bigquery",
    ):
        plan = reread_pagination_plan(src_type=src, incremental=False)
        assert plan["mode"] == "scan"
        assert plan["use_offset"] is False
        assert isinstance(plan["scan_state"], dict)


def test_reread_plan_incremental_keeps_cursor_path():
    plan = reread_pagination_plan(src_type="snowflake", incremental=True)
    assert plan["mode"] == "cursor_or_offset"
    assert plan["scan_state"] is None


def test_file_auto_reread_into_warehouse_not_same_file(monkeypatch):
    from services.source_reread import should_reread_file_source

    monkeypatch.delenv("DATAFLOW_RECONCILE_SOURCE_REREAD", raising=False)
    monkeypatch.delenv("DATAWRAP_RECONCILE_SOURCE_REREAD", raising=False)
    monkeypatch.delenv("RECONCILE_SOURCE_REREAD", raising=False)
    assert should_reread_file_source(file_type="excel", dest_type="postgresql") is True
    assert should_reread_file_source(file_type="csv", dest_type="sqlite") is True
    assert should_reread_file_source(file_type="csv", dest_type="s3") is False
    assert should_reread_file_source(
        file_type="excel", dest_type="postgresql", incremental=True
    ) is False


def test_file_reread_off_cannot_suppress_partial_resume(monkeypatch):
    from services.source_reread import should_reread_file_source

    monkeypatch.setenv("DATAFLOW_RECONCILE_SOURCE_REREAD", "0")
    assert should_reread_file_source(file_type="excel", dest_type="postgresql") is False
    assert should_reread_file_source(
        file_type="csv",
        dest_type="sqlite",
        partial_write_pass=True,
    ) is True


def test_identity_alignment_requires_the_same_keys():
    """Same cell hashes under different keys are not an aligned population.

    The value digest ignores the row key so a destination re-read can match.
    Identity alignment is the separate proof that the second source parse
    named the same rows.
    """
    from services.fingerprint_accumulator import FingerprintAccumulator
    from services.source_reread import align_source_populations

    left = FingerprintAccumulator()
    right = FingerprintAccumulator()
    left.add_many([("id:1", "fp-a"), ("id:2", "fp-b")])
    right.add_many([("id:2", "fp-b"), ("id:1", "fp-a")])
    aligned = align_source_populations(left, right)
    assert aligned["identity_hash_aligned"] is True
    assert left.digest() == right.digest()

    shifted = FingerprintAccumulator()
    shifted.add_many([("id:9", "fp-a"), ("id:8", "fp-b")])
    again = FingerprintAccumulator()
    again.add_many([("id:1", "fp-a"), ("id:2", "fp-b")])
    # Value digests match — keys do not.
    assert shifted.identity_digest() != again.identity_digest()
    mismatch = align_source_populations(shifted, again)
    assert mismatch["identity_hash_aligned"] is False
    assert mismatch["reason"] == "identity_digest_differs"


def test_source_reread_checksum_mode_earns_full_checksum(monkeypatch):
    dest = EndpointConfig(kind="database", format="postgresql", table="customer")

    def fake_verify(*_a, **_k):
        return 150_000, "independent-digest"

    monkeypatch.setattr("src.transfer.reconcile_step.verify_target", fake_verify)
    monkeypatch.setattr(
        "src.transfer.reconcile_step._engine_digest_enabled",
        lambda: False,
    )
    monkeypatch.setattr(
        "src.transfer.reconcile_step._writer_supplied_engine_digests",
        lambda *_a, **_k: None,
    )

    report = run_reconciliation(
        endpoint=dest,
        records=[],
        columns=["c_custkey"],
        rows_written=150_000,
        writer_checksum="independent-digest",
        dest_summary={
            "source_row_count": 150_000,
            "checksum_mode": "source_reread",
            "source_independently_reread": True,
            "dest_count_before": 0,
            "sync_mode": "full_refresh_overwrite",
        },
        mappings=[{"source": "C_CUSTKEY", "target": "c_custkey"}],
        source_schema={"C_CUSTKEY": "DECIMAL(38,0)"},
        validation_mode="strict",
    )
    assert report["source_checksum_provenance"] == SOURCE_DIGEST_SOURCE_REREAD
    assert report["assurance_level"] == "full_checksum"
    assert report["coverage"] == "full_checksum"
    assert report.get("phase") == "post_write_verified"


def test_snowflake_scan_pins_time_travel_when_meta_bound():
    from connectors.snowflake_reader import read_table_scan_batch
    from services.source_snapshot import (
        activate_snapshot,
        end_snowflake_time_travel,
        release_active_snapshot,
    )

    cur = MagicMock()
    cur.description = [("C_CUSTKEY",)]
    cur.fetchone.return_value = (3,)
    cur.fetchmany.side_effect = [[(1,)], []]
    conn = MagicMock()
    conn.cursor.return_value = cur
    executed: list[tuple] = []

    def execute(sql, *args):
        executed.append((sql, args[0] if args else ()))
        return None

    cur.execute.side_effect = execute
    activate_snapshot(
        None,
        {
            "engine": "snowflake",
            "guarantee": "snowflake_time_travel",
            "time_travel_ts": "2026-08-15T20:00:00+00:00",
        },
        end_snowflake_time_travel,
    )
    try:
        with (
            patch("connectors.snowflake_reader.get_connection", return_value=conn),
            patch("connectors.snowflake_reader.normalize_account", return_value="acct"),
            patch(
                "connectors.snowflake_reader.resolve_or_fold_snowflake_table",
                return_value="CUSTOMER",
            ),
        ):
            state: dict = {}
            read_table_scan_batch(
                host="acct",
                port=443,
                database="SNOWFLAKE_SAMPLE_DATA",
                username="u",
                password="p",
                schema="TPCH_SF1",
                connection_string="",
                warehouse="COMPUTE_WH",
                table="CUSTOMER",
                columns=["C_CUSTKEY"],
                offset=0,
                limit=2,
                scan_state=state,
            )
    finally:
        release_active_snapshot(commit=True)

    snapshot_sql = [s for s, _ in executed if "C_CUSTKEY" in s.upper() and "ORDER BY" in s.upper()]
    assert snapshot_sql, executed
    assert "AT (TIMESTAMP => %s)" in snapshot_sql[0]
    assert "OFFSET" not in snapshot_sql[0].upper()
    bound = [args for s, args in executed if "AT (TIMESTAMP => %s)" in s]
    assert bound and bound[0] == ("2026-08-15T20:00:00+00:00",)
