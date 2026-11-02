"""Gate-6 regression pins for the batch-1 fixes.

- ``boundary_append_insert_sql`` must compare the staging cursor against the
  watermark literal in a separator-normalized text form on every dialect:
  the bookmark literal is rendered ISO ``YYYY-MM-DDT…`` while an engine's
  temporal ``::text``/CHAR cast renders space-separated, so a raw text-eq
  never matched and the boundary row re-inserted on run 2 (UniqueViolation).
- ``dest_count_from_recon`` must accept ``target_rows`` as the measured dest
  COUNT(*) when the report's scope is an engine-measured dest count
  (``dest_pk_join_equals_staging`` / ``dest_count_equals_source_snapshot``),
  even when the report is ``unproven`` on value fidelity — cardinality was
  genuinely measured and closes the keyed ledger.
"""

from services.copy_incremental import boundary_append_insert_sql
from services.row_conservation import (
    DEST_READBACK,
    dest_count_from_recon,
)


def test_boundary_append_insert_sql_normalizes_iso_separator_pg():
    sql = boundary_append_insert_sql(
        'public."dst"',
        'public."_df_stg_dst"',
        ["id", "updated_at"],
        lambda c: f'"{c}"',
        dialect="postgresql",
        cursor_col="updated_at",
        watermark="2024-12-02T12:00:00",
        pk_col="id",
    )
    # The literal is normalized to the engine's space-separated text form…
    assert "'2024-12-02 12:00:00'" in sql
    # …and the staging side applies the same ISO-T → space normalization.
    assert 'substring(s."updated_at"::text from 1 for 10)' in sql
    assert 's."updated_at"::text ~ ' in sql


def test_boundary_append_insert_sql_normalizes_iso_separator_sqlite():
    sql = boundary_append_insert_sql(
        '"dst"',
        '"_df_stg_dst"',
        ["id", "updated_at"],
        lambda c: f'"{c}"',
        dialect="sqlite",
        cursor_col="updated_at",
        watermark="2024-12-02T12:00:00",
        pk_col="id",
    )
    assert "'2024-12-02 12:00:00'" in sql
    # sqlite reuses the shared cursor sort normalizer on the staging side.
    assert 'CAST(s."updated_at" AS TEXT)' in sql and "GLOB" in sql


def test_dest_count_from_recon_accepts_engine_measured_pk_join_scope():
    # A keyed engine copy measured dest COUNT(*) / the dest key join itself.
    # ``unproven`` marks unproven VALUE fidelity, not cardinality — the keyed
    # ledger needs the real dest COUNT to close.
    report = {
        "passed": True,
        "unproven": True,
        "coverage": "none",
        "phase": "post_write_pending",
        "checksum_scope": "staging_count_equals_source_and_dest_pk_join_equals_staging",
        "checksum_match": False,
        "target_rows": 4,
        "target_rows_before": 3,
        "target_checksum": "41babf" + "0" * 58,
        "skipped_readback": None,
        "dest_count_source": None,
        "message": "Row count verified by engine copy: 4 row(s).",
    }
    assert dest_count_from_recon(report) == (4, DEST_READBACK)


def test_dest_count_from_recon_still_rejects_writer_ack_unproven():
    # Unproven with no engine-measured-count scope: target_rows is a writer
    # claim, not a measurement — must stay UNMEASURED.
    report = {
        "passed": True,
        "unproven": True,
        "target_rows": 4,
        "target_checksum": "",
        "message": "file/object export: dest not readable",
    }
    count, source = dest_count_from_recon(report)
    assert count is None
    assert source != DEST_READBACK
