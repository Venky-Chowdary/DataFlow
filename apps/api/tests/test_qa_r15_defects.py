"""Round 15 handover blockers — pickup, checksum, duplicates, replay, lenient."""

from __future__ import annotations

from decimal import Decimal

from services.duplicate_policy import (
    current_match_action,
    reset_match_action,
    resolve_duplicate_policy,
    set_match_action,
)
from services.file_pickup import (
    RemoteFile,
    file_upsert_rereads,
    is_file_pattern,
    plan_pickup,
    select_new_files,
)
from services.reconciliation import fingerprint_for_reconcile
from services.transform_engine import ambiguous_date_columns


def test_glob_schedule_picks_only_new_files_and_names_a_late_file():
    assert is_file_pattern("drop/*.csv")
    assert is_file_pattern("drop/")
    assert is_file_pattern("drop/{date}.csv")
    assert not is_file_pattern("drop/orders.csv")
    files = [
        RemoteFile("drop/a.csv", size=10, checksum="aaa", mtime="1"),
        RemoteFile("drop/b.csv", size=20, checksum="bbb", mtime="2"),
        RemoteFile("other/c.csv", size=1, checksum="c", mtime="3"),
    ]
    first = plan_pickup(pattern="drop/*.csv", files=files, manifest=[], wait_seconds=0)
    assert first.action == "load"
    assert [item.name for item in first.files] == ["drop/a.csv", "drop/b.csv"]
    manifest = [
        {"name": "drop/a.csv", "size": 10, "checksum": "aaa", "mtime": "1"},
    ]
    second = select_new_files(files, manifest, "drop/*.csv")
    assert [item.name for item in second] == ["drop/b.csv"]
    # Same name, new checksum — the corrected file is new.
    changed = [
        RemoteFile("drop/a.csv", size=10, checksum="zzz", mtime="9"),
    ]
    assert [item.name for item in select_new_files(changed, manifest, "drop/*.csv")] == [
        "drop/a.csv"
    ]
    # SFTP lists basenames inside the drop directory.
    assert [item.name for item in select_new_files(
        [RemoteFile("a.csv", size=1, checksum="a", mtime="1")], [], "drop/*.csv"
    )] == ["a.csv"]
    waiting = plan_pickup(
        pattern="drop/*.csv", files=[], manifest=[], wait_seconds=300, waited_seconds=0
    )
    assert waiting.retryable is True
    assert "file not arrived" in waiting.message
    assert "no new file yet" in waiting.message
    elapsed = plan_pickup(
        pattern="drop/*.csv", files=[], manifest=[], wait_seconds=300, waited_seconds=300
    )
    assert elapsed.retryable is False
    assert "wait window elapsed" in elapsed.message
    missing = plan_pickup(pattern="drop/*.csv", files=[], manifest=[], wait_seconds=0)
    assert "no wait window" in missing.message


def test_changed_file_under_upsert_is_not_watermark_filtered():
    assert file_upsert_rereads(
        sync_mode="upsert",
        already_loaded=False,
        has_key=True,
    )
    # The first incremental load stays on the cursor so identity COPY still
    # has a mapped cursor_field. A checksum change is a new file: reprocess.
    assert not file_upsert_rereads(
        sync_mode="incremental_deduped",
        already_loaded=False,
        has_key=True,
    )
    assert file_upsert_rereads(
        sync_mode="incremental_deduped",
        already_loaded=False,
        has_key=True,
        checksum_changed=True,
    )
    assert not file_upsert_rereads(
        sync_mode="incremental_deduped",
        already_loaded=True,
        has_key=True,
        checksum_changed=True,
    )
    assert not file_upsert_rereads(
        sync_mode="upsert",
        duplicate_policy="append",
        already_loaded=False,
        has_key=True,
    )


def test_append_with_a_key_upserts_unless_append_is_explicit():
    # full_refresh_append already means insert. An empty policy does not
    # rewrite it; the second run still lands 2N. Explicit policy does.
    implied = resolve_duplicate_policy(
        sync_mode="full_refresh_append",
        business_key=True,
        write_mode="insert",
    )
    assert implied.write_mode == "insert"
    explicit_upsert = resolve_duplicate_policy(
        duplicate_policy="upsert",
        sync_mode="full_refresh_append",
        business_key=True,
        write_mode="insert",
    )
    assert explicit_upsert.write_mode == "upsert"
    assert explicit_upsert.explicit is True
    explicit = resolve_duplicate_policy(
        duplicate_policy="append",
        sync_mode="full_refresh_append",
        business_key=True,
        write_mode="insert",
    )
    assert explicit.write_mode == "insert"
    assert explicit.explicit is True
    skip = resolve_duplicate_policy(
        duplicate_policy="skip_duplicates",
        business_key=True,
        write_mode="insert",
    )
    assert skip.policy == "dedupe_skip"
    assert skip.match_action == "skip"


def test_snowflake_skip_duplicates_inserts_only_when_not_matched():
    from connectors.snowflake_writer import build_snowflake_merge_sql

    token = set_match_action("skip")
    try:
        sql = build_snowflake_merge_sql(
            "ORDERS", "ORDERS_STAGE", ["ID", "AMOUNT"], ["ID"]
        )
    finally:
        reset_match_action(token)
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "WHEN MATCHED" not in sql
    assert current_match_action() == "update"


def test_decimal_scale_and_null_and_timestamp_share_one_fingerprint():
    # A typed NUMERIC carrier folds scale. A bare Auto string ``1.500`` stays
    # opaque: it can mean 1 or 1000, and Decimal(text) must not invent 1.
    assert fingerprint_for_reconcile(Decimal("1.500")) == "1.5"
    assert fingerprint_for_reconcile("1.500", ddl_type="NUMERIC") == fingerprint_for_reconcile(
        Decimal("1.500"), ddl_type="NUMERIC"
    )
    assert fingerprint_for_reconcile("1.500") == "1.500"
    assert fingerprint_for_reconcile("10.50") == fingerprint_for_reconcile(Decimal("10.50"))
    assert fingerprint_for_reconcile(None) == fingerprint_for_reconcile("__DF_SQL_NULL__")
    from datetime import datetime, timezone

    aware = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert fingerprint_for_reconcile(aware) == fingerprint_for_reconcile(
        "2026-01-01 12:00:00+00"
    )


def test_iso_timestamp_does_not_raise_a_locale_warning():
    rows = [{"ts": "2026-01-02 15:04:05"}, {"ts": "2026-01-03T00:00:00Z"}]
    assert ambiguous_date_columns(rows, ["ts"]) == []


def test_single_cell_quarantine_edit_keeps_the_original_record():
    from services.quarantine_dlq import overlay_cell_edit

    durable = [
        {
            "row": 4,
            "column": "amount",
            "value": "not-a-number",
            "source_values": {
                "id": "4",
                "name": "ada",
                "amount": "not-a-number",
                "city": "paris",
                "qty": "2",
                "note": "ok",
                "flag": "y",
            },
        }
    ]
    merged = overlay_cell_edit(
        {"row": 4, "column": "amount", "value": "12.50"},
        durable,
    )
    assert merged["source_values"]["amount"] == "12.50"
    assert merged["source_values"]["name"] == "ada"
    assert merged["source_values"]["city"] == "paris"
    assert len(merged["source_values"]) == 7


def test_text_to_decimal_plan_matches_the_write_resolver():
    from services.transform_resolver import resolve_transform, write_plan_transform

    mapping = {
        "source": "qty",
        "target": "qty",
        "transform": "none",
        "target_type": "DECIMAL",
    }
    types = {"qty": "TEXT"}
    planned = write_plan_transform(mapping, column_types=types)
    runtime = resolve_transform(mapping, column_types=types, dest_types={"qty": "DECIMAL"})
    assert planned == runtime == "decimal"


def test_lenient_is_reported_as_lenient():
    from services.validation_mode_contract import mode_contract, normalize_validation_mode

    assert normalize_validation_mode("lenient") == "lenient"
    assert mode_contract("lenient")["id"] == "lenient"
    assert mode_contract("lenient")["confidence_floor"] == 0.75
