"""D409 omitted-column poll, D405 job text, D387 BigQuery host, D383 MariaDB, D381 parity.

Each case is the fixture named in the 17 Nov 2026 story. A pass is that
fixture only. It is not a live retest of the original job.
"""

from __future__ import annotations

from src.transfer.models import EndpointConfig


def test_verify_write_pairs_skip_an_omitted_column_and_keep_a_rename():
    from services.mapping_constraints import verify_write_pairs

    pairs = verify_write_pairs(
        [
            {"source": "id", "target": "id"},
            {"source": "note", "target": "memo"},
            {"source": "long_txt", "target": "long_txt", "transform": "omit"},
        ],
        key_target="id",
        key_source="id",
    )
    assert ("long_txt", "long_txt") not in pairs
    assert ("note", "memo") in pairs
    assert [target for _source, target in pairs] == ["id", "memo"]


def test_keyed_noop_poll_reads_only_written_columns(monkeypatch):
    import src.transfer.reconcile_step as step

    seen: dict[str, list[str]] = {}

    def fake_read(db_type, cfg, *, table, columns, postgres_default=False):
        seen[db_type] = list(columns or [])
        if db_type == "sqlserver":
            return [{"id": 1, "memo": "hello"}]
        return [{"id": 1, "note": "hello"}]

    monkeypatch.setattr(step, "_read_verify_rows", fake_read)
    out = step._covered_keyed_noop_verify(
        endpoint=EndpointConfig(format="sqlserver", host="localhost", table="orders", schema="dbo"),
        source_endpoint=EndpointConfig(format="postgresql", host="localhost", table="orders"),
        dest_summary={},
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "note", "target": "memo"},
            {"source": "long_txt", "target": "long_txt", "transform": "omit"},
        ],
        sync_mode="incremental_upsert",
        rows_before=4,
        target_rows=4,
        pk_column="id",
        db_type="sqlserver",
    )
    assert "long_txt" not in seen["sqlserver"]
    assert seen["sqlserver"] == ["id", "memo"]
    assert seen["postgresql"] == ["id", "note"]
    assert out is None or "long_txt" not in str(out.get("message") or "")


def test_keyed_noop_poll_message_has_no_statement_or_row_values(monkeypatch):
    import src.transfer.reconcile_step as step

    secret = "person@example.com"

    def fake_read(*_a, **_k):
        raise RuntimeError(
            "(207, Invalid column name 'long_txt') "
            "[SQL: SELECT email FROM orders] "
            f"[parameters: {{'email': '{secret}'}}]"
        )

    monkeypatch.setattr(step, "_read_verify_rows", fake_read)
    out = step._covered_keyed_noop_verify(
        endpoint=EndpointConfig(format="sqlserver", host="localhost", table="orders"),
        source_endpoint=EndpointConfig(format="postgresql", host="localhost", table="orders"),
        dest_summary={},
        mappings=[{"source": "id", "target": "id"}],
        sync_mode="incremental_upsert",
        rows_before=4,
        target_rows=4,
        pk_column="id",
        db_type="sqlserver",
    )
    message = out["message"]
    assert secret not in message
    assert "[SQL:" not in message
    assert "[parameters:" not in message
    assert "Reference " in message
    assert "Not migration_proven." in message


def test_stored_raw_failure_withholds_sql_and_parameters():
    from services.error_handling import format_raw_failure

    secret = "person@example.com"
    raw = format_raw_failure(
        RuntimeError(
            "(207, Invalid column name 'long_txt') "
            "[SQL: SELECT email FROM orders] "
            f"[parameters: {{'email': '{secret}'}}]"
        )
    )
    assert secret not in raw
    assert "[SQL: withheld]" in raw
    assert "[parameters: withheld]" in raw


def test_bigquery_empty_host_stays_empty_and_an_emulator_host_stays():
    from src.transfer.adapters import resolve_connector_config

    empty = resolve_connector_config(EndpointConfig(format="bigquery", host=""))
    assert empty["host"] == ""
    emulator = resolve_connector_config(
        EndpointConfig(format="bigquery", host="127.0.0.1", port=9050)
    )
    assert emulator["host"] == "127.0.0.1"
    sql = resolve_connector_config(EndpointConfig(format="mysql", host=""))
    assert sql["host"] == "localhost"


def test_mariadb_save_names_mysql_and_mysql_itself_is_not_refused():
    from src.ai.copilot.connector_create import driver_missing_refusal
    from src.transfer.connector_capabilities import unproven_wire_brand_refusal

    for catalog_id in ("mariadb", "tidb", "singlestore"):
        message = unproven_wire_brand_refusal(catalog_id)
        assert "mysql" in message
        assert catalog_id in message
        refusal = driver_missing_refusal(catalog_id)
        assert "Nothing was saved." in refusal
        assert "mysql" in refusal
    assert unproven_wire_brand_refusal("mysql") == ""


def _cell_match_table() -> dict:
    return {
        "unexplained": 0,
        "name": "orders",
        "status": "match",
        "check": "full_checksum",
        "explained": 0,
        "_note": "",
        "_at_least": False,
        "_sample": None,
    }


def _proven_job(**overrides) -> dict:
    job = {
        "status": "completed",
        "_id": "6afbac505eafee38493e1710",
        "reconciliation": {
            "passed": True,
            "phase": "post_write_verified",
            "coverage": "full_checksum",
            "checksum_match": True,
            "source_checksum": "abc",
            "target_checksum": "abc",
            "checksum_scope": "whole_table",
            "source_checksum_provenance": "independent_source_reread",
            "identity_hash_aligned": True,
        },
        "destination_summary": {"ddl_hash": "ddl-1"},
        "mapping_proof": {"mapping_hash": "map-1"},
        "connector_versions": {
            "source": "postgresql 16.2",
            "destination": "sqlserver 16.0",
        },
    }
    job.update(overrides)
    return job


def test_parity_does_not_say_proven_when_the_certificate_requires_alignment():
    from services.parity_report import _verdict

    job = _proven_job()
    job["reconciliation"] = {**job["reconciliation"], "identity_hash_aligned": False}
    verdict, reason = _verdict(job, [_cell_match_table()], {})
    assert verdict != "proven"
    assert "identity/hash alignment" in reason
    assert "No differences" not in reason


def test_parity_stays_proven_when_the_certificate_has_nothing_left_to_refuse():
    from services.parity_report import _verdict

    verdict, reason = _verdict(_proven_job(), [_cell_match_table()], {})
    assert verdict == "proven"
    assert reason == "No differences: the destination matches the source."


def test_parity_quotes_a_missing_release_when_that_is_the_certificate_blocker():
    from services.parity_report import _verdict

    job = _proven_job(connector_versions={"source": "postgresql", "destination": "sqlserver"})
    verdict, reason = _verdict(job, [_cell_match_table()], {})
    assert verdict != "proven"
    assert "captured release" in reason
