"""Regression tests for the 21 Nov 2026 production defects.

Each test names the story it locks. Live engines are not required.
"""

from __future__ import annotations


def test_d47_snowflake_scd2_and_mirror_predicates_do_not_use_is():
    """DW-D47 covers both writers. SCD2 close-version uses
    ``scd2_is_current_predicate`` (``WHERE is_current = TRUE``). Mirror
    delete/update uses ``sql_bool_is_true`` / ``sql_bool_is_not_true`` on
    ``_deleted`` (reactivate and soft-delete). Neither emits ``IS TRUE``.
    """
    from services.dialect_profiles import sql_bool_is_not_true, sql_bool_is_true
    from services.scd2_engine import scd2_is_current_predicate

    close_version = scd2_is_current_predicate("snowflake", '"is_current"')
    mirror_deleted = sql_bool_is_true("snowflake", '"_deleted"')
    mirror_active = sql_bool_is_not_true("snowflake", '"_deleted"')
    for sql in (close_version, mirror_deleted, mirror_active):
        assert "IS TRUE" not in sql.upper()
        assert "IS NOT TRUE" not in sql.upper()
        assert "DISTINCT" not in sql.upper()
    assert close_version == '"is_current" = TRUE'
    assert mirror_deleted == '"_deleted" = TRUE'
    assert mirror_active == '("_deleted" IS NULL OR "_deleted" = FALSE)'


def test_d47_scd2_error_keeps_the_snowflake_syntax_line():
    from src.transfer.stream_scd2 import _scd2_rolled_back

    exc = Exception(
        "001003 (42000): SQL compilation error:\n"
        "syntax error line 1 at position 257 unexpected 'IS'."
    )
    wrapped = _scd2_rolled_back(exc, '"ANALYTICS"."T"', 0)
    text = str(wrapped)
    assert "unexpected 'IS'" in text
    assert "0 batch" in text


def test_d72_uuid_varchar36_on_snowflake_is_full_confidence():
    from services.create_new_risk_stamp import apply_create_new_risk_stamps
    from services.mapping_quality import refine_mappings_with_quality

    rows = apply_create_new_risk_stamps(
        [{
            "source": "u",
            "target": "u",
            "source_type": "uuid",
            "target_type": "VARCHAR(36)",
            "confidence": 0.72,
            "requires_review": True,
            "create_new": True,
            "assignment_strategy": "identity_passthrough",
            "fidelity": "preserve",
            "fidelity_reason": "uuid → VARCHAR(36) round-trips without loss.",
        }],
        "snowflake",
        source_db_type="postgresql",
    )
    row = rows[0]
    assert row["confidence"] >= 0.99
    assert row["requires_review"] is False
    refined = refine_mappings_with_quality(rows, destination_db_type="snowflake")
    assert refined[0]["confidence"] >= 0.99
    assert refined[0]["requires_review"] is False
    from services.create_new_risk_stamp import create_new_risk_locks_review
    from services.type_coercion import assess_create_new_type_risk

    bare_risks = assess_create_new_type_risk("uuid", "VARCHAR", destination_db_type="snowflake")
    assert any(create_new_risk_locks_review(r) for r in bare_risks)


def test_d320_varbit_left_pads_into_mysql_bit_and_bit1_stays_bit():
    from connectors.sql_bind import bind_bitstring_wire, coerce_bitstring_wire
    from connectors.sql_bind_route import BindRoute
    from services.decision_kernel.type_invent import ddl_type

    assert ddl_type("mysql", "BIT(1)") == "BIT(1)"
    assert ddl_type("mysql", "bit(8)") == "BIT(8)"
    assert coerce_bitstring_wire("101", width=16, varying=False) == "0000000000000101"
    assert coerce_bitstring_wire("10101010", width=8) == "10101010"
    route = BindRoute(
        kind="bitstring", upper="BIT(16)", ddl_arg="16", eng="mysql",
        bit_width=16, bit_varying=False,
    )
    assert bind_bitstring_wire("101010101010", route) == int("0000101010101010", 2)


def test_sync_now_content_hash_holds_when_ddl_is_unchanged():
    """PR 392 follow-up: a saved schedule whose Map DDL did not change must run."""
    from services.conversion_contract import approved_mapping_ddl_fingerprint
    from services.decision_kernel.execute_gate import (
        build_artifact_from_mappings,
        enforce_decision_artifact,
    )
    from services.schema_fingerprint import fingerprint_schema

    maps = [{
        "source": "id",
        "target": "id",
        "source_type": "BIGINT",
        "target_type": "BIGINT",
        "confidence": 0.99,
        "create_new": True,
    }]
    old_fp = fingerprint_schema(["id", "note"], {"id": "BIGINT", "note": "TEXT"})
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="mysql",
        source_db="mysql",
        source_fingerprint=old_fp,
        dest_fingerprint="",
        sync_mode="incremental_append",
        route_id="validate:mysql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    saved = [{**maps[0], "confidence": 0.72}]
    ddl = approved_mapping_ddl_fingerprint(saved, dest_db="mysql")
    live_src = fingerprint_schema(["id", "note"], {"id": "bigint", "note": "varchar"})
    dest_types = {"id": "bigint"}
    live_dest = fingerprint_schema(["id"], dest_types)
    err, art = enforce_decision_artifact(
        mappings=saved,
        dest_db="mysql",
        source_db="mysql",
        approved_content_hash=stamped.content_hash,
        approved_ddl_identity_hash=ddl,
        source_fingerprint=live_src,
        dest_fingerprint=live_dest,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id"],
        sync_mode="incremental_append",
        schema_policy="manual_review",
    )
    assert err is None, err
    assert art is not None


def test_sync_now_still_refuses_a_real_map_edit_and_unaccounted_column():
    from services.conversion_contract import approved_mapping_ddl_fingerprint
    from services.decision_kernel.execute_gate import (
        build_artifact_from_mappings,
        enforce_decision_artifact,
    )
    from services.schema_fingerprint import fingerprint_schema

    maps = [{
        "source": "id",
        "target": "id",
        "source_type": "BIGINT",
        "target_type": "BIGINT",
        "create_new": True,
    }]
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="postgresql",
        dest_fingerprint="",
        route_id="validate:postgresql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    ddl = approved_mapping_ddl_fingerprint(maps, dest_db="postgresql")
    edited = [{**maps[0], "target_type": "VARCHAR(8)"}]
    dest_types = {"id": "bigint"}
    live = fingerprint_schema(["id"], dest_types)
    err, art = enforce_decision_artifact(
        mappings=edited,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        approved_ddl_identity_hash=ddl,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        sync_mode="incremental_append",
    )
    assert err is not None and art is None

    err2, art2 = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        approved_ddl_identity_hash=ddl,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id", "tier"],
        schema_policy="manual_review",
        sync_mode="cdc",
    )
    assert err2 is not None and "tier" in err2 and art2 is None
    err3, art3 = enforce_decision_artifact(
        mappings=maps,
        dest_db="mysql",
        approved_content_hash=stamped.content_hash,
        approved_ddl_identity_hash=approved_mapping_ddl_fingerprint(maps, dest_db="mysql"),
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id", "tier"],
        schema_policy="propagate_columns",
        sync_mode="cdc",
    )
    assert err3 is None, err3
    assert art3 is not None


def test_d334_echoed_stamp_widens_and_managed_table_approves_evolution():
    from services.schema_drift import (
        destination_evolution_approved,
        plan_destination_evolution,
    )

    assert destination_evolution_approved("manual_review", managed_table=True)
    assert not destination_evolution_approved("type_locked", managed_table=True)
    assert destination_evolution_approved("propagate_columns")
    maps = [{
        "source": "status",
        "target": "status",
        "source_type": "varchar(10)",
        "target_type": "varchar(10)",
        "confidence": 0.99,
    }]
    planned, types, applied = plan_destination_evolution(
        maps,
        source_columns=["status", "tier"],
        source_schema={"status": "varchar(30)", "tier": "integer"},
        dest_types={"status": "varchar(10)"},
        dest_db="postgresql",
        adopt_unmapped=True,
    )
    assert types["status"].lower().startswith("varchar(30)")
    assert any(a.get("source") == "tier" for a in applied)
    assert any(m.get("source") == "tier" for m in planned)


def test_d335_add_column_does_not_keep_the_boundary_row_on_append():
    from src.transfer.stream_boundary import _boundary_append_dedupe

    headers = ["id", "updated_at", "tier"]
    rows = [["8", "2026-11-01T00:00:00", "1"]]
    # Fingerprint of the boundary row without the new column, as the
    # destination stored it. new_cols is empty on append (D335).
    from connectors.writer_common import row_fingerprints

    fps = {
        fp
        for _k, fp in row_fingerprints(
            [("2026-11-01T00:00:00", "8")],
            ["updated_at", "id"],
            dest_db_type="postgresql",
            dest_types={"updated_at": "timestamp", "id": "integer"},
        )
    }
    dropped = _boundary_append_dedupe(
        headers,
        rows,
        fps,
        match_cols=["updated_at", "id"],
        new_cols=[],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "updated_at", "target": "updated_at"},
            {"source": "tier", "target": "tier"},
        ],
        target_cols=["updated_at", "id", "tier"],
        column_types={"id": "integer", "updated_at": "timestamp", "tier": "integer"},
        stream_error_policy="quarantine",
        digest_dest_types={"updated_at": "timestamp", "id": "integer", "tier": "integer"},
        dest_bind_dialect="postgresql",
    )
    assert dropped == {0}


def test_d407_retry_of_own_create_is_not_dest_schema_drift():
    """Cancelled incremental upsert created the table. Retry from start sees it.

    The live carriers are the planned DDL (MySQL display width included).
    That is not drift. A widened column or an extra column still is.
    """
    from services.decision_kernel.execute_gate import (
        build_artifact_from_mappings,
        enforce_decision_artifact,
    )
    from services.schema_fingerprint import fingerprint_schema

    maps = [
        {"source": "id", "target": "id", "source_type": "BIGINT", "target_type": "BIGINT", "create_new": True},
        {"source": "name", "target": "name", "source_type": "VARCHAR(40)", "target_type": "VARCHAR(40)", "create_new": True},
        {"source": "qty", "target": "qty", "source_type": "BIGINT", "target_type": "BIGINT", "create_new": True},
    ]
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="mysql",
        source_db="postgresql",
        source_fingerprint="aabbccddeeff0011",
        dest_fingerprint="",
        sync_mode="incremental_deduped",
        route_id="validate:mysql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    planned = dict(stamped.ddl.column_ddl)
    assert planned
    live = {name: typ.lower() for name, typ in planned.items()}
    # Catalog spelling of the same DDL: integer display width, lower case.
    live = {
        "id": "bigint(20)",
        "name": live.get("name", "varchar(40)"),
        "qty": "bigint",
    }
    # Keep the planned name width if the kernel emitted one.
    name_ddl = planned.get("name") or planned.get("NAME") or "VARCHAR(40)"
    live["name"] = name_ddl.lower()
    live_fp = fingerprint_schema(list(live), live)

    def _gate(types: dict[str, str]):
        return enforce_decision_artifact(
            mappings=maps,
            dest_db="mysql",
            source_db="postgresql",
            approved_content_hash=stamped.content_hash,
            artifact_payload=stamped.to_dict(),
            source_fingerprint="1122334455667788",
            dest_fingerprint=fingerprint_schema(list(types), types),
            destination_table_exists=True,
            dest_column_names=list(types),
            source_column_names=["id", "name", "qty"],
            live_column_types=types,
            sync_mode="incremental_deduped",
        )

    err, art = _gate(live)
    assert err is None, err
    assert art is not None

    widened = {**live, "name": "varchar(80)"}
    err_w, art_w = _gate(widened)
    assert err_w and "drifted" in err_w.lower()
    assert art_w is None

    extra = {**live, "note": "int"}
    err_x, art_x = _gate(extra)
    assert err_x and "drifted" in err_x.lower()
    assert art_x is None
    assert live_fp


def test_mysql_3750_is_a_plan_time_finding_not_a_raw_driver_error():
    from services.dest_require_primary_key import (
        keyless_create_refusal,
        mysql_create_primary_key_message,
    )

    class _Exc(Exception):
        def __init__(self):
            super().__init__("Table must have a primary key")
            self.args = (3750, "Unable to create a table without PK")

    text = mysql_create_primary_key_message(_Exc(), table="mahi_qa")
    assert text and "3750" in text and "PRIMARY KEY" in text
    assert mysql_create_primary_key_message(Exception("other")) is None

    class _Req:
        variable = "sql_require_primary_key"
        unreadable = ""

    # The plan-time sentence still names the variable. The live probe is
    # covered by test_r14_mysql_require_primary_key_e2e.
    from services import dest_require_primary_key as rpk

    original = rpk.server_requires_primary_key
    rpk.server_requires_primary_key = lambda cfg: _Req()
    try:
        refusal = keyless_create_refusal(
            dest_db_type="mysql",
            destination_config={"host": "db.example"},
            table_exists=False,
            sync_mode="incremental_append",
            destination_table="mahi_qa",
            key_hint="",
            mappings=[{"source": "note", "target": "note", "target_type": "text"}],
        )
    finally:
        rpk.server_requires_primary_key = original
    assert refusal and "sql_require_primary_key=ON" in refusal
