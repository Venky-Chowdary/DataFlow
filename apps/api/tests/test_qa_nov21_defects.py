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


def test_d320_mysql_write_quarantine_keeps_short_varbit():
    """The write quarantine pre-check is the path every SQL writer runs first."""
    from connectors.writer_common import apply_write_quarantine_matrix

    # Three short varbit values and two that already fill BIT(16). id 2's
    # BIT(8) is the exact mask from the QA table.
    rows = [
        (1, "1", "00000001", "1"),
        (2, "0", "10101010", "101"),
        (3, "1", "11110000", "11"),
        (4, "0", "00001111", "1010101010101010"),
        (5, "1", "11111111", "1111000011110000"),
    ]
    cols = ["id", "b1", "b8", "bv"]
    types = ["BIGINT", "BIT(1)", "BIT(8)", "BIT(16)"]
    details: list[dict] = []
    kept = apply_write_quarantine_matrix(
        rows, cols, types, details, "quarantine", dest_db="mysql"
    )
    assert details == []
    assert len(kept) == 5
    assert kept[1][2] == "10101010"
    assert kept[1][3] == "0000000000000101"
    pg_details: list[dict] = []
    pg_kept = apply_write_quarantine_matrix(
        rows, cols, types, pg_details, "quarantine", dest_db="postgresql"
    )
    assert len(pg_kept) == 2
    assert len(pg_details) == 3
    from services.population_fit_scan import (
        CARRIER_BYTES,
        BoundedTarget,
        _fit_predicate,
    )

    mysql_fit = _fit_predicate(
        BoundedTarget(source="bv", target="bv", target_type="BIT(16)", carrier=CARRIER_BYTES),
        dest_db="mysql",
        dialect_label="mysql",
    )
    pg_fit = _fit_predicate(
        BoundedTarget(source="bv", target="bv", target_type="BIT(16)", carrier=CARRIER_BYTES),
        dest_db="postgresql",
        dialect_label="postgresql",
    )
    assert mysql_fit("101") is None
    assert pg_fit("101") is not None


def test_mysql_boolean_and_json_are_the_planned_ddl():
    from services.decision_kernel.execute_gate import planned_ddl_matches_live

    assert planned_ddl_matches_live(
        {"flag": "BOOLEAN", "payload": "JSON"},
        {"flag": "tinyint(1)", "payload": "json"},
    )
    assert planned_ddl_matches_live({"flag": "BOOLEAN"}, {"flag": "bit(1)"})
    assert not planned_ddl_matches_live({"flag": "BOOLEAN"}, {"flag": "tinyint(4)"})
    assert not planned_ddl_matches_live(
        {"name": "VARCHAR(40)"}, {"name": "varchar(80)"}
    )


def test_confidence_only_change_keeps_the_decision_hash():
    """A scoring pass is not Map drift. A target-type change still is.

    On the previous hash, confidence was inside the digest, so this refused.
    """
    from services.decision_kernel.execute_gate import (
        build_artifact_from_mappings,
        enforce_decision_artifact,
    )
    from services.schema_fingerprint import fingerprint_schema

    def _maps(confidence: float, target_type: str = "BIGINT") -> list[dict]:
        return [{
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": target_type,
            "confidence": confidence,
            "create_new": True,
        }]

    src_fp = fingerprint_schema(["id"], {"id": "BIGINT"})
    high = build_artifact_from_mappings(
        _maps(0.99),
        dest_db="mysql",
        source_db="mysql",
        source_fingerprint=src_fp,
        dest_fingerprint="",
        sync_mode="incremental_append",
        error_policy="quarantine",
        route_id="validate:mysql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    low = build_artifact_from_mappings(
        _maps(0.72),
        dest_db="mysql",
        source_db="mysql",
        source_fingerprint=src_fp,
        dest_fingerprint="",
        sync_mode="incremental_append",
        error_policy="quarantine",
        route_id="validate:mysql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    assert high.content_hash == low.content_hash
    edited = build_artifact_from_mappings(
        _maps(0.99, "VARCHAR(8)"),
        dest_db="mysql",
        source_db="mysql",
        source_fingerprint=src_fp,
        dest_fingerprint="",
        sync_mode="incremental_append",
        error_policy="quarantine",
        route_id="validate:mysql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    assert edited.content_hash != high.content_hash
    live = fingerprint_schema(["id"], {"id": "bigint"})
    err, art = enforce_decision_artifact(
        mappings=_maps(0.72),
        dest_db="mysql",
        source_db="mysql",
        approved_content_hash=high.content_hash,
        source_fingerprint=src_fp,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id"],
        live_column_types={"id": "bigint"},
        sync_mode="incremental_append",
        error_policy="quarantine",
    )
    assert err is None, err
    assert art is not None
    err_type, art_type = enforce_decision_artifact(
        mappings=_maps(0.99, "VARCHAR(8)"),
        dest_db="mysql",
        source_db="mysql",
        approved_content_hash=high.content_hash,
        source_fingerprint=src_fp,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id"],
        live_column_types={"id": "bigint"},
        sync_mode="incremental_append",
        error_policy="quarantine",
    )
    assert err_type and art_type is None


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
    from services.type_system import is_bitstring_carrier

    assert ddl_type("mysql", "BIT(1)") == "BIT(1)"
    assert ddl_type("mysql", "bit(8)") == "BIT(8)"
    # BIT(1) stays a boolean carrier. Only the MySQL CREATE spelling is BIT(1).
    assert is_bitstring_carrier("BIT(1)") is False
    assert coerce_bitstring_wire("10101010", width=8) == "10101010"
    # Shared path does not left-pad (PostgreSQL refuses a short BIT(n)).
    try:
        coerce_bitstring_wire("101", width=16, varying=False)
        raise AssertionError("expected length refusal")
    except ValueError as exc:
        assert "does not match" in str(exc)
    # b'01' is the integer 12337, not the bit text "01".
    assert coerce_bitstring_wire(b"01", width=None).startswith("00110000")
    route = BindRoute(
        kind="bitstring", upper="BIT(16)", ddl_arg="16", eng="mysql",
        bit_width=16, bit_varying=False,
    )
    assert bind_bitstring_wire("101010101010", route) == int("0000101010101010", 2)
    pg = BindRoute(
        kind="bitstring", upper="BIT(16)", ddl_arg="16", eng="postgresql",
        bit_width=16, bit_varying=False,
    )
    try:
        bind_bitstring_wire("101", pg)
        raise AssertionError("postgres must not left-pad")
    except ValueError:
        pass


def test_sync_now_relaxes_only_dest_exists_not_source_mode_or_policy():
    """Dest appearing after Validate is not drift. A changed source fingerprint,
    sync mode, or error policy still is. The stored hash is not rewritten.
    """
    from services.decision_kernel.execute_gate import (
        build_artifact_from_mappings,
        enforce_decision_artifact,
    )
    from services.schedule_decision_stamp import decision_hash_for_sync
    from services.schema_fingerprint import fingerprint_schema

    maps = [{
        "source": "id",
        "target": "id",
        "source_type": "BIGINT",
        "target_type": "BIGINT",
        "confidence": 0.99,
        "create_new": True,
    }]
    src_fp = fingerprint_schema(["id"], {"id": "BIGINT"})
    drifted_fp = fingerprint_schema(["id", "tier"], {"id": "BIGINT", "tier": "INTEGER"})
    assert src_fp != drifted_fp
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="mysql",
        source_db="mysql",
        source_fingerprint=src_fp,
        dest_fingerprint="",
        sync_mode="incremental_append",
        error_policy="quarantine",
        route_id="validate:mysql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    dest_types = {"id": "bigint"}
    live_dest = fingerprint_schema(["id"], dest_types)

    def _run(**overrides):
        args = dict(
            mappings=maps,
            dest_db="mysql",
            source_db="mysql",
            approved_content_hash=stamped.content_hash,
            source_fingerprint=src_fp,
            dest_fingerprint=live_dest,
            destination_table_exists=True,
            dest_column_names=["id"],
            source_column_names=["id"],
            sync_mode="incremental_append",
            error_policy="quarantine",
        )
        args.update(overrides)
        return enforce_decision_artifact(**args)

    err, art = _run()
    assert err is None, err
    assert art is not None

    err_src, art_src = _run(source_fingerprint=drifted_fp)
    assert err_src and art_src is None

    err_mode, art_mode = _run(sync_mode="full_refresh_overwrite")
    assert err_mode and art_mode is None

    err_pol, art_pol = _run(error_policy="fail")
    assert err_pol and art_pol is None

    stored = stamped.content_hash
    assert decision_hash_for_sync({
        "approved_decision_artifact_hash": stored,
        "sync_mode": "full_refresh_overwrite",
        "mappings": maps,
        "approved_ddl_identity_hash": "ab" * 32,
    }) == stored.lower()


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
    )
    assert err2 is not None and "tier" in err2 and art2 is None
    err3, art3 = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        approved_ddl_identity_hash=ddl,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id", "tier"],
        schema_policy="propagate_columns",
    )
    assert err3 is None, err3
    assert art3 is not None


def test_d334_managed_table_widens_non_key_columns_only():
    from services.schema_drift import (
        destination_evolution_approved,
        managed_non_key_widen_only,
        plan_destination_evolution,
    )

    assert not destination_evolution_approved("manual_review", managed_table=True)
    assert not destination_evolution_approved("type_locked", managed_table=True)
    assert destination_evolution_approved("propagate_columns")
    assert managed_non_key_widen_only(
        "manual_review", managed_table=True, acknowledged=False
    )
    assert not managed_non_key_widen_only(
        "manual_review", managed_table=False, acknowledged=False
    )
    assert not managed_non_key_widen_only(
        "propagate_columns", managed_table=True, acknowledged=False
    )
    maps = [{
        "source": "status",
        "target": "status",
        "source_type": "varchar(10)",
        "target_type": "varchar(10)",
        "confidence": 0.99,
    }, {
        "source": "id",
        "target": "id",
        "source_type": "varchar(10)",
        "target_type": "varchar(10)",
        "confidence": 0.99,
    }]
    # Propagate adds the new column. manual_review on an owned table does not.
    _planned, types, applied = plan_destination_evolution(
        maps,
        source_columns=["status", "id", "tier"],
        source_schema={"status": "varchar(30)", "id": "varchar(20)", "tier": "integer"},
        dest_types={"status": "varchar(10)", "id": "varchar(10)"},
        dest_db="postgresql",
        adopt_unmapped=False,
        key_groups=[["id"]],
        key_widens_approved=False,
    )
    assert types["status"].lower().startswith("varchar(30)")
    assert types["id"].lower().startswith("varchar(10)")
    assert not any(a.get("source") == "tier" for a in applied)
    assert not any(str(a.get("target") or "").lower() == "id" and a.get("kind") == "widen_type" for a in applied)


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

    src_fp = "aabbccddeeff0011"

    def _gate(types: dict[str, str], **overrides):
        args = dict(
            mappings=maps,
            dest_db="mysql",
            source_db="postgresql",
            approved_content_hash=stamped.content_hash,
            artifact_payload=stamped.to_dict(),
            source_fingerprint=src_fp,
            dest_fingerprint=fingerprint_schema(list(types), types),
            destination_table_exists=True,
            dest_column_names=list(types),
            source_column_names=["id", "name", "qty"],
            live_column_types=types,
            sync_mode="incremental_deduped",
            error_policy="quarantine",
            plan_created_destination=True,
        )
        args.update(overrides)
        return enforce_decision_artifact(**args)

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

    err_src, art_src = _gate(live, source_fingerprint="1122334455667788")
    assert err_src and "source schema" in err_src.lower()
    assert art_src is None

    err_mode, art_mode = _gate(live, sync_mode="full_refresh_overwrite")
    assert err_mode and "sync_mode" in err_mode.lower()
    assert art_mode is None

    # Validated against a table that already existed. A later fingerprint
    # change is drift even when the column types still look like the plan
    # and this run did not create the table.
    prior = fingerprint_schema(["id", "name", "qty"], {"id": "BIGINT", "name": "VARCHAR(40)", "qty": "BIGINT"})
    preexisting = build_artifact_from_mappings(
        maps,
        dest_db="mysql",
        source_db="postgresql",
        source_fingerprint=src_fp,
        dest_fingerprint=prior,
        sync_mode="incremental_deduped",
        route_id="validate:mysql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    err_own, art_own = enforce_decision_artifact(
        mappings=maps,
        dest_db="mysql",
        source_db="postgresql",
        approved_content_hash=preexisting.content_hash,
        artifact_payload=preexisting.to_dict(),
        source_fingerprint=src_fp,
        dest_fingerprint="0123456789abcdef",
        destination_table_exists=True,
        dest_column_names=list(live),
        source_column_names=["id", "name", "qty"],
        live_column_types=live,
        sync_mode="incremental_deduped",
        error_policy="quarantine",
        plan_created_destination=False,
    )
    assert err_own and "drifted" in err_own.lower()
    assert art_own is None


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
