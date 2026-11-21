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
    from tests.snowflake_grammar import snowflake_grammar_violations

    close_version = scd2_is_current_predicate("snowflake", '"is_current"')
    mirror_deleted = sql_bool_is_true("snowflake", '"_deleted"')
    mirror_active = sql_bool_is_not_true("snowflake", '"_deleted"')
    for sql in (close_version, mirror_deleted, mirror_active):
        assert "IS TRUE" not in sql.upper()
        assert "IS NOT TRUE" not in sql.upper()
        # Snowflake does have IS [NOT] DISTINCT FROM; only the boolean
        # IS [NOT] TRUE/FALSE test is rejected (strict grammar fixture).
        assert snowflake_grammar_violations(f"SELECT 1 FROM t WHERE {sql}") == []
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
    """The write quarantine pre-check is the path every SQL writer runs first.

    QA's matrix (5 kept / 0 quarantined) holds because the planned MySQL
    column for a PostgreSQL ``varbit(16)`` is ``VARCHAR(16)`` 0/1 text, so
    every value lands exactly as it was. A short value is never left-padded
    into a fixed ``BIT(n)``: '101' and '0000000000000101' differ.
    """
    from connectors.writer_common import apply_write_quarantine_matrix
    from services.decision_kernel.type_invent import ddl_type

    planned_bv = ddl_type("mysql", "BIT VARYING(16)")
    assert planned_bv == "VARCHAR(16)"
    # Three short varbit values and two that already fill 16 bits. id 2's
    # BIT(8) is the exact mask from the QA table.
    rows = [
        (1, "1", "00000001", "1"),
        (2, "0", "10101010", "101"),
        (3, "1", "11110000", "11"),
        (4, "0", "00001111", "1010101010101010"),
        (5, "1", "11111111", "1111000011110000"),
    ]
    cols = ["id", "b1", "b8", "bv"]
    types = ["BIGINT", ddl_type("mysql", "BIT(1)"), ddl_type("mysql", "BIT(8)"), planned_bv]
    details: list[dict] = []
    kept = apply_write_quarantine_matrix(
        rows, cols, types, details, "quarantine", dest_db="mysql"
    )
    assert details == []
    assert len(kept) == 5
    assert kept[1][2] == "10101010"
    # Value-exact: the short varbit is kept as written, not padded.
    assert [r[3] for r in kept] == [r[3] for r in rows]

    # Into an operator's existing fixed BIT(16) a short value is refused on
    # every engine (MySQL included) rather than silently changed.
    for dest in ("mysql", "postgresql"):
        held: list[dict] = []
        fixed = apply_write_quarantine_matrix(
            rows, cols, ["BIGINT", "BIT(1)", "BIT(8)", "BIT(16)"], held, "quarantine",
            dest_db=dest,
        )
        assert len(fixed) == 2, dest
        assert len(held) == 3, dest
    from services.population_fit_scan import (
        CARRIER_BYTES,
        BoundedTarget,
        _fit_predicate,
    )

    for dest in ("mysql", "postgresql"):
        fit = _fit_predicate(
            BoundedTarget(source="bv", target="bv", target_type="BIT(16)", carrier=CARRIER_BYTES),
            dest_db=dest,
            dialect_label=dest,
        )
        assert fit("101") is not None, dest
        assert fit("1010101010101010") is None, dest


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
    assert not planned_ddl_matches_live(
        {"qty": "BIGINT"}, {"qty": "bigint(20) unsigned"}
    )
    assert planned_ddl_matches_live({"qty": "BIGINT"}, {"qty": "bigint(20)"})


def test_base_stamped_hash_replays_and_scores_are_not_drift():
    """A content hash stamped with confidence still inside it must replay.

    Changing that digest broke every stored approval. Scoring is a separate
    structural hash: a confidence-only change of the stored artifact still
    runs, and a target-type change still refuses. On the previous gate the
    confidence-only replay refused.
    """
    from services.decision_kernel.execute_gate import (
        build_artifact_from_mappings,
        enforce_decision_artifact,
    )
    from services.decision_kernel.models import (
        decision_artifact_from_dict,
        structural_decision_hash,
    )
    from services.schema_fingerprint import fingerprint_schema

    def _maps(
        confidence: float,
        target_type: str = "BIGINT",
        *,
        key: bool = False,
        target: str = "id",
    ) -> list[dict]:
        return [{
            "source": "id",
            "target": target,
            "source_type": "BIGINT",
            "target_type": target_type,
            "confidence": confidence,
            "create_new": True,
            "is_primary_key": key,
        }]

    src_fp = fingerprint_schema(["id"], {"id": "BIGINT"})
    common = dict(
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
    high = build_artifact_from_mappings(_maps(0.99), **common)
    low = build_artifact_from_mappings(_maps(0.72), **common)
    # One design (merge of #435 into fix/qa-batch1): scores are not part of
    # the content hash; they are compared separately. A base stamp (scores
    # inside) is still accepted — see test_schedules_saved_with_the_base_...
    assert high.content_hash == low.content_hash
    assert structural_decision_hash(high) == structural_decision_hash(low)
    from services.decision_kernel.models import base_content_hash

    assert base_content_hash(high.to_dict()) != base_content_hash(low.to_dict())
    revived = decision_artifact_from_dict(high.to_dict())
    assert revived.content_hash == high.content_hash
    keyed = build_artifact_from_mappings(_maps(0.99, key=True), **common)
    # The key flag is structure: it moves the hash.
    assert keyed.content_hash != high.content_hash
    assert structural_decision_hash(keyed) != structural_decision_hash(high)
    assert decision_artifact_from_dict(keyed.to_dict()).content_hash == keyed.content_hash
    named = high.to_dict()
    named["ddl"]["column_ddl"]["confidence"] = "VARCHAR(8)"
    assert structural_decision_hash(named) != structural_decision_hash(high.to_dict())
    live = fingerprint_schema(["id"], {"id": "bigint"})
    err, art = enforce_decision_artifact(
        mappings=_maps(0.99),
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
        route_id="validate:mysql",
    )
    assert err is None, err
    assert art is not None
    err_score, art_score = enforce_decision_artifact(
        mappings=_maps(0.72),
        dest_db="mysql",
        source_db="mysql",
        approved_content_hash=high.content_hash,
        artifact_payload=high.to_dict(),
        source_fingerprint=src_fp,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id"],
        live_column_types={"id": "bigint"},
        sync_mode="incremental_append",
        error_policy="quarantine",
        route_id="validate:mysql",
    )
    assert err_score is None, err_score
    assert art_score is not None
    # A base stamp (feature/Cleanup digest) of the stored artifact replays too.
    err_base, art_base = enforce_decision_artifact(
        mappings=_maps(0.72),
        dest_db="mysql",
        source_db="mysql",
        approved_content_hash=base_content_hash(high.to_dict()),
        artifact_payload={**high.to_dict(), "content_hash": base_content_hash(high.to_dict())},
        source_fingerprint=src_fp,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id"],
        live_column_types={"id": "bigint"},
        sync_mode="incremental_append",
        error_policy="quarantine",
        route_id="validate:mysql",
    )
    assert err_base is None, err_base
    assert art_base is not None
    err_type, art_type = enforce_decision_artifact(
        mappings=_maps(0.99, "VARCHAR(8)"),
        dest_db="mysql",
        source_db="mysql",
        approved_content_hash=high.content_hash,
        artifact_payload=high.to_dict(),
        source_fingerprint=src_fp,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        source_column_names=["id"],
        live_column_types={"id": "bigint"},
        sync_mode="incremental_append",
        error_policy="quarantine",
        route_id="validate:mysql",
    )
    assert err_type and art_type is None


def test_d72_uuid_varchar36_on_snowflake_is_full_confidence():
    """A stamp that came in at 0.72 / review (the QA run) leaves the identity
    band and review-free; the scorer then grades it exactly like a same-type
    uuid → uuid create (``is_exact_value_carrier``), not above it."""
    from services.create_new_risk_stamp import apply_create_new_risk_stamps
    from services.mapping_quality import refine_mappings_with_quality

    def _row(target_type: str) -> dict:
        return {
            "source": "u",
            "target": "u",
            "source_type": "uuid",
            "target_type": target_type,
            "confidence": 0.72,
            "requires_review": True,
            "create_new": True,
            "assignment_strategy": "identity_passthrough",
            "fidelity": "preserve",
            "fidelity_reason": "uuid → VARCHAR(36) round-trips without loss.",
        }

    rows = apply_create_new_risk_stamps([_row("VARCHAR(36)")], "snowflake", source_db_type="postgresql")
    row = rows[0]
    assert row["confidence"] >= 0.95
    assert row["requires_review"] is False
    refined = refine_mappings_with_quality(rows, destination_db_type="snowflake")
    identity = refine_mappings_with_quality(
        apply_create_new_risk_stamps([_row("UUID")], "postgresql", source_db_type="postgresql"),
        destination_db_type="postgresql",
    )
    assert refined[0]["confidence"] == identity[0]["confidence"]
    assert refined[0]["requires_review"] is False
    from services.create_new_risk_stamp import create_new_risk_locks_review
    from services.type_coercion import assess_create_new_type_risk

    # Snowflake STRING / TEXT / bare VARCHAR are VARCHAR(16777216): one type,
    # one grade — a lossless carrier, not the exact-36 contract.
    graded = {
        t: [
            sorted(r.get("kind", "") for r in assess_create_new_type_risk(
                "uuid", t, destination_db_type="snowflake"
            ))
        ]
        for t in ("STRING", "TEXT", "VARCHAR", "VARCHAR(16777216)")
    }
    assert len({str(v) for v in graded.values()}) == 1, graded
    assert not any(
        create_new_risk_locks_review(r)
        for r in assess_create_new_type_risk("uuid", "STRING", destination_db_type="snowflake")
    )
    # A bound narrower than 36 truncates and still holds review.
    narrow = assess_create_new_type_risk("uuid", "VARCHAR(32)", destination_db_type="snowflake")
    assert any(create_new_risk_locks_review(r) for r in narrow)


def test_d320_varbit_never_left_pads_and_bit1_stays_bit():
    from connectors.sql_bind import bind_bitstring_wire, coerce_bitstring_wire
    from connectors.sql_bind_route import BindRoute
    from services.decision_kernel.type_invent import ddl_type
    from services.type_system import is_bitstring_carrier

    assert ddl_type("mysql", "BIT(1)") == "BIT(1)"
    assert ddl_type("mysql", "bit(8)") == "BIT(8)"
    # Varying or wider than BIT's 64: 0/1 digit text, never a clamped BIT.
    assert ddl_type("mysql", "BIT VARYING(16)") == "VARCHAR(16)"
    assert ddl_type("mysql", "BIT(100)") == "VARCHAR(100)"
    # BIT(1) stays a boolean carrier. Only the MySQL CREATE spelling is BIT(1).
    assert is_bitstring_carrier("BIT(1)") is False
    assert coerce_bitstring_wire("10101010", width=8) == "10101010"
    try:
        coerce_bitstring_wire("101", width=16, varying=False)
        raise AssertionError("expected length refusal")
    except ValueError as exc:
        assert "does not match" in str(exc)
    # b'01' is the integer 12337, not the bit text "01".
    assert coerce_bitstring_wire(b"01", width=None).startswith("00110000")
    for eng in ("mysql", "postgresql"):
        route = BindRoute(
            kind="bitstring", upper="BIT(16)", ddl_arg="16", eng=eng,
            bit_width=16, bit_varying=False,
        )
        try:
            bind_bitstring_wire("101010101010", route)
            raise AssertionError(f"{eng} must not left-pad")
        except ValueError:
            pass
    full = BindRoute(
        kind="bitstring", upper="BIT(16)", ddl_arg="16", eng="mysql",
        bit_width=16, bit_varying=False,
    )
    assert bind_bitstring_wire("0000101010101010", full) == int("0000101010101010", 2)


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

    # The exact-DDL test is the D407 retry exception's own condition (review
    # Q3): a widened or extra column does not qualify for it. What such a
    # table gets is then the create-new dest-exists hold's answer, as before
    # #435 (it judges the mapped shape, not the carriers).
    from services.decision_kernel.execute_gate import _planned_create_still_holds

    def _exception_holds(types: dict[str, str]) -> bool:
        return _planned_create_still_holds(
            planned=planned, live_column_types=types, destination_table_exists=True,
            mappings=maps, dest_column_names=list(types), source_column_names=["id", "name", "qty"],
            column_nullability=None, column_defaults=None, identity_columns=None,
            generated_columns=None, schema_policy="", dest_db="mysql",
        )[0]

    assert _exception_holds(live)
    assert not _exception_holds({**live, "name": "varchar(80)"})
    assert not _exception_holds({**live, "note": "int"})
    for drifted in ({**live, "name": "varchar(80)"}, {**live, "note": "int"}):
        hold, _ = _gate(drifted, plan_created_destination=False)
        held, _ = _gate(drifted)
        assert (hold is None) == (held is None), (drifted, hold, held)

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
    """CREATE refused with 3750 reads as the plan-time finding; a keyless
    append on a forced-key server is the approvable surrogate gate, and
    without approval it blocks (the refusal path when the user says no)."""
    from services.dest_require_primary_key import (
        GATE_ID,
        SURROGATE_KEY_COLUMN,
        keyless_create_refusal,
        mysql_create_primary_key_message,
        require_primary_key_gate,
    )

    class _Exc(Exception):
        def __init__(self):
            super().__init__("Table must have a primary key")
            self.args = (3750, "Unable to create a table without PK")

    text = mysql_create_primary_key_message(_Exc(), table="mahi_qa")
    assert text and "3750" in text and "PRIMARY KEY" in text
    assert SURROGATE_KEY_COLUMN in text  # names the approval path, not "cannot CREATE"
    assert mysql_create_primary_key_message(Exception("other")) is None

    class _Maria(Exception):
        def __init__(self):
            super().__init__("This table type requires a primary key")
            self.args = (1173, "This table type requires a primary key")

    maria = mysql_create_primary_key_message(_Maria(), table="mahi_qa")
    assert maria and "1173" in maria and "innodb_force_primary_key" in maria

    class _Req:
        variable = "sql_require_primary_key"
        unreadable = ""
        server_version = "8.0.36"

    # The plan-time sentence still names the variable. The live probe is
    # covered by test_r14_mysql_require_primary_key_e2e and the surrogate
    # write by test_d396_require_primary_key_surrogate_e2e.
    from services import dest_require_primary_key as rpk

    args = dict(
        dest_db_type="mysql",
        destination_config={"host": "db.example"},
        table_exists=False,
        sync_mode="incremental_append",
        key_hint="",
        mappings=[{"source": "note", "target": "note", "target_type": "text"}],
    )
    original = rpk.server_requires_primary_key
    rpk.server_requires_primary_key = lambda cfg: _Req()
    try:
        # Not an unclearable refusal: the operator can approve the surrogate.
        assert keyless_create_refusal(destination_table="mahi_qa", **args) is None
        blocked = require_primary_key_gate(
            acknowledged=False, destination_table="mahi_qa", **args
        )
        approved = require_primary_key_gate(
            acknowledged=True, destination_table="mahi_qa", **args
        )
    finally:
        rpk.server_requires_primary_key = original
    assert blocked and blocked["id"] == GATE_ID and blocked["status"] == "block"
    assert "sql_require_primary_key=ON" in blocked["message"]
    assert "3750" in blocked["message"]
    assert approved and approved["status"] == "pass"
    added = approved["details"]["datawrap_added_columns"]
    assert added[0]["name"] == SURROGATE_KEY_COLUMN and added[0]["excluded_from_parity"]


def test_mysql_writer_keeps_the_restore_guard():
    """The forced-key helpers were inserted between ``@writer_restore_guard``
    and ``write_mapped_rows``, so the guard wrapped a helper and the writer
    lost its widen-restore bucket (D368). The writer must own it."""
    from connectors import mysql_writer

    assert hasattr(mysql_writer.write_mapped_rows, "__wrapped__")
    assert not hasattr(mysql_writer._plan_declares_primary_key, "__wrapped__")


def test_d407_ownership_probe_runs_only_for_a_create_new_plan(monkeypatch):
    """The ownership probe is a destination round trip: only a create-new
    plan (empty Validate dest fingerprint) can use its answer."""
    from types import SimpleNamespace

    from src.transfer import engine_identity

    calls: list[bool] = []

    def _probe(request, exists):
        calls.append(bool(exists))
        return False

    monkeypatch.setattr(engine_identity, "destination_created_by_plan", _probe)
    req = SimpleNamespace(approved_ddl_identity_hash="", schema_policy="")
    for dest_fp, expected in (("abc123", 0), ("", 1)):
        calls.clear()
        try:
            engine_identity._enforce_decision_artifact_once(
                None, [], dest_db="mysql", decision_artifact={"dest_fingerprint": dest_fp},
                destination_table_exists=True, request=req,
            )
        except Exception:  # noqa: BLE001 — only whether the probe ran matters here
            pass
        assert len(calls) == expected, (dest_fp, calls)


def _base_digest(payload: dict) -> str:
    """The content hash as feature/Cleanup (70cb4669c) computed it."""
    import hashlib
    import json

    body = {k: v for k, v in payload.items()
            if k not in {"content_hash", "policy_signature", "created_at", "artifact_id"}}
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _interim_digest(payload: dict) -> str:
    """The content hash as PR #435's interim build (e83b6dc7a) computed it."""
    import hashlib
    import json

    drop = {"content_hash", "policy_signature", "created_at", "artifact_id", "confidence",
            "calibration_reason", "score", "score_gap", "alternatives"}

    def strip(value):
        if isinstance(value, dict):
            return {str(k): strip(v) for k, v in value.items() if str(k) not in drop}
        if isinstance(value, (list, tuple)):
            return [strip(v) for v in value]
        return value

    return hashlib.sha256(
        json.dumps(strip(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def test_decision_hash_keeps_name_keyed_columns_out_of_the_score_strip():
    """Scores leave only mapping rows: a column named ``created_at`` or
    ``score`` stays in the structural (replay) hash's DDL part."""
    from services.decision_kernel.execute_gate import build_artifact_from_mappings
    from services.decision_kernel.models import structural_decision_hash

    maps = [
        {"source": "created_at", "target": "created_at", "source_type": "BIGINT",
         "target_type": "BIGINT", "confidence": 0.9, "create_new": True},
        {"source": "score", "target": "score", "source_type": "BIGINT",
         "target_type": "BIGINT", "confidence": 0.8, "create_new": True},
    ]
    art = build_artifact_from_mappings(
        maps, dest_db="mysql", dest_fingerprint="", route_id="validate:mysql",
        artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )
    for name in ("created_at", "score"):
        edited = art.to_dict()
        edited["ddl"]["column_ddl"][name] = "VARCHAR(8)"
        assert structural_decision_hash(edited) != structural_decision_hash(art.to_dict()), name


def test_schedules_saved_with_the_base_or_the_interim_hash_still_run():
    """A schedule stamped by feature/Cleanup (confidence inside the digest)
    and one stamped by #435's interim build (scores stripped at every depth)
    both still pass the schedule-side check and the Execute gate a beat
    runs. A rescored Map still runs; a target-type change still refuses."""
    from types import SimpleNamespace

    from services.decision_kernel.execute_gate import build_artifact_from_mappings
    from services.decision_kernel.models import decision_artifact_from_dict
    from services.schedule_approvals import create_new_stamp_matches_schedule
    from src.transfer.engine_identity import _enforce_decision_artifact_once

    maps = [{"source": "id", "target": "id", "source_type": "BIGINT",
             "target_type": "BIGINT", "confidence": 0.93, "create_new": True}]
    art = build_artifact_from_mappings(
        maps, dest_db="mysql", source_db="", source_fingerprint="", dest_fingerprint="",
        sync_mode="incremental_append", route_id="validate:mysql", tenant_id="anonymous",
        artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )
    base = _base_digest(art.to_dict())
    interim = _interim_digest(art.to_dict())
    assert base != interim

    for label, stamp in (("base", base), ("interim", interim)):
        sched = SimpleNamespace(
            mappings=maps, approved_decision_artifact_hash=stamp, sync_mode="incremental_append",
            stream_contracts=[], source_connector_id="", workspace_id="", source_schema_fingerprint="",
        )
        assert create_new_stamp_matches_schedule(sched, dest_db="mysql"), label
        # The Execute gate the beat runs (inline stamp, as the scheduler sends it).
        err, ok = _enforce_decision_artifact_once(
            None, maps, dest_db="mysql", approved_decision_artifact_hash=stamp,
            skip_preflight=True, sync_mode="incremental_append",
        )
        assert err is None and ok, (label, err)
        # The stored payload under that digest still loads (no "tampered").
        assert decision_artifact_from_dict({**art.to_dict(), "content_hash": stamp})
        # A different target type is still drift.
        edited = [{**maps[0], "target_type": "VARCHAR(8)"}]
        err_t, _ = _enforce_decision_artifact_once(
            None, edited, dest_db="mysql", approved_decision_artifact_hash=stamp,
            skip_preflight=True, sync_mode="incremental_append",
        )
        assert err_t, label
    # The scores-only change runs under a current stamp and the interim stamp
    # (neither holds scores). A base stamp held its score, so an inline replay
    # of a rescored Map is matched only through the stored artifact payload.
    rescored = [{**maps[0], "confidence": 0.61}]
    err_c, ok_c = _enforce_decision_artifact_once(
        None, rescored, dest_db="mysql", approved_decision_artifact_hash=art.content_hash,
        skip_preflight=True, sync_mode="incremental_append",
    )
    assert err_c is None and ok_c, err_c
    err_s, ok_s = _enforce_decision_artifact_once(
        None, rescored, dest_db="mysql", approved_decision_artifact_hash=interim,
        skip_preflight=True, sync_mode="incremental_append",
    )
    assert err_s is None and ok_s, err_s


def test_d47_scd2_error_drops_postgres_context_lines():
    """Joining every line (D47) pulled PostgreSQL's CONTEXT line into the
    cause and pushed "rolled back" past the sentence the job shows."""
    from services.error_taxonomy import driver_sentence
    from src.transfer.stream_scd2 import _scd2_rolled_back

    exc = Exception(
        "(psycopg2.errors.RaiseException) w2dat injected failure\n"
        "CONTEXT:  PL/pgSQL function w2dat_s2fn_x line 1 at RAISE\n"
        "[SQL: INSERT INTO t (id) VALUES (%(id__0)s)]"
    )
    text = str(_scd2_rolled_back(exc, '"public"."t"', 2))
    assert "CONTEXT" not in text and "[SQL" not in text
    assert "w2dat injected failure" in text
    assert "rolled back" in driver_sentence(text, limit=400)
