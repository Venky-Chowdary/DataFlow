"""Schema drift detection tests."""

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) in sys.path:
    sys.path.remove(str(_API_ROOT))
sys.path.insert(0, str(_API_ROOT))

from services.schema_drift import classify_schema_change, detect_schema_drift
from services.schema_fingerprint import fingerprint_schema


def test_no_drift_when_schemas_match():
    cols = ["id", "email"]
    schema = {"id": "INTEGER", "email": "VARCHAR"}
    fp = fingerprint_schema(cols, schema)
    mappings = [
        {"source": "id", "target": "user_id", "confidence": 0.95},
        {"source": "email", "target": "email", "confidence": 0.99},
    ]
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["user_id", "email"],
        target_schema={"user_id": "INTEGER", "email": "VARCHAR"},
        stored_source_fp=fp,
        stored_target_fp=fingerprint_schema(["user_id", "email"], {"user_id": "INTEGER", "email": "VARCHAR"}),
        mappings=mappings,
    )
    assert report["drift_detected"] is False
    assert report["severity"] == "none"
    assert report["mapping_coverage"] == 1.0


def test_detects_source_schema_change():
    cols = ["id", "email", "created_at"]
    old_fp = fingerprint_schema(["id", "email"], {"id": "INTEGER", "email": "VARCHAR"})
    report = detect_schema_drift(
        source_columns=cols,
        source_schema={"id": "INTEGER", "email": "VARCHAR", "created_at": "TIMESTAMP"},
        target_columns=["user_id", "email"],
        target_schema={"user_id": "INTEGER", "email": "VARCHAR"},
        stored_source_fp=old_fp,
        mappings=[{"source": "id", "target": "user_id", "confidence": 0.9}],
        previous_source_columns=["id", "email"],
        previous_source_schema={"id": "INTEGER", "email": "VARCHAR"},
        schema_policy="manual_review",
    )
    assert report["source_changed"] is True
    assert report["drift_detected"] is True
    # Additive new column — not hard-breaking under Airbyte-class classify.
    assert report["severity"] in {"additive", "warning"}
    assert "created_at" in report["unmapped_sources"]
    evo = report["schema_evolution"]
    assert evo["action"] == "review"
    assert not evo["should_pause"]


def test_propagate_auto_maps_additive_column():
    from services.schema_drift import apply_propagate_mappings

    cols = ["id", "email", "created_at"]
    old_fp = fingerprint_schema(["id", "email"], {"id": "INTEGER", "email": "VARCHAR"})
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "email", "target": "email", "confidence": 0.99},
    ]
    report = detect_schema_drift(
        source_columns=cols,
        source_schema={"id": "INTEGER", "email": "VARCHAR", "created_at": "TIMESTAMP"},
        target_columns=["id", "email"],
        target_schema={"id": "INTEGER", "email": "VARCHAR"},
        stored_source_fp=old_fp,
        mappings=mappings,
        previous_source_columns=["id", "email"],
        previous_source_schema={"id": "INTEGER", "email": "VARCHAR"},
        schema_policy="propagate_columns",
    )
    evo = report["schema_evolution"]
    assert evo["should_propagate"] is True
    assert evo["should_pause"] is False
    assert report["severity"] == "additive"
    new_maps, applied = apply_propagate_mappings(
        mappings,
        source_columns=cols,
        source_schema={"id": "INTEGER", "email": "VARCHAR", "created_at": "TIMESTAMP"},
        evolution=evo,
        schema_policy="propagate_columns",
    )
    assert any(m.get("source") == "created_at" and m.get("propagated") for m in new_maps)
    assert applied and applied[0]["source"] == "created_at"


def test_hard_breaking_pauses_even_under_propagate():
    report = detect_schema_drift(
        source_columns=["sku", "email"],
        source_schema={"sku": "VARCHAR", "email": "VARCHAR"},
        target_columns=["id", "email"],
        target_schema={"id": "INTEGER", "email": "VARCHAR"},
        mappings=[
            {"source": "sku", "target": "id", "confidence": 0.9},
            {"source": "email", "target": "email", "confidence": 0.9},
        ],
        previous_source_columns=["id", "email"],
        previous_source_schema={"id": "INTEGER", "email": "VARCHAR"},
        previous_primary_key=["id"],
        live_primary_key=["sku"],
        schema_policy="propagate_columns",
    )
    evo = report["schema_evolution"]
    assert evo["should_pause"] is True
    assert evo["action"] == "pause"
    assert report["severity"] == "breaking"
    assert any(h.get("kind") == "primary_key_change" for h in evo["hard_breaking"])


def test_cursor_removed_always_pauses():
    report = detect_schema_drift(
        source_columns=["id", "email"],
        source_schema={"id": "INTEGER", "email": "VARCHAR"},
        target_columns=["id", "email"],
        target_schema={"id": "INTEGER", "email": "VARCHAR"},
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "email", "target": "email", "confidence": 1.0},
        ],
        previous_source_columns=["id", "email", "updated_at"],
        previous_source_schema={
            "id": "INTEGER",
            "email": "VARCHAR",
            "updated_at": "TIMESTAMP",
        },
        cursor_fields=["updated_at"],
        schema_policy="propagate_all",
    )
    evo = report["schema_evolution"]
    assert evo["should_pause"] is True
    assert any(h.get("kind") == "cursor_removed" for h in evo["hard_breaking"])


def test_soft_drop_net_additive_under_propagate():
    """Fivetran-class: column drop keeps dest history — soft under propagate."""
    report = detect_schema_drift(
        source_columns=["id"],
        source_schema={"id": "INTEGER"},
        target_columns=["id", "legacy"],
        target_schema={"id": "INTEGER", "legacy": "VARCHAR"},
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        previous_source_columns=["id", "legacy"],
        previous_source_schema={"id": "INTEGER", "legacy": "VARCHAR"},
        schema_policy="propagate_columns",
    )
    evo = report["schema_evolution"]
    assert evo["should_pause"] is False
    assert any(s.get("kind") == "drop" for s in evo["soft_net_additive"])
    assert evo["action"] in {"propagate", "continue"}

def test_warns_on_unmapped_destination_columns():
    report = detect_schema_drift(
        source_columns=["id"],
        source_schema={"id": "INTEGER"},
        target_columns=["id", "legacy_flag"],
        target_schema={"id": "INTEGER", "legacy_flag": "BOOLEAN"},
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        table_exists=True,
    )
    assert report["orphan_targets"] == ["legacy_flag"]
    assert report["severity"] == "warning"


def test_ignores_case_only_target_name_differences():
    report = detect_schema_drift(
        source_columns=["id"],
        source_schema={"id": "INTEGER"},
        target_columns=["USER_ID"],
        target_schema={"USER_ID": "INTEGER"},
        mappings=[{"source": "id", "target": "user_id", "confidence": 1.0}],
        table_exists=True,
    )
    assert report["orphan_targets"] == []


def test_mongodb_does_not_raise_type_mismatch_drift_for_document_fields():
    report = detect_schema_drift(
        source_columns=["amount", "status"],
        source_schema={"amount": "DECIMAL", "status": "VARCHAR"},
        target_columns=["amount", "status"],
        target_schema={"amount": "DOUBLE", "status": "STRING"},
        mappings=[
            {"source": "amount", "target": "amount", "confidence": 0.95},
            {"source": "status", "target": "status", "confidence": 0.98},
        ],
        destination_db_type="mongodb",
    )
    assert report["type_mismatches"] == []
    assert report["severity"] == "none"
    assert report["drift_detected"] is False


def test_varchar_to_number_not_breaking_drift_when_samples_coerce():
    report = detect_schema_drift(
        source_columns=["population"],
        source_schema={"population": "VARCHAR"},
        target_columns=["population"],
        target_schema={"population": "NUMBER(38,0)"},
        mappings=[{"source": "population", "target": "population", "confidence": 0.93}],
        destination_db_type="snowflake",
        sample_rows=[{"population": "331002651"}, {"population": "42"}],
        table_exists=True,
    )
    assert report["type_mismatches"] == []
    assert report["severity"] == "none"


def test_mongo_aliases_treated_as_schemaless_in_drift_engine():
    report = detect_schema_drift(
        source_columns=["amount"],
        source_schema={"amount": "DECIMAL"},
        target_columns=["amount"],
        target_schema={"amount": "INT"},
        mappings=[{"source": "amount", "target": "amount", "confidence": 0.9}],
        destination_db_type="mongodb+srv",
    )
    assert report["type_mismatches"] == []
    assert report["severity"] == "none"


def test_redis_target_fingerprint_churn_is_not_breaking():
    """Schemaless dests synthesize target columns from mappings — fingerprint churn ≠ DDL break."""
    cols = ["id", "skills"]
    schema = {"id": "VARCHAR", "skills": "ARRAY"}
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "skills", "target": "skills", "confidence": 0.93},
    ]
    old_target_fp = fingerprint_schema(["id"], {"id": "VARCHAR"})
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["id", "skills"],
        target_schema={},
        stored_target_fp=old_target_fp,
        mappings=mappings,
        destination_db_type="redis",
    )
    assert report["target_changed"] is False
    assert report["severity"] == "none"
    assert not any("Destination schema changed" in i for i in report["issues"])


def test_snowflake_target_fingerprint_churn_is_breaking():
    cols = ["id"]
    schema = {"id": "INTEGER"}
    old_fp = fingerprint_schema(["id"], {"id": "VARCHAR"})
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["id"],
        target_schema={"id": "INTEGER"},
        stored_target_fp=old_fp,
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        destination_db_type="snowflake",
        schema_policy="pause_on_change",
        table_exists=True,
    )
    assert report["target_changed"] is True
    assert report["schema_evolution"]["should_pause"] is True
    assert report["severity"] == "breaking"
    assert any("Destination schema changed" in i for i in report["issues"])


def test_create_new_ignores_stale_target_fingerprint_and_orphans():
    """Projected CREATE must not block as 'Destination schema changed'."""
    cols = ["a", "b", "c"]
    schema = {c: "VARCHAR" for c in cols}
    mappings = [{"source": c, "target": c, "confidence": 0.93} for c in cols]
    stale_fp = fingerprint_schema(["old"], {"old": "TEXT"})
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["old", "x"],
        target_schema={"old": "TEXT", "x": "INTEGER"},
        stored_target_fp=stale_fp,
        mappings=mappings,
        destination_db_type="postgresql",
        schema_policy="manual_review",
        table_exists=False,
    )
    assert report["create_new"] is True
    assert report["target_changed"] is False
    assert report["orphan_targets"] == []
    assert not any("Destination schema changed" in i for i in report["issues"])
    assert report["schema_evolution"]["action"] == "continue"


def test_create_new_preflight_passes_with_stale_dest_schema_map():
    """Studio may still hold a prior table's column map while create-new is selected."""
    from services.preflight_service import run_file_preflight

    cols = [f"c{i}" for i in range(9)]
    schema = {c: "VARCHAR" for c in cols}
    mappings = [
        {"source": c, "target": c, "confidence": 0.93, "transform": "none"} for c in cols
    ]
    sample = [{c: "x" for c in cols} for _ in range(3)]
    stale = {"legacy_id": "INTEGER", "legacy_name": "TEXT"}
    result = run_file_preflight(
        columns=cols,
        column_types=schema,
        row_count=13,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sample_rows=sample,
        destination_column_types=stale,
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
        sync_mode="full_refresh_append",
        estimated_bytes=1000,
    )
    assert result["passed"] is True, result.get("blockers")
    assert not any(
        g.get("id") == "schema_drift" and g.get("status") == "block"
        for g in result["gates"]
    )
    assert not any(
        "Destination schema changed" in str(i)
        for i in ((result.get("schema_drift") or {}).get("issues") or [])
    )


def test_manual_review_still_blocks_real_source_drift():
    from services.preflight_service import run_file_preflight
    from services.schema_fingerprint import fingerprint_schema as fp

    cols = ["id", "email", "new_col"]
    schema = {"id": "INTEGER", "email": "VARCHAR", "new_col": "VARCHAR"}
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": "none"},
        {"source": "email", "target": "email", "confidence": 0.99, "transform": "none"},
    ]
    sample = [{"id": "1", "email": "a@b.com", "new_col": "x"}]
    result = run_file_preflight(
        columns=cols,
        column_types=schema,
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sample_rows=sample,
        destination_column_types={"id": "INTEGER", "email": "VARCHAR"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
        sync_mode="full_refresh_append",
        estimated_bytes=1000,
        stored_source_fp=fp(["id", "email"], {"id": "INTEGER", "email": "VARCHAR"}),
        previous_source_columns=["id", "email"],
        previous_source_schema={"id": "INTEGER", "email": "VARCHAR"},
    )
    assert result["passed"] is False
    assert any(b.get("id") == "schema_drift" for b in result["blockers"])


def test_fingerprint_synonyms_do_not_flip_contract():
    from services.schema_fingerprint import (
        fingerprint_schema,
        fingerprint_schema_legacy,
        schemas_match,
    )

    cols = ["id", "note"]
    assert fingerprint_schema(cols, {"id": "INT", "note": "TEXT"}) == fingerprint_schema(
        cols, {"id": "INTEGER", "note": "VARCHAR"}
    )
    assert fingerprint_schema(cols, {"id": "INTEGER", "note": "VARCHAR"}) == fingerprint_schema(
        cols, {"id": "INTEGER", "note": "VARCHAR(255)"}
    )
    stored = fingerprint_schema_legacy(cols, {"id": "INTEGER", "note": "TEXT"})
    assert schemas_match(stored, cols, {"id": "INT", "note": "VARCHAR(100)"})


def test_unknown_table_exists_ignores_stale_dest_map():
    report = detect_schema_drift(
        source_columns=["a", "b"],
        source_schema={"a": "VARCHAR", "b": "VARCHAR"},
        target_columns=["legacy"],
        target_schema={"legacy": "INTEGER"},
        mappings=[
            {"source": "a", "target": "a", "confidence": 0.9},
            {"source": "b", "target": "b", "confidence": 0.9},
        ],
        stored_target_fp=fingerprint_schema(["legacy"], {"legacy": "TEXT"}),
        destination_db_type="postgresql",
        schema_policy="manual_review",
        table_exists=None,
    )
    assert report["live_ddl_contract"] is False
    assert report["target_changed"] is False
    assert report["orphan_targets"] == []
    assert report["type_mismatches"] == []
    assert report["schema_evolution"]["action"] == "continue"


def test_intentional_subset_mapping_create_new_continues():
    """Operator mapped 3 of 5 columns on first create-new — not schema drift."""
    cols = ["id", "a", "b", "c", "d"]
    schema = {c: "VARCHAR" for c in cols}
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "a", "target": "a", "confidence": 0.9},
        {"source": "b", "target": "b", "confidence": 0.9},
    ]
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["id", "a", "b"],
        target_schema={},
        mappings=mappings,
        destination_db_type="postgresql",
        schema_policy="manual_review",
        table_exists=False,
    )
    assert report["schema_evolution"]["action"] == "continue"
    assert report["evolution_unmapped_sources"] == []
    assert not report["drift_detected"]


def test_intentional_omit_transform_excluded_from_write_coverage():
    """Explicit Map omit is accounted policy — not unmapped drift / not a write column."""
    from connectors.writer_common import resolve_target_columns
    from services.mapping_constraints import is_intentional_omit, write_mappings

    cols = ["id", "ssn", "name"]
    schema = {c: "VARCHAR" for c in cols}
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": "none"},
        {"source": "ssn", "target": "", "confidence": 1.0, "transform": "omit"},
        {"source": "name", "target": "name", "confidence": 0.95, "transform": "none"},
    ]
    assert is_intentional_omit(mappings[1])
    assert [m["source"] for m in write_mappings(mappings)] == ["id", "name"]
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["id", "name"],
        target_schema={},
        mappings=mappings,
        destination_db_type="postgresql",
        schema_policy="manual_review",
        table_exists=False,
    )
    assert report["unmapped_sources"] == []
    assert report["intentional_omits"] == ["ssn"]
    assert report["schema_evolution"]["action"] == "continue"
    assert not report["drift_detected"]
    targets, _types = resolve_target_columns(mappings, schema, table_exists=False)
    assert targets == ["id", "name"]


def test_case_insensitive_source_mapping_not_unmapped():
    report = detect_schema_drift(
        source_columns=["ID", "Name"],
        source_schema={"ID": "INTEGER", "Name": "VARCHAR"},
        target_columns=["id", "name"],
        target_schema={"id": "INTEGER", "name": "VARCHAR"},
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "name", "target": "name", "confidence": 1.0},
        ],
        destination_db_type="snowflake",
        table_exists=True,
        schema_policy="manual_review",
    )
    assert report["unmapped_sources"] == []
    assert report["schema_evolution"]["action"] == "continue"


def test_propagate_policy_synonym_target_fp_does_not_require_ack():
    from services.preflight_service import run_file_preflight
    from services.schema_fingerprint import fingerprint_schema_legacy

    cols = ["id", "note"]
    schema = {"id": "INTEGER", "note": "VARCHAR"}
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": "none"},
        {"source": "note", "target": "note", "confidence": 0.99, "transform": "none"},
    ]
    sample = [{"id": "1", "note": "x"}]
    # Stored physical TEXT; live VARCHAR — synonym only
    stale = fingerprint_schema_legacy(cols, {"id": "INTEGER", "note": "TEXT"})
    result = run_file_preflight(
        columns=cols,
        column_types=schema,
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sample_rows=sample,
        destination_column_types={"id": "INTEGER", "note": "VARCHAR"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="propagate_columns",
        sync_mode="full_refresh_overwrite",
        estimated_bytes=1000,
        stored_target_fp=stale,
    )
    assert result["passed"] is True, result.get("blockers")
    assert not any(g.get("id") == "schema_drift" and g.get("status") == "block" for g in result["gates"])


def test_classify_no_change():
    schema = {
        "columns": {"id": "INTEGER", "name": "VARCHAR"},
        "nullable": {"id": False, "name": True},
        "primary_key": ["id"],
    }
    report = classify_schema_change(schema, schema)
    assert report["severity"] == "none"
    assert report["additive"] == []
    assert report["breaking"] == []


def test_is_wider_type_decimal_preserves_integer_and_scale():
    from connectors.schema_drift import is_wider_type

    # Growing scale at the cost of integer digits is not a widen.
    assert is_wider_type("DECIMAL(10,2)", "DECIMAL(10,4)") is False
    # More total precision with the same scale is a widen.
    assert is_wider_type("DECIMAL(10,2)", "DECIMAL(12,2)") is True
    # Growing both integer and scale capacity is a widen.
    assert is_wider_type("DECIMAL(10,2)", "DECIMAL(12,4)") is True
    # Equal types are not widens (no ALTER needed).
    assert is_wider_type("DECIMAL(10,2)", "DECIMAL(10,2)") is False


def test_is_wider_type_decimal_to_float_is_lossy():
    from connectors.schema_drift import is_wider_type

    assert is_wider_type("DECIMAL(10,2)", "DOUBLE") is False
    assert is_wider_type("DECIMAL(5,2)", "REAL") is False


def test_is_wider_type_unsigned_int_needs_signed_widen():
    from connectors.schema_drift import is_wider_type

    # INT UNSIGNED holds values above signed INT max → not a no-op widen to INT.
    assert is_wider_type("INT UNSIGNED", "INT") is False
    assert is_wider_type("INT", "INT UNSIGNED") is True
    # Signed BIGINT covers INT UNSIGNED range.
    assert is_wider_type("INT UNSIGNED", "BIGINT") is True
    assert is_wider_type("MEDIUMINT UNSIGNED", "INT") is True
    assert is_wider_type("SMALLINT UNSIGNED", "SMALLINT") is False


def test_sanitize_ddl_type_rejects_injection():
    from connectors.schema_drift import _sanitize_ddl_type

    assert _sanitize_ddl_type("VARCHAR(255)") == "VARCHAR(255)"
    assert _sanitize_ddl_type("DECIMAL(38,10)") == "DECIMAL(38,10)"
    assert _sanitize_ddl_type("TIMESTAMP WITH TIME ZONE") == "TIMESTAMP WITH TIME ZONE"
    for bad in ("VARCHAR; DROP TABLE t", "TEXT'--", 'TEXT"--', "TEXT/*x*/", "TEXT\\n", "TEXT--"):
        try:
            _sanitize_ddl_type(bad)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected ValueError for {bad!r}")


def test_classify_additive_nullable_column_and_widen():
    old = {
        "columns": {"id": "INTEGER", "amount": "INTEGER"},
        "nullable": {"id": False, "amount": True},
        "primary_key": ["id"],
    }
    new = {
        "columns": {"id": "INTEGER", "amount": "DECIMAL", "note": "VARCHAR"},
        "nullable": {"id": False, "amount": True, "note": True},
        "primary_key": ["id"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "additive"
    kinds = {c["kind"] for c in report["additive"]}
    assert "add_column" in kinds
    assert "widen_type" in kinds
    assert report["breaking"] == []


def test_classify_breaking_drop_and_narrow():
    old = {
        "columns": {"id": "INTEGER", "amount": "DECIMAL", "legacy": "VARCHAR"},
        "nullable": {"id": False, "amount": True, "legacy": True},
        "primary_key": ["id"],
    }
    new = {
        "columns": {"id": "INTEGER", "amount": "INTEGER"},
        "nullable": {"id": False, "amount": True},
        "primary_key": ["id"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    kinds = {c["kind"] for c in report["breaking"]}
    assert "drop" in kinds
    assert "narrow_type" in kinds


def test_classify_breaking_pk_change():
    old = {
        "columns": {"id": "INTEGER", "sku": "VARCHAR"},
        "nullable": {"id": False, "sku": False},
        "primary_key": ["id"],
    }
    new = {
        "columns": {"id": "INTEGER", "sku": "VARCHAR"},
        "nullable": {"id": False, "sku": False},
        "primary_key": ["sku"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    assert any(c["kind"] == "primary_key_change" for c in report["breaking"])


def test_classify_rename_as_breaking():
    old = {"columns": {"full_name": "VARCHAR"}, "nullable": {"full_name": True}, "primary_key": []}
    new = {"columns": {"name": "VARCHAR"}, "nullable": {"name": True}, "primary_key": []}
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    assert any(c["kind"] == "rename" for c in report["breaking"])


def test_classify_multi_column_renames_not_as_drops_and_adds():
    old = {
        "columns": {"cust_id": "INTEGER", "full_name": "VARCHAR", "amt": "DECIMAL"},
        "nullable": {"cust_id": False, "full_name": True, "amt": True},
        "primary_key": ["cust_id"],
    }
    new = {
        "columns": {"customer_id": "INTEGER", "name": "VARCHAR", "amount": "DECIMAL"},
        "nullable": {"customer_id": False, "name": True, "amount": True},
        "primary_key": ["customer_id"],
    }
    report = classify_schema_change(old, new)
    kinds = [c["kind"] for c in report["breaking"]]
    assert kinds.count("rename") == 3
    assert "drop" not in kinds
    assert "add_column" not in {c["kind"] for c in report["additive"]}


def test_classify_add_not_null_is_breaking():
    old = {"columns": {"id": "INTEGER"}, "nullable": {"id": False}, "primary_key": ["id"]}
    new = {
        "columns": {"id": "INTEGER", "code": "VARCHAR"},
        "nullable": {"id": False, "code": False},
        "primary_key": ["id"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    assert any(c["kind"] == "add_not_null" for c in report["breaking"])


def test_classify_flat_schema_dicts():
    report = classify_schema_change(
        {"id": "INT", "name": "VARCHAR(50)"},
        {"id": "INT", "name": "VARCHAR(200)"},
    )
    assert report["severity"] == "additive"
    assert any(c["kind"] == "widen_type" for c in report["additive"])
