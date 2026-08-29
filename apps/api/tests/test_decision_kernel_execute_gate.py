"""Phase C2/C11 — Decision Artifact Execute gate + kernel invent SSOT."""

from __future__ import annotations

from services.decision_kernel import (
    ConversionClass,
    build_artifact_from_mappings,
    ddl_type,
    enforce_decision_artifact,
    is_lossy_coercion,
    materialize_dest_ddl,
)


def test_kernel_invent_ssot_matches_width_safe_defaults():
    assert ddl_type("postgresql", "integer") == "BIGINT"
    assert materialize_dest_ddl("postgresql", "BIGINT") in {"BIGINT", "bigint"}
    assert is_lossy_coercion("BIGINT", "INTEGER", dest_db="postgresql") is True


def test_inline_skip_preflight_stamps_artifact():
    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "transform": "none",
            "create_new": True,
        }
    ]
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        skip_preflight=True,
    )
    assert err is None
    assert art is not None
    assert len(art.content_hash) == 64
    assert art.ddl.ddl_identity_hash
    assert art.mappings[0].conversion.conversion_class in {
        ConversionClass.IDENTITY,
        ConversionClass.EQUIVALENT,
        ConversionClass.LOSSLESS,
        ConversionClass.WIDENING,
    }


def test_approved_hash_mismatch_refuses():
    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "transform": "none",
        }
    ]
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash="0" * 64,
        skip_preflight=False,
    )
    assert err is not None
    assert "mismatch" in err.lower()
    assert art is None


def test_approved_hash_match_allows():
    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "transform": "none",
        }
    ]
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="postgresql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        skip_preflight=False,
    )
    assert err is None
    assert art is not None
    assert art.content_hash == stamped.content_hash


def test_without_hash_or_skip_refuses():
    maps = [
        {
            "source": "a",
            "target": "a",
            "source_type": "TEXT",
            "target_type": "TEXT",
        }
    ]
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        skip_preflight=False,
    )
    assert err is not None
    assert "Validate" in err
    assert art is None


def test_build_artifact_never_narrows_bigint_create_new():
    maps = [
        {
            "source": "big_val",
            "target": "big_val",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "create_new": True,
        }
    ]
    art = build_artifact_from_mappings(maps, dest_db="postgresql")
    assert art.ddl.column_ddl.get("big_val", "").upper() == "BIGINT"
    # Map targets are not live dest identity — empty dest_fp, not engine name.
    assert art.dest_fingerprint == ""
    # Map source_type stamps are not live source identity — empty, not "map".
    assert art.source_fingerprint == ""


def test_approved_artifact_refuses_dest_schema_fingerprint_drift():
    from services.schema_fingerprint import fingerprint_schema

    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
        }
    ]
    dest_approved = {"id": "BIGINT"}
    dest_drifted = {"id": "BIGINT", "email": "VARCHAR"}
    fp_ok = fingerprint_schema(list(dest_approved.keys()), dest_approved)
    fp_drift = fingerprint_schema(list(dest_drifted.keys()), dest_drifted)
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="postgresql",
        dest_fingerprint=fp_ok,
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        dest_fingerprint=fp_drift,
        skip_preflight=False,
    )
    assert err is not None
    assert "mismatch" in err.lower() or "Validate" in err
    assert art is None
    ok_err, ok_art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        dest_fingerprint=fp_ok,
        skip_preflight=False,
    )
    assert ok_err is None
    assert ok_art is not None
    assert ok_art.dest_fingerprint == fp_ok


def test_live_dest_schema_fingerprint_does_not_invent_map_columns():
    from services.schema_fingerprint import live_dest_schema_fingerprint

    assert live_dest_schema_fingerprint(
        {},
        destination_table_exists=True,
        sync_mode="full_refresh_append",
    ) == ""
    assert live_dest_schema_fingerprint(
        {"id": "BIGINT", "email": "VARCHAR"},
        destination_table_exists=True,
        sync_mode="full_refresh_overwrite",
    ) == ""
    assert live_dest_schema_fingerprint(
        {"id": "BIGINT"},
        destination_table_exists=False,
        sync_mode="full_refresh_append",
    ) == ""
    live = live_dest_schema_fingerprint(
        {"id": "BIGINT", "email": "TEXT"},
        destination_table_exists=True,
        sync_mode="full_refresh_append",
    )
    assert live
    assert len(live) == 16


def test_approved_artifact_refuses_source_schema_fingerprint_drift():
    from services.schema_fingerprint import fingerprint_schema

    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
        }
    ]
    src_approved = {"id": "BIGINT", "email": "VARCHAR"}
    src_drifted = {"id": "BIGINT", "email": "INTEGER"}
    fp_ok = fingerprint_schema(list(src_approved.keys()), src_approved)
    fp_drift = fingerprint_schema(list(src_drifted.keys()), src_drifted)
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="postgresql",
        source_fingerprint=fp_ok,
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        source_fingerprint=fp_drift,
        skip_preflight=False,
    )
    assert err is not None
    assert "mismatch" in err.lower() or "Validate" in err
    assert art is None
    ok_err, ok_art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        source_fingerprint=fp_ok,
        skip_preflight=False,
    )
    assert ok_err is None
    assert ok_art is not None
    assert ok_art.source_fingerprint == fp_ok


def test_hash_only_create_new_matches_validate_route_id():
    """Studio stamps validate:{dest}; Execute must not require execute:{dest}."""
    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
        }
    ]
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="postgresql",
        source_db="mysql",
        dest_fingerprint="",
        sync_mode="full_refresh_append",
        route_id="validate:postgresql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        source_db="mysql",
        approved_content_hash=stamped.content_hash,
        dest_fingerprint="",
        destination_table_exists=False,
        sync_mode="full_refresh_append",
        skip_preflight=False,
    )
    assert err is None
    assert art is not None


def test_create_new_validate_stamp_holds_after_dest_exists_append():
    """Named fixture: create-new Validate + dest-exists Execute (append) must pass.

    Measured on this fixture only — dest appearing after the first write is not
    dest schema drift when Map still holds.
    """
    from services.schema_fingerprint import fingerprint_schema

    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "create_new": True,
        },
        {
            "source": "email",
            "target": "email",
            "source_type": "TEXT",
            "target_type": "TEXT",
            "create_new": True,
        },
    ]
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="snowflake",
        dest_fingerprint="",
        sync_mode="full_refresh_append",
        route_id="validate:snowflake",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    dest_types = {"ID": "NUMBER", "EMAIL": "VARCHAR"}
    live = fingerprint_schema(list(dest_types.keys()), dest_types)
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="snowflake",
        approved_content_hash=stamped.content_hash,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=list(dest_types.keys()),
        sync_mode="full_refresh_append",
        skip_preflight=False,
    )
    assert err is None
    assert art is not None


def test_create_new_stamp_still_refuses_real_map_edit():
    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
        }
    ]
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="postgresql",
        dest_fingerprint="",
        sync_mode="full_refresh_append",
        route_id="validate:postgresql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    edited = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "VARCHAR(8)",
        }
    ]
    from services.schema_fingerprint import fingerprint_schema

    dest_types = {"id": "BIGINT"}
    live = fingerprint_schema(["id"], dest_types)
    err, art = enforce_decision_artifact(
        mappings=edited,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=["id"],
        sync_mode="full_refresh_append",
        skip_preflight=False,
    )
    assert err is not None
    assert art is None


def test_create_new_stamp_refuses_dest_only_not_null():
    maps = [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
        }
    ]
    stamped = build_artifact_from_mappings(
        maps,
        dest_db="postgresql",
        dest_fingerprint="",
        sync_mode="full_refresh_append",
        route_id="validate:postgresql",
        artifact_id="da_inline",
        created_at="1970-01-01T00:00:00+00:00",
    )
    from services.schema_fingerprint import fingerprint_schema

    dest_types = {"id": "BIGINT", "audit_user": "TEXT"}
    live = fingerprint_schema(list(dest_types.keys()), dest_types)
    err, art = enforce_decision_artifact(
        mappings=maps,
        dest_db="postgresql",
        approved_content_hash=stamped.content_hash,
        dest_fingerprint=live,
        destination_table_exists=True,
        dest_column_names=list(dest_types.keys()),
        column_nullability={"audit_user": False},
        sync_mode="full_refresh_append",
        skip_preflight=False,
    )
    assert err is not None
    assert "NOT NULL" in err or "Validate" in err
    assert art is None


def test_live_source_schema_fingerprint_does_not_invent_map_columns():
    from services.schema_fingerprint import live_source_schema_fingerprint

    assert live_source_schema_fingerprint({}, authoritative=True) == ""
    assert live_source_schema_fingerprint(
        {"id": "BIGINT", "email": "VARCHAR"},
        authoritative=False,
    ) == ""
    overwrite = live_source_schema_fingerprint(
        {"id": "BIGINT", "email": "VARCHAR"},
        authoritative=True,
    )
    assert overwrite
    assert len(overwrite) == 16
