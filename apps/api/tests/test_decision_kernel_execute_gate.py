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
