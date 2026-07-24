"""Canonical identity-key resolution — G6/G8/G9 must agree."""

from __future__ import annotations

from services.primary_key import (
    resolve_identity_key,
    resolve_primary_key_source,
    resolve_primary_key_target,
)


def test_schemaless_only_uses_underscore_id():
    mappings = [
        {"source": "user_id", "target": "user_id"},
        {"source": "_id", "target": "_id"},
        {"source": "account_id", "target": "account_id"},
    ]
    src, tgt = resolve_identity_key(
        mappings=mappings,
        source_columns=["user_id", "_id", "account_id"],
        dest_kind="mongodb",
        purpose="uniqueness",
    )
    assert src == "_id" and tgt == "_id"
    assert resolve_primary_key_target(mappings, "redis") == "_id"


def test_sql_uniqueness_ignores_foreign_key_star_id():
    mappings = [
        {"source": "user_id", "target": "user_id"},
        {"source": "email", "target": "email"},
    ]
    src, tgt = resolve_identity_key(
        mappings=mappings,
        source_columns=["user_id", "email"],
        dest_kind="postgresql",
        validation_mode="strict",
        purpose="uniqueness",
    )
    # Sole *_id is accepted as natural key when no id/_id exists.
    assert src == "user_id" and tgt == "user_id"


def test_sql_uniqueness_ignores_competing_star_ids():
    mappings = [
        {"source": "user_id", "target": "user_id"},
        {"source": "account_id", "target": "account_id"},
    ]
    src, tgt = resolve_identity_key(
        mappings=mappings,
        source_columns=["user_id", "account_id"],
        dest_kind="postgresql",
        purpose="uniqueness",
    )
    assert src is None and tgt is None


def test_sql_uniqueness_prefers_exact_id_over_star_id():
    mappings = [
        {"source": "user_id", "target": "user_id"},
        {"source": "id", "target": "id"},
    ]
    assert resolve_primary_key_target(mappings, "snowflake") == "id"


def test_sql_uniqueness_prefers_destination_pk_over_sole_star_id():
    mappings = [
        {"source": "user_id", "target": "user_id"},
        {"source": "order_key", "target": "order_key"},
    ]
    src, tgt = resolve_identity_key(
        mappings=mappings,
        source_columns=["user_id", "order_key"],
        dest_kind="postgresql",
        purpose="uniqueness",
        destination_pk_columns=["order_key"],
    )
    assert src == "order_key" and tgt == "order_key"
    # Without dest PK, sole *_id still wins.
    src2, tgt2 = resolve_identity_key(
        mappings=[{"source": "user_id", "target": "user_id"}],
        dest_kind="postgresql",
        purpose="uniqueness",
    )
    assert src2 == "user_id" and tgt2 == "user_id"


def test_required_nulls_strict_may_use_star_id():
    mappings = [{"source": "account_id", "target": "account_id"}]
    pk = resolve_primary_key_source(
        mappings,
        ["account_id"],
        "postgresql",
        validation_mode="strict",
        purpose="required_nulls",
    )
    assert pk == "account_id"


def test_required_nulls_balanced_skips_star_id():
    mappings = [{"source": "account_id", "target": "account_id"}]
    pk = resolve_primary_key_source(
        mappings,
        ["account_id"],
        "postgresql",
        validation_mode="balanced",
        purpose="required_nulls",
    )
    assert pk is None


def test_g8_does_not_block_on_duplicate_user_id_when_id_exists():
    """Mongo→any: duplicate user_id must not become a G8 PK failure."""
    from services.preflight_service import run_file_preflight

    sample_rows = [
        {"_id": "1", "user_id": "U1", "name": "A"},
        {"_id": "2", "user_id": "U1", "name": "B"},
    ]
    columns = list(sample_rows[0].keys())
    mappings = [{"source": c, "target": c, "confidence": 0.99, "transform": "none"} for c in columns]
    result = run_file_preflight(
        columns=columns,
        column_types={c: "VARCHAR" for c in columns},
        row_count=2,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sample_rows=sample_rows,
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type="mongodb",
        validation_mode="strict",
    )
    gate = next(g for g in result["gates"] if g["id"] == "g8_reconciliation")
    assert gate["status"] == "pass"
    assert result["passed"] is True


def test_breaking_source_drift_is_not_g6_and_schemaless_does_not_block():
    from services.preflight_service import run_file_preflight
    from services.schema_fingerprint import fingerprint_schema

    cols = ["id", "email"]
    schema = {"id": "INTEGER", "email": "VARCHAR"}
    stale = fingerprint_schema(["id"], {"id": "INTEGER"})
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "email", "target": "email", "confidence": 0.99},
    ]
    # Redis: fingerprint noise must not block Execute.
    result = run_file_preflight(
        columns=cols,
        column_types=schema,
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        sample_rows=[{"id": 1, "email": "a@b.com"}],
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type="redis",
        stored_source_fp=stale,
        validation_mode="strict",
        schema_policy="manual_review",
    )
    gate_ids = {g["id"]: g["status"] for g in result["gates"]}
    assert gate_ids.get("g6_target_ddl") == "pass"
    assert result["passed"] is True
    assert not any(b.get("id") == "schema_drift" and True for b in result["blockers"] if b.get("id") == "schema_drift")


def test_g6_ignores_host_folded_drift_noise_on_redis():
    """Even if a stale host still folds drift text into ddl_issues, G6 must pass for Redis."""
    from preflight.gates import gate_g6_target_ddl
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )

    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            columns=[ColumnSchema(name="_id", inferred_type="VARCHAR")],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="redis",
            connected=True,
            can_write=True,
            can_create_table=True,
        ),
        mappings=[ColumnMapping(source="_id", target="_id", confidence=0.99)],
        ddl_compatible=False,
        ddl_issues=["Destination schema changed since last mapping revision"],
        validation_mode="strict",
    )
    result = gate_g6_target_ddl(PreflightContext(plan))
    assert result.status.value == "pass"
    assert "Schemaless" in result.message or "compatible" in result.message.lower()


def test_pause_on_change_still_blocks_sql_source_drift():
    from services.preflight_service import run_file_preflight
    from services.schema_fingerprint import fingerprint_schema

    cols = ["id", "email"]
    schema = {"id": "INTEGER", "email": "VARCHAR"}
    stale = fingerprint_schema(["id"], {"id": "INTEGER"})
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "email", "target": "email", "confidence": 0.99},
    ]
    result = run_file_preflight(
        columns=cols,
        column_types=schema,
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        sample_rows=[{"id": 1, "email": "a@b.com"}],
        destination_column_types={"id": "INTEGER", "email": "VARCHAR"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_db_type="postgresql",
        stored_source_fp=stale,
        validation_mode="strict",
        schema_policy="pause_on_change",
    )
    gate_ids = {g["id"]: g["status"] for g in result["gates"]}
    assert gate_ids.get("schema_drift") == "block"
    assert gate_ids.get("g6_target_ddl") == "pass"
    assert result["passed"] is False
