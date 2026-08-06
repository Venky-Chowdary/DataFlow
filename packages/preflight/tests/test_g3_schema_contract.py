"""G3 schema contract tests with expanded lossy coercion detection."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PREFLIGHT_ROOT = _REPO_ROOT / "packages" / "preflight" / "src"
_API_ROOT = _REPO_ROOT / "apps" / "api"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from preflight.gates import gate_g3_schema_contract  # noqa: E402
from preflight.risk_contract import make_clearing_risk_contract  # noqa: E402
from preflight.models import ColumnMapping, ColumnSchema, DestinationConfig, PreflightContext, SourceConfig, TransferPlan  # noqa: E402


def _clear_with_contract(mapping: ColumnMapping, **kwargs) -> None:
    """Attach a verified continue-policy Risk Contract (boolean alone never clears)."""
    mapping.risk_acknowledged = True  # legacy flag may still be set by UI
    mapping.risk_contract = make_clearing_risk_contract(
        column=mapping.source,
        source_type=kwargs.get("source_type", "TEXT"),
        destination_type=kwargs.get("destination_type", "INTEGER"),
        **{k: v for k, v in kwargs.items() if k not in ("source_type", "destination_type")},
    )


def _ctx(source_types: dict[str, str], dest_types: dict[str, str], mappings: list[tuple[str, str]]):
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name=s, inferred_type=t) for s, t in source_types.items()],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[ColumnSchema(name=t, inferred_type=dt) for t, dt in dest_types.items()],
        ),
        mappings=[
            ColumnMapping(source=s, target=t, confidence=0.95) for s, t in mappings
        ],
    )
    return PreflightContext(plan=plan)


def test_g3_blocks_varchar_to_integer():
    result = gate_g3_schema_contract(
        _ctx(
            {"amount": "VARCHAR"},
            {"amount": "INTEGER"},
            [("amount", "amount")],
        )
    )
    assert result.status.value == "block"


def test_g3_allows_integer_to_varchar():
    result = gate_g3_schema_contract(
        _ctx(
            {"id": "INTEGER"},
            {"id": "VARCHAR"},
            [("id", "id")],
        )
    )
    assert result.status.value == "pass"


def test_g3_blocks_decimal_to_integer():
    result = gate_g3_schema_contract(
        _ctx(
            {"qty": "DECIMAL"},
            {"qty": "INTEGER"},
            [("qty", "qty")],
        )
    )
    assert result.status.value == "block"


def test_g3_float_to_decimal_not_sample_soft_passed():
    """IEEE→DECIMAL stays G3 block even when coercion probe reports sample-ok."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {
                "sampled_rows": 2,
                "by_source": {
                    "amt": {
                        "severity": "ok",
                        "sampled": 2,
                        "failed": 0,
                        "sentinel_nulls": 0,
                        "sample_failures": [],
                    }
                },
            }

    plan = _ctx(
        {"amt": "FLOAT"},
        {"amt": "DECIMAL(12,4)"},
        [("amt", "amt")],
    ).plan
    result = gate_g3_schema_contract(_Ctx(plan=plan))
    assert result.status.value == "block"
    assert any("float→decimal" in i.lower() or "ieee" in i.lower() for i in (result.details or {}).get("issues", []))

    _clear_with_contract(
        plan.mappings[0], source_type="FLOAT", destination_type="DECIMAL(12,4)"
    )
    cleared = gate_g3_schema_contract(_Ctx(plan=plan))
    assert cleared.status.value == "pass"
    warns = (cleared.details or {}).get("warnings", []) or []
    assert any("risk contract" in str(w).lower() or "risk" in str(w).lower() for w in warns)


def test_g3_boolean_ack_alone_does_not_clear_fidelity_collapse():
    """GA: risk_acknowledged without signed contract must keep G3 blocked."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {
                "sampled_rows": 2,
                "by_source": {
                    "amt": {
                        "severity": "ok",
                        "sampled": 2,
                        "failed": 0,
                        "sentinel_nulls": 0,
                        "sample_failures": [],
                    }
                },
            }

    plan = _ctx(
        {"amt": "FLOAT"},
        {"amt": "DECIMAL(12,4)"},
        [("amt", "amt")],
    ).plan
    plan.mappings[0].risk_acknowledged = True
    still = gate_g3_schema_contract(_Ctx(plan=plan))
    assert still.status.value == "block"


def test_g3_not_null_does_not_block_on_nullable_meta_with_clean_samples():
    """Mongo/file default nullable=True must not false-block NOT NULL dests."""
    plan = _ctx(
        {"email": "VARCHAR"},
        {"email": "VARCHAR"},
        [("email", "email")],
    ).plan
    plan.source.columns[0].nullable = True
    plan.destination.target_columns[0].nullable = False
    plan.destination.table_exists = True
    result = gate_g3_schema_contract(
        PreflightContext(plan=plan, sample_rows=[{"email": "a@b.com"}, {"email": "c@d.com"}])
    )
    assert result.status.value == "pass"
    issues = (result.details or {}).get("issues", []) or []
    assert not any("NOT NULL" in str(i) for i in issues)


def test_g3_typed_sink_missing_probe_is_unproven_not_soft_green():
    """Samples exist but typed column has no by_source row — fail-closed in strict."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {"sampled_rows": 3, "by_source": {}}

    plan = _ctx(
        {"amount": "VARCHAR"},
        {"amount": "INTEGER"},
        [("amount", "amount")],
    ).plan
    result = gate_g3_schema_contract(_Ctx(plan=plan))
    assert result.status.value == "block"
    issues = (result.details or {}).get("issues", [])
    assert any("unproven" in i.lower() for i in issues)
    details = (result.details or {}).get("issues_detail") or []
    assert any(d.get("probe_unproven") for d in details)


def test_g3_declared_lossy_not_sample_soft_passed_without_risk_ack():
    """VARCHAR→INTEGER with clean head samples still blocks until risk_acknowledged."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {
                "sampled_rows": 2,
                "by_source": {
                    "qty": {
                        "severity": "ok",
                        "sampled": 2,
                        "failed": 0,
                        "sentinel_nulls": 0,
                        "sample_failures": [],
                    }
                },
            }

    plan = _ctx(
        {"qty": "VARCHAR"},
        {"qty": "INTEGER"},
        [("qty", "qty")],
    ).plan
    result = gate_g3_schema_contract(_Ctx(plan=plan))
    assert result.status.value == "block"
    issues = (result.details or {}).get("issues", [])
    assert any("soft-pass" in i.lower() or "declared lossy" in i.lower() for i in issues)

    _clear_with_contract(
        plan.mappings[0], source_type="VARCHAR", destination_type="INTEGER"
    )
    result_ack = gate_g3_schema_contract(_Ctx(plan=plan))
    assert result_ack.status.value == "pass"


def test_g3_failed_samples_pass_with_cast_and_continue_contract():
    """Signed CAST_AND_CONTINUE: sample cast failures hold out — G3 must not re-block."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {
                "sampled_rows": 2,
                "by_source": {
                    "qty": {
                        "severity": "block",
                        "sampled": 2,
                        "failed": 1,
                        "sentinel_nulls": 0,
                        "sample_failures": ["row 2: cannot cast 'x' to INTEGER"],
                    }
                },
            }

    plan = _ctx(
        {"qty": "VARCHAR"},
        {"qty": "INTEGER"},
        [("qty", "qty")],
    ).plan
    blocked = gate_g3_schema_contract(_Ctx(plan=plan))
    assert blocked.status.value == "block"

    _clear_with_contract(
        plan.mappings[0],
        source_type="VARCHAR",
        destination_type="INTEGER",
        execution_policy="CAST_AND_CONTINUE",
    )
    cleared = gate_g3_schema_contract(_Ctx(plan=plan))
    assert cleared.status.value == "pass", cleared.message
    details = (cleared.details or {}).get("issues_detail") or []
    assert any(d.get("contracted_holdout") for d in details)


def test_g3_objectid_to_text_risk_contract_clears_block():
    """ObjectId→TEXT is domain polarity — Risk Contract unlocks; hex values stay."""
    plan = _ctx(
        {"_id": "OBJECTID"},
        {"user_id": "TEXT"},
        [("_id", "user_id")],
    ).plan
    plan.destination.table_exists = True
    plan.destination.db_type = "postgresql"
    blocked = gate_g3_schema_contract(PreflightContext(plan=plan, sample_rows=[
        {"_id": "507f1f77bcf86cd799439011"},
    ]))
    assert blocked.status.value == "block"
    blob = str((blocked.details or {}).get("issues", [])) + blocked.message
    assert "ObjectId" in blob or "risk" in blob.lower()

    _clear_with_contract(
        plan.mappings[0], source_type="OBJECTID", destination_type="TEXT"
    )
    cleared = gate_g3_schema_contract(PreflightContext(plan=plan, sample_rows=[
        {"_id": "507f1f77bcf86cd799439011"},
    ]))
    assert cleared.status.value == "pass"
    warns = (cleared.details or {}).get("warnings", []) or []
    assert any("ObjectId" in str(w) for w in warns)


def test_g3_decimal_to_float_not_sample_soft_passed():
    """DECIMAL→FLOAT is fidelity collapse — never soft-pass on clean head samples."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {
                "sampled_rows": 2,
                "by_source": {
                    "amt": {
                        "severity": "ok",
                        "sampled": 2,
                        "failed": 0,
                        "sentinel_nulls": 0,
                        "sample_failures": [],
                    }
                },
            }

    plan = _ctx(
        {"amt": "DECIMAL(20,6)"},
        {"amt": "FLOAT"},
        [("amt", "amt")],
    ).plan
    result = gate_g3_schema_contract(_Ctx(plan=plan))
    assert result.status.value == "block"
    issues = (result.details or {}).get("issues", [])
    assert any("decimal→float" in i.lower() or "ieee" in i.lower() for i in issues)


def test_g3_timestamptz_to_ntz_not_sample_soft_passed():
    """Airbyte-class TZ polarity drop must hard-block, not green on head samples."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {
                "sampled_rows": 1,
                "by_source": {
                    "ts": {
                        "severity": "ok",
                        "sampled": 1,
                        "failed": 0,
                        "sentinel_nulls": 0,
                        "sample_failures": [],
                    }
                },
            }

    plan = _ctx(
        {"ts": "TIMESTAMPTZ"},
        {"ts": "TIMESTAMP_NTZ"},
        [("ts", "ts")],
    ).plan
    result = gate_g3_schema_contract(_Ctx(plan=plan))
    assert result.status.value == "block"
    assert any("ntz" in i.lower() or "timezone" in i.lower() for i in (result.details or {}).get("issues", []))


def test_g3_struct_to_json_blocks_without_struct_policy():
    result = gate_g3_schema_contract(
        _ctx(
            {"addr": "STRUCT<city:STRING, zip:INTEGER>"},
            {"addr": "JSONB"},
            [("addr", "addr")],
        )
    )
    assert result.status.value == "block"
    assert any("nested→document" in i.lower() or "nested->document" in i.lower()
               or "document" in i.lower()
               for i in (result.details or {}).get("issues", []))


def test_g3_struct_to_json_warns_with_struct_policy():
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="addr", inferred_type="STRUCT<city:STRING>")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[ColumnSchema(name="addr", inferred_type="JSONB")],
        ),
        mappings=[
            ColumnMapping(
                source="addr",
                target="addr",
                confidence=0.95,
                struct_policy="store_as_json",
            )
        ],
    )
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert result.status.value == "pass"
    assert (result.details or {}).get("warnings")


def test_g3_struct_field_mismatch_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"addr": "STRUCT<city:STRING, zip:INTEGER>"},
            {"addr": "STRUCT<city:STRING>"},
            [("addr", "addr")],
        )
    )
    assert result.status.value == "block"
    assert any("field" in i.lower() for i in (result.details or {}).get("issues", []))


def test_g3_array_element_narrow_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"tags": "ARRAY<FLOAT>"},
            {"tags": "ARRAY<INTEGER>"},
            [("tags", "tags")],
        )
    )
    assert result.status.value == "block"
    assert any(
        "array" in i.lower() or "element" in i.lower() or "nested" in i.lower()
        for i in (result.details or {}).get("issues", [])
    )


def test_g3_array_element_widen_passes():
    result = gate_g3_schema_contract(
        _ctx(
            {"tags": "ARRAY<INTEGER>"},
            {"tags": "ARRAY<DECIMAL>"},
            [("tags", "tags")],
        )
    )
    assert result.status.value == "pass"


def test_g3_decimal_param_narrow_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"amt": "DECIMAL(38,10)"},
            {"amt": "DECIMAL(12,2)"},
            [("amt", "amt")],
        )
    )
    assert result.status.value == "block"
    assert any(
        "decimal" in i.lower() or "narrow" in i.lower() or "scale" in i.lower()
        for i in (result.details or {}).get("issues", [])
    )


def test_g3_varchar_width_narrow_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"name": "VARCHAR(255)"},
            {"name": "VARCHAR(50)"},
            [("name", "name")],
        )
    )
    assert result.status.value == "block"
    assert any("width" in i.lower() or "varchar" in i.lower() for i in (result.details or {}).get("issues", []))


def test_g3_text_to_varchar_narrow_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"body": "TEXT"},
            {"body": "VARCHAR(10)"},
            [("body", "body")],
        )
    )
    assert result.status.value == "block"


def test_g3_unsigned_int_to_signed_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"qty": "INT UNSIGNED"},
            {"qty": "INTEGER"},
            [("qty", "qty")],
        )
    )
    assert result.status.value == "block"
    assert any("unsigned" in i.lower() for i in (result.details or {}).get("issues", []))


def test_g3_unsigned_int_to_bigint_passes():
    result = gate_g3_schema_contract(
        _ctx(
            {"qty": "INT UNSIGNED"},
            {"qty": "BIGINT"},
            [("qty", "qty")],
        )
    )
    assert result.status.value == "pass"


def test_g3_varbinary_width_narrow_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"blob": "VARBINARY(64)"},
            {"blob": "VARBINARY(16)"},
            [("blob", "blob")],
        )
    )
    assert result.status.value == "block"


def test_g3_enum_domain_shrink_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"status": "ENUM('a','b','c')"},
            {"status": "ENUM('a','b')"},
            [("status", "status")],
        )
    )
    assert result.status.value == "block"


def test_g3_interval_family_collapse_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"dur": "INTERVAL YEAR TO MONTH"},
            {"dur": "INTERVAL DAY TO SECOND"},
            [("dur", "dur")],
        )
    )
    assert result.status.value == "block"


def test_g3_geography_polarity_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"g": "GEOMETRY"},
            {"g": "GEOGRAPHY"},
            [("g", "g")],
        )
    )
    assert result.status.value == "block"


def test_g3_generated_always_overwrite_blocks():
    result = gate_g3_schema_contract(
        _ctx(
            {"id": "INTEGER"},
            {"id": "INTEGER GENERATED ALWAYS"},
            [("id", "id")],
        )
    )
    assert result.status.value == "block"


def test_g3_not_null_contract_warns_on_nullable_meta_without_null_samples():
    """Default nullable=True (Mongo/file) must not hard-block without null evidence."""
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="email", inferred_type="VARCHAR", nullable=True)],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[
                ColumnSchema(name="email", inferred_type="VARCHAR", nullable=False)
            ],
        ),
        mappings=[ColumnMapping(source="email", target="email", confidence=0.95)],
    )
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert result.status.value == "pass"
    warns = (result.details or {}).get("warnings", []) or []
    assert any("not null" in str(w).lower() for w in warns)


def test_g3_not_null_contract_blocks_when_samples_contain_nulls():
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="email", inferred_type="VARCHAR", nullable=True)],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[
                ColumnSchema(name="email", inferred_type="VARCHAR", nullable=False)
            ],
        ),
        mappings=[ColumnMapping(source="email", target="email", confidence=0.95)],
    )
    result = gate_g3_schema_contract(PreflightContext(
        plan=plan,
        sample_rows=[{"email": "a@b.com"}, {"email": None}, {"email": ""}],
    ))
    assert result.status.value == "block"
    assert any("not null" in i.lower() for i in (result.details or {}).get("issues", []))


def test_g3_not_null_blocks_stop_column_null_invent_policy():
    """STOP_COLUMN / coerce cannot invent NULL into a NOT NULL destination."""
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="email", inferred_type="VARCHAR", nullable=True)],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[
                ColumnSchema(name="email", inferred_type="VARCHAR", nullable=False)
            ],
        ),
        mappings=[ColumnMapping(source="email", target="email", confidence=0.95)],
    )
    _clear_with_contract(
        plan.mappings[0],
        source_type="VARCHAR",
        destination_type="VARCHAR",
        execution_policy="STOP_COLUMN",
        reason="omit bad email cells",
    )
    result = gate_g3_schema_contract(
        PreflightContext(plan=plan, sample_rows=[{"email": "a@b.com"}])
    )
    assert result.status.value == "block", result.message
    details = (result.details or {}).get("issues_detail") or []
    assert any(d.get("null_invent_policy_blocked") for d in details)


def test_g3_empty_dest_without_stamp_blocks():
    """Never PASS schema contract when dest has zero columns and no target_type."""
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="id", inferred_type="INTEGER")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[],
            db_type="postgresql",
        ),
        mappings=[
            ColumnMapping(
                source="id",
                target="id",
                confidence=0.99,
                create_new=True,
            )
        ],
    )
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert result.status.value == "block"


def test_g3_empty_dest_with_stamp_examines_create_new():
    """Stamped create-new target_type must be examined (not silent skip/PASS)."""
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="amount", inferred_type="DECIMAL")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[],
            db_type="postgresql",
        ),
        mappings=[
            ColumnMapping(
                source="amount",
                target="amount",
                confidence=0.99,
                create_new=True,
                target_type="INTEGER",
            )
        ],
    )
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert result.status.value == "block"
    issues = (result.details or {}).get("issues", []) or []
    assert any("amount" in str(i).lower() for i in issues)


def test_g3_json_scalar_wrap_blocks_without_risk_ack():
    """Bare scalar→JSON wrap must not soft-PASS Validate without Map Accept risk."""

    class _Ctx(PreflightContext):
        def coercion_report(self):
            return {
                "sampled_rows": 1,
                "by_source": {
                    "payload": {
                        "severity": "warn",
                        "sampled": 1,
                        "failed": 0,
                        "sentinel_nulls": 0,
                        "json_scalar_wraps": 1,
                        "sample_failures": [],
                        "suggested_fix": "Accept risk on Map if intentional",
                    }
                },
            }

    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="payload", inferred_type="INTEGER")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            db_type="snowflake",
            target_columns=[ColumnSchema(name="payload", inferred_type="VARIANT")],
        ),
        mappings=[
            ColumnMapping(source="payload", target="payload", confidence=0.95),
        ],
    )
    blocked = gate_g3_schema_contract(_Ctx(plan=plan))
    assert blocked.status.value == "block", (blocked.status, blocked.message, blocked.details)
    assert any("json" in i.lower() or "wrap" in i.lower() for i in (blocked.details or {}).get("issues", []))

    _clear_with_contract(
        plan.mappings[0], source_type="VARCHAR", destination_type="VARIANT"
    )
    cleared = gate_g3_schema_contract(_Ctx(plan=plan))
    assert cleared.status.value == "pass", (cleared.status, cleared.message, cleared.details)
    warns = (cleared.details or {}).get("warnings", []) or []
    assert any("wrap" in str(w).lower() or "json" in str(w).lower() for w in warns)
