"""Production honesty regressions for Gate-8 / Validate / catalog / invent.

Covers P0/P1 gaps from the deep algorithm audit: circular row accounting,
SKIP-as-pass unlock, SFTP/email transfer_ready lie, writer-as-source checksum,
invent-under-unknown-existence, and structure certify-after-CREATE.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
_PREFLIGHT_SRC = Path(__file__).resolve().parents[3] / "packages" / "preflight" / "src"
for _p in (_API_ROOT, _PREFLIGHT_SRC):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from preflight.engine import PreflightEngine
from preflight.gates import gate_g3_schema_contract
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    GateId,
    GateResult,
    GateStatus,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from services.destination_requirements_gate import build_destination_requirements_gate
from services.mapping_pipeline import run_mapping_pipeline
from services.schema_fidelity import (
    CreateFidelityPlan,
    SchemaFidelityItem,
    SchemaFidelityReport,
    certify_structure_on_destination,
)
from src.transfer.connector_capabilities import (
    enrich_catalog_entry,
    get_capabilities,
    transfer_live_driver_types,
    transfer_ready,
)
from src.transfer.models import EndpointConfig
from src.transfer.reconcile_step import _compute_source_checksum, run_reconciliation


# ---------------------------------------------------------------------------
# P0 — catalog / marketing honesty
# ---------------------------------------------------------------------------


def test_email_is_not_transfer_ready_without_preflight():
    """Write-only with no read-back cannot claim a transfer tier.

    SFTP used to share this test. It earned preflight, introspect and a Gate-8
    read-back (test_sftp_live_transfer.py, against a real server), so it is no
    longer an example of the lie this guards. email still is: it writes and can
    never be read back, so nothing it sends is verifiable.
    """
    caps = get_capabilities("email")
    assert caps.get("preflight") is False
    assert transfer_ready(caps) is False
    enriched = enrich_catalog_entry({"id": "email", "status": "live"})
    assert enriched.get("transfer_ready") is False
    assert enriched.get("certification_tier") != "certified"


def test_transfer_live_driver_count_excludes_email_and_matches_fe_constant():
    from src.transfer.connector_capabilities import driver_available

    live = transfer_live_driver_types()
    assert "email" not in live
    # Package-available list, not a marketing constant. Missing paramiko must
    # not invent a transfer-ready SFTP tile (catalog count ≠ live).
    if driver_available("sftp"):
        assert "sftp" in live
        assert len(live) == 43
    else:
        assert "sftp" not in live
        assert len(live) == 42


def test_mongodb_registry_does_not_claim_sql_merge():
    from services.connector_capability_registry import get_connector_capability

    caps = get_connector_capability("mongodb")
    assert caps.get("supports_upsert") is True
    assert caps.get("supports_merge") is False
    assert "at-least-once" in (caps.get("cdc_prerequisites") or "").lower()


# ---------------------------------------------------------------------------
# P0 — preflight SKIP must not unlock Execute
# ---------------------------------------------------------------------------


def test_preflight_engine_all_skip_does_not_pass(monkeypatch):
    from preflight import engine as eng_mod

    def _skip(_ctx):
        return GateResult(
            gate_id=GateId.G3_SCHEMA_CONTRACT,
            status=GateStatus.SKIP,
            message="schemaless",
            duration_ms=1,
        )

    monkeypatch.setattr(
        eng_mod,
        "PREFLIGHT_GATES",
        [
            ("g3_schema_contract", _skip),
            ("g8_reconciliation", _skip),
        ],
    )
    plan = TransferPlan(
        source=SourceConfig(kind="database", db_type="mongodb"),
        destination=DestinationConfig(
            kind="database", db_type="postgresql", table_exists=True
        ),
        mappings=[],
    )
    result = PreflightEngine(fail_fast=False).run(PreflightContext(plan=plan))
    assert result.passed is False


def test_preflight_engine_skip_plus_pass_unlocks(monkeypatch):
    from preflight import engine as eng_mod

    def _pass(_ctx):
        return GateResult(
            gate_id=GateId.G1_SOURCE,
            status=GateStatus.PASS,
            message="ok",
            duration_ms=1,
        )

    def _skip(_ctx):
        return GateResult(
            gate_id=GateId.G3_SCHEMA_CONTRACT,
            status=GateStatus.SKIP,
            message="schemaless",
            duration_ms=1,
        )

    monkeypatch.setattr(
        eng_mod,
        "PREFLIGHT_GATES",
        [
            ("g1_source", _pass),
            ("g3_schema_contract", _skip),
        ],
    )
    plan = TransferPlan(
        source=SourceConfig(kind="database", db_type="mongodb", connected=True),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            table_exists=True,
            connected=True,
            can_write=True,
        ),
        mappings=[],
    )
    result = PreflightEngine(fail_fast=False).run(PreflightContext(plan=plan))
    assert result.passed is True


def test_g14_unmeasured_is_flagged_not_falsely_passed():
    gate = build_destination_requirements_gate(
        destination_table_exists=True,
        column_nullability={},
        column_defaults={},
        identity_columns=[],
        generated_columns=[],
        mappings=[{"source": "id", "target": "id"}],
    )
    assert gate is not None
    # Unmeasured must never look like a proven green pass — but it also must not
    # false-block a legitimate transfer (NOT NULL is enforced fail-closed at
    # write). Honest posture: skip carrying an explicit unmeasured flag.
    assert gate["status"] == "skip"
    assert gate["details"].get("unmeasured") is True


def test_g14_measured_unfilled_required_column_blocks():
    gate = build_destination_requirements_gate(
        destination_table_exists=True,
        column_nullability={"id": False, "tenant_id": False},
        column_defaults={},
        identity_columns=[],
        generated_columns=[],
        mappings=[{"source": "id", "target": "id"}],
    )
    assert gate is not None
    assert gate["status"] == "block"
    assert "tenant_id" in gate["details"]["unfilled_required_columns"]


# ---------------------------------------------------------------------------
# P0/P1 — Gate-8 independent source digest + unmeasured row count
# ---------------------------------------------------------------------------


def test_compute_source_checksum_prefers_remapped_records_over_writer():
    records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    columns = ["id", "name"]
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
    ]
    writer_lie = "deadbeef" * 8
    digest = _compute_source_checksum(
        records,
        columns,
        mappings,
        {"id": "INTEGER", "name": "TEXT"},
        writer_lie,
        dest_db_type="postgresql",
        dest_types={"id": "INTEGER", "name": "TEXT"},
    )
    assert digest
    assert digest != writer_lie


def test_reconcile_fails_closed_when_source_row_count_unmeasured():
    endpoint = EndpointConfig(kind="database", format="postgresql", table="t")
    report = run_reconciliation(
        endpoint=endpoint,
        records=[],
        columns=["id"],
        rows_written=10,
        writer_checksum="",
        dest_summary={
            "schema": "public",
            "table": "t",
            "source_row_count_source": "unmeasured",
            "rejected_rows": 0,
            "coerced_null_rows": 0,
        },
        mappings=[{"source": "id", "target": "id"}],
        source_schema={"id": "INTEGER"},
        validation_mode="strict",
    )
    assert report.get("passed") is False
    assert report.get("unproven") is True
    assert "unmeasured" in (report.get("message") or "").lower()


def test_file_export_unmeasured_source_count_stays_operational_unproven():
    """File/object Gate-8 is already unproven — do not hard-fail the job."""
    endpoint = EndpointConfig(kind="file_export", format="csv", output_path="out.csv")
    report = run_reconciliation(
        endpoint=endpoint,
        records=[],
        columns=["id"],
        rows_written=10,
        writer_checksum="abc123",
        dest_summary={"rejected_rows": 0, "coerced_null_rows": 0},
        mappings=[{"source": "id", "target": "id"}],
        source_schema={"id": "INTEGER"},
        validation_mode="strict",
    )
    assert report.get("passed") is True
    assert report.get("unproven") is True
    assert report.get("migration_proven") is False


# ---------------------------------------------------------------------------
# P1 — Map invent refuses when existence unknown
# ---------------------------------------------------------------------------


def test_mapping_pipeline_refuses_invent_when_table_exists_unknown():
    # Named target exists in Studio list but catalog types + existence are
    # unknown — must not invent create-new widths (false-green cliff).
    result = run_mapping_pipeline(
        ["amount"],
        ["amount"],
        source_schemas=[
            {
                "name": "amount",
                "inferred_type": "DECIMAL(18,4)",
                "samples": ["1.2345"],
            }
        ],
        target_schemas=[],
        destination_db_type="postgresql",
        destination_table_exists=None,
        use_llm=False,
        prior_mappings=[
            {
                "source": "amount",
                "target": "amount",
                "confidence": 0.99,
                "target_type": "",
                "reasoning": "operator prior",
            }
        ],
    )
    assert result["mappings"]
    for m in result["mappings"]:
        assert not str(m.get("target_type") or "").strip(), m


# ---------------------------------------------------------------------------
# P1 — G3 passes dest_table_exists into is_lossy_coercion
# ---------------------------------------------------------------------------


def test_g3_existing_table_string_to_jsonb_is_not_false_collapse():
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            parseable=True,
            columns=[
                ColumnSchema(
                    name="payload",
                    inferred_type="TEXT",
                    samples=['{"a":1}'],
                )
            ],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            can_write=True,
            table_exists=True,
            target_columns=[
                ColumnSchema(name="payload", inferred_type="JSONB"),
            ],
        ),
        mappings=[
            ColumnMapping(
                source="payload",
                target="payload",
                confidence=0.99,
                target_type="JSONB",
            )
        ],
    )
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    # Existing-table string→JSONB is a load, not invent — must not hard-block.
    assert result.status != GateStatus.BLOCK, result.message


# ---------------------------------------------------------------------------
# P1 — structure certify downgrades emit-only PK claims
# ---------------------------------------------------------------------------


def test_certify_structure_downgrades_missing_primary_key():
    report = SchemaFidelityReport(
        items=[
            SchemaFidelityItem(
                aspect="primary_key",
                name="id",
                status="carried",
                reason="PRIMARY KEY emitted on CREATE TABLE.",
                dest_ddl="PRIMARY KEY (id)",
            ),
            SchemaFidelityItem(
                aspect="not_null",
                name="email",
                status="carried",
                reason="NOT NULL carried from source nullability.",
                dest_ddl="NOT NULL",
            ),
        ]
    )
    plan = CreateFidelityPlan(report=report)

    def fetchall(sql: str, params: tuple):
        sql_l = sql.lower()
        if "primary key" in sql_l or "is_primary_key" in sql_l:
            return []
        if "is_nullable" in sql_l or "nullable" in sql_l:
            return [("email", "YES")]
        return []

    certify_structure_on_destination(
        plan,
        dialect="postgresql",
        schema="public",
        table="t",
        fetchall=fetchall,
    )
    by_aspect = {i.aspect: i for i in plan.report.items}
    assert by_aspect["primary_key"].status == "unsupported"
    assert by_aspect["not_null"].status == "unsupported"


def test_certify_structure_against_live_sqlite_confirms_and_downgrades():
    """Real SQLite catalog re-read — the SQL templates must execute and settle.

    The destination table is created WITHOUT the claimed UNIQUE(email) and with
    a nullable ``email``, so a truthful certificate must downgrade those emit
    claims while confirming the PK that genuinely landed.
    """
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, status TEXT DEFAULT 'active')"
    )
    conn.commit()

    report = SchemaFidelityReport(
        items=[
            SchemaFidelityItem(
                aspect="primary_key", name="id", status="carried",
                reason="PRIMARY KEY emitted on CREATE TABLE.", dest_ddl="PRIMARY KEY (id)",
            ),
            SchemaFidelityItem(
                aspect="not_null", name="email", status="carried",
                reason="NOT NULL carried from source nullability.", dest_ddl="NOT NULL",
            ),
            SchemaFidelityItem(
                aspect="default", name="status", status="carried",
                reason="Safe DEFAULT literal carried.", dest_ddl="DEFAULT 'active'",
            ),
            SchemaFidelityItem(
                aspect="unique", name="email", status="carried",
                reason="UNIQUE constraint emitted on CREATE TABLE.", dest_ddl="UNIQUE (email)",
            ),
        ]
    )
    plan = CreateFidelityPlan(report=report)

    certify_structure_on_destination(
        plan,
        dialect="sqlite",
        schema="",
        table="t",
        fetchall=lambda sql, params: list(conn.execute(sql, params).fetchall()),
    )
    conn.close()

    by = {i.aspect: i for i in plan.report.items}
    # PK genuinely landed → stays carried; DEFAULT genuinely landed → carried.
    assert by["primary_key"].status == "carried"
    assert by["default"].status == "carried"
    # email is nullable on the real table and has no UNIQUE → both downgrade.
    assert by["not_null"].status == "unsupported"
    assert by["unique"].status == "unsupported"


def _structure_plan(*items):
    return CreateFidelityPlan(
        report=SchemaFidelityReport(items=[SchemaFidelityItem(**kw) for kw in items])
    )


def test_pk_verify_is_casefold_for_folding_dialects():
    """Oracle returns UPPERCASE catalog names; a PK that landed must not be
    falsely downgraded on case alone."""
    plan = _structure_plan(
        dict(aspect="primary_key", name="id", status="carried",
             reason="PK emitted.", dest_ddl="PRIMARY KEY (id)"),
    )

    def fetchall(sql, params):
        return [("ID",)]  # folded catalog identifier

    certify_structure_on_destination(
        plan, dialect="oracle", schema="APP", table="T", fetchall=fetchall
    )
    assert plan.report.items[0].status == "carried"


def test_pk_wider_live_key_is_not_certified():
    """Claimed id, catalog reports composite (id, tenant_id): the planned key
    shape did not land, so it must not stay carried."""
    plan = _structure_plan(
        dict(aspect="primary_key", name="id", status="carried",
             reason="PK emitted.", dest_ddl="PRIMARY KEY (id)"),
    )

    def fetchall(sql, params):
        return [("id",), ("tenant_id",)]

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    assert plan.report.items[0].status == "unsupported"


def test_composite_unique_matches_exactly_not_by_membership():
    """A single-column UNIQUE claim must NOT be certified merely because the
    column participates in a wider composite unique key."""
    plan = _structure_plan(
        dict(aspect="unique", name="email", status="carried",
             reason="UNIQUE emitted.", dest_ddl="UNIQUE (email)"),
        dict(aspect="unique", name="a,b", status="carried",
             reason="UNIQUE emitted.", dest_ddl="UNIQUE (a, b)"),
    )

    def fetchall(sql, params):
        # Only a composite UNIQUE(email, org) and a composite UNIQUE(a, b) exist.
        return [("uq_eo", "email"), ("uq_eo", "org"), ("uq_ab", "a"), ("uq_ab", "b")]

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    by_name = {i.name: i for i in plan.report.items}
    # email alone is only part of a wider composite → not the claimed key.
    assert by_name["email"].status == "unsupported"
    # (a, b) matches a real composite exactly → carried.
    assert by_name["a,b"].status == "carried"


def test_default_value_mismatch_is_not_certified():
    """A DEFAULT claim is only carried when the destination default MATCHES; a
    different literal (or engine default) must downgrade."""
    plan = _structure_plan(
        dict(aspect="default", name="status", status="carried",
             reason="DEFAULT emitted.", dest_ddl="DEFAULT 'active'"),
        dict(aspect="default", name="score", status="carried",
             reason="DEFAULT emitted.", dest_ddl="DEFAULT 0"),
    )

    def fetchall(sql, params):
        # status default differs ('inactive'); score matches (0 vs 0::int).
        return [("status", "'inactive'::text"), ("score", "0")]

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    by_name = {i.name: i for i in plan.report.items}
    assert by_name["status"].status == "unsupported"
    assert by_name["score"].status == "carried"


def test_default_cast_and_paren_equivalence_is_carried():
    """Postgres often stores 'active'::text; parens/cast noise must not cause a
    false downgrade of a faithfully-carried default."""
    plan = _structure_plan(
        dict(aspect="default", name="status", status="carried",
             reason="DEFAULT emitted.", dest_ddl="DEFAULT 'active'"),
        dict(aspect="default", name="created", status="carried",
             reason="DEFAULT emitted.", dest_ddl="DEFAULT CURRENT_TIMESTAMP"),
    )

    def fetchall(sql, params):
        return [("status", "('active'::text)"), ("created", "now()")]

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    assert all(i.status == "carried" for i in plan.report.items)


def test_default_nvarchar_prefix_and_clock_precision_equivalence():
    """SQL Server N'…' nvarchar literals, multi-word casts, and clock precision
    must not cause a false downgrade of a faithfully-carried default."""
    plan = _structure_plan(
        dict(aspect="default", name="status", status="carried",
             reason="DEFAULT emitted.", dest_ddl="DEFAULT 'active'"),
        dict(aspect="default", name="label", status="carried",
             reason="DEFAULT emitted.", dest_ddl="DEFAULT 'x'"),
        dict(aspect="default", name="created", status="carried",
             reason="DEFAULT emitted.", dest_ddl="DEFAULT CURRENT_TIMESTAMP"),
    )

    def fetchall(sql, params):
        return [
            ("status", "N'active'"),                 # SQL Server nvarchar literal
            ("label", "'x'::character varying"),      # PG multi-word cast
            ("created", "current_timestamp(6)"),      # clock with precision
        ]

    certify_structure_on_destination(
        plan, dialect="sqlserver", schema="dbo", table="t", fetchall=fetchall
    )
    assert all(i.status == "carried" for i in plan.report.items), [
        (i.name, i.status, i.reason) for i in plan.report.items
    ]


def test_certify_check_column_coverage_live_sqlite():
    """A carried CHECK is only certified when a live CHECK on the destination
    references its columns — proven against a real SQLite catalog (sqlite_master).

    The table has CHECK (age >= 0) but nothing on ``status``; the plan claims both
    were carried, so ``status`` must downgrade while ``age`` stays carried.
    """
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0), "
        "status TEXT)"
    )
    conn.commit()

    plan = _structure_plan(
        dict(aspect="check", name="ck_age", status="carried",
             reason="re-rendered", dest_ddl="CHECK (age >= 0)",
             source_detail="age >= 0"),
        dict(aspect="check", name="ck_status", status="carried",
             reason="re-rendered", dest_ddl="CHECK (status IN ('a','b'))",
             source_detail="status IN ('a','b')"),
    )
    plan.dest_columns = ["id", "age", "status"]

    certify_structure_on_destination(
        plan, dialect="sqlite", schema="", table="t",
        fetchall=lambda sql, params: list(conn.execute(sql, params).fetchall()),
    )
    conn.close()

    by = {i.name: i for i in plan.report.items}
    assert by["ck_age"].status == "carried"
    assert by["ck_status"].status == "unsupported"


def test_certify_check_downgrades_when_destination_has_none():
    plan = _structure_plan(
        dict(aspect="check", name="ck_age", status="carried",
             reason="re-rendered", dest_ddl="CHECK (age >= 0)",
             source_detail="age >= 0"),
    )
    plan.dest_columns = ["age"]

    def fetchall(sql, params):
        return []  # catalog read succeeded: table has no CHECK constraints

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    assert plan.report.items[0].status == "unsupported"


def test_certify_check_unknown_when_catalog_unreadable():
    plan = _structure_plan(
        dict(aspect="check", name="ck_age", status="carried",
             reason="re-rendered", dest_ddl="CHECK (age >= 0)",
             source_detail="age >= 0"),
    )
    plan.dest_columns = ["age"]

    def fetchall(sql, params):
        raise RuntimeError("catalog unreachable")

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    # Unreadable ≠ absent: an unverifiable CHECK is unknown, never a green claim.
    assert plan.report.items[0].status == "unknown"


def test_certify_check_survives_predicate_rewrite_by_column_coverage():
    """PG rewrites ``status IN (...)`` to ``= ANY (ARRAY[...])`` — the column name
    survives, so column-coverage keeps the CHECK certified, not falsely downgraded."""
    plan = _structure_plan(
        dict(aspect="check", name="ck_status", status="carried",
             reason="re-rendered", dest_ddl="CHECK (status IN ('a','b'))",
             source_detail="status IN ('a','b')"),
    )
    plan.dest_columns = ["status"]

    def fetchall(sql, params):
        return [("((status)::text = ANY ((ARRAY['a'::character varying, "
                 "'b'::character varying])::text[]))",)]

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    assert plan.report.items[0].status == "carried"


def test_check_mysql_probe_joins_table_constraints():
    """MySQL information_schema.CHECK_CONSTRAINTS has no table_name column — the
    probe must join table_constraints or it raises and forces every CHECK to
    unknown."""
    from services.schema_fidelity import _STRUCTURE_CHECK_QUERY

    q = _STRUCTURE_CHECK_QUERY["mysql"].lower()
    assert "table_constraints" in q
    assert "constraint_type = 'check'" in q


def test_check_unmatched_coverage_is_unknown_not_carried():
    """Live CHECKs exist but the carried predicate's columns cannot be resolved:
    coverage is unproven, so the item is unknown — never a green carried claim."""
    plan = _structure_plan(
        dict(aspect="check", name="ck", status="carried",
             reason="re-rendered", dest_ddl="CHECK (mystery_expr)",
             source_detail="mystery_expr"),
    )
    plan.dest_columns = ["id", "age"]  # predicate names none of these

    def fetchall(sql, params):
        return [("age > 0",)]  # a real CHECK exists, but not for our predicate

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    assert plan.report.items[0].status == "unknown"


def test_check_column_named_like_keyword_not_falsely_matched():
    """A destination column named ``text`` must not be 'covered' by a ``::text``
    cast token appearing in an unrelated live CHECK."""
    plan = _structure_plan(
        dict(aspect="check", name="ck_text", status="carried",
             reason="re-rendered", dest_ddl="CHECK (text > 0)",
             source_detail="text > 0"),
    )
    plan.dest_columns = ["text", "other"]

    def fetchall(sql, params):
        # Only a CHECK on `other`, whose rewrite mentions ::text (a cast, not col).
        return [("((other)::text <> ''::text)",)]

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    assert plan.report.items[0].status == "unsupported"


def test_pure_not_null_filter_keeps_compound_predicates():
    from services.schema_fidelity import _pure_not_null_clause

    assert _pure_not_null_clause("qty IS NOT NULL") is True
    assert _pure_not_null_clause('("qty") IS NOT NULL') is True
    # Compound predicate is a REAL check, not a NOT NULL echo.
    assert _pure_not_null_clause("qty IS NOT NULL AND qty > 0") is False
    assert _pure_not_null_clause("status IN ('a','b')") is False


def test_sqlite_check_scanner_skips_string_literals():
    from services.schema_fidelity import _extract_sqlite_checks

    ddl = (
        "CREATE TABLE t (id INTEGER, note TEXT DEFAULT 'check (1)', "
        "qty INTEGER CHECK (qty > 0))"
    )
    checks = _extract_sqlite_checks(ddl)
    assert checks == ["(qty > 0)"]

    # A literal that merely contains the word check must not invent a clause.
    ddl2 = "CREATE TABLE t (id INTEGER, label TEXT DEFAULT 'check (1)')"
    assert _extract_sqlite_checks(ddl2) == []


def test_predicate_normalizer_handles_parens_inside_string_literal():
    from services.physical_state_diff import _normalize_predicate

    # ``)`` inside a literal must not truncate the predicate via paren balancing.
    a = _normalize_predicate("status <> ')'")
    b = _normalize_predicate('("status") <> \')\'')
    assert a == b and a


def test_predicate_normalizer_preserves_distinct_string_values():
    """CHECK drift that differs only in the literal value must NOT normalize
    identically — otherwise a changed allow-list reads as carried."""
    from services.physical_state_diff import _normalize_predicate

    assert _normalize_predicate("status <> 'a'") != _normalize_predicate("status <> 'b'")
    assert (
        _normalize_predicate("status IN ('a','b')")
        != _normalize_predicate("status IN ('a','c')")
    )


def test_sqlite_check_scanner_handles_paren_inside_literal():
    from services.schema_fidelity import _extract_sqlite_checks

    ddl = "CREATE TABLE t (s TEXT CHECK (s <> ')'))"
    assert _extract_sqlite_checks(ddl) == ["(s <> ')')"]


def test_check_coverage_not_satisfied_by_literal_value():
    """A live CHECK on ``note`` whose literal is ``'age'`` must not certify a
    carried CHECK on the column ``age`` (identifier vs. string value)."""
    plan = _structure_plan(
        dict(aspect="check", name="ck_age", status="carried",
             reason="re-rendered", dest_ddl="CHECK (age >= 0)",
             source_detail="age >= 0"),
    )
    plan.dest_columns = ["age", "note"]

    def fetchall(sql, params):
        return [("(note <> 'age')",)]  # constrains note; 'age' is a literal value

    certify_structure_on_destination(
        plan, dialect="postgresql", schema="public", table="t", fetchall=fetchall
    )
    assert plan.report.items[0].status == "unsupported"
