"""Oracle '' into a NOT NULL text column is decided at Validate, never silently.

Owner decision (lead, 2026-11-21): Oracle stores ``''`` as NULL, so a NOT NULL
column refuses it (ORA-01400). Validate scans the source for empty strings;
when there are some, a blocking gate names the column and count. On a new
table the one approvable option is "Create <col> as NULL-able; empty strings
land as NULL", recorded as a signed Migration Risk Contract; the table is then
created NULL-able there, Gate-8 reports the rows as explained by the contract,
and the proof pack lists it. An existing NOT NULL column is never altered.

The live legs (PostgreSQL -> Oracle Database Free) are in
tests/test_oracle_story_live_retests.py and test_typed_fidelity_transfer_matrix_e2e.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services import oracle_empty_not_null as oe

_MAPS = [
    {"source": "id", "target": "id", "target_type": "BIGINT"},
    {"source": "note", "target": "note", "target_type": "TEXT"},
    {"source": "memo", "target": "memo", "target_type": "TEXT"},
    {"source": "code", "target": "code", "target_type": "TEXT", "transform": "trim"},
]
_NULLS = {"id": False, "note": False, "memo": True, "code": False}
_ROWS = [
    {"id": 1, "note": "", "memo": "", "code": "  "},
    {"id": 2, "note": "x", "memo": None, "code": "a"},
    {"id": 3, "note": "", "memo": "y", "code": "b"},
]


def _targets(**kw):
    args = dict(
        destination_db_type="oracle", mappings=_MAPS, column_nullability=_NULLS,
        destination_table_exists=False, destination_column_nullability={},
        key_columns=["id"],
    )
    args.update(kw)
    return oe.candidates(**args)


def test_only_not_null_text_columns_into_oracle_are_candidates():
    names = {t["source"]: t for t in _targets()}
    # memo is nullable in the source, id is not text.
    assert set(names) == {"note", "code"}
    assert names["note"]["kind"] == "create_new" and names["note"]["approvable"]
    assert _targets(destination_db_type="postgresql") == []
    existing = _targets(
        destination_table_exists=True,
        destination_column_nullability={"note": False, "memo": False, "code": True},
    )
    assert {t["source"]: t["kind"] for t in existing} == {"note": "existing", "memo": "existing"}
    assert not any(t["approvable"] for t in existing)


def test_the_scan_counts_the_written_value_after_the_transform():
    scan = oe.count_empty_strings(_ROWS, _targets())
    # note: two '' rows; code: trim("  ") is '' once.
    assert scan["counts"] == {"note->note": 2, "code->code": 1}
    assert scan["rows_scanned"] == 3 and scan["truncated"] == ""
    capped = oe.count_empty_strings(_ROWS, _targets(), max_rows=1)
    assert capped["truncated"] == "rows" and capped["rows_scanned"] == 1


def test_no_empty_string_means_no_gate():
    rows = [{"id": 1, "note": "a", "code": "b"}]
    assert oe.preflight_gate(
        destination_db_type="oracle", mappings=_MAPS, column_nullability=_NULLS,
        destination_table_exists=False, destination_column_nullability={},
        column_types={}, key_columns=["id"], sample_rows=[], walk=lambda cols: iter(rows),
    ) == (None, [])


def test_empty_strings_block_with_column_count_and_one_approvable_option():
    walked: list[list[str]] = []

    def walk(cols):
        walked.append(cols)
        return iter(_ROWS)

    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=_MAPS, column_nullability=_NULLS,
        destination_table_exists=False, destination_column_nullability={},
        column_types={}, key_columns=["id"], sample_rows=[], walk=walk,
    )
    assert walked == [["code", "note"]]  # only the candidates are projected
    assert gate["id"] == oe.GATE_ID and gate["status"] == "block"
    assert "note" in gate["message"] and "2 empty string(s)" in gate["message"]
    assert "ORA-01400" in gate["message"] and "every source row Execute will read" in gate["message"]
    (blocker,) = blockers
    option = next(a for a in blocker["suggested_actions"] if a["id"] == oe.OPTION)
    assert option["label"].startswith("Create note as NULL-able; empty strings land as NULL")
    assert option["risk_acceptance"]["option"] == oe.OPTION
    assert set(option["risk_acceptance"]["columns"]) == {"note", "code"}


def test_an_existing_not_null_column_blocks_with_no_option_to_alter():
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=_MAPS, column_nullability=_NULLS,
        destination_table_exists=True, destination_column_nullability={"note": False},
        column_types={}, key_columns=[], sample_rows=[], walk=lambda cols: iter(_ROWS),
    )
    assert gate["status"] == "block" and "existing NOT NULL column" in gate["message"]
    ids = [a["id"] for a in blockers[0]["suggested_actions"]]
    assert oe.OPTION not in ids
    assert any(a["id"] == "oracle_default_transform" for a in blockers[0]["suggested_actions"])
    # Approval does not unlock an existing column.
    signed = oe.sign_empty_as_null(
        [dict(m) for m in _MAPS], {"option": oe.OPTION, "columns": ["note"],
                                   "approved_by": "qa", "reason": "test"}, table="T",
    )
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=signed, column_nullability=_NULLS,
        destination_table_exists=True, destination_column_nullability={"note": False},
        column_types={}, key_columns=[], sample_rows=[], walk=lambda cols: iter(_ROWS),
    )
    assert gate["status"] == "block" and blockers


def test_a_key_column_cannot_be_approved_nullable():
    targets = _targets(key_columns=["note"])
    assert {t["source"]: t["approvable"] for t in targets}["note"] is False


def test_the_approval_signs_a_verifiable_contract_and_clears_the_gate():
    acceptance = {"option": oe.OPTION, "columns": ["note", "code"],
                  "approved_by": "qa@example.com", "reason": "legacy blanks are absent values"}
    signed = oe.sign_empty_as_null([dict(m) for m in _MAPS], acceptance, table="ORDERS")
    by = {m["source"]: m for m in signed}
    contract = oe.approved_contract(by["note"])
    assert contract and contract["loss_classification"] == oe.RISK_KIND
    assert contract["approved_by"] == "qa@example.com" and contract["table"] == "ORDERS"
    assert contract["execution_policy"] == "FAIL_JOB"  # nothing else is relaxed
    assert "risk_contract" not in by["note"]  # no cast/quarantine contract is minted
    assert oe.approved_contract(by["memo"]) is None
    gate, blockers = oe.preflight_gate(
        destination_db_type="oracle", mappings=signed, column_nullability=_NULLS,
        destination_table_exists=False, destination_column_nullability={},
        column_types={}, key_columns=["id"], sample_rows=[], walk=lambda cols: iter(_ROWS),
    )
    assert blockers == [] and gate["status"] == "pass"
    assert {a["source"]: a["count"] for a in gate["details"]["approved"]} == {"note": 2, "code": 1}
    # A tampered contract is not an approval.
    by["note"][oe.CONTRACT_KEY] = {**by["note"][oe.CONTRACT_KEY], "approved_by": "someone else"}
    assert oe.approved_contract(by["note"]) is None


@pytest.mark.parametrize("bad", [
    {"option": oe.OPTION, "columns": ["note"], "reason": "r"},
    {"option": oe.OPTION, "columns": [], "approved_by": "a", "reason": "r"},
    {"option": oe.OPTION, "columns": ["nope"], "approved_by": "a", "reason": "r"},
])
def test_an_incomplete_or_unmapped_approval_signs_nothing(bad):
    with pytest.raises(ValueError, match="Nothing was signed"):
        oe.sign_empty_as_null([dict(m) for m in _MAPS], bad, table="T")


def test_the_route_signing_entry_point_takes_the_option():
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    out = _sign_required_risk_contracts(
        [dict(m) for m in _MAPS],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"},
        table="T",
    )
    assert oe.approved_contract(next(m for m in out if m["source"] == "note"))


def test_create_new_drops_not_null_only_on_the_approved_column():
    from services.schema_fidelity import resolve_create_fidelity_plan

    catalog = {
        "dialect": "postgresql",
        "columns": ["id", "note", "code"],
        "primary_key": ["id"],
        "nullable": {"id": False, "note": False, "code": False},
    }
    signed = oe.sign_empty_as_null(
        [dict(m) for m in _MAPS if m["source"] != "memo"],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"},
        table="T",
    )
    plan = resolve_create_fidelity_plan(
        source_schema_catalog=catalog, mappings=signed,
        target_columns=["id", "note", "code"], target_types=["NUMBER(19)", "CLOB", "CLOB"],
        dest_dialect="oracle",
    )
    assert "note" not in plan.not_null_columns
    assert "NOT NULL" not in (plan.column_suffixes.get("note") or [])
    assert "code" in plan.not_null_columns and "id" in plan.not_null_columns
    relaxed = [i for i in plan.report.items if i.status == "relaxed_by_contract"]
    assert [i.name for i in relaxed] == ["note"]
    # Not on another engine: '' is a value there.
    pg = resolve_create_fidelity_plan(
        source_schema_catalog=catalog, mappings=signed,
        target_columns=["id", "note", "code"], target_types=["BIGINT", "TEXT", "TEXT"],
        dest_dialect="postgresql",
    )
    assert "note" in pg.not_null_columns


def test_gate8_names_the_explained_rows_without_flipping_its_verdict():
    signed = oe.sign_empty_as_null(
        [dict(m) for m in _MAPS],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"},
        table="T",
    )
    recon = {"passed": True, "message": "Row fidelity verified."}
    out = oe.explain_empty_as_null(
        recon, endpoint=SimpleNamespace(format="oracle"), mappings=signed, records=_ROWS,
    )
    (explained,) = out["explained_changes"]
    assert explained["column"] == "note" and explained["rows"] == 2
    assert explained["kind"] == oe.RISK_KIND and explained["risk_id"].startswith("mrc-")
    assert out["passed"] is True
    assert "explained by signed Risk Contract" in out["message"]
    # Other engines and unapproved runs are untouched.
    assert oe.explain_empty_as_null(
        recon, endpoint=SimpleNamespace(format="postgresql"), mappings=signed, records=_ROWS,
    ) is recon
    assert oe.explain_empty_as_null(
        recon, endpoint=SimpleNamespace(format="oracle"), mappings=_MAPS, records=_ROWS,
    ) is recon


def test_the_proof_pack_lists_the_contract_among_accepted_risks():
    from services.signed_proof_pack import collect_accepted_risks_from_job

    signed = oe.sign_empty_as_null(
        [dict(m) for m in _MAPS],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"},
        table="T",
    )
    risks = collect_accepted_risks_from_job({"mappings": signed})
    assert [r["loss_classification"] for r in risks] == [oe.RISK_KIND]


def test_gate8_counts_at_the_destination_when_the_run_holds_no_records(monkeypatch):
    signed = oe.sign_empty_as_null(
        [dict(m) for m in _MAPS],
        {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"},
        table="T",
    )
    asked: list[tuple] = []

    def fake(endpoint, table, columns, scope=None):
        asked.append((table, columns, getattr(scope, "kind", None)))
        return {"note": 5}

    monkeypatch.setattr(oe, "destination_null_rows", fake)
    out = oe.explain_empty_as_null(
        {"passed": True, "message": ""}, endpoint=SimpleNamespace(format="oracle", table="T"),
        mappings=signed, records=None,
        dest_summary={"table": "SCH.T", "sync_mode": "full_refresh_overwrite"},
    )
    assert asked == [("SCH.T", ["note"], "table_written_by_run")]
    (explained,) = out["explained_changes"]
    assert explained["rows"] == 5 and explained["counted_by"] == "NULL cells in the rows this run wrote"
    monkeypatch.setattr(oe, "destination_null_rows", lambda *a, **k: None)
    out = oe.explain_empty_as_null(
        {"passed": True, "message": ""}, endpoint=SimpleNamespace(format="oracle", table="T"),
        mappings=signed, records=None, dest_summary={"sync_mode": "full_refresh_overwrite"},
    )
    assert "explained_changes" not in out and out["passed"] is True
    assert out["unscoped_null_cells"][0]["table_wide_null_cells"] is None


def test_the_destination_null_count_reads_the_table_through_the_writers_engine(tmp_path, monkeypatch):
    import sqlite3

    from src.transfer.models import EndpointConfig

    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "dest.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INT, NOTE TEXT, code VARCHAR(5))")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(1, None, "a"), (2, "x", None), (3, None, "b")])
    conn.commit()
    conn.close()
    endpoint = EndpointConfig(kind="database", format="sqlite", database=str(db), table="t")
    assert oe.destination_null_rows(endpoint, "t", ["note", "code"]) == {"note": 2, "code": 1}
    assert oe.destination_null_rows(endpoint, "missing", ["note"]) is None


def _validate(mappings, rows, **kw):
    from services.preflight_service import run_file_preflight

    args = dict(
        columns=["id", "note"],
        column_types={"id": "INTEGER", "note": "TEXT"},
        row_count=len(rows),
        mappings=mappings,
        column_nullability={"id": False, "note": False},
        destination_connected=True,
        sample_rows=rows,
        population_rows=rows,
        rows_are_population=True,
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type="oracle",
        source_kind="database",
        source_format="postgresql",
        sync_mode="full_refresh_overwrite",
    )
    args.update(kw)
    return run_file_preflight(**args)


def _ids(result, bucket):
    return [b.get("id") or b.get("gate") for b in result.get(bucket) or []]


def test_validate_blocks_the_oracle_run_and_the_signed_option_clears_it():
    maps = [
        {"source": "id", "target": "id", "confidence": 0.99, "target_type": "INTEGER"},
        {"source": "note", "target": "note", "confidence": 0.99, "target_type": "TEXT"},
    ]
    rows = [{"id": 1, "note": ""}, {"id": 2, "note": "x"}]
    blocked = _validate(maps, rows)
    assert blocked["passed"] is False
    assert oe.GATE_ID in _ids(blocked, "blockers")
    gate = next(g for g in blocked["gates"] if g.get("id") == oe.GATE_ID)
    assert gate["status"] == "block" and "1 empty string(s)" in gate["message"]

    signed = oe.sign_empty_as_null(
        maps, {"option": oe.OPTION, "columns": ["note"], "approved_by": "qa", "reason": "r"},
        table="T",
    )
    cleared = _validate(signed, rows)
    assert oe.GATE_ID not in _ids(cleared, "blockers")
    gate = next(g for g in cleared["gates"] if g.get("id") == oe.GATE_ID)
    assert gate["status"] == "pass"
    # No empty string, no gate at all; and never on another engine.
    clean = _validate(maps, [{"id": 1, "note": "a"}])
    assert all(g.get("id") != oe.GATE_ID for g in clean["gates"])
    pg = _validate(maps, rows, destination_db_type="postgresql")
    assert all(g.get("id") != oe.GATE_ID for g in pg["gates"])


def test_the_refusal_carries_the_exact_approval_and_the_alternative():
    gate, _ = oe.preflight_gate(
        destination_db_type="oracle", mappings=_MAPS, column_nullability=_NULLS,
        destination_table_exists=False, destination_column_nullability={},
        column_types={}, key_columns=["id", "code"], sample_rows=[], walk=lambda cols: iter(_ROWS),
    )
    fix = gate["details"]["recommended_fix"]
    assert '"option": "oracle_empty_as_null", "columns": ["note"]' in fix
    assert "code (key column) would receive 1 empty string(s)" in gate["message"]
    assert "default:<value> on code" in fix
