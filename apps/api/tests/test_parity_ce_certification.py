"""The CE1 certification file, its generator and the docs table stay in step."""

from __future__ import annotations

from scripts.gen_parity_ce_certification import build, doc_table, main


def test_docs_table_matches_the_certification_file() -> None:
    assert main(["--check"]) == 0


def test_a_pair_is_certified_only_for_kinds_both_engines_rendered() -> None:
    record = [
        {"kind": "kind", "server": "postgresql16", "case": "integer", "ce_kind": "integer", "ddl": "bigint", "values": 9},
        {"kind": "kind", "server": "postgresql16", "case": "uuid", "ce_kind": "uuid", "ddl": "uuid", "values": 9},
        {"kind": "kind", "server": "sqlserver16", "case": "integer", "ce_kind": "integer", "ddl": "bigint", "values": 9},
        {"kind": "pair", "source_server": "postgresql16", "destination_server": "sqlserver16",
         "kinds": ["integer", "uuid"], "rows": 400},
    ]
    data = build(record, run_id="42", run_date="2026-11-21")
    pair = data["pairs"]["postgresql16->sqlserver16"]
    assert pair["kinds"] == ["integer"]  # uuid was never rendered on SQL Server
    assert (pair["source_family"], pair["destination_family"]) == ("postgresql", "sqlserver")
    assert "postgresql16 | sqlserver16 | integer" in doc_table(data)


def test_scan_evidence_records_certification_per_column() -> None:
    from services.parity_scale.certification import assess
    from services.parity_scale.plan import build_plan

    types = {"id": "bigint", "f": "double precision"}
    plan = build_plan([{"source": c, "target": c} for c in types], source_types=types, dest_types=types,
                      source_engine="postgresql", dest_engine="postgresql", key_columns=["id"])
    result = assess(plan, "postgresql16", "postgresql16")
    assert result["enforced"] is False
    by = {c["column"]: c for c in result["columns"]}
    assert by["f"]["certified"] is False  # a native rendering is never certified across runs
