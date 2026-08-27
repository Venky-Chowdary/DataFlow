"""Validate must name a bounded-carrier defect that Execute would hit.

The production run these cover: 1M CSV rows, ``DECIMAL(12,9) → NUMBER(11,8)``,
Validate green on 25 preview rows, Run failed with 0 rows committed. Preflight
now scans the rows the caller actually holds and states the evidence, and the
file Execute path hands it a fresh read-only pass over the same bytes the writer
is about to stream.
"""

from __future__ import annotations

import csv
import io

from services.population_fit_scan import GATE_ID
from services.preflight_service import run_file_preflight

MAPPINGS = [
    {
        "source": "arr_time",
        "target": "arr_time",
        "confidence": 0.93,
        "target_type": "NUMBER(11,8)",
    }
]
COLUMN_TYPES = {"arr_time": "DECIMAL(12,9)"}
DEST_TYPES = {"arr_time": "NUMBER(11,8)"}


def _rows(count: int, *, unfit_at: tuple[int, ...] = ()) -> list[dict[str, str]]:
    return [
        {"arr_time": "9999.99999999" if i in unfit_at else "12.34567890"}
        for i in range(1, count + 1)
    ]


def _preflight(**kw):
    params = dict(
        columns=["arr_time"],
        column_types=COLUMN_TYPES,
        row_count=1_000,
        mappings=MAPPINGS,
        destination_connected=True,
        destination_column_types=DEST_TYPES,
        destination_db_type="snowflake",
        estimated_bytes=4096,
    )
    params.update(kw)
    return run_file_preflight(**params)


def _gate(result: dict) -> dict:
    gates = [g for g in result.get("gates") or [] if g.get("id") == GATE_ID]
    assert gates, "the population fit gate must always be stated"
    return gates[0]


def test_preview_only_validate_warns_and_never_claims_population_fit() -> None:
    rows = _rows(1_000, unfit_at=(431,))
    result = _preflight(sample_rows=rows[:25])

    gate = _gate(result)
    assert gate["status"] == "warn"
    assert result["population_fit"]["evidence"] == "sampled"
    assert result["population_fit"]["scanned_population"] is False
    assert not any(b.get("id") == GATE_ID for b in result["blockers"])


def test_population_rows_block_validate_with_the_offending_row_numbers() -> None:
    rows = _rows(1_000, unfit_at=(431, 433))
    result = _preflight(
        sample_rows=rows[:25],
        population_rows=rows,
        rows_are_population=True,
    )

    blocker = next(b for b in result["blockers"] if b.get("id") == GATE_ID)
    assert result["passed"] is False
    assert _gate(result)["status"] == "block"
    assert result["population_fit"]["evidence"] == "exact"
    assert result["population_fit"]["rows_scanned"] == 1_000
    assert blocker["details"]["findings"][0]["example_rows"] == [431, 433]


def test_clean_population_does_not_block_and_reports_exact_evidence() -> None:
    rows = _rows(500)
    result = _preflight(
        row_count=500,
        sample_rows=rows[:25],
        population_rows=rows,
        rows_are_population=True,
    )

    assert _gate(result)["status"] == "pass"
    assert result["population_fit"]["evidence"] == "exact"
    assert not any(b.get("id") == GATE_ID for b in result["blockers"])


def test_a_generator_of_population_rows_is_accepted_and_consumed_once() -> None:
    """The file Execute path streams rows; preflight must not need a list."""
    rows = _rows(300, unfit_at=(299,))
    result = _preflight(
        row_count=300,
        sample_rows=rows[:25],
        population_rows=(r for r in rows),
        rows_are_population=True,
    )

    assert result["passed"] is False
    assert result["population_fit"]["rows_scanned"] == 300
    assert result["population_fit"]["findings"][0]["example_rows"] == [299]


def test_widening_declaration_keeps_the_gate_passing_without_rows() -> None:
    """Warehouse DDL can skip the scan. File preflight defaults to source_kind=file,
    so an inferred DECIMAL(11,8) is not a declared domain — the sample is scanned.
    """
    result = _preflight(
        source_kind="database",
        source_format="snowflake",
        column_types={"arr_time": "DECIMAL(11,8)"},
        sample_rows=_rows(5),
    )
    gate = _gate(result)

    assert gate["status"] == "pass"
    assert "no value scan required" in gate["message"]
    assert result["population_fit"]["safe_by_declaration"] == ["arr_time"]


def test_file_inferred_matching_typmod_still_scans_float32_clock_residue() -> None:
    """flights-1m.csv class: peek inferred NUMBER(9,6), dest is NUMBER(9,6).

    Treating that as a declared domain skipped the population scan. Row 293
    ``7.9166665`` (float32 of 7+55/60) then failed at Snowflake write.
    """
    rows = [{"arr_time": "12.345678"} for _ in range(292)]
    rows.append({"arr_time": "7.9166665"})
    result = _preflight(
        source_kind="file",
        source_format="csv",
        column_types={"arr_time": "NUMBER(9,6)"},
        destination_column_types={"arr_time": "NUMBER(9,6)"},
        mappings=[
            {
                "source": "arr_time",
                "target": "arr_time",
                "confidence": 0.93,
                "target_type": "NUMBER(9,6)",
            }
        ],
        row_count=len(rows),
        sample_rows=rows[:25],
        population_rows=rows,
        rows_are_population=True,
    )

    assert result["passed"] is False
    assert result["population_fit"]["safe_by_declaration"] == []
    finding = result["population_fit"]["findings"][0]
    assert finding["example_rows"] == [293]
    assert finding["example_values"] == ["7.9166665"]
    assert finding["suggested_target_type"] == "NUMBER(10,7)"
    assert "NUMBER(10,7)" in (finding.get("suggested_fix") or "")
    assert _gate(result)["status"] == "block"
    kernel = result["validation_findings"]
    assert kernel, "population-fit overflow must light Validate Remap"
    assert kernel[0]["suggested_target_type"] == "NUMBER(10,7)"
    assert kernel[0]["failure_class"] == "OVERFLOW"
    assert kernel[0]["row_number"] == 293
    assert "g3f_population_fit" in kernel[0]["gate_ids"]
    proof_findings = (result.get("proof_bundle") or {}).get("validation_findings") or []
    assert proof_findings and proof_findings[0]["suggested_target_type"] == "NUMBER(10,7)"


def test_existing_dest_stays_blocked_after_map_type_remap() -> None:
    """Remap on an existing table must not green Validate.

    Operator clicked Remap → mapping.target_type NUMBER(10,7). Live Snowflake
    is still NUMBER(9,6). Execute binds live DDL. Validate used to pass and
    Run failed again — the errors-every-Run loop.
    """
    rows = [{"arr_time": "12.345678"} for _ in range(292)]
    rows.append({"arr_time": "7.9166665"})
    result = _preflight(
        source_kind="file",
        source_format="csv",
        column_types={"arr_time": "NUMBER(9,6)"},
        destination_column_types={"arr_time": "NUMBER(9,6)"},
        destination_table_exists=True,
        sync_mode="full_refresh_append",
        mappings=[
            {
                "source": "arr_time",
                "target": "arr_time",
                "confidence": 0.93,
                "target_type": "NUMBER(10,7)",
            }
        ],
        row_count=len(rows),
        sample_rows=rows[:25],
        population_rows=rows,
        rows_are_population=True,
    )

    assert result["passed"] is False
    finding = result["population_fit"]["findings"][0]
    assert finding["target_type"] == "NUMBER(9,6)"
    assert finding["binds_live_ddl"] is True
    assert finding["suggested_target_type"] == "NUMBER(10,7)"
    assert "does not ALTER" in (finding.get("suggested_fix") or "")
    assert _gate(result)["status"] == "block"
    kernel = result["validation_findings"]
    assert kernel and kernel[0]["failure_class"] == "OVERFLOW"
    assert kernel[0]["suggested_target_type"] == "NUMBER(10,7)"


def test_csv_bytes_past_preview_stamp_kernel_widen() -> None:
    """A real CSV of 293 rows: last cell ``7.9166665`` vs dest NUMBER(9,6).

    Peek inference would have typed the column NUMBER(9,6). The 25-row
    preview is clean. Kernel findings must still name NUMBER(10,7).
    """
    from transfer.file_stream import iter_source_rows

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["DEP_TIME"])
    writer.writeheader()
    for i in range(1, 294):
        writer.writerow({"DEP_TIME": "7.9166665" if i == 293 else "12.345678"})
    content = buf.getvalue().encode("utf-8")

    result = _preflight(
        columns=["DEP_TIME"],
        source_kind="file",
        source_format="csv",
        column_types={"DEP_TIME": "NUMBER(9,6)"},
        destination_column_types={"DEP_TIME": "NUMBER(9,6)"},
        mappings=[
            {
                "source": "DEP_TIME",
                "target": "DEP_TIME",
                "confidence": 0.93,
                "target_type": "NUMBER(9,6)",
            }
        ],
        row_count=293,
        sample_rows=[{"DEP_TIME": "12.345678"} for _ in range(25)],
        population_rows=iter_source_rows(content, "flights-clock.csv"),
        rows_are_population=True,
    )

    assert result["passed"] is False
    finding = result["validation_findings"][0]
    assert finding["suggested_target_type"] == "NUMBER(10,7)"
    assert finding["failure_class"] == "OVERFLOW"
    assert finding["row_number"] == 293
    assert "truncate" in (finding.get("recommended_action") or "").lower()


def test_file_row_iterator_replays_every_row_of_a_csv() -> None:
    """``iter_source_rows`` is the pre-write pass the file engine hands preflight."""
    from transfer.file_stream import iter_source_rows

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["arr_time"])
    writer.writeheader()
    for row in _rows(2_000, unfit_at=(1_999,)):
        writer.writerow(row)
    content = buf.getvalue().encode("utf-8")

    seen = list(iter_source_rows(content, "flights.csv", batch_size=250))
    assert len(seen) == 2_000
    assert seen[1_998]["arr_time"] == "9999.99999999"

    # Read-only: a second pass sees the same rows, so the writer's own stream is
    # untouched by the scan.
    assert len(list(iter_source_rows(content, "flights.csv"))) == 2_000


def test_file_iterator_feeds_preflight_end_to_end() -> None:
    from transfer.file_stream import iter_source_rows

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["arr_time"])
    writer.writeheader()
    for row in _rows(5_000, unfit_at=(4_812,)):
        writer.writerow(row)
    content = buf.getvalue().encode("utf-8")

    result = _preflight(
        row_count=5_000,
        sample_rows=_rows(25),
        population_rows=iter_source_rows(content, "flights.csv"),
        rows_are_population=True,
    )

    assert result["passed"] is False
    assert result["population_fit"]["evidence"] == "exact"
    assert result["population_fit"]["findings"][0]["example_rows"] == [4_812]


def test_source_file_id_scans_stored_upload_past_preview(tmp_path, monkeypatch) -> None:
    """Studio Validate posts 25 preview rows. The stored upload is the population.

    flights-1m class: peek-inferred NUMBER(9,6), last cell 7.9166665 at row 293.
    Without file_id the gate would warn on a clean preview and Execute would
    fail-closed at write.
    """
    from services import file_parser as fp

    monkeypatch.setattr(fp, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(fp, "REGISTRY_PATH", tmp_path / "upload_registry.json")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["DEP_TIME"])
    writer.writeheader()
    for i in range(1, 294):
        writer.writerow({"DEP_TIME": "7.9166665" if i == 293 else "12.345678"})
    record = fp.store_upload("flights-clock.csv", buf.getvalue().encode("utf-8"))

    result = _preflight(
        columns=["DEP_TIME"],
        source_kind="file",
        source_format="csv",
        column_types={"DEP_TIME": "NUMBER(9,6)"},
        destination_column_types={"DEP_TIME": "NUMBER(9,6)"},
        mappings=[
            {
                "source": "DEP_TIME",
                "target": "DEP_TIME",
                "confidence": 0.93,
                "target_type": "NUMBER(9,6)",
            }
        ],
        row_count=293,
        sample_rows=[{"DEP_TIME": "12.345678"} for _ in range(25)],
        source_file_id=record["file_id"],
    )

    assert result["passed"] is False
    assert result["population_fit"]["evidence"] == "exact"
    assert result["population_fit"]["scanned_population"] is True
    assert result["population_fit"]["rows_scanned"] == 293
    kernel = result["validation_findings"]
    assert kernel, "stored-file scan must light Validate Remap"
    assert kernel[0]["suggested_target_type"] == "NUMBER(10,7)"
    assert kernel[0]["failure_class"] == "OVERFLOW"
    assert kernel[0]["row_number"] == 293


def test_unknown_source_file_id_stays_sampled_and_does_not_claim_fit() -> None:
    result = _preflight(
        sample_rows=_rows(25),
        source_file_id="does-not-exist",
    )
    gate = _gate(result)
    assert gate["status"] == "warn"
    assert result["population_fit"]["evidence"] == "sampled"
    assert result["population_fit"]["scanned_population"] is False


def test_plan_validate_scans_source_file_id(tmp_path, monkeypatch) -> None:
    """The primary Studio path is plan preflight, not POST /preflight/run."""
    from services import file_parser as fp
    from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
    from services.transfer_plan_store import create_plan

    monkeypatch.setattr(fp, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(fp, "REGISTRY_PATH", tmp_path / "upload_registry.json")
    monkeypatch.setattr(
        "services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json"
    )
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["DEP_TIME"])
    writer.writeheader()
    for i in range(1, 294):
        writer.writerow({"DEP_TIME": "7.9166665" if i == 293 else "12.345678"})
    record = fp.store_upload("flights-clock.csv", buf.getvalue().encode("utf-8"))

    def _inspect(**_kw):
        return {
            "connected": True,
            "table_exists": True,
            "can_create_table": True,
            "can_write": True,
            "db_type": "snowflake",
            "column_types": {"DEP_TIME": "NUMBER(9,6)"},
            "message": "ok",
        }

    monkeypatch.setattr(
        "src.services.preflight_service.inspect_destination_for_preflight",
        _inspect,
    )
    monkeypatch.setattr(
        "services.preflight_service.inspect_destination_for_preflight",
        _inspect,
    )

    plan = create_plan(
        {
            "name": "flights-clock",
            "source": {
                "kind": "file",
                "format": "csv",
                "file_id": record["file_id"],
                "filename": "flights-clock.csv",
            },
            "destination": {
                "kind": "database",
                "format": "snowflake",
                "table": "TREE",
            },
            "source_columns": ["DEP_TIME"],
            "source_schema": {"DEP_TIME": "NUMBER(9,6)"},
            "target_columns": ["DEP_TIME"],
            "target_schema": {"DEP_TIME": "NUMBER(9,6)"},
            "row_count_estimate": 293,
            "sample_rows": [{"DEP_TIME": "12.345678"} for _ in range(25)],
            "policies": {
                "validation_mode": "strict",
                "sync_mode": "full_refresh_overwrite",
                "schema_policy": "manual_review",
            },
        }
    )
    sync_plan_mappings(
        plan.id,
        [
            {
                "source": "DEP_TIME",
                "target": "DEP_TIME",
                "confidence": 0.93,
                "target_type": "NUMBER(9,6)",
            }
        ],
    )

    result = run_plan_preflight(plan.id)
    assert result["passed"] is False
    assert result["population_fit"]["evidence"] == "exact"
    kernel = result["validation_findings"]
    assert kernel[0]["suggested_target_type"] == "NUMBER(10,7)"
    assert kernel[0]["row_number"] == 293


def test_stored_file_integer_and_varchar_overflows_stamp_dest_widen(
    tmp_path, monkeypatch
) -> None:
    """One algorithm, three carriers — integer and VARCHAR must name a dest widen."""
    from services import file_parser as fp

    monkeypatch.setattr(fp, "UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(fp, "REGISTRY_PATH", tmp_path / "upload_registry.json")

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["qty", "code"])
    writer.writeheader()
    for i in range(1, 40):
        writer.writerow(
            {
                "qty": "99999999999" if i == 39 else "12",
                "code": ("X" * 20) if i == 39 else "ok",
            }
        )
    record = fp.store_upload("fit-int-vc.csv", buf.getvalue().encode("utf-8"))

    result = _preflight(
        columns=["qty", "code"],
        source_kind="file",
        source_format="csv",
        column_types={"qty": "INTEGER", "code": "VARCHAR(8)"},
        destination_column_types={"qty": "INTEGER", "code": "VARCHAR(8)"},
        destination_db_type="postgresql",
        mappings=[
            {
                "source": "qty",
                "target": "qty",
                "confidence": 0.9,
                "target_type": "INTEGER",
            },
            {
                "source": "code",
                "target": "code",
                "confidence": 0.9,
                "target_type": "VARCHAR(8)",
            },
        ],
        row_count=39,
        sample_rows=[{"qty": "12", "code": "ok"} for _ in range(25)],
        source_file_id=record["file_id"],
    )

    assert result["passed"] is False
    suggested = {
        f["source_column"]: f["suggested_target_type"]
        for f in result["validation_findings"]
    }
    classes = {
        f["source_column"]: f["failure_class"] for f in result["validation_findings"]
    }
    assert "BIGINT" in (suggested.get("qty") or "").upper()
    assert classes.get("qty") == "OVERFLOW"
    assert "VARCHAR" in (suggested.get("code") or "").upper()
    assert classes.get("code") == "LENGTH_OVERFLOW"
