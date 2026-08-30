"""Create-new carriers sized by the peek must not quarantine the population.

The reported failure: ``flights-1m.csv → Snowflake EMPLOYEE_DB.ert`` created
``DEP_TIME NUMBER(11,8)`` from the peeked rows, then aborted the 1M load with
zero rows committed because rows 2,868 / 2,880 / 3,954 hold ``0.016666668``
(scale 9). The destination table did not exist — nothing forced that width but
our own sample. Widen it from the measured population and re-prove on the same
rows, or leave the block standing.
"""

from __future__ import annotations

from typing import Any

from services.population_fit_scan import (
    EVIDENCE_PARTIAL,
    build_population_fit_gate,
    create_new_population_widen,
    scan_population_fit,
)

# Scale <= 7 in every peeked row; the scale-9 tail arrives past the peek.
_PEEKED = ["0.5416667", "0.25", "0.7083333", "0.0208333", "0.9166667"]
_TAIL = ["0.016666668", "0.033333335", "0.083333336"]
_TAIL_ROWS = {2868, 2880, 3954}


def _population(total: int = 20000) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for i in range(total):
        value = _TAIL[i % 3] if i in _TAIL_ROWS else _PEEKED[i % len(_PEEKED)]
        rows.append({"DEP_TIME": value, "ID": str(i)})
    return rows


def _mappings(target_type: str) -> list[dict[str, Any]]:
    return [
        {
            "source": "DEP_TIME",
            "target": "DEP_TIME",
            "target_type": target_type,
            "confidence": 0.99,
        },
        {"source": "ID", "target": "ID", "target_type": "INTEGER", "confidence": 0.99},
    ]


def _scan_kwargs(**over: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = dict(
        dest_types={},
        source_types={},
        dest_db="snowflake",
        dialect_label="snowflake",
        job_error_policy="fail",
        rows_are_population=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_overwrite",
        dest_table_exists=False,
    )
    kwargs.update(over)
    return kwargs


def _scan(rows: list[dict[str, str]], mappings: list[dict[str, Any]], **over: Any):
    kwargs = _scan_kwargs(**over)
    return scan_population_fit(iter(rows), mappings, rows_total=len(rows), **kwargs)


def test_peek_sized_create_new_blocks_before_the_write() -> None:
    """Validate names the failure Run would hit — never a green then an abort."""
    rows = _population()
    report = _scan(rows, _mappings("DECIMAL(7,7)"))
    gate = build_population_fit_gate(report)

    assert report.evidence == "exact"
    assert gate["status"] == "block"
    assert report.unfit_rows == len(_TAIL_ROWS)


def test_create_new_widens_from_population_and_re_proves() -> None:
    """The scale-9 tail resizes the CREATE instead of quarantining six rows."""
    rows = _population()
    mappings = _mappings("DECIMAL(7,7)")
    report = _scan(rows, mappings)

    widen = create_new_population_widen(
        report,
        mappings,
        lambda: iter(rows),
        scan_kwargs=_scan_kwargs(rows_total=len(rows)),
    )

    assert widen is not None
    assert build_population_fit_gate(widen.report)["status"] == "pass"
    assert not widen.report.findings
    applied = {a["column"]: a["to"] for a in widen.applied}
    assert applied["DEP_TIME"] != "DECIMAL(7,7)"
    # The CREATE reads the mapping, so the widen has to land on it.
    assert mappings[0]["target_type"] == applied["DEP_TIME"]
    assert mappings[0]["dest_type"] == applied["DEP_TIME"]
    # And the widened carrier really holds the values that failed.
    after = _scan(rows, mappings)
    assert not after.findings


def test_unreplayable_population_leaves_the_block_standing() -> None:
    """An exhausted iterator would 'prove' a clean scan over zero rows."""
    rows = _population()
    mappings = _mappings("DECIMAL(7,7)")
    report = _scan(rows, mappings)

    widen = create_new_population_widen(
        report, mappings, lambda: None, scan_kwargs=_scan_kwargs()
    )

    assert widen is None
    assert mappings[0]["target_type"] == "DECIMAL(7,7)"


def test_partial_evidence_never_widens() -> None:
    """A sampled scan cannot size a carrier for rows it never read."""
    rows = _population()
    mappings = _mappings("DECIMAL(7,7)")
    report = _scan(rows, mappings)
    partial = type(report)(
        evidence=EVIDENCE_PARTIAL,
        rows_scanned=report.rows_scanned,
        rows_total=report.rows_total,
        targets=report.targets,
        findings=report.findings,
    )

    widen = create_new_population_widen(
        partial, mappings, lambda: iter(rows), scan_kwargs=_scan_kwargs()
    )

    assert widen is None


def test_live_destination_ddl_is_not_rewritten_by_a_map_type() -> None:
    """Map cannot ALTER an existing Snowflake column — the block must stand.

    Append writes into the live object, so the live carrier binds no matter
    what Map says. (Overwrite is different: it drops and recreates, so the
    mapping type is the one the CREATE will use.)
    """
    rows = _population()
    mappings = _mappings("NUMBER(11,8)")
    report = _scan(
        rows,
        mappings,
        dest_types={"DEP_TIME": "NUMBER(11,8)", "ID": "NUMBER(38,0)"},
        dest_table_exists=True,
        sync_mode="full_refresh_append",
    )
    assert report.findings

    widen = create_new_population_widen(
        report,
        mappings,
        lambda: iter(rows),
        scan_kwargs=_scan_kwargs(
            dest_types={"DEP_TIME": "NUMBER(11,8)", "ID": "NUMBER(38,0)"},
            dest_table_exists=True,
            sync_mode="full_refresh_append",
            rows_total=len(rows),
        ),
    )

    assert widen is None
    assert mappings[0]["target_type"] == "NUMBER(11,8)"
