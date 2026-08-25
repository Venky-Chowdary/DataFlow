"""Number locale contract — fail closed on 1,234 / 1.234 unless US or EU is set.

Currency marks and both-separator forms still parse without a contract.
A naive US default would silently rewrite EU ``1,234`` as 1234.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.transform_engine import (  # noqa: E402
    ambiguous_number_columns,
    apply_transform,
    infer_number_locale,
    reset_active_number_locale,
    set_active_number_locale,
)


def _sqlite_amount(monkeypatch, number_locale: str, amount: str) -> tuple[object, list]:
    import sqlite3
    import tempfile

    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    class _FakeMongo:
        def __init__(self):
            self.jobs: dict[str, dict] = {}

        def get_job(self, job_id: str) -> dict | None:
            return self.jobs.get(job_id)

        def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
            self.jobs.setdefault(job_id, {})
            self.jobs[job_id].update(kwargs)
            self.jobs[job_id]["status"] = status
            return True

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "nloc.db"
        content = f'id,amount\n1,"{amount}"\n'.encode("utf-8")
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(db_path), table="nloc"
            ),
            source_content=content,
            source_filename="nloc.csv",
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
            validation_mode="balanced",
            number_locale=number_locale,
            column_types={"id": "integer", "amount": "decimal"},
            mappings=[
                {"source": "id", "target": "id"},
                {
                    "source": "amount",
                    "target": "amount",
                    "transform": "decimal",
                    "target_type": "DECIMAL",
                },
            ],
        )
        result = UniversalTransferEngine().execute_tracked(request, "0" * 24)
        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute("SELECT id, amount FROM nloc ORDER BY id").fetchall()
        except sqlite3.OperationalError:
            rows = []
        conn.close()
        return result, rows


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("$1,000.00", "1000.00"),
        ("€2.000,50", "2000.50"),
        ("USD 1000000.89", "1000000.89"),
        ("1,234,567.89", "1234567.89"),
        ("1.234.567,89", "1234567.89"),
        ("12.34", "12.34"),
        ("12,34", "12.34"),
        ("1000", "1000"),
    ],
)
def test_auto_parses_unambiguous_money_and_both_separators(raw, expected):
    value, err = apply_transform(raw, "decimal")
    assert err is None, err
    assert str(value) == expected


@pytest.mark.parametrize("raw", ["1,234", "1.234", "01,234"])
def test_auto_refuses_lone_three_digit_group(raw):
    value, err = apply_transform(raw, "decimal")
    assert value is None
    assert err is not None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1,234", "1234"),
        ("1.234", "1.234"),
        ("1,234.50", "1234.50"),
        ("$1,234", "1234"),
    ],
)
def test_us_locale_comma_thousands_dot_decimal(raw, expected):
    token = set_active_number_locale("US")
    try:
        value, err = apply_transform(raw, "decimal")
        assert err is None, err
        assert str(value) == expected
    finally:
        reset_active_number_locale(token)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1,234", "1.234"),
        ("1.234", "1234"),
        ("1.234,50", "1234.50"),
        ("€1.234", "1234"),
    ],
)
def test_eu_locale_dot_thousands_comma_decimal(raw, expected):
    token = set_active_number_locale("EU")
    try:
        value, err = apply_transform(raw, "decimal")
        assert err is None, err
        assert str(value) == expected
    finally:
        reset_active_number_locale(token)


def test_dollar_cell_disambiguates_without_operator_locale():
    value, err = apply_transform("$1,234", "decimal")
    assert err is None
    assert str(value) == "1234"


def test_euro_cell_disambiguates_without_operator_locale():
    value, err = apply_transform("€1.234", "decimal")
    assert err is None
    assert str(value) == "1234"


def test_infer_number_locale_from_dollar_column():
    rows = [{"amt": "$1,000.00"}, {"amt": "$2,500.00"}]
    assert infer_number_locale(rows, ["amt"]) == "US"


def test_infer_number_locale_from_euro_column():
    rows = [{"amt": "€1.000,00"}, {"amt": "€2.500,50"}]
    assert infer_number_locale(rows, ["amt"]) == "EU"


def test_infer_number_locale_empty_when_us_and_eu_mix():
    rows = [{"amt": "$1,000.00"}, {"amt": "€2.000,50"}]
    assert infer_number_locale(rows, ["amt"]) == ""


def test_infer_number_locale_empty_when_only_ambiguous_groups():
    rows = [{"amt": "1,234"}, {"amt": "5,678"}]
    assert infer_number_locale(rows, ["amt"]) == ""


def test_ambiguous_number_columns_name_the_next_action():
    rows = [{"amt": "1,234"}, {"note": "ok"}]
    findings = ambiguous_number_columns(rows, ["amt", "note"])
    assert len(findings) == 1
    assert findings[0]["column"] == "amt"
    assert "1,234" in findings[0]["samples"]
    assert "US or EU" in findings[0]["next_action"]


def test_preflight_surfaces_ambiguous_grouping_with_next_action():
    from services.preflight_service import run_file_preflight

    pf = run_file_preflight(
        columns=["amt"],
        column_types={"amt": "string"},
        row_count=2,
        mappings=[{"source": "amt", "target": "amt"}],
        sample_rows=[{"amt": "1,234"}, {"amt": "5,678"}],
        destination_connected=True,
    )
    report = pf.get("number_locale_report") or {}
    assert report.get("decision") == "set_locale"
    cols = [c.get("column") for c in report.get("ambiguous_columns") or []]
    assert "amt" in cols
    warns = pf.get("warnings") or []
    assert any(
        (isinstance(w, dict) and w.get("id") == "number_locale")
        or (isinstance(w, str) and "number locale" in w.lower())
        for w in warns
    )


def test_ambiguous_number_columns_empty_when_locale_set():
    token = set_active_number_locale("US")
    try:
        rows = [{"amt": "1,234"}]
        assert ambiguous_number_columns(rows, ["amt"]) == []
    finally:
        reset_active_number_locale(token)


def test_execute_auto_quarantines_lone_group_instead_of_guessing(monkeypatch):
    result, rows = _sqlite_amount(monkeypatch, "", "1,234")
    written = [r[1] for r in rows]
    assert "1234" not in {str(v) for v in written}
    assert "1.234" not in {str(v) for v in written}
    rejected = int((result.destination_summary or {}).get("rejected_rows") or 0)
    assert rejected >= 1 or result.records_transferred == 0


def test_execute_us_writes_comma_group_as_thousands(monkeypatch):
    result, rows = _sqlite_amount(monkeypatch, "US", "1,234")
    assert result.success is True, result.error
    assert rows
    assert str(rows[0][1]) in {"1234", "1234.0", "1234.00"}


def test_execute_eu_writes_comma_group_as_decimal(monkeypatch):
    result, rows = _sqlite_amount(monkeypatch, "EU", "1,234")
    assert result.success is True, result.error
    assert rows
    assert str(rows[0][1]) in {"1.234", "1.2340"}
