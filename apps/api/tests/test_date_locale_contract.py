"""Date locale Auto fail-closes 01/02/2024 and names the next action."""

from services.preflight_service import run_file_preflight
from services.transform_engine import (
    ambiguous_date_columns,
    infer_date_locale,
    reset_active_date_locale,
    set_active_date_locale,
)


def test_ambiguous_date_columns_name_the_next_action():
    rows = [{"event_date": "01/02/2024"}, {"event_date": "03/04/2024"}, {"note": "ok"}]
    findings = ambiguous_date_columns(rows, ["event_date", "note"])
    assert len(findings) == 1
    assert findings[0]["column"] == "event_date"
    assert "01/02/2024" in findings[0]["samples"]
    assert "DMY or MDY" in findings[0]["next_action"]


def test_ambiguous_date_columns_empty_when_locale_set():
    token = set_active_date_locale("MDY")
    try:
        rows = [{"event_date": "01/02/2024"}]
        assert ambiguous_date_columns(rows, ["event_date"]) == []
    finally:
        reset_active_date_locale(token)


def test_infer_date_locale_empty_when_only_ambiguous_pairs():
    rows = [{"event_date": "01/02/2024"}, {"event_date": "03/04/2024"}]
    assert infer_date_locale(rows, ["event_date"]) == ""


def test_infer_date_locale_uses_unambiguous_majority():
    rows = [
        {"event_date": "31/12/2024"},
        {"event_date": "01/02/2024"},
    ]
    assert infer_date_locale(rows, ["event_date"]) == "DMY"


def test_preflight_surfaces_ambiguous_dates_with_next_action():
    pf = run_file_preflight(
        columns=["event_date"],
        column_types={"event_date": "date"},
        row_count=2,
        mappings=[{"source": "event_date", "target": "event_date"}],
        sample_rows=[{"event_date": "01/02/2024"}, {"event_date": "03/04/2024"}],
        destination_connected=True,
    )
    report = pf.get("date_locale_report") or {}
    assert report.get("decision") == "set_locale"
    cols = [c.get("column") for c in report.get("ambiguous_columns") or []]
    assert "event_date" in cols
    warns = pf.get("warnings") or []
    assert any(
        (isinstance(w, dict) and w.get("id") == "date_locale")
        or (isinstance(w, str) and "date locale" in w.lower())
        for w in warns
    )


def test_preflight_ok_when_unambiguous_day_forces_dmy():
    pf = run_file_preflight(
        columns=["event_date"],
        column_types={"event_date": "date"},
        row_count=2,
        mappings=[{"source": "event_date", "target": "event_date"}],
        sample_rows=[{"event_date": "31/12/2024"}, {"event_date": "01/02/2024"}],
        destination_connected=True,
    )
    report = pf.get("date_locale_report") or {}
    assert report.get("decision") == "ok"
    assert report.get("date_locale") == "DMY"
