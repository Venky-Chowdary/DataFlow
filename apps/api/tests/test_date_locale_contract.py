"""Date locale Auto fail-closes 01/02/2024 and names the next action."""

from services.preflight_service import run_file_preflight
from services.transform_engine import (
    ambiguous_date_columns,
    canonical_date_locale,
    canonical_number_locale,
    infer_date_locale,
    infer_transform_for_mapping,
    reset_active_date_locale,
    samples_are_auto_ambiguous_dates,
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


def test_samples_are_auto_ambiguous_dates_settled_by_unambiguous_member():
    assert samples_are_auto_ambiguous_dates(["01/02/2024", "03/04/2024"]) is True
    assert samples_are_auto_ambiguous_dates(["31/12/2024", "01/02/2024"]) is False
    assert samples_are_auto_ambiguous_dates(["2024-01-02"]) is False
    token = set_active_date_locale("MDY")
    try:
        assert samples_are_auto_ambiguous_dates(["01/02/2024"]) is False
    finally:
        reset_active_date_locale(token)


def test_name_does_not_invent_date_transform_into_a_text_sink():
    """event_date → VARCHAR is identity even when samples are omitted.

    Write-path resolve_transform used to re-infer Date→ISO from the name after
    Map stripped transform=none, then Validate blocked 01/02/2024 as
    INVALID_TIMESTAMP on a VARCHAR destination.
    """
    assert infer_transform_for_mapping("event_date", "event_date", "VARCHAR", "VARCHAR") == "none"
    assert (
        infer_transform_for_mapping(
            "event_date",
            "event_date",
            "VARCHAR",
            "VARCHAR",
            source_samples=["01/02/2024", "03/04/2024"],
        )
        == "none"
    )
    assert (
        infer_transform_for_mapping(
            "event_date",
            "event_date",
            "VARCHAR",
            "TEXT",
            source_samples=["31/12/2024"],
        )
        == "none"
    )


def test_name_does_not_invent_date_transform_on_auto_ambiguous_samples():
    assert (
        infer_transform_for_mapping(
            "event_date",
            "event_date",
            "VARCHAR",
            "VARCHAR",
            source_samples=["01/02/2024", "03/04/2024"],
        )
        == "none"
    )
    assert (
        infer_transform_for_mapping(
            "event_date",
            "event_date",
            "VARCHAR",
            "DATE",
            source_samples=["01/02/2024", "03/04/2024"],
        )
        == "date"
    )


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


def test_varchar_event_date_preflight_does_not_fail_as_invalid_timestamp():
    """Identity VARCHAR write of 01/02/2024 is not a sample-transform failure."""
    pf = run_file_preflight(
        columns=["event_date"],
        column_types={"event_date": "VARCHAR"},
        row_count=2,
        mappings=[{"source": "event_date", "target": "event_date", "target_type": "VARCHAR"}],
        sample_rows=[{"event_date": "01/02/2024"}, {"event_date": "03/04/2024"}],
        destination_connected=True,
    )
    text = str(pf).lower()
    assert "invalid date" not in text
    assert "invalid_timestamp" not in text
    blockers = pf.get("blockers") or []
    assert not any("fidelity collapse" in str(b).lower() for b in blockers)
    report = pf.get("date_locale_report") or {}
    assert report.get("decision") == "set_locale"


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


def test_locale_tokens_outside_the_allowlist_are_auto():
    assert canonical_date_locale("mdy") == "MDY"
    assert canonical_date_locale("dmy") == "DMY"
    assert canonical_date_locale("'); DROP TABLE dates; --") == ""
    assert canonical_number_locale("eu") == "EU"
    assert canonical_number_locale("'); DROP TABLE amounts; --") == ""
    token = set_active_date_locale("'); DROP TABLE dates; --")
    try:
        from services.transform_engine import _DATE_LOCALE_VAR, _active_date_locale

        stored = _DATE_LOCALE_VAR.get()
        assert "DROP" not in stored
        assert stored not in {"MDY", "DMY"}
        assert _active_date_locale() == ""
    finally:
        reset_active_date_locale(token)


def test_rejected_date_pin_does_not_inherit_deployment_order(monkeypatch):
    from services.transform_engine import _active_date_locale, _env_date_locale

    monkeypatch.setenv("DATAFLOW_DATE_ORDER", "MDY")
    _env_date_locale.cache_clear()
    rejected = set_active_date_locale("not-a-locale")
    try:
        assert _active_date_locale() == ""
    finally:
        reset_active_date_locale(rejected)
        _env_date_locale.cache_clear()
    empty = set_active_date_locale("")
    try:
        _env_date_locale.cache_clear()
        assert _active_date_locale() == "MDY"
    finally:
        reset_active_date_locale(empty)
        _env_date_locale.cache_clear()


def test_unknown_date_locale_does_not_echo_and_still_infers():
    pf = run_file_preflight(
        columns=["event_date"],
        column_types={"event_date": "date"},
        row_count=2,
        mappings=[{"source": "event_date", "target": "event_date"}],
        sample_rows=[{"event_date": "31/12/2024"}, {"event_date": "01/02/2024"}],
        destination_connected=True,
        date_locale="'); DROP TABLE dates; --",
    )
    assert pf.get("date_locale") == "DMY"
    assert "DROP" not in str(pf.get("date_locale_report"))


def test_pinned_date_locale_still_infers_number_grouping():
    """An operator date locale must not skip number inference."""
    pf = run_file_preflight(
        columns=["dob", "amount"],
        column_types={"dob": "date", "amount": "varchar"},
        row_count=1,
        mappings=[
            {"source": "dob", "target": "dob", "target_type": "DATE", "transform": "date_iso"},
            {"source": "amount", "target": "amount", "target_type": "NUMERIC"},
        ],
        sample_rows=[{"dob": "01/15/2024", "amount": "1.234,56"}],
        destination_connected=True,
        source_kind="file",
        date_locale="MDY",
    )
    assert pf.get("date_locale") == "MDY"
    report = pf.get("number_locale_report") or {}
    assert report.get("number_locale") == "EU", report
    assert "DROP" not in str(report)


def test_unknown_number_locale_does_not_echo_and_still_infers():
    pf = run_file_preflight(
        columns=["amount"],
        column_types={"amount": "varchar"},
        row_count=1,
        mappings=[{"source": "amount", "target": "amount", "target_type": "NUMERIC"}],
        sample_rows=[{"amount": "1.234,56"}],
        destination_connected=True,
        number_locale="'); DROP TABLE amounts; --",
    )
    report = pf.get("number_locale_report") or {}
    assert report.get("number_locale") == "EU", report
    assert "DROP" not in str(report)
