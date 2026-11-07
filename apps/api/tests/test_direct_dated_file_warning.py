"""A directly chosen older dated file warns that today has not arrived.

The warning already existed for a glob. An exact key such as
``daily_20261015.csv`` on 2026-10-16 must warn in preflight and on the job,
without a schedule. Today's own file does not warn.
"""

from __future__ import annotations

from datetime import date

from services.file_pickup import direct_dated_file_warning
from services.file_pickup_load import pickup_job_outcome
from services.preflight_service import run_file_preflight


TODAY = date(2026, 10, 16)
OLDER = "daily_20261015.csv"
CURRENT = "daily_20261016.csv"


def _warning_texts(payload: dict) -> list[str]:
    texts = []
    for item in payload.get("warnings") or []:
        texts.append(str(item.get("message") if isinstance(item, dict) else item))
    return texts


def test_direct_older_key_warns_and_todays_file_does_not(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.file_pickup._pickup_day", lambda _today=None: TODAY
    )
    assert direct_dated_file_warning(OLDER, TODAY) == "expected file for 2026-10-16 not arrived"
    assert direct_dated_file_warning(CURRENT, TODAY) == ""
    assert direct_dated_file_warning("orders.csv", TODAY) == ""

    preflight = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=1,
        mappings=[{"source": "id", "target": "id", "confidence": 1, "transform": "none"}],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_table_exists=False,
        source_connected=True,
        source_kind="database",
        source_format="s3",
        source_table=OLDER,
        sync_mode="full_refresh_overwrite",
        sample_rows=[{"id": "1"}],
        destination_db_type="postgresql",
        validation_mode="balanced",
        schema_policy="propagate_columns",
    )
    assert "expected file for 2026-10-16 not arrived" in _warning_texts(preflight)
    assert preflight["passed"] is True

    today_run = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=1,
        mappings=[{"source": "id", "target": "id", "confidence": 1, "transform": "none"}],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_table_exists=False,
        source_connected=True,
        source_kind="database",
        source_format="s3",
        source_table=CURRENT,
        sync_mode="full_refresh_overwrite",
        sample_rows=[{"id": "1"}],
        destination_db_type="postgresql",
        validation_mode="balanced",
        schema_policy="propagate_columns",
    )
    assert "not arrived" not in " ".join(_warning_texts(today_run))

    outcome = pickup_job_outcome(
        {"warnings": [{"id": "expected_file_not_arrived", "message": direct_dated_file_warning(OLDER, TODAY)}]},
        4,
    )
    assert outcome is not None
    assert outcome["status"] == "completed_with_warnings"
    assert outcome["message"] == "expected file for 2026-10-16 not arrived"
