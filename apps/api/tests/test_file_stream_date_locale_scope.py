"""A file transfer's date order must not outlive the transfer on its thread.

``stream_file_to_database`` settles how the file's dates read (DMY, MDY or ISO
from the file's own values, or the operator's choice) and pins it for the
coercions it runs. ``@date_locale_scoped`` restores the caller's pin when the
call returns. A helper later inserted between the decorator and the function
took the decorator, so the pin stayed on the thread: the next read there (a
pooled job thread, a later job) was judged against this file's order and, for
an ISO file, refused ``01/02/2024`` as "mixed date formats".
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from connectors.sql_temporal import coerce_sql_temporal  # noqa: E402
from services import transform_engine as te  # noqa: E402
from src.transfer.file_stream import stream_file_to_database  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402


def _run(tmp_path: Path, *, date_locale: str = "") -> None:
    src = tmp_path / "events.csv"
    src.write_text(
        "id,happened_on\n1,2024-01-15\n2,2024-02-20\n3,2024-03-31\n",
        encoding="utf-8",
    )
    dest = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "out.db"), table="events"
    )
    rows, _ddl, _summary, _cols = stream_file_to_database(
        str(src),
        "events.csv",
        dest,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "happened_on", "target": "happened_on"},
        ],
        schema={"id": "INTEGER", "happened_on": "DATE"},
        date_locale=date_locale,
    )
    assert rows == 3


@pytest.mark.parametrize("copy_fast_path", ["1", "0"])
def test_an_iso_file_leaves_no_date_order_on_the_thread(tmp_path, monkeypatch, copy_fast_path):
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", copy_fast_path)
    token = te._DATE_LOCALE_VAR.set("")
    try:
        _run(tmp_path)
        assert te._DATE_LOCALE_VAR.get() == ""
        # The next read on this thread is not judged against the ISO file:
        # Auto leaves an ambiguous slash date to the engine instead of
        # refusing it as "mixed date formats".
        coerce_sql_temporal("01/02/2024", "DATE")
    finally:
        te._DATE_LOCALE_VAR.reset(token)


@pytest.mark.parametrize("caller_pin", ["", "MDY", "DMY"])
def test_an_operator_date_order_is_restored_to_the_callers(tmp_path, monkeypatch, caller_pin):
    monkeypatch.setenv("DATAFLOW_CSV_LOCAL_COPY", "0")
    token = te._DATE_LOCALE_VAR.set(caller_pin)
    try:
        _run(tmp_path, date_locale="DMY" if caller_pin != "DMY" else "MDY")
        assert te._DATE_LOCALE_VAR.get() == caller_pin
    finally:
        te._DATE_LOCALE_VAR.reset(token)


def test_the_scope_wraps_the_transfer_itself():
    # functools.wraps leaves the undecorated function on __wrapped__.
    assert getattr(stream_file_to_database, "__wrapped__", None) is not None
