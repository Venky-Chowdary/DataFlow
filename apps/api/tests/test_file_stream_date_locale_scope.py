"""A file load's date reading must not outlive the load (CI: shard order made it visible).

``stream_file_to_database`` settles how its own file's dates read (DMY / MDY /
ISO) and pins that for the parse. Called directly (the COPY and file-stream
tests do, outside ``execute_tracked``'s own scope) the pin stayed on the
calling context, so a later, unrelated bind of ``01/02/2024`` was refused as
"mixed date formats ... this read's dates are ISO".
"""

from __future__ import annotations

from pathlib import Path

from services import transform_engine as te
from src.transfer.file_stream import stream_file_to_database
from src.transfer.models import EndpointConfig


def _load_iso_dated_file(tmp_path: Path) -> int:
    source = tmp_path / "orders.csv"
    source.write_text("id,placed_on\n1,2024-01-15\n2,2024-02-20\n3,2024-03-31\n", encoding="utf-8")
    dest = EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "out.db"), table="orders")
    rows, _ddl, _summary, _columns = stream_file_to_database(
        str(source), "orders.csv", dest,
        mappings=[{"source": "id", "target": "id"}, {"source": "placed_on", "target": "placed_on"}],
        schema={"id": "INTEGER", "placed_on": "DATE"},
    )
    return rows


def test_file_load_leaves_the_callers_date_locale_unchanged(tmp_path):
    before = te._DATE_LOCALE_VAR.get()
    assert _load_iso_dated_file(tmp_path) == 3
    assert te._DATE_LOCALE_VAR.get() == before
    # The next, unrelated read is not judged by that file's dates.
    assert te.mixed_date_format_reason("01/02/2024") == ""


def test_file_load_keeps_a_pin_the_caller_made(tmp_path):
    token = te.set_active_date_locale("DMY")
    try:
        assert _load_iso_dated_file(tmp_path) == 3
        assert te._DATE_LOCALE_VAR.get() == "DMY"
    finally:
        te.reset_active_date_locale(token)
