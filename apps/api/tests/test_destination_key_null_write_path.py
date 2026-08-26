"""Append collision keys use present_cell_text, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like a present batch key, so
the probe searched for the sentinel spelling and True vs dest "true"
missed. NULL keys stay skipped (not an IN-list token). Empty / whitespace
stay blank.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.destination_key_collision_probe import (  # noqa: E402
    batch_key_texts,
    probe_destination_key_collisions,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
    present_cell_text,
)


def test_present_cell_text_matches_reader_wire():
    assert present_cell_text(None) is None
    assert present_cell_text("") is None
    assert present_cell_text("   ") is None
    assert present_cell_text(SQL_NULL_SENTINEL) is None
    assert present_cell_text(DF_MISSING_SENTINEL) is None
    assert present_cell_text(Missing) is None
    assert present_cell_text(0) == "0"
    assert present_cell_text(True) == "true"
    assert present_cell_text(True) != str(True)
    assert present_cell_text("kept") == "kept"


def test_batch_keys_skip_reader_null():
    assert batch_key_texts(
        [SQL_NULL_SENTINEL, None, "", "   ", DF_MISSING_SENTINEL, Missing, 1, True, "true"]
    ) == ["1", "true", "true"]


def test_sentinel_only_batch_is_skipped_no_values():
    res = probe_destination_key_collisions(
        destination_config={"type": "sqlite", "connection_string": "sqlite:///:memory:"},
        destination_db_type="sqlite",
        destination_table="jobs",
        key_column="id",
        values=[SQL_NULL_SENTINEL, None, ""],
    )
    assert res.status == "skipped_no_values"
    assert res.values_probed == 0
    assert res.ran is False


def test_bool_batch_key_collides_with_dest_true(tmp_path: Path):
    import sqlite3

    from services.destination_key_collision_probe import probe_destination_key_collisions

    db = tmp_path / "dest.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES (?, ?)", ("true", "kept"))
    conn.commit()
    conn.close()
    res = probe_destination_key_collisions(
        destination_config={"type": "sqlite", "connection_string": f"sqlite:///{db}"},
        destination_db_type="sqlite",
        destination_table="jobs",
        key_column="id",
        values=[True, SQL_NULL_SENTINEL],
    )
    assert res.status == "ran"
    assert res.values_probed == 1
    assert any(f.get("value") == "true" for f in res.findings)
