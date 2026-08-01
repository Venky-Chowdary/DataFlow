"""Oracle BYTE/CHAR width + PG/SS computed GENERATED ALWAYS omit — enterprise SSOT."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    omit_generated_always_columns,
    quarantine_unfit_strings,
)
from services.schema_introspect import _pg_fetch_columns  # noqa: E402
from services.type_system import is_generated_always_column  # noqa: E402


def test_oracle_byte_quarantine_holds_multibyte():
    details: list[dict] = []
    out = quarantine_unfit_strings(
        [("你",), ("ab",), ("你你",)],
        ["name"],
        ["VARCHAR(3 BYTE)"],
        details,
        policy="quarantine",
        dialect_label="VARCHAR2",
    )
    assert ("你",) in out  # 3 UTF-8 bytes
    assert ("ab",) in out
    assert ("你你",) not in out  # 6 bytes
    assert details and "exceeds" in details[0]["reason"].lower()


def test_pg_attgenerated_annotates_generated_always():
    cur = MagicMock()
    cur.fetchall.return_value = [
        # name, dtype, nullable, attidentity, default, coll, coll_det, attgenerated
        ("id", "integer", "NO", "a", None, "", True, ""),
        ("full_name", "text", "YES", "", None, "", True, "s"),
        ("email", "character varying(100)", "YES", "", None, "", True, ""),
    ]
    cols = _pg_fetch_columns(cur, "public", "users")
    by_name = {c["name"]: c for c in cols}
    assert "GENERATED ALWAYS" in by_name["id"]["inferred_type"].upper()
    assert "GENERATED ALWAYS" in by_name["full_name"]["inferred_type"].upper()
    assert "GENERATED ALWAYS" not in by_name["email"]["inferred_type"].upper()
    assert is_generated_always_column(by_name["full_name"]["inferred_type"])


def test_omit_computed_generated_from_insert_projection():
    cols, types, rows, omitted = omit_generated_always_columns(
        ["id", "full_name", "email"],
        ["INTEGER GENERATED ALWAYS", "TEXT GENERATED ALWAYS", "VARCHAR(100)"],
        [(1, "Ada Lovelace", "a@b.c")],
    )
    assert cols == ["email"]
    assert types == ["VARCHAR(100)"]
    assert rows == [("a@b.c",)]
    assert set(omitted) == {"id", "full_name"}
