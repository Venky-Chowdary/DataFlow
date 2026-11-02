from __future__ import annotations

import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from connectors.cdc_eos_sql import apply_change_batch_exactly_once, dest_engine_count
from services.cdc_engine import ChangeBatch
from services.cdc_exactly_once import (
    ExactlyOnceRouteError,
    batch_apply_checksum,
)


_MAPPINGS = [
    {"source": name, "target": name, "confidence": 1.0}
    for name in ("id", "c", "b", "v")
]
_TYPES = {name: "string" for name in ("id", "c", "b", "v")}


def _apply(path: str, change: ChangeBatch):
    return apply_change_batch_exactly_once(
        dest_type="sqlite",
        dest_cfg={"database": path},
        dest_table="rows",
        change=change,
        mappings=_MAPPINGS,
        column_types=_TYPES,
        headers=["id", "c", "b", "v"],
        pk_target_cols=["id"],
        cursor_key="lookup|rows",
    )


def test_lookup_column_redelivery_is_already_committed_and_noop() -> None:
    with TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "lookup.db")
        token = "0/50"
        lookup = {"rows": ["c", "b"]}
        _apply(
            path,
            ChangeBatch(
                inserts=[{"id": "1", "c": "old", "b": "old-b", "v": "stable"}],
                resume_token=token,
                table="rows",
                lookup_columns=lookup,
            ),
        )
        rows, _checksum, summary, _deleted = _apply(
            path,
            ChangeBatch(
                updates=[{"id": "1", "c": "new", "b": "new-b", "v": "stable"}],
                resume_token=token,
                table="rows",
                lookup_columns=lookup,
            ),
        )
        assert rows == 0
        assert summary["eos_already_committed"] is True
        assert dest_engine_count({"database": path}, "rows") == 1
        with sqlite3.connect(path) as conn:
            assert conn.execute("SELECT c, b, v FROM rows").fetchone() == (
                "old",
                "old-b",
                "stable",
            )


def test_lookup_columns_do_not_hide_nonlob_conflicts() -> None:
    with TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "lookup-conflict.db")
        token = "0/60"
        lookup = {"rows": ["c", "b"]}
        _apply(
            path,
            ChangeBatch(
                inserts=[{"id": "1", "c": "old", "b": "old-b", "v": "stable"}],
                resume_token=token,
                table="rows",
                lookup_columns=lookup,
            ),
        )
        with pytest.raises(ExactlyOnceRouteError, match="checksum mismatch"):
            _apply(
                path,
                ChangeBatch(
                    updates=[{"id": "1", "c": "new", "b": "new-b", "v": "conflict"}],
                    resume_token=token,
                    table="rows",
                    lookup_columns=lookup,
                ),
            )


def test_empty_lookup_columns_preserve_legacy_checksum() -> None:
    position = "scn:2816643.p0x000063.00003b45.00ec.0000000000"
    batch = ChangeBatch(
        updates=[
            {
                "ID": "2",
                "C": "lob-before",
                "B": "YmxvYi1iZWZvcmU=",
                "V": "step",
                "N": "__DF_SQL_NULL__",
            }
        ],
        table="T",
        resume_token=position,
    )
    assert batch.lookup_columns == {}
    assert (
        batch_apply_checksum(batch, incoming_lsn=position, pk_cols=["ID"])
        == "7d0638aba6b24fa0f7a24cecd3c5993401c26ec08b14458f20b6ff5647c8c1bb"
    )
