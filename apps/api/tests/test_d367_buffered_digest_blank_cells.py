"""D367/R12: the buffered (keep_last) Gate-8 source digest applies the writer's blank-cell rule.

A file overwrite with a declared dedupe_key reads the file whole (buffered
path). The writer lands an empty field into a DATE as SQL NULL (D1, R12:
NULL is an empty unquoted field), but the source remap hashed ``''``, so a
correct load failed with "Checksum mismatch" (QA R5 slash-date file).
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

from src.transfer import reconcile_step as rs

_MAPPINGS = [
    {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
    {"source": "d", "target": "d", "source_type": "DATE", "target_type": "DATE"},
]
_DEST = {"id": "INTEGER", "d": "DATE"}


def _source_digest(rows: list[dict], *, blanks_null: bool) -> str:
    digest, _ = rs._compute_source_checksum(
        rows, ["id", "d"], _MAPPINGS, {"id": "INTEGER", "d": "DATE"}, "",
        ["id", "d"], dest_db_type="postgresql", dest_types=_DEST,
        validation_mode="balanced", empty_cells_as_null=blanks_null,
    )
    return digest


def test_blank_date_in_a_file_digest_matches_the_null_it_landed_as():
    rows = [{"id": "1", "d": "31/12/2024"}, {"id": "2", "d": ""}]
    landed = rs.checksum_rows(
        [[1, dt.date(2024, 12, 31)], [2, None]], ["id", "d"],
        dest_db_type="postgresql", dest_types=_DEST,
    )
    assert _source_digest(rows, blanks_null=True) == landed
    # A wrong write still fails: a shifted day is a different digest.
    wrong = rs.checksum_rows(
        [[1, dt.date(2024, 12, 30)], [2, None]], ["id", "d"],
        dest_db_type="postgresql", dest_types=_DEST,
    )
    assert _source_digest(rows, blanks_null=True) != wrong


def test_only_file_and_object_sources_take_the_blank_cell_rule():
    assert rs._record_batch_blanks_are_null(SimpleNamespace(kind="database", format="sftp"))
    assert rs._record_batch_blanks_are_null(SimpleNamespace(kind="file", format="csv"))
    assert not rs._record_batch_blanks_are_null(SimpleNamespace(kind="database", format="postgresql"))
    assert not rs._record_batch_blanks_are_null(None)
