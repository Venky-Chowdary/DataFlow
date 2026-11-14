"""Re-review: the writer and Gate-8 agree on a blank inline tsv_null.

Before: file_writer_dest_extra let an inline ``tsv_null: ""`` (or None) mask a
saved connector's ``"\\N"`` (the writer wrote NULL as an empty cell) while
Gate-8's tsv_null_from skipped the blank and read the saved ``"\\N"`` — the
read-back then disagreed with the file. A blank value is "not set" on both
sides now; an explicit ``"empty"`` (what the web sends for Empty cell) wins
over a saved ``"\\N"`` on both sides.
"""

from __future__ import annotations

import pytest


class _Dest:
    def __init__(self, extra):
        self.extra = extra


def _cfg(saved: dict, inline: dict) -> dict:
    """The resolved destination cfg: saved nested extra, inline extra flattened on top."""
    return {"extra": dict(saved), **inline}


@pytest.mark.parametrize(
    "saved, inline, expected",
    [
        ({"tsv_null": "\\N"}, {"tsv_null": ""}, "\\N"),
        ({"tsv_null": "\\N"}, {"tsv_null": None}, "\\N"),
        ({"tsv_null": "\\N"}, {"tsv_null": "  "}, "\\N"),
        ({"tsv_null": "\\N"}, {"tsv_null": "empty"}, "empty"),
        ({"tsv_null": "\\N"}, {}, "\\N"),
        ({}, {"tsv_null": "\\N"}, "\\N"),
        ({}, {"tsv_null": ""}, "empty"),
    ],
)
def test_writer_and_gate8_read_the_same_tsv_null(saved, inline, expected):
    from services.export_dialect import tsv_null_from
    from src.transfer.connector_dispatch import file_writer_dest_extra

    cfg = _cfg(saved, inline)
    writer_view = tsv_null_from(file_writer_dest_extra(cfg, _Dest(inline)))
    gate8_view = tsv_null_from(cfg)
    assert writer_view == gate8_view == expected
