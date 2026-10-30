"""Object-store Gate-8 digest reads a sentinel-spelling cell as text (D113/D115).

The write pass fingerprints a source literal ``'__DF_SQL_NULL__'`` as text (a
``WireLiteral``); the artifact read-back hands the CSV/JSONL cell over as a
plain ``str``. Without wire-encoding the read-back, the destination digest
read that literal as NULL, so a correct PG -> S3 CSV load failed Gate-8 and a
file that stored the sentinel where the source held NULL verified clean.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_precount import checksum_artifact_stream  # noqa: E402
from services.reconciliation import canonical_checksum  # noqa: E402
from services.value_serializer import unescape_wire_text  # noqa: E402

COLS = ["id", "name"]
TYPES = {"id": "INTEGER", "name": "VARCHAR"}


def _write_pass(rows: list[tuple[int, str | None]]) -> str:
    # What the writer fingerprints: a source literal arrives escaped on the
    # wire and decodes to a WireLiteral; NULL is None.
    return canonical_checksum(
        [
            {
                "id": i,
                "name": None
                if v is None
                else unescape_wire_text(
                    "__DF_ESC__" + v if v.strip().lower() == "__df_sql_null__" else v
                ),
            }
            for i, v in rows
        ],
        COLS,
        dest_db_type="s3",
        dest_types=TYPES,
    )


def _artifact(body: bytes, name: str) -> str:
    return checksum_artifact_stream(
        io.BytesIO(body), name=name, columns=COLS, dest_db_type="s3", dest_types=TYPES
    )[1]


ROWS = [(1, "ok"), (2, None), (3, ""), (4, "__DF_SQL_NULL__")]


def test_csv_literal_sentinel_matches_the_write_pass():
    body = b'id,name\n1,ok\n2,\n3,""\n4,__DF_SQL_NULL__\n'
    assert _artifact(body, "x.csv") == _write_pass(ROWS)


def test_jsonl_literal_sentinel_matches_the_write_pass():
    body = (
        b'{"id": 1, "name": "ok"}\n{"id": 2, "name": null}\n'
        b'{"id": 3, "name": ""}\n{"id": 4, "name": "__DF_SQL_NULL__"}\n'
    )
    assert _artifact(body, "x.jsonl") == _write_pass(ROWS)


def test_sentinel_text_where_the_source_held_null_is_detected():
    # Corruption: the file stored the NULL sentinel as text for row 2.
    body = b'id,name\n1,ok\n2,__DF_SQL_NULL__\n3,""\n4,__DF_SQL_NULL__\n'
    assert _artifact(body, "x.csv") != _write_pass(ROWS)
