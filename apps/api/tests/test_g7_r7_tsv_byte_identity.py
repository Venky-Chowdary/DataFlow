"""G7 re-review B1: TSV quoting must not change bytes of ordinary values.

The H1 fix quoted any TSV cell containing ``"``, so a default ("off") TSV of
``5" pipe`` became ``"5"" pipe"``. Loaders that read the TSV without quote
handling (the S3→MySQL LOAD DATA fast path) then stored the literal quotes.

Rule: a TSV cell is quoted only when it contains a tab, CR or LF, or starts
with ``"``. A mid-field ``"`` stays raw — every product TSV reader takes it
literally — so rows without those characters are byte-identical to the
pre-H1 writer (aacea0e05).
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

# No tab / CR / LF / leading quote: these rows must be byte-identical to aacea0e05.
ORDINARY = [
    ["1", '5" pipe', "a\\b", 'say "hi"'],
    ["2", "=1+1", "a,b", "x\"\"y"],
    ["3", "café", "", "-5"],
]
ORDINARY_TSV = (
    "id\tnote\tpath\tquote\n"
    '1\t5" pipe\ta\\b\tsay "hi"\n'
    '2\t=1+1\ta,b\tx""y\n'
    "3\tcafé\t\t-5\n"
).encode("utf-8")
COLS = ["id", "note", "path", "quote"]

NEEDS_QUOTING = [
    ["1", "a\tb", "c\nd", "e\rf"],
    ["2", '"lead', 'mid"tab\there', "plain"],
]


def test_format_converter_tsv_is_byte_identical_for_ordinary_values():
    from services.format_converter import convert_rows

    content, _mime = convert_rows(COLS, ORDINARY, source_format="csv", target_format="tsv")
    assert content == ORDINARY_TSV


def test_transfer_file_export_tsv_default_is_the_export_dialect():
    """The transfer file export writes PR 402's export dialect (services.export_dialect).

    "off" adds nothing to it: CRLF rows, NULL an empty cell, '' quoted "",
    and any cell holding the delimiter, a quote or a line break quoted.
    """
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    content, _name, _summary = write_destination_file(
        EndpointConfig(kind="file_export", format="tsv"),
        [dict(zip(COLS, r)) for r in ORDINARY],
        list(COLS),
        source_format="csv",
    )
    assert content == (
        "id\tnote\tpath\tquote\r\n"
        '1\t"5"" pipe"\ta\\b\t"say ""hi"""\r\n'
        '2\t=1+1\ta,b\t"x""""y"\r\n'
        '3\tcafé\t""\t-5\r\n'
    ).encode("utf-8")


def test_object_store_tsv_is_byte_identical_for_ordinary_values():
    from connectors.object_store_common import serialize_object_store_export

    export = serialize_object_store_export(
        key="out/data.tsv",
        mapped_rows=[tuple(r) for r in ORDINARY],
        target_cols=list(COLS),
        dest_types={c: "TEXT" for c in COLS},
    )
    body = export.read_all()
    # '' is the empty cell on the object-store wire as well.
    assert body.replace(b"\r\n", b"\n") == ORDINARY_TSV


def test_s3_copy_row_formatter_keeps_mid_field_quotes_raw():
    from services.copy_s3_common import s3_format_delimited_row

    assert s3_format_delimited_row(['5" pipe', "a\\b", "plain"], "\t") == '5" pipe\ta\\b\tplain'


@pytest.mark.parametrize(
    "cell, encoded",
    [
        ("a\tb", '"a\tb"'),
        ("c\nd", '"c\nd"'),
        ("e\rf", '"e\rf"'),
        ('"lead', '"""lead"'),
        ('mid"tab\there', '"mid""tab\there"'),
        ('5" pipe', '5" pipe'),
        ("plain", "plain"),
    ],
)
def test_tsv_cell_quoting_rule(cell, encoded):
    from services.csv_codec import csv_cell_encode, format_csv_row

    assert format_csv_row([csv_cell_encode(cell)], "\t") == encoded


def _tsv_text(rows: list[list[str]]) -> str:
    from services.csv_codec import csv_cell_encode, format_csv_row

    lines = [format_csv_row(list(COLS), "\t")]
    lines += [format_csv_row([csv_cell_encode(c) for c in r], "\t") for r in rows]
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize("rows", [ORDINARY, NEEDS_QUOTING])
def test_product_tsv_readers_read_the_exact_values(tmp_path, rows):
    from services.copy_s3_common import s3_iter_delimited_rows
    from services.csv_codec import iter_csv_records
    from services.file_parser import FileParser

    text = _tsv_text(rows)
    expected = [[c if c != "" else None for c in r] for r in rows]

    parsed = list(csv.reader(io.StringIO(text, newline=""), delimiter="\t"))[1:]
    assert parsed == rows

    records = list(iter_csv_records(io.StringIO(text, newline=""), "\t"))[1:]
    assert records == expected

    path = tmp_path / "data.tsv"
    path.write_bytes(text.encode("utf-8"))
    assert list(s3_iter_delimited_rows(str(path), "\t")) == expected

    result = FileParser.parse(text.encode("utf-8"), "data.tsv")
    assert result.success, result.error
    got = [[("" if rec.get(c) is None else str(rec.get(c))) for c in COLS] for rec in result.data]
    assert got == rows


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
@pytest.mark.parametrize("protection", ["off", "prefix"])
def test_export_dialect_round_trips_null_empty_and_guard_through_gate8(tmp_path, fmt, protection):
    """PR 402's dialect keeps NULL and '' apart; the G7 guard is restored on read-back."""
    from services.dest_precount import _iter_artifact_records
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig
    from src.transfer.reconcile_step import _file_export_cell_readback

    cols = ["id", "=hdr", "v"]
    records = [
        {"id": "1", "=hdr": None, "v": ""},
        {"id": "2", "=hdr": "", "v": None},
        {"id": "3", "=hdr": "=HYPERLINK(\"x\")", "v": "a\tb\nc"},
        {"id": "4", "=hdr": "'=q", "v": '"lead'},
        {"id": "5", "=hdr": "-5", "v": "@SUM(A1)"},
    ]
    content, _name, summary = write_destination_file(
        EndpointConfig(kind="file_export", format=fmt, extra={"formula_protection": protection}),
        [dict(r) for r in records], list(cols), source_format="csv",
    )
    path = tmp_path / f"export.{fmt}"
    path.write_bytes(content)
    with open(path, "rb") as fh:
        rows = list(_iter_artifact_records(fh, name=path.name, fmt=fmt, written_here=True))
    header = "'=hdr" if protection == "prefix" else "=hdr"
    assert [r[header] for r in rows][:2] == [None, ""]
    assert [r["v"] for r in rows][:2] == ["", None]
    mappings = [{"source": c, "target": c, "target_type": "text"} for c in cols]
    compare = _file_export_cell_readback(
        [dict(r) for r in records], list(cols), mappings,
        {**summary, "path": str(path), "primary_key_columns": ["id"]}, fmt=fmt,
    )
    assert compare is not None and compare.get("passed") is True, compare
    assert compare.get("rows_compared") == len(records), compare
