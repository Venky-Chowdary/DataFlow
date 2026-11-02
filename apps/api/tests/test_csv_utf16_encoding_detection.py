"""A BOM-declared UTF-16 CSV must be read, not decoded as latin-1.

UTF-16 bytes fail the UTF-8 probe, and the latin-1 fallback turns every second
byte into a NUL character, so the reader died with ``line contains NUL`` on a
file whose first two bytes state its encoding.
"""

from pathlib import Path

from services.csv_profiler import detect_encoding, parse_csv_full
from src.transfer.file_stream import _batch_iterator_for_type, peek_file_source

ROWS = "id,name\n1,ada\n2,zo\u00eb\n"


def test_bom_encodings_are_authoritative() -> None:
    assert detect_encoding(ROWS.encode("utf-16")) == "utf-16"
    assert detect_encoding(b"\xfe\xff" + ROWS.encode("utf-16-be")) == "utf-16"
    assert detect_encoding(ROWS.encode("utf-32")) == "utf-32"
    assert detect_encoding(ROWS.encode("utf-8-sig")) == "utf-8-sig"
    assert detect_encoding(ROWS.encode("utf-8")) == "utf-8"


def test_utf16_csv_parses() -> None:
    headers, rows, _enc, _delim = parse_csv_full(ROWS.encode("utf-16"))
    assert headers == ["id", "name"]
    assert rows[1][1] == "zo\u00eb"


def test_utf16_csv_streams_from_a_path(tmp_path: Path) -> None:
    path = tmp_path / "rows.csv"
    path.write_bytes(ROWS.encode("utf-16"))
    headers, _schema, total, _sample = peek_file_source(str(path), path.name)
    assert headers == ["id", "name"]
    assert total == 2
    batches = list(_batch_iterator_for_type("csv", str(path), 10))
    assert batches[0][1]["name"] == "zo\u00eb"
