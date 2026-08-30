"""``.psv`` is delimited text, not an unknown binary blob.

Both detectors in ``services.file_parser`` classify by extension first, and the
delimited reader already sniffs ``|`` as a candidate separator, so a pipe file
only failed because the extension was unlisted.
"""

from services.file_parser import FileParser, detect_format

SAMPLE = b"id|name|amount\n1|ada|10.5\n2|grace|11.5\n"


def test_detect_format_reads_psv_as_delimited() -> None:
    assert detect_format("dirty.psv", SAMPLE) == "csv"


def test_detect_file_type_reads_psv_as_delimited() -> None:
    assert FileParser.detect_file_type("dirty.psv", SAMPLE) == "csv"


def test_psv_parses_with_sniffed_pipe_delimiter() -> None:
    result = FileParser.parse_csv(SAMPLE)
    assert result.success, result.error
    assert result.columns == ["id", "name", "amount"]
    assert result.row_count == 2
    assert result.data[1]["name"] == "grace"
