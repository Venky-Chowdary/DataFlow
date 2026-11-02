"""An on-disk XML source must be measured, not read as though the path were the document.

``prepare_stream_content`` hands a filesystem path down as ``str``, while the XML
readers treat ``str`` as document text (``artifact_byte_source``). Every path-based
XML source therefore counted as unmeasured and the engine refused a well-formed
list-of-object file.
"""

from pathlib import Path

from src.transfer.file_stream import _batch_iterator_for_type, peek_file_source

DOC = """<?xml version="1.0" encoding="UTF-8"?>
<records>
  <record><id>1</id><name>ada</name></record>
  <record><id>2</id><name>grace</name></record>
  <record><id>3</id><name>zo\u00eb</name></record>
</records>
"""


def _write(tmp_path: Path) -> Path:
    path = tmp_path / "rows.xml"
    path.write_text(DOC, encoding="utf-8")
    return path


def test_peek_measures_xml_from_a_str_path(tmp_path: Path) -> None:
    path = _write(tmp_path)
    headers, _schema, total, sample = peek_file_source(str(path), path.name)
    assert total == 3
    assert headers == ["id", "name"]
    assert sample[0]["name"] == "ada"


def test_batches_stream_xml_from_a_str_path(tmp_path: Path) -> None:
    path = _write(tmp_path)
    batches = list(_batch_iterator_for_type("xml", str(path), 2))
    assert [len(b) for b in batches] == [2, 1]
    assert batches[1][0]["name"] == "zo\u00eb"


def test_bytes_payload_still_measured(tmp_path: Path) -> None:
    _headers, _schema, total, _sample = peek_file_source(
        DOC.encode("utf-8"), "rows.xml"
    )
    assert total == 3
