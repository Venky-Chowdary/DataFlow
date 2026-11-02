"""Local gzip XML dest COUNT is measured, not None.

Without defusedxml the ImportError fallback used Path.read_text on
compressed bytes (UnicodeDecodeError → unmeasured). Gzip CSV/JSONL
already streamed. XML must use the same dest-count byte source.
"""

from __future__ import annotations

import gzip
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.dest_precount import count_artifact_rows  # noqa: E402
from services.file_parser import _xml_count_as_text, count_xml_records  # noqa: E402
from services.format_converter import convert_rows  # noqa: E402


def _two_row_xml() -> bytes:
    body, _ = convert_rows(
        ["id", "v"],
        [["1", "a"], ["2", "b"]],
        source_format="csv",
        target_format="xml",
    )
    return body


def test_plain_xml_still_counts():
    body = _two_row_xml()
    assert count_xml_records(body) == 2


def test_gzip_xml_path_counts_two(tmp_path: Path):
    body = _two_row_xml()
    path = tmp_path / "export.xml.gz"
    path.write_bytes(gzip.compress(body))
    assert count_xml_records(path) == 2
    assert count_artifact_rows(path, fmt="xml") == 2


def test_gzip_fallback_text_is_decompressed_xml(tmp_path: Path):
    body = _two_row_xml()
    path = tmp_path / "export.xml.gz"
    path.write_bytes(gzip.compress(body))
    text = _xml_count_as_text(path)
    assert text is not None
    assert "<record>" in text
    assert text.count("<record>") == 2
    assert not text.startswith("\x1f\x8b")
