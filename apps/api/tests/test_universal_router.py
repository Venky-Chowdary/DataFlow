"""Tests for universal route scoring."""

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from services.universal_router import analyze_route  # noqa: E402


def test_file_csv_to_mongodb_supported():
    r = analyze_route("file", "csv", "database", "mongodb")
    assert r["supported"] is True
    assert r["operation"] == "upload"
    assert r["score"] >= 90


def test_file_csv_to_json_export_convert():
    r = analyze_route("file", "csv", "file_export", "json")
    assert r["supported"] is True
    assert r["operation"] == "convert"
    assert r["conversion_needed"] is True
    assert r["conversion_supported"] is True


def test_unsupported_route_has_alternatives():
    r = analyze_route("file", "csv", "file_export", "protobuf")
    assert r["supported"] is False
    assert isinstance(r.get("alternatives"), list)
