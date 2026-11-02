"""JSON tabular unwrap — preview and stream must accept the same shapes."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.json_tabular import (  # noqa: E402
    detect_ijson_records_prefix,
    extract_json_records,
    load_json_records,
)
from services.error_handling import humanize_transfer_failure  # noqa: E402


def test_extract_root_array():
    rows = extract_json_records([{"id": 1}, {"id": 2}])
    assert len(rows) == 2
    assert rows[0]["id"] == 1


def test_extract_countries_wrapper():
    rows = extract_json_records({"countries": [{"name": "India"}, {"name": "USA"}], "count": 2})
    assert len(rows) == 2
    assert rows[0]["name"] == "India"


def test_extract_geojson_features():
    rows = extract_json_records(
        {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "properties": {"name": "A"}, "geometry": None},
                {"type": "Feature", "properties": {"name": "B"}, "geometry": None},
            ],
        }
    )
    assert len(rows) == 2
    assert rows[0]["type"] == "Feature"


def test_extract_single_object_row():
    rows = extract_json_records({"name": "solo", "code": "X"})
    assert rows == [{"name": "solo", "code": "X"}]


def test_extract_array_of_scalars_fails():
    with pytest.raises(ValueError, match="objects"):
        extract_json_records([1, 2, 3])


def test_detect_prefix_root_array():
    assert detect_ijson_records_prefix(b'[{"a":1}]') == "item"


def test_detect_prefix_wrapper():
    assert detect_ijson_records_prefix(b'{"countries":[{"a":1}]}') == "countries.item"


def test_load_and_stream_batches_match():
    from services.json_tabular import iter_json_record_dicts
    import io

    payload = json.dumps({"data": [{"x": i} for i in range(5)]}).encode()
    loaded = load_json_records(payload)
    assert len(loaded) == 5

    batches = list(iter_json_record_dicts(lambda c: io.BytesIO(c), payload, chunk_size=2))
    flat = [r for b in batches for r in b]
    assert [r["x"] for r in flat] == list(range(5))


def test_extract_nested_envelope():
    rows = extract_json_records(
        {"response": {"meta": {"ok": True}, "data": [{"id": 1}, {"id": 2}]}}
    )
    assert [r["id"] for r in rows] == [1, 2]


def test_file_paths_agree_for_any_destination(tmp_path: Path):
    """Peek + batch + FileParser must unwrap the same — dest (Redis/SF/…) is irrelevant."""
    import io

    from services.file_parser import FileParser
    from services.json_tabular import iter_json_record_dicts
    from src.transfer.file_stream import peek_file_source

    payload = {
        "countries": [
            {"name": "India", "code": "IN"},
            {"name": "USA", "code": "US"},
        ]
    }
    raw = json.dumps(payload).encode()
    path = tmp_path / "countries.json"
    path.write_bytes(raw)

    parsed = FileParser.parse(raw, "countries.json")
    assert parsed.success
    assert parsed.row_count == 2

    headers, _schema, total, sample = peek_file_source(raw, "countries.json")
    assert total == 2
    assert "name" in headers
    assert len(sample) == 2

    batches = list(iter_json_record_dicts(lambda c: io.BytesIO(c), raw, chunk_size=10))
    flat = [r for b in batches for r in b]
    assert len(flat) == 2

    # Path-based streaming (same as Redis/Snowflake file jobs) must also unwrap.
    headers2, _, total2, _ = peek_file_source(str(path), "countries.json")
    assert total2 == 2
    assert set(headers2) >= {"name", "code"}


def test_humanize_json_shape_error():
    human = humanize_transfer_failure(ValueError("JSON file must be an array of objects"))
    assert human["code"] == "json_shape_unsupported"
    assert human["confidence"] == "high"
    assert "wrapper" in human["fix"].lower() or "[{...}]" in human["fix"]


def test_count_json_records_unique_path_not_ingest_document_or_guess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Dest COUNT streams the unique array-of-object. Ingest fallbacks stay ingest.

    extract_json_records treats a lone object as one row and ranks preferred
    wrappers. Dest COUNT does neither. Sibling collections stay unmeasured.
    """
    from services.json_tabular import count_json_records

    assert count_json_records(b'[{"id":1},{"id":2},{"id":3}]') == 3
    assert count_json_records(b"[]") == 0
    assert count_json_records(b'{"records":[{"id":1}]}') == 1
    assert count_json_records(b'{"records":[]}') == 0
    assert count_json_records(b'{"note":{"to":"T","from":"F"}}') is None
    assert count_json_records(b"[1,2,3]") is None
    assert count_json_records(b"not-json") is None
    sibling = (
        b'{"orders":[{"id":1},{"id":2}],"items":[{"id":"a"},{"id":"b"}]}'
    )
    assert count_json_records(sibling) is None
    nested = (
        b'{"records":['
        b'{"id":1,"items":[{"sku":"a"},{"sku":"b"}]},'
        b'{"id":2,"items":[{"sku":"c"},{"sku":"d"}]},'
        b'{"id":3,"items":[{"sku":"e"},{"sku":"f"}]}'
        b"]}"
    )
    assert count_json_records(nested) == 3

    wide = tmp_path / "wide.json"
    with wide.open("w", encoding="utf-8") as handle:
        handle.write("[")
        for i in range(5000):
            if i:
                handle.write(",")
            handle.write(f'{{"id":{i}}}')
        handle.write("]")
    assert count_json_records(wide) == 5000

    pytest.importorskip("ijson")

    def _dom_forbidden(*_a, **_k):
        raise AssertionError("json.loads must not run when ijson COUNT is available")

    monkeypatch.setattr("json.loads", _dom_forbidden)
    assert count_json_records(nested) == 3


def test_iter_json_dicts_unique_path_not_ingest_or_inner_items():
    """Gate-8 emits COUNT's unique path. Ingest ranking and inner lists stay ingest."""
    pytest.importorskip("ijson")
    from services.json_tabular import iter_json_dicts

    rows = list(iter_json_dicts(b'[{"id":1},{"id":2},{"id":3}]'))
    assert [r["id"] for r in rows] == [1, 2, 3]
    wrapped = list(iter_json_dicts(b'{"records":[{"id":1},{"id":2},{"id":3}]}'))
    assert [r["id"] for r in wrapped] == [1, 2, 3]
    assert list(iter_json_dicts(b"[]")) == []
    assert list(iter_json_dicts(b'{"records":[]}')) == []
    nested = (
        b'{"records":['
        b'{"id":1,"items":[{"sku":"a"},{"sku":"b"}]},'
        b'{"id":2,"items":[{"sku":"c"},{"sku":"d"}]},'
        b'{"id":3,"items":[{"sku":"e"},{"sku":"f"}]}'
        b"]}"
    )
    nested_rows = list(iter_json_dicts(nested))
    assert [r["id"] for r in nested_rows] == [1, 2, 3]
    assert all("items" in r for r in nested_rows)

    from services.dest_precount import UnmeasuredArtifact

    sibling = (
        b'{"orders":[{"id":1},{"id":2}],"items":[{"id":"a"},{"id":"b"}]}'
    )
    with pytest.raises(UnmeasuredArtifact):
        list(iter_json_dicts(sibling))
    with pytest.raises(UnmeasuredArtifact):
        list(iter_json_dicts(b'{"note":{"to":"T","from":"F"}}'))
    with pytest.raises(UnmeasuredArtifact):
        list(iter_json_dicts(b"[1,2,3]"))
    with pytest.raises(UnmeasuredArtifact):
        list(iter_json_dicts(b"not-json"))

