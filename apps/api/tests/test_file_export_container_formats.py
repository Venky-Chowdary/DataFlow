"""A container export is the container the destination asked for.

A database source has no file format of its own, so the old guard
``can_convert(source_format, fmt)`` was false for every DB → avro/orc/xml
route and the writer fell through to the JSON branch: 29 MB of JSON bytes
landed under an ``.avro`` name, the run reported success, and every Avro
reader refused the file (``read length must be non-negative or -1``).
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_file  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

RECORDS = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
COLUMNS = ["id", "name"]
TYPES = {"id": "INTEGER", "name": "TEXT"}

MAGIC = {
    "avro": b"Obj\x01",
    "orc": b"ORC",
    "parquet": b"PAR1",
    "excel": b"PK\x03\x04",
    "xml": b"<?xml",
}


def _export(fmt: str, records: list[dict], *, source_format: str = "postgresql"):
    endpoint = EndpointConfig(kind="file_export", format=fmt, table="t")
    return write_destination_file(
        endpoint,
        records,
        COLUMNS,
        source_format=source_format,
        column_types=TYPES,
    )


@pytest.mark.parametrize("fmt", sorted(MAGIC))
def test_database_source_writes_the_declared_container(fmt: str) -> None:
    content, filename, summary = _export(fmt, RECORDS)
    assert content.startswith(MAGIC[fmt]), f"{fmt} export is not a {fmt} file"
    assert not filename.endswith(".json")
    assert summary["mime"] != "application/json"


@pytest.mark.parametrize("fmt", sorted(MAGIC))
def test_an_empty_population_is_still_that_container(fmt: str) -> None:
    content, _, _ = _export(fmt, [])
    assert content.startswith(MAGIC[fmt])


def test_avro_export_reads_back_record_for_record() -> None:
    import fastavro

    content, _, _ = _export("avro", RECORDS)
    read = list(fastavro.reader(io.BytesIO(content)))
    assert [r["name"] for r in read] == ["a", "b"]
    assert len(read) == len(RECORDS)


def test_unsupported_format_refuses_instead_of_landing_json() -> None:
    with pytest.raises(ValueError, match="not supported"):
        _export("toml", RECORDS)


def test_yaml_export_is_yaml_not_json() -> None:
    content, filename, summary = _export("yaml", RECORDS)
    assert filename.endswith(".yaml")
    assert summary["mime"] == "application/yaml"
    assert content.startswith(b"- ")
    assert not content.strip().startswith(b"[")
    from services.yaml_tabular import count_yaml_records, iter_yaml_dicts

    assert count_yaml_records(content) == 2
    assert [r["name"] for r in iter_yaml_dicts(content)] == ["a", "b"]


def test_empty_yaml_export_is_still_yaml() -> None:
    content, filename, _ = _export("yaml", [])
    assert filename.endswith(".yaml")
    assert content == b"[]\n"


@pytest.mark.parametrize(
    ("fmt", "head"),
    [("csv", b"id,name"), ("tsv", b"id\tname"), ("jsonl", b'{"id"'), ("json", b"[")],
)
def test_text_formats_keep_their_own_writers(fmt: str, head: bytes) -> None:
    content, filename, _ = _export(fmt, RECORDS)
    assert content.startswith(head)
    assert filename.endswith(f".{fmt}")
