"""YAML and fixed-width are transfer-live file sources, not catalog theatre.

YAML 1.1 must not coerce ``yes``/``on`` into booleans. Fixed-width must not
guess column widths. Nested YAML and undeclared layouts fail closed.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.file_parser import FileParser, detect_format  # noqa: E402
from services.fixed_width_layout import (  # noqa: E402
    FixedWidthError,
    count_fixed_width_records,
    iter_fixed_width_dicts,
    layout_header_line,
)
from services.read_options import ReadOptions  # noqa: E402
from services.yaml_tabular import (  # noqa: E402
    YAMLTabularError,
    count_yaml_records,
    iter_yaml_dicts,
)
from src.transfer.registry import PRODUCTION_SKU, validate_transfer  # noqa: E402

YAML_TWO = b"""- id: "1"
  amount: "1000.00"
  flag: yes
- id: "2"
  amount: "2000.50"
  flag: no
"""

FWF_TWO = (
    layout_header_line((("id", 8), ("amount", 16)))
    + "\n"
    + "1".ljust(8)
    + "1000.00".ljust(16)
    + "\n"
    + "2".ljust(8)
    + "2000.50".ljust(16)
    + "\n"
).encode()


def test_detect_yaml_and_fwf_extensions() -> None:
    assert detect_format("ledger.yaml", b"- a: 1\n") == "yaml"
    assert detect_format("ledger.yml", b"- a: 1\n") == "yaml"
    assert detect_format("ledger.fwf", b"#layout: id:1\n1\n") == "fixed_width"
    assert FileParser.detect_file_type("ledger.yaml") == "yaml"
    assert FileParser.detect_file_type("ledger.fwf") == "fixed_width"


def test_txt_is_not_silently_fixed_width() -> None:
    assert detect_format("notes.txt", b"hello world\n") != "fixed_width"
    assert FileParser.detect_file_type("notes.txt", b"hello world\n") != "fixed_width"


def test_yaml_sequence_keeps_yes_as_text() -> None:
    rows = list(iter_yaml_dicts(YAML_TWO))
    assert len(rows) == 2
    assert rows[0]["flag"] == "yes"
    assert rows[1]["flag"] == "no"
    assert rows[0]["amount"] == "1000.00"
    assert count_yaml_records(YAML_TWO) == 2


def test_yaml_wrapper_list_is_the_population() -> None:
    body = b"records:\n  - id: '1'\n    amount: '10.00'\n  - id: '2'\n    amount: '20.00'\n"
    rows = list(iter_yaml_dicts(body))
    assert [r["id"] for r in rows] == ["1", "2"]
    assert count_yaml_records(body) == 2


def test_yaml_nested_cell_is_refused() -> None:
    nested = b"- id: '1'\n  extra:\n    inner: 2\n"
    with pytest.raises(YAMLTabularError, match="nested"):
        list(iter_yaml_dicts(nested))
    assert count_yaml_records(nested) is None


def test_yaml_alias_is_refused() -> None:
    aliased = b"- &row\n  id: '1'\n- *row\n"
    with pytest.raises(YAMLTabularError, match="alias"):
        list(iter_yaml_dicts(aliased))


def test_yaml_duplicate_key_is_refused() -> None:
    dup = b"- id: '1'\n  id: '2'\n"
    with pytest.raises(YAMLTabularError, match="repeats"):
        list(iter_yaml_dicts(dup))


def test_yaml_routes_are_live() -> None:
    ok, msg = validate_transfer("file", "yaml", "database", "postgresql")
    assert ok, msg
    ok, msg = validate_transfer("file", "yaml", "database", "sqlite")
    assert ok, msg
    assert ("file", "yaml", "database", "postgresql") in PRODUCTION_SKU


def test_fixed_width_needs_a_layout() -> None:
    with pytest.raises(FixedWidthError, match="declared layout"):
        list(iter_fixed_width_dicts(b"12345678\n"))
    assert count_fixed_width_records(b"12345678\n") is None


def test_fixed_width_header_layout_round_trip() -> None:
    rows = list(iter_fixed_width_dicts(FWF_TWO))
    assert rows == [
        {"id": "1", "amount": "1000.00"},
        {"id": "2", "amount": "2000.50"},
    ]
    assert count_fixed_width_records(FWF_TWO) == 2


def test_fixed_width_short_line_is_refused() -> None:
    body = layout_header_line((("id", 8), ("amount", 16))) + "\nshort\n"
    with pytest.raises(FixedWidthError, match="exactly 24"):
        list(iter_fixed_width_dicts(body.encode()))


def test_fixed_width_sidecar_layout(tmp_path: Path) -> None:
    path = tmp_path / "rows.fwf"
    path.write_text("1".ljust(8) + "1000.00".ljust(16) + "\n", encoding="utf-8")
    sidecar = Path(str(path) + ".layout.json")
    sidecar.write_text(
        json.dumps([{"name": "id", "width": 8}, {"name": "amount", "width": 16}]),
        encoding="utf-8",
    )
    rows = list(iter_fixed_width_dicts(path))
    assert rows == [{"id": "1", "amount": "1000.00"}]


def test_fixed_width_layout_mismatch_fails_closed() -> None:
    opts = ReadOptions(fixed_width_layout=(("id", 4), ("amount", 8)))
    with pytest.raises(FixedWidthError, match="disagrees"):
        list(iter_fixed_width_dicts(FWF_TWO, opts.fixed_width_layout))


def test_fixed_width_routes_are_live() -> None:
    ok, msg = validate_transfer("file", "fixed_width", "database", "postgresql")
    assert ok, msg
    assert ("file", "fixed_width", "database", "sqlite") in PRODUCTION_SKU


def test_file_parser_preview_yaml_and_fwf() -> None:
    yaml_parsed = FileParser.parse(YAML_TWO, "ledger.yaml")
    assert yaml_parsed.success, yaml_parsed.error
    assert yaml_parsed.row_count == 2
    assert yaml_parsed.data[0]["flag"] == "yes"

    fwf_parsed = FileParser.parse(FWF_TWO, "ledger.fwf")
    assert fwf_parsed.success, fwf_parsed.error
    assert fwf_parsed.row_count == 2
    assert fwf_parsed.columns == ["id", "amount"]


def test_yaml_to_sqlite_dest_count(tmp_path: Path) -> None:
    import sqlite3

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    db = tmp_path / "ledger.db"
    body = b'- id: "1"\n  amount: "1000.00"\n- id: "2"\n  amount: "2000.50"\n'
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="yaml"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(db), table="ledger"
        ),
        source_content=body,
        source_filename="ledger.yaml",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ],
    )
    result = UniversalTransferEngine().execute_tracked(request, "yaml-sqlite")
    assert result.success, getattr(result, "error", result)
    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        amounts = [row[0] for row in conn.execute("SELECT amount FROM ledger ORDER BY id")]
    finally:
        conn.close()
    assert n == 2
    assert [str(a) for a in amounts] == ["1000.00", "2000.50"]


def test_fixed_width_to_sqlite_dest_count(tmp_path: Path) -> None:
    import sqlite3

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    db = tmp_path / "ledger.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="fixed_width"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(db), table="ledger"
        ),
        source_content=FWF_TWO,
        source_filename="ledger.fwf",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "amount", "target": "amount"},
        ],
    )
    result = UniversalTransferEngine().execute_tracked(request, "fwf-sqlite")
    assert result.success, getattr(result, "error", result)
    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
    finally:
        conn.close()
    assert n == 2
