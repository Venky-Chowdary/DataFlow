"""G3 schema contract tests with expanded lossy coercion detection."""

from __future__ import annotations

import sys
from pathlib import Path

_PREFLIGHT_ROOT = Path(__file__).resolve().parents[2] / "packages" / "preflight" / "src"
_API_ROOT = Path(__file__).resolve().parents[2] / "apps" / "api"
if str(_PREFLIGHT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_ROOT))
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from preflight.gates import gate_g3_schema_contract  # noqa: E402
from preflight.models import ColumnMapping, ColumnSchema, DestinationConfig, PreflightContext, SourceConfig, TransferPlan  # noqa: E402


def _ctx(source_types: dict[str, str], dest_types: dict[str, str], mappings: list[tuple[str, str]]):
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name=s, inferred_type=t) for s, t in source_types.items()],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[ColumnSchema(name=t, inferred_type=dt) for t, dt in dest_types.items()],
        ),
        mappings=[
            ColumnMapping(source=s, target=t, confidence=0.95) for s, t in mappings
        ],
    )
    return PreflightContext(plan=plan)


def test_g3_blocks_varchar_to_integer():
    result = gate_g3_schema_contract(
        _ctx(
            {"amount": "VARCHAR"},
            {"amount": "INTEGER"},
            [("amount", "amount")],
        )
    )
    assert result.status.value == "block"


def test_g3_allows_integer_to_varchar():
    result = gate_g3_schema_contract(
        _ctx(
            {"id": "INTEGER"},
            {"id": "VARCHAR"},
            [("id", "id")],
        )
    )
    assert result.status.value == "pass"


def test_g3_blocks_decimal_to_integer():
    result = gate_g3_schema_contract(
        _ctx(
            {"qty": "DECIMAL"},
            {"qty": "INTEGER"},
            [("qty", "qty")],
        )
    )
    assert result.status.value == "block"
