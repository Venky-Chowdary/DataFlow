"""G11: Redshift is named by its own limit, 127 bytes.

Redshift loads through the PostgreSQL writer, which named every Redshift
column with PostgreSQL's 63 (and Validate refused 64-127 byte names Redshift
stores). Redshift now has its own entry; PostgreSQL stays 63.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _maps(*targets: str) -> list[dict]:
    return [{"source": f"c{i}", "target": t, "confidence": 1.0} for i, t in enumerate(targets)]


def _gates(dest_type: str, targets, **kw) -> dict[str, dict]:
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_append", dest_type=dest_type, source_type="postgresql",
        source_kind="database", mappings=_maps(*targets),
        destination_table=kw.pop("destination_table", "t"), **kw,
    )
    return {g["id"]: g for g in gates if str(g.get("id", "")).startswith("g23")}


def test_redshift_limit_is_127_bytes():
    assert _gates("redshift", ["r" * 127]) == {}
    gates = _gates("redshift", ["r" * 128])
    assert "at most 127" in gates["g23c_target_names"]["message"]
    assert _gates("amazon_redshift", ["r" * 100]) == {}
    # PostgreSQL itself stays 63.
    assert "g23c_target_names" in _gates("postgresql", ["r" * 64])


def test_postgresql_writer_keeps_long_names_for_redshift_only():
    from connectors.destination_naming import writer_identifier

    assert writer_identifier("redshift")("r" * 100) == "r" * 100
    assert writer_identifier("postgresql")("r" * 100) == "r" * 63
    src = (_API_ROOT / "connectors" / "postgresql_writer.py").read_text(encoding="utf-8")
    call = src[src.index("target_cols, logical_types = resolve_target_columns("):][:400]
    assert "identifier=" in call, call


