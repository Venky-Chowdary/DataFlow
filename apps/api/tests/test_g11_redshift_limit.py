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


def test_postgresql_writer_names_redshift_columns_by_the_redshift_limit():
    """Behaviour: the writer's resolved columns, Redshift vs PostgreSQL engine."""
    base = dict(
        host="h", port=5439, database="d", username="u", password="p", schema="public",
        connection_string="", ssl=False, table_name="t", headers=["v"],
        data_rows=[["1"]], mappings=[{"source": "v", "target": "r" * 100}],
        column_types={"v": "INTEGER"}, create_table=True,
    )
    shift = _writer_target_cols("connectors.postgresql_writer", {**base, "engine": "redshift"},
                                "get_connection")
    pg = _writer_target_cols("connectors.postgresql_writer", {**base, "engine": "postgresql"},
                             "get_connection")
    assert shift == ["r" * 100]
    assert pg == ["r" * 63]


class _Stop(RuntimeError):
    """Raised where the writer would first touch a database."""


def _writer_target_cols(
    module: str, call_kwargs: dict, stop_at: str, offline: dict | None = None
) -> list[str]:
    """The target columns a writer resolved, stopped before it touches a database.

    ``offline`` replaces module attributes that would connect before the
    columns are resolved (name -> return value).
    """
    import contextlib
    import importlib
    from unittest.mock import patch

    mod = importlib.import_module(module)
    seen: list[list[str]] = []
    real = mod.resolve_target_columns

    def spy(*args, **kwargs):
        cols, types = real(*args, **kwargs)
        seen.append(list(cols))
        return cols, types

    def stop(*_a, **_k):
        raise _Stop("stop before connecting")

    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(mod, "resolve_target_columns", side_effect=spy))
        stack.enter_context(patch.object(mod, stop_at, side_effect=stop))
        for name, value in (offline or {}).items():
            stack.enter_context(patch.object(mod, name, return_value=value))
        try:
            mod.write_mapped_rows(**call_kwargs)
        except _Stop:
            pass
    assert seen, "writer did not resolve target columns"
    return seen[0]
