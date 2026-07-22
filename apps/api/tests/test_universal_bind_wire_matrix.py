"""Universal bind/wire matrix — every top connector × every logical type.

Always-on (no live DBs required). Proves the class of bug that hit Transfer Studio:
ISO ``…T…Z`` labeled as TEXT still binds safely when destination DDL is temporal,
and every DDL destination has an entry for every logical type.

Writes ``apps/api/data/proofs/universal_bind_wire_matrix.json`` with pass/fail counts.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_writer import (  # noqa: E402
    _apply_physical_temporal_types,
    _to_mysql_value,
)
from connectors.sqlite_writer import _to_sqlite_value  # noqa: E402
from connectors.sql_temporal import (  # noqa: E402
    coerce_sql_temporal,
    format_wire_value,
    is_temporal_ddl,
    sql_base_type,
    wire_check_temporal,
)
from connectors.warehouse_temporal import (  # noqa: E402
    format_bigquery_bind,
    format_snowflake_bind,
)
from connectors.writer_common import normalize_temporal_cells  # noqa: E402
from services.type_system import DDL_TYPES, ddl_type  # noqa: E402
from tests.universal_bind_samples import (  # noqa: E402
    ALL_LOGICALS,
    ISO_Z_DATETIME,
    SAMPLES,
    TOP_CONNECTOR_DESTS,
)

_PROOF = _API_ROOT / "data" / "proofs" / "universal_bind_wire_matrix.json"

# Destinations that reject ISO-Z string literals for temporal DDL (SQL bind class).
_SQL_BIND_ENGINES = frozenset({
    "postgresql",
    "mysql",
    "sqlserver",
    "oracle",
    "redshift",
    "sqlite",
    "generic_sql",
    "duckdb",
    "clickhouse",
    "trino",
    "presto",
    "databricks",
    "snowflake",
    "bigquery",
})


def _looks_like_iso_z_literal(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    raw = value.strip()
    if "T" not in raw:
        return False
    return raw.endswith(("Z", "z")) or "+" in raw[10:] or raw.count("-") >= 3 and ":" in raw


def _bind_value(ddl_key: str, ddl: str, value: Any) -> Any:
    """Route through the same helpers writers use for this destination family."""
    if value is None:
        return None
    eng = ddl_key

    if eng == "mysql":
        return _to_mysql_value(value, ddl)

    if eng == "sqlite":
        # SQLite stores temporals as TEXT wire after coerce.
        return _to_sqlite_value(value, sql_base_type(ddl) or ddl)

    if eng == "snowflake":
        if is_temporal_ddl(ddl):
            return format_snowflake_bind(value, ddl)
        return value

    if eng == "bigquery":
        if is_temporal_ddl(ddl) or sql_base_type(ddl) == "INTERVAL":
            return format_bigquery_bind(value, ddl)
        return value

    if eng in _SQL_BIND_ENGINES and is_temporal_ddl(ddl):
        rows = normalize_temporal_cells([(value,)], [ddl], engine=eng)
        return rows[0][0] if rows else value

    # Document / KV / search: string wire is acceptable; still normalize when
    # shared temporal helpers apply via logical datetime labels.
    if is_temporal_ddl(ddl) or ddl.lower() in {"date", "datetime", "timestamp"}:
        return coerce_sql_temporal(value, ddl if is_temporal_ddl(ddl) else "DATETIME")
    return value


def _assert_temporal_safe(bound: Any, *, ddl: str, sample: Any, dest: str) -> None:
    """Temporal DDL must not leave MySQL-1292-class ISO-Z string literals."""
    if not is_temporal_ddl(ddl) and sql_base_type(ddl) not in {
        "DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ",
        "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
    }:
        return
    if bound is None:
        return
    # Accept native temporals or space-separated / date-only wire strings.
    if isinstance(bound, (datetime, date, time)):
        return
    if isinstance(bound, str):
        raw = bound.strip()
        # Allowed: "2026-07-04 06:57:37", "2026-07-04", "06:57:37", BQ RFC3339 Z for TIMESTAMP
        if dest in {"bigquery"} and sql_base_type(ddl) == "TIMESTAMP":
            # BQ TIMESTAMP wire is intentionally RFC3339 UTC (may end with Z).
            assert "T" in raw or " " in raw, (dest, ddl, sample, bound)
            return
        assert not (
            "T" in raw and (raw.endswith(("Z", "z")) or ("+" in raw[10:] and "T" in raw))
        ), (
            f"{dest} {ddl}: ISO-Z/offset literal survived bind: "
            f"sample={sample!r} bound={bound!r}"
        )
        return
    # Unexpected type — still fail closed for SQL engines.
    if dest in _SQL_BIND_ENGINES:
        raise AssertionError(
            f"{dest} {ddl}: unexpected bound type {type(bound).__name__}: {bound!r}"
        )


@pytest.mark.parametrize("catalog,ddl_key", TOP_CONNECTOR_DESTS)
@pytest.mark.parametrize("logical", ALL_LOGICALS)
def test_ddl_entry_exists_for_top_connector(catalog: str, ddl_key: str, logical: str):
    assert ddl_key in DDL_TYPES, f"missing DDL map for {ddl_key} (catalog={catalog})"
    ddl = ddl_type(ddl_key, logical)
    assert ddl, f"{catalog}/{ddl_key}: empty DDL for logical={logical}"
    assert logical in DDL_TYPES[ddl_key], (
        f"{catalog}/{ddl_key}: logical {logical} missing from DDL_TYPES"
    )


@pytest.mark.parametrize("catalog,ddl_key", TOP_CONNECTOR_DESTS)
@pytest.mark.parametrize("logical", ALL_LOGICALS)
def test_bind_samples_for_top_connector(catalog: str, ddl_key: str, logical: str):
    ddl = ddl_type(ddl_key, logical)
    samples = SAMPLES.get(logical) or [None]
    for sample in samples:
        bound = _bind_value(ddl_key, ddl, sample)
        if logical in {"datetime", "date", "time"} or is_temporal_ddl(ddl):
            _assert_temporal_safe(bound, ddl=ddl, sample=sample, dest=ddl_key)
        # NULL identity when sample is None (not in list, but keep contract).
        if sample is None:
            assert bound is None


def test_mysql_physical_override_fixes_text_labeled_iso_z():
    """Exact Transfer Studio failure: mapping said TEXT, column is DATETIME."""
    types = _apply_physical_temporal_types(
        ["last_updated"],
        ["TEXT"],
        {"last_updated": "DATETIME(6)"},
    )
    assert sql_base_type(types[0]) == "DATETIME"
    bound = _to_mysql_value(ISO_Z_DATETIME, types[0])
    assert isinstance(bound, datetime), bound
    assert bound == datetime(2026, 7, 4, 6, 57, 37)


def test_wire_check_flags_iso_z_for_mysql_datetime():
    check = wire_check_temporal(ISO_Z_DATETIME, "DATETIME(6)")
    assert check["ok"] is True
    assert check["needs_normalize"] is True
    wire = check["wire_value"] or format_wire_value(ISO_Z_DATETIME, "DATETIME(6)")
    assert wire is not None
    assert "T" not in wire and not wire.endswith("Z")


def test_write_universal_bind_wire_proof():
    """Enumerate all cells; write pass/fail/skip proof artifact."""
    cells: list[dict[str, Any]] = []
    passed = failed = 0
    for catalog, ddl_key in TOP_CONNECTOR_DESTS:
        for logical in ALL_LOGICALS:
            ddl = ddl_type(ddl_key, logical)
            sample_results: list[dict[str, Any]] = []
            cell_ok = True
            err = ""
            for sample in SAMPLES.get(logical) or []:
                try:
                    bound = _bind_value(ddl_key, ddl, sample)
                    if is_temporal_ddl(ddl) or logical in {
                        "date", "datetime", "time",
                    }:
                        _assert_temporal_safe(
                            bound, ddl=ddl, sample=sample, dest=ddl_key
                        )
                    sample_results.append({
                        "sample": repr(sample)[:80],
                        "bound_type": type(bound).__name__,
                        "bound": repr(bound)[:80],
                        "ok": True,
                    })
                except Exception as exc:  # noqa: BLE001 — collect matrix failures
                    cell_ok = False
                    err = str(exc)[:240]
                    sample_results.append({
                        "sample": repr(sample)[:80],
                        "ok": False,
                        "error": err,
                    })
            if cell_ok:
                passed += 1
                status = "PASS"
            else:
                failed += 1
                status = "FAIL"
            cells.append({
                "catalog": catalog,
                "ddl_key": ddl_key,
                "logical": logical,
                "ddl": ddl,
                "status": status,
                "error": err,
                "samples": sample_results[:3],  # keep artifact small
            })

    proof = {
        "title": "Universal bind/wire matrix — top connectors × logical types",
        "destinations": len(TOP_CONNECTOR_DESTS),
        "logicals": len(ALL_LOGICALS),
        "cells": len(cells),
        "passed": passed,
        "failed": failed,
        "skipped": 0,
        "honesty": (
            "Bind/wire only — not live transfer round-trip. "
            "Live typed e2e is separate (typed_fidelity_transfer_matrix)."
        ),
        "results": cells,
    }
    _PROOF.parent.mkdir(parents=True, exist_ok=True)
    _PROOF.write_text(json.dumps(proof, indent=2, default=str) + "\n", encoding="utf-8")
    assert failed == 0, (
        f"bind/wire matrix failures={failed}; see {_PROOF}. "
        f"First fails: {[c for c in cells if c['status']=='FAIL'][:5]}"
    )
    assert passed == len(TOP_CONNECTOR_DESTS) * len(ALL_LOGICALS)
