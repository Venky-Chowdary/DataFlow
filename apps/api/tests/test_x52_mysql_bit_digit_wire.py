"""X-52: MySQL BIT(n) travels as its n-char '0'/'1' digit string — never base64.

Hermetic cells run in the suite; live legs (MySQL BIT(1)/BIT(8)/BIT(64) ->
mssql/sqlite/postgres/duckdb, clean exact + tamper) skip-gate on
DATAFLOW_X52_LIVE + DATAFLOW_TEST_MYSQL_* env.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_reader import (  # noqa: E402
    _bit_widths_from_types,
    _mysql_select_list,
    mysql_bit_projection,
)
from connectors.sql_bind import coerce_bitstring_wire  # noqa: E402
from services.reconciliation import fingerprint_for_reconcile  # noqa: E402


def test_x52_select_list_projects_bit_as_digit_string():
    sel = _mysql_select_list(
        None, [("id", "int"), ("t", "bit(8)"), ("b", "bit(1)"), ("v", "bit(64)")]
    )
    assert sel is not None
    assert "LPAD(BIN(`t`), 8, '0')" in sel
    assert "LPAD(BIN(`b`), 1, '0')" in sel
    assert "LPAD(BIN(`v`), 64, '0')" in sel
    # NULL stays NULL (the CASE guard) — never the digit string '0'*n.
    assert sel.count("CASE WHEN") == 3
    assert "`id`" in sel


def test_x52_bit_widths_parsed_from_column_type():
    types = [("a", "bit(8)"), ("b", "bit(64)"), ("c", "bit"), ("d", "varchar(9)")]
    assert _bit_widths_from_types(types) == {"a": 8, "b": 64, "c": 1}


def test_x52_projection_sql_shape():
    out = mysql_bit_projection("`t`", 8)
    assert out == (
        "CASE WHEN `t` IS NULL THEN NULL ELSE LPAD(BIN(`t`), 8, '0') END AS `t`"
    )


def test_x52_digit_wire_binds_and_base64_still_refused():
    assert coerce_bitstring_wire("00000101", width=8) == "00000101"
    with pytest.raises(ValueError):
        coerce_bitstring_wire("BQ==", width=8)  # the old base64 wire


def test_x52_fingerprint_equates_digit_and_value_forms():
    # PG bit(8) read-back '00000101', int 5 and raw bytes 0x05 are one value.
    assert fingerprint_for_reconcile(
        "00000101", ddl_type="bit(8)", engine="postgres"
    ) == fingerprint_for_reconcile(5, ddl_type="bit(8)", engine="postgres")
    assert fingerprint_for_reconcile(
        "00000101", ddl_type="bit(8)", engine="postgres"
    ) != fingerprint_for_reconcile("00000110", ddl_type="bit(8)", engine="postgres")


# --- live legs (skip-gated; Venkatesh's engine bench) -----------------------

_LIVE = pytest.mark.skipif(
    os.environ.get("DATAFLOW_X52_LIVE") != "1", reason="DATAFLOW_X52_LIVE=1 for live legs"
)


@_LIVE
@pytest.mark.parametrize("dest_engine", ["mssql", "sqlite", "postgresql", "duckdb"])
@pytest.mark.parametrize("bit_width", [1, 8, 64])
@pytest.mark.parametrize("tamper", ["none", "value"])
def test_x52_mysql_bit_to_dest(dest_engine, bit_width, tamper, monkeypatch, tmp_path):
    """MySQL BIT(w) -> {mssql, sqlite, postgres, duckdb}: clean = verified with
    exact digit-string values on read-back (never base64); tamper detected."""
    from tests import test_x50_json_null_semantics_live as x50

    src_cfg = x50._cfg("DATAFLOW_TEST_MYSQL", 3306)
    if not src_cfg or not x50._reachable(src_cfg["host"], src_cfg["port"]):
        pytest.skip("set DATAFLOW_TEST_MYSQL_* (reachable)")
    if dest_engine in x50._DEST_ENVS:
        prefix, port = x50._DEST_ENVS[dest_engine]
        dest_cfg = x50._cfg(prefix, port)
        if not dest_cfg or not x50._reachable(dest_cfg["host"], dest_cfg["port"]):
            pytest.skip(f"set {prefix}_* (reachable)")
    else:
        dest_cfg = {"database": str(tmp_path / f"x52_{dest_engine}_{bit_width}_{tamper}.db")}

    src_table = f"x52bit{bit_width}_{dest_engine}"
    dest_table = f"x52out_{dest_engine}_{bit_width}_{tamper}"
    maxv = (1 << bit_width) - 1

    conn = x50._mysql_conn(src_cfg)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")  # nosec B608
        cur.execute(
            f"CREATE TABLE `{src_table}` (id INT PRIMARY KEY, t BIT({bit_width}))"  # nosec B608
        )
        cur.execute(
            f"INSERT INTO `{src_table}` VALUES (1, 0), (2, {maxv}), (3, NULL), (4, 5)"  # nosec B608
        )
    finally:
        conn.close()

    tampered = {"done": False}
    orig_recon = x50.engine_mod.run_reconciliation

    def _tampering_recon(*args, **kwargs):
        if tamper != "none" and not tampered["done"]:
            x50._tamper_value(dest_engine, dest_cfg, dest_table)
            tampered["done"] = True
        return orig_recon(*args, **kwargs)

    monkeypatch.setattr(x50.engine_mod, "run_reconciliation", _tampering_recon)
    try:
        result = x50._run_job(
            "mysql", src_cfg, src_table, dest_engine, dest_cfg, dest_table,
            f"1111111111111111111111{dest_engine[:2]}{bit_width}{tamper[:2]}",
        )
    finally:
        conn = x50._mysql_conn(src_cfg)
        try:
            conn.autocommit = True
            conn.cursor().execute(f"DROP TABLE IF EXISTS `{src_table}`")  # nosec B608
        finally:
            conn.close()

    if tamper == "none":
        assert result.success is True, (
            f"clean MySQL BIT({bit_width}) -> {dest_engine} must pass: "
            f"{getattr(result, 'error', '') or getattr(result, 'message', '')}"
        )
        # The wire must never be base64 ('BQ==' family) — verifier compares
        # the digit string (or decoded value) cell-for-cell.
        dest_text = str(getattr(result, "dest_summary", "") or "")
        assert "BQ==" not in dest_text and "AA==" not in dest_text
        return
    assert tampered["done"], "tamper hook never fired"
    assert result.success is False, (
        f"tamper on MySQL BIT({bit_width}) -> {dest_engine} must fail: "
        f"{getattr(result, 'message', '')}"
    )
