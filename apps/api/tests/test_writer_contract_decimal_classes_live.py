"""A signed CAST_AND_CONTINUE / COERCE_NULL contract is honoured for every class.

Every value that does not fit the destination ``DECIMAL(10,2)`` — a fraction
too long, an integer part too long, both, NaN, infinity — is written as NULL
under a signed contract whose quarantine policy is ``COERCE_NULL``, and held out
(quarantined, never rounded, never NULLed) without one. The real writer of each
engine writes; the table is read back; and Gate-8's source replay
(``map_rows_for_fingerprint``) must predict exactly what landed — the same rows
and, cell by cell, the same fingerprint.

The two mismatches this pins (Parity at Scale's property tests found them):
the write-time type matrix held a value out under a COERCE_NULL contract (A3),
and a NaN into SQLite was NULLed by the writer while Gate-8 held it out (A2).

Engines: SQLite always; PostgreSQL, MySQL, MariaDB and SQL Server when
configured (``tests/parity_engines.py``). An unconfigured engine is not
collected; the ``parity-ce-matrix`` job requires every leg
(``test_matrix_job_has_every_leg`` in ``test_parity_ce_matrix.py``).
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402

VALUES = {
    1: Decimal("12.345"),          # fraction too long
    2: Decimal("123456789"),       # integer part too long
    3: Decimal("123456789.123"),   # both
    4: Decimal("NaN"),
    5: Decimal("Infinity"),
    6: Decimal("12.34"),           # fits
    7: None,
}
DECIMAL_DDL = {"sqlite": "NUMERIC(10,2)", "postgresql": "numeric(10,2)", "mysql": "DECIMAL(10,2)",
               "mariadb": "DECIMAL(10,2)", "sqlserver": "DECIMAL(10,2)"}
ENGINES = ["sqlite"] + [e for e in ("postgresql", "mysql", "mariadb", "sqlserver") if parity_engines.available(e)]


def _mapping(contract: bool) -> list[dict[str, Any]]:
    amt: dict[str, Any] = {"source": "amt", "target": "amt"}
    if contract:
        from services.migration_risk_contract import create_migration_risk_contract

        signed = create_migration_risk_contract(
            column="amt", source_type="DECIMAL(20,6)", destination_type="DECIMAL(10,2)",
            approved_by="qa@example.com", reason="accepted narrowing",
            execution_policy="CAST_AND_CONTINUE", quarantine_policy="COERCE_NULL",
        )
        amt["risk_contract"] = signed.to_dict()
    return [{"source": "id", "target": "id"}, amt]


def _wire(fn, *args, **kwargs):
    from services.transform_engine import reset_active_number_locale, set_active_number_locale

    token = set_active_number_locale("WIRE")  # a typed database source's number locale
    try:
        return fn(*args, **kwargs)
    finally:
        reset_active_number_locale(token)


def _write_and_read(engine: str, contract: bool, tmp_path: Path, monkeypatch) -> dict[int, Any]:
    table = f"psd1_dec_{uuid.uuid4().hex[:8]}"
    ddl = DECIMAL_DDL[engine]
    rows = [[k, v] for k, v in VALUES.items()]
    if engine == "sqlite":
        monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
        from connectors.sqlite_writer import write_mapped_rows

        path = tmp_path / f"{table}.db"
        conn = sqlite3.connect(path)
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, amt {ddl})")
        conn.commit()
        conn.close()
        kwargs = {"host": "", "port": 0, "database": str(path), "username": "", "password": "",
                  "schema": "", "connection_string": "", "ssl": False, "table_name": table}
    else:
        write_mapped_rows = parity_engines.writer(engine)
        conn = parity_engines.connect(engine)
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, amt {ddl})")
        conn.commit()
        conn.close()
        kwargs = parity_engines.writer_kwargs(engine, table)
    try:
        result = _wire(write_mapped_rows, **kwargs, headers=["id", "amt"], data_rows=rows,
                       mappings=_mapping(contract), column_types={"id": "INTEGER", "amt": ddl},
                       create_table=False, error_policy="quarantine")
        assert result.ok, result.error
        if engine == "sqlite":
            conn = sqlite3.connect(kwargs["database"])
            stored = dict(conn.execute(f"SELECT id, amt FROM {table}").fetchall())
            conn.close()
        else:
            conn = parity_engines.connect(engine)
            with conn.cursor() as cur:
                cur.execute(f"SELECT id, amt FROM {table}")
                stored = {int(r[0]): r[1] for r in cur.fetchall()}
            conn.close()
    finally:
        if engine != "sqlite":
            conn = parity_engines.connect(engine)
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE {table}")
            conn.commit()
            conn.close()
    return stored


def _replay(engine: str, contract: bool) -> dict[int, Any]:
    from connectors.writer_common import map_rows_for_fingerprint

    ddl = DECIMAL_DDL[engine]
    mapped, _rejected = _wire(
        map_rows_for_fingerprint, headers=["id", "amt"], data_rows=[[k, v] for k, v in VALUES.items()],
        mappings=_mapping(contract), target_cols=["id", "amt"], dest_types={"id": "INTEGER", "amt": ddl},
        dest_kind=engine if engine != "mariadb" else "mysql", error_policy="quarantine",
    )
    return {int(r[0]): r[1] for r in mapped}


def _fp(value: Any, engine: str) -> str:
    from services.reconciliation import fingerprint_for_reconcile

    return fingerprint_for_reconcile(value, ddl_type=DECIMAL_DDL[engine],
                                     engine=engine if engine != "mariadb" else "mysql")


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("contract", [True, False], ids=["coerce_null_contract", "no_contract"])
def test_every_unfit_class_follows_the_contract_and_gate8_agrees(engine, contract, tmp_path, monkeypatch) -> None:
    stored = _write_and_read(engine, contract, tmp_path, monkeypatch)
    nan_kept = engine == "postgresql"  # PostgreSQL NUMERIC(p,s) stores NaN: nothing is lost
    if contract:
        expected_ids = set(VALUES)
        for k in (1, 2, 3, 5):
            assert stored[k] is None, (engine, k, stored[k])
        assert (stored[4] is not None and Decimal(stored[4]).is_nan()) if nan_kept else stored[4] is None
    else:
        expected_ids = {6, 7} | ({4} if nan_kept else set())
    assert set(stored) == expected_ids, (engine, sorted(stored))
    assert Decimal(str(stored[6])) == Decimal("12.34")
    # Gate-8's replay predicts exactly what landed.
    replay = _replay(engine, contract)
    assert set(replay) == set(stored), (engine, sorted(replay), sorted(stored))
    for k in stored:
        assert _fp(replay[k], engine) == _fp(stored[k], engine), (engine, k, replay[k], stored[k])
