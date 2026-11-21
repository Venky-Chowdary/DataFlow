"""D362 on a real Oracle: the NULL-marker probe must see the CLOB it reflected.

``_exact_in`` compares an Oracle CLOB/NCLOB through ``DBMS_LOB.SUBSTR``
(ORA-22848: a LOB is not a comparison key), and its unit test passed. The
probe itself built its table from *untyped* ``sa.column(name)`` objects, so
the type test never saw the CLOB and the live query sent a plain ``IN``. The
oracle-live CI job (Oracle Database Free) caught it: every run whose Oracle
destination has a CLOB carrier ended "NULL-marker check could not be
measured (ORA-22848)" and the overwrite was refused.

This drives ``count_marker_rows`` end to end with the Oracle dialect and a
reflected CLOB, and reads the SQL it sends.
"""

from __future__ import annotations

import contextlib
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import oracle


class _Conn:
    def __init__(self, sent: list[str]) -> None:
        self.sent = sent

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, stmt: Any) -> list[tuple]:
        self.sent.append(
            str(stmt.compile(dialect=oracle.dialect(), compile_kwargs={"literal_binds": True}))
        )
        return []


class _Engine:
    def __init__(self, sent: list[str]) -> None:
        self.dialect = oracle.dialect()
        self._sent = sent

    def connect(self) -> _Conn:
        return _Conn(self._sent)


class _Inspector:
    def __init__(self, columns: list[dict[str, Any]]) -> None:
        self._columns = columns

    def get_columns(self, table: str, schema: str | None = None) -> list[dict[str, Any]]:
        return self._columns


@pytest.fixture
def probe(monkeypatch):
    from services import null_marker_probe as nmp

    sent: list[str] = []
    columns = [
        {"name": "ID", "type": sa.Numeric(10, 0)},
        {"name": "NOTE", "type": oracle.CLOB()},
        {"name": "MEMO", "type": oracle.NCLOB()},
        {"name": "NAME", "type": sa.String(40)},
    ]
    monkeypatch.setattr(nmp, "probe_engine", lambda cfg: _Engine(sent))
    monkeypatch.setattr(sa, "inspect", lambda conn: _Inspector(columns))
    monkeypatch.setattr(nmp, "statement_timeout", lambda *a, **k: contextlib.nullcontext(True))
    monkeypatch.setattr("services.engine_pool.release_engine", lambda engine: None)
    return nmp, sent


def _flat(sql: str) -> str:
    """Upper-case SQL without quotes, the table qualifier or spaces."""
    return sql.upper().replace('"', "").replace("DATAFLOW.T.", "").replace(" ", "")


def test_the_probe_compares_a_reflected_clob_through_dbms_lob_substr(probe) -> None:
    nmp, sent = probe
    result = nmp.count_marker_rows({"schema": "DATAFLOW"}, "T", ["NOTE", "MEMO", "NAME"])
    assert result.rows == 0, result
    assert len(sent) == 1, sent
    sql = _flat(sent[0])
    assert "DBMS_LOB.SUBSTR(NOTE,1000,1)IN(" in sql, sent[0]
    assert "DBMS_LOB.SUBSTR(MEMO,1000,1)IN(" in sql, sent[0]
    # A VARCHAR2 is still an ordinary IN key.
    assert "NAMEIN(" in sql and "DBMS_LOB.SUBSTR(NAME" not in sql, sent[0]


def test_the_run_keys_scope_still_filters_on_the_key_column(probe) -> None:
    nmp, sent = probe
    scope = nmp.ProbeScope("run_keys", key_column="ID", keys=("1", "2"))
    result = nmp.count_marker_rows({"schema": "DATAFLOW"}, "T", ["NOTE"], scope)
    assert result.rows == 0, result
    sql = _flat(sent[0])
    assert "DBMS_LOB.SUBSTR(NOTE,1000,1)IN(" in sql, sent[0]
    assert "IDIN('1','2')" in sql, sent[0]


def test_an_upper_case_oracle_key_is_found_in_its_reflected_lower_case_spelling(monkeypatch) -> None:
    """The run's key is ``ID``; SQLAlchemy reflects Oracle's ``ID`` as ``id``.

    oracle-live (Oracle Database Free), test_family_b_run2_own_table_e2e
    [oracle-incremental_upsert]: "NULL-marker check ... could not be measured
    (the key column 'ID' is not in ...)" and the strict upsert failed.
    """
    from services import null_marker_probe as nmp

    sent: list[str] = []
    columns = [{"name": "id", "type": sa.Numeric(10, 0)}, {"name": "note", "type": sa.String(40)}]
    monkeypatch.setattr(nmp, "probe_engine", lambda cfg: _Engine(sent))
    monkeypatch.setattr(sa, "inspect", lambda conn: _Inspector(columns))
    monkeypatch.setattr(nmp, "statement_timeout", lambda *a, **k: contextlib.nullcontext(True))
    monkeypatch.setattr("services.engine_pool.release_engine", lambda engine: None)
    scope = nmp.ProbeScope("run_keys", key_column="ID", keys=("7",))
    result = nmp.count_marker_rows({"schema": "DATAFLOW"}, "T", ["NOTE"], scope)
    assert result.rows == 0, result
    assert "IDIN('7')" in _flat(sent[0]), sent[0]


def test_two_columns_differing_only_by_case_never_guess_the_key(monkeypatch) -> None:
    from services import null_marker_probe as nmp

    sent: list[str] = []
    columns = [
        {"name": "Id", "type": sa.Numeric(10, 0)},
        {"name": "iD", "type": sa.Numeric(10, 0)},
        {"name": "note", "type": sa.String(40)},
    ]
    monkeypatch.setattr(nmp, "probe_engine", lambda cfg: _Engine(sent))
    monkeypatch.setattr(sa, "inspect", lambda conn: _Inspector(columns))
    monkeypatch.setattr(nmp, "statement_timeout", lambda *a, **k: contextlib.nullcontext(True))
    monkeypatch.setattr("services.engine_pool.release_engine", lambda engine: None)
    scope = nmp.ProbeScope("run_keys", key_column="ID", keys=("7",))
    result = nmp.count_marker_rows({"schema": "DATAFLOW"}, "T", ["NOTE"], scope)
    assert result.rows is None and "is not in" in str(result.reason), result
    assert sent == []
