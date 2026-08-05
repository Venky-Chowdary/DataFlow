"""Map≡ALTER fidelity — explicit Map stamps are a hard widen ceiling."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from connectors.writer_common import desired_types_honoring_map_stamps


def test_explicit_stamp_refuses_wider_candidate():
    desired, refusals = desired_types_honoring_map_stamps(
        target_cols=["amount", "note"],
        current_target_types=["DECIMAL(10,2)", "VARCHAR(32)"],
        mappings=[
            {"source": "amt", "target": "amount", "target_type": "DECIMAL(10,2)"},
            {"source": "n", "target": "note", "target_type": "VARCHAR(32)"},
        ],
        candidate_by_col={
            "amount": "DECIMAL(18,4)",
            "note": "TEXT",
        },
    )
    assert desired == ["DECIMAL(10,2)", "VARCHAR(32)"]
    assert len(refusals) == 2
    assert {r["reason"] for r in refusals} == {"explicit_map_stamp_ceiling"}
    assert {r["column"] for r in refusals} == {"amount", "note"}


def test_non_explicit_allows_widen():
    desired, refusals = desired_types_honoring_map_stamps(
        target_cols=["amount"],
        current_target_types=["DECIMAL(10,2)"],
        mappings=[
            {"source": "amt", "target": "amount"},  # no target_type stamp
        ],
        candidate_by_col={"amount": "DECIMAL(18,4)"},
    )
    assert desired == ["DECIMAL(18,4)"]
    assert refusals == []


def test_explicit_keeps_stamp_when_candidate_narrower_or_equal():
    desired, refusals = desired_types_honoring_map_stamps(
        target_cols=["amount"],
        current_target_types=["DECIMAL(18,4)"],
        mappings=[
            {"source": "amt", "target": "amount", "target_type": "DECIMAL(18,4)"},
        ],
        candidate_by_col={"amount": "DECIMAL(10,2)"},
    )
    assert desired == ["DECIMAL(18,4)"]
    assert refusals == []


def test_explicit_columns_set_survives_polluted_mapping_target_type():
    """generic_sql may stamp target_type onto non-explicit widen proposals.

    ``explicit_columns`` is the authoritative Map-stamp membership so a
    polluted mapping cannot freeze an invented widen, and a true Map stamp
    cannot be overwritten even when candidate is wider.
    """
    desired, refusals = desired_types_honoring_map_stamps(
        target_cols=["amount", "note"],
        current_target_types=["DECIMAL(10,2)", "VARCHAR(32)"],
        mappings=[
            {"source": "amt", "target": "amount", "target_type": "DECIMAL(10,2)"},
            # Polluted: prior widen wrote a target_type that was never Map-stamped
            {"source": "n", "target": "note", "target_type": "VARCHAR(32)"},
        ],
        candidate_by_col={
            "amount": "DECIMAL(18,4)",
            "note": "TEXT",
        },
        # Only amount was an original Map stamp
        explicit_columns={"amount"},
    )
    assert desired[0] == "DECIMAL(10,2)"
    assert desired[1] == "TEXT"  # non-explicit may widen
    assert len(refusals) == 1
    assert refusals[0]["column"] == "amount"


def test_snowflake_widen_clamps_to_map_stamp():
    """Live ALTER must not push NUMBER past the approved Map stamp."""
    from connectors import snowflake_writer as sw

    executed: list[str] = []

    class _Cur:
        def execute(self, sql, params=None):
            executed.append(str(sql))
            if "information_schema" in str(sql).lower():
                self._rows = [("AMOUNT", 10, 2)]
            else:
                self._rows = []

        def fetchall(self):
            return getattr(self, "_rows", [])

    # Map stamp NUMBER(10,2); candidate batch wants wider — must not ALTER.
    refusals = sw._widen_existing_number_columns(
        _Cur(),
        "PUBLIC",
        "ORDERS",
        ["AMOUNT"],
        ["NUMBER(10,2)"],
        stamp_ceiling_by_col={"AMOUNT": "NUMBER(10,2)"},
        candidate_types=["NUMBER(18,4)"],
    )
    assert not any("ALTER" in s.upper() and "SET DATA TYPE" in s.upper() for s in executed)
    assert len(refusals) == 1
    assert refusals[0]["reason"] == "explicit_map_stamp_ceiling"
    assert refusals[0]["refused_wider"] == "NUMBER(18,4)"


def test_snowflake_widen_aligns_live_up_to_stamp_only():
    from connectors import snowflake_writer as sw

    executed: list[str] = []

    class _Cur:
        def execute(self, sql, params=None):
            executed.append(str(sql))
            if "information_schema" in str(sql).lower():
                # Live table is narrower than Map stamp
                self._rows = [("AMOUNT", 8, 2)]
            else:
                self._rows = []

        def fetchall(self):
            return getattr(self, "_rows", [])

    refusals = sw._widen_existing_number_columns(
        _Cur(),
        "PUBLIC",
        "ORDERS",
        ["AMOUNT"],
        ["NUMBER(10,2)"],
        stamp_ceiling_by_col={"AMOUNT": "NUMBER(10,2)"},
    )
    alters = [s for s in executed if "SET DATA TYPE" in s.upper()]
    assert len(alters) == 1
    assert "NUMBER(10,2)" in alters[0].upper().replace(" ", "")
    assert refusals == []


def test_generic_sql_sa_widen_refuses_past_map_stamp(monkeypatch):
    """Defense-in-depth: live SA ALTER never exceeds stamp ceiling."""
    from connectors import generic_sql as gs

    executed: list[str] = []

    class _FakeType:
        def compile(self, dialect=None):
            return "NUMERIC(10, 2)"

    class _FakeConn:
        def execute(self, stmt):
            executed.append(str(stmt))
            return MagicMock()

        def commit(self):
            return None

    class _FakeInspector:
        def get_columns(self, table_name, schema=None):
            return [{"name": "amount", "type": _FakeType()}]

    monkeypatch.setattr(gs.sa, "inspect", lambda _conn: _FakeInspector())

    engine = SimpleNamespace(dialect=SimpleNamespace(name="mssql"))
    refusals: list[dict] = []
    log = gs._widen_existing_columns_sa(
        _FakeConn(),
        engine,
        "mssql",
        "dbo",
        "orders",
        ["amount"],
        # Poisoned desired type wider than Map stamp
        {"amount": "NUMERIC(18,4)"},
        stamp_ceiling_by_col={"amount": "NUMERIC(10,2)"},
        refusals_out=refusals,
    )
    # Live already at stamp — no ALTER; refusal recorded for the poison candidate
    assert log == []
    assert len(refusals) == 1
    assert refusals[0]["reason"] == "explicit_map_stamp_ceiling"
    assert refusals[0]["refused_wider"] == "NUMERIC(18,4)"
    assert not any("ALTER" in s.upper() for s in executed)


def test_generic_sql_sa_widen_aligns_live_up_to_stamp(monkeypatch):
    from connectors import generic_sql as gs

    executed: list[str] = []

    class _FakeType:
        def compile(self, dialect=None):
            return "NUMERIC(8, 2)"

    class _FakeConn:
        def execute(self, stmt):
            executed.append(str(stmt))
            return MagicMock()

        def commit(self):
            return None

    class _FakeInspector:
        def get_columns(self, table_name, schema=None):
            return [{"name": "amount", "type": _FakeType()}]

    monkeypatch.setattr(gs.sa, "inspect", lambda _conn: _FakeInspector())

    engine = SimpleNamespace(dialect=SimpleNamespace(name="mssql"))
    refusals: list[dict] = []
    log = gs._widen_existing_columns_sa(
        _FakeConn(),
        engine,
        "mssql",
        "dbo",
        "orders",
        ["amount"],
        {"amount": "NUMERIC(10,2)"},
        stamp_ceiling_by_col={"amount": "NUMERIC(10,2)"},
        refusals_out=refusals,
    )
    assert len(log) == 1
    assert "NUMERIC(10,2)" in log[0].upper().replace(" ", "")
    assert refusals == []
