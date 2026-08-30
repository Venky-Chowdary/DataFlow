"""Named fixture: Mongo _id leftover + SQLite TEXT→DECIMAL invent.

Measured on this file only. ``100%`` here means every cell in this leftover
mapping fixture plus the sqlite TEXT live writes in
``test_unique_engine_leftovers_live`` (sqlite dest COUNT=1 and postgresql
TEXT dest COUNT=1). Not a 5×5 cartesian claim. Not PRODUCTION_SKU tenant
execute. CDC remains at-least-once upsert.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest

from services.unique_engine_leftovers import (
    CORE_ENGINES,
    leftover_column_mappings,
    leftover_g13_accounted,
    leftover_sqlite_dest_invents_decimal,
    leftover_text_carrier_invents_numeric,
)


MONGO_COLS = ["_id", "id", "amount"]
SQLITE_COLS = ["id", "amount"]


@pytest.mark.fake_mongo
@pytest.mark.parametrize("dest", CORE_ENGINES)
def test_mongo_source_accounts_id_on_every_core_dest(dest: str) -> None:
    maps = leftover_column_mappings(
        source_format="mongodb",
        dest_format=dest,
        source_columns=MONGO_COLS,
    )
    assert leftover_g13_accounted(maps, MONGO_COLS)
    omit = [m for m in maps if m.get("intentional_omit") and m.get("source") == "_id"]
    bound = [m for m in maps if m.get("source") == "_id" and m.get("target") == "_id"]
    assert omit or bound


@pytest.mark.fake_mongo
@pytest.mark.parametrize("source", CORE_ENGINES)
def test_sqlite_dest_never_invents_decimal_affinity(source: str) -> None:
    cols = MONGO_COLS if source == "mongodb" else SQLITE_COLS
    maps = leftover_column_mappings(
        source_format=source,
        dest_format="sqlite",
        source_columns=cols,
    )
    assert leftover_sqlite_dest_invents_decimal(maps, "sqlite") is False
    amount = next(m for m in maps if m.get("source") == "amount")
    assert str(amount.get("target_type") or "").upper() == "TEXT"


def test_sqlite_source_keeps_text_carrier_on_typed_dests() -> None:
    """sqlite TEXT digits are a carrier. Dest NUMERIC/DECIMAL is invent."""
    for dest in ("postgresql", "mysql"):
        maps = leftover_column_mappings(
            source_format="sqlite",
            dest_format=dest,
            source_columns=SQLITE_COLS,
        )
        amount = next(m for m in maps if m.get("source") == "amount")
        assert str(amount.get("source_type") or "").upper() == "TEXT", dest
        assert str(amount.get("target_type") or "").upper() == "TEXT", dest
        assert leftover_text_carrier_invents_numeric(maps) is False, dest
        ident = next(m for m in maps if m.get("source") == "id")
        assert str(ident.get("source_type") or "").upper() == "INTEGER", dest


def test_text_carrier_invent_predicate_flags_numeric_stamp() -> None:
    from services.unique_engine_leftovers import leftover_text_carrier_invents_numeric

    honest = leftover_column_mappings(
        source_format="sqlite", dest_format="postgresql", source_columns=SQLITE_COLS
    )
    assert leftover_text_carrier_invents_numeric(honest) is False
    invented = [
        {
            "source": "amount",
            "target": "amount",
            "confidence": 0.99,
            "source_type": "TEXT",
            "target_type": "DECIMAL(18,2)",
        }
    ]
    assert leftover_text_carrier_invents_numeric(invented) is True


@pytest.mark.fake_mongo
def test_leftover_cartesian_cells_are_g13_complete() -> None:
    """4×4 mapping contract — not a live write matrix."""
    for src in CORE_ENGINES:
        for dst in CORE_ENGINES:
            cols = MONGO_COLS if src == "mongodb" else SQLITE_COLS
            maps = leftover_column_mappings(
                source_format=src,
                dest_format=dst,
                source_columns=cols,
            )
            assert leftover_g13_accounted(maps, cols), f"{src}->{dst}"
            if dst == "sqlite":
                assert leftover_sqlite_dest_invents_decimal(maps, dst) is False
            if src == "sqlite":
                assert leftover_text_carrier_invents_numeric(maps) is False, f"{src}->{dst}"
