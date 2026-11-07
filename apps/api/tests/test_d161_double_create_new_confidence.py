"""D161 — a declared DOUBLE is not an "IEEE/Excel float artifact".

The create-new risk stamp began judging columns on their samples (D148). Its
``ieee_float_artifact`` chip, meant for a FLOAT *invented* from Excel/CSV text
("111.89999999999999"), then fired on every relational DOUBLE/FLOAT/REAL
column holding a value with 15+ significant digits: confidence capped at 0.70,
below the 0.85 strict floor, and the load blocked as "Mapping confidence below
floor" for a lossless DOUBLE -> DOUBLE PRECISION copy.

A source that declared its own carriers has nothing to reconsider; a file or a
document store, whose types were inferred, still gets the chip.
"""

from __future__ import annotations

import uuid

import pytest

from services.create_new_risk_stamp import apply_create_new_risk_stamps
from services.type_coercion import assess_create_new_type_risk

RESIDUE = ["1.23456789012345", "0.30000000000000004", "111.89999999999999"]


def _mapping() -> dict:
    return {
        "source": "f",
        "target": "f",
        "source_type": "DOUBLE",
        "target_type": "DOUBLE PRECISION",
        "assignment_strategy": "identity_passthrough",
        "create_new": True,
        "confidence": 0.95,
    }


def _stamp(source_db_type: str) -> dict:
    return apply_create_new_risk_stamps(
        [_mapping()],
        "postgresql",
        source_samples={"f": RESIDUE},
        dest_table_exists=False,
        source_db_type=source_db_type,
    )[0]


@pytest.mark.parametrize("engine", ["mysql", "postgresql", "sqlserver", "sqlite"])
def test_a_declared_double_keeps_its_confidence(engine):
    row = _stamp(engine)
    kinds = [r.get("kind") for r in row.get("create_new_risks") or []]
    assert "ieee_float_artifact" not in kinds
    assert float(row["confidence"]) >= 0.85


def test_a_bson_double_is_a_declared_double():
    row = _stamp("mongodb")
    assert "ieee_float_artifact" not in [r.get("kind") for r in row.get("create_new_risks") or []]
    assert float(row["confidence"]) >= 0.85


@pytest.mark.parametrize("fmt", ["csv", "xlsx"])
def test_an_inferred_float_still_gets_the_chip(fmt):
    row = _stamp(fmt)
    kinds = [r.get("kind") for r in row.get("create_new_risks") or []]
    assert "ieee_float_artifact" in kinds
    assert float(row["confidence"]) < 0.85


def test_assess_skips_the_chip_only_for_declared_sources():
    declared = assess_create_new_type_risk(
        "DOUBLE", "DOUBLE PRECISION", destination_db_type="postgresql",
        samples=RESIDUE, source_types_declared=True,
    )
    inferred = assess_create_new_type_risk(
        "FLOAT", "DOUBLE PRECISION", destination_db_type="postgresql", samples=RESIDUE,
    )
    assert not any(r.get("kind") == "ieee_float_artifact" for r in declared)
    assert any(r.get("kind") == "ieee_float_artifact" for r in inferred)


@pytest.fixture()
def _isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)


def test_mysql_double_with_full_precision_loads_into_postgres(_isolated):
    """End to end, preflight on: the user-visible symptom was a blocked load."""
    import psycopg2
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src_table = f"d161_s_{uuid.uuid4().hex[:8]}"
    dst_table = f"d161_d_{uuid.uuid4().hex[:8]}"
    my = pymysql.connect(
        host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    pg = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    pg.autocommit = True
    try:
        with my.cursor() as cur:
            cur.execute(f"CREATE TABLE `{src_table}` (id int primary key, f double)")
            cur.executemany(
                f"INSERT INTO `{src_table}` VALUES (%s, %s)",
                [(1, 1.23456789012345), (2, 0.30000000000000004), (3, None)],
            )
        request = TransferRequest(
            source=EndpointConfig(
                kind="database", format="mysql", table=src_table, host="127.0.0.1",
                port=3306, database="dataflow", username="dataflow", password="dataflow",
            ),
            destination=EndpointConfig(
                kind="database", format="postgresql", schema="public", table=dst_table,
                host="127.0.0.1", port=5432, database="dataflow",
                username="dataflow", password="dataflow",
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
        )
        result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        with pg.cursor() as cur:
            cur.execute(f'SELECT id, f::text FROM public."{dst_table}" ORDER BY id')
            assert cur.fetchall() == [(1, "1.23456789012345"), (2, "0.30000000000000004"), (3, None)]
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
        my.close()
        pg.close()


DECLARED_DOUBLES = [
    # (source engine, declared source type, destination engine, destination carrier)
    ("mysql", "DOUBLE", "postgresql", "DOUBLE PRECISION"),
    ("mysql", "DOUBLE", "sqlserver", "FLOAT"),
    ("mysql", "DOUBLE", "mysql", "DOUBLE"),
    ("postgresql", "DOUBLE PRECISION", "sqlite", "REAL"),
    ("sqlserver", "FLOAT", "postgresql", "DOUBLE PRECISION"),
    ("oracle", "BINARY_DOUBLE", "postgresql", "DOUBLE PRECISION"),
    ("snowflake", "FLOAT", "postgresql", "DOUBLE PRECISION"),
]
G4_FLOOR = 0.85


def _stamp_with(source_db_type: str, samples: list) -> dict:
    return apply_create_new_risk_stamps(
        [{**_mapping(), "source_type": "FLOAT"}],
        "postgresql",
        source_samples={"f": samples},
        dest_table_exists=False,
        source_db_type=source_db_type,
    )[0]


@pytest.mark.parametrize(("src_db", "src_type", "dst_db", "dst_type"), DECLARED_DOUBLES)
def test_declared_double_clears_the_g4_floor_across_engines(src_db, src_type, dst_db, dst_type):
    row = apply_create_new_risk_stamps(
        [{**_mapping(), "source_type": src_type, "target_type": dst_type}],
        dst_db,
        source_samples={"f": ["0.14285714285714285", "-1.5e-10", None]},
        dest_table_exists=False,
        source_db_type=src_db,
    )[0]
    kinds = [r.get("kind") for r in row.get("create_new_risks") or []]
    assert "ieee_float_artifact" not in kinds
    assert not row.get("requires_review")
    assert float(row["confidence"]) >= G4_FLOOR


def test_a_csv_column_of_plain_ratios_is_not_flagged():
    row = _stamp_with("csv", ["0.5", "0.25", "0.125"])
    assert "ieee_float_artifact" not in [r.get("kind") for r in row.get("create_new_risks") or []]
    assert float(row["confidence"]) >= G4_FLOOR


def test_a_csv_column_with_binary_residue_is_still_stamped():
    row = _stamp_with("csv", ["0.30000000000000004", "111.89999999999999"])
    assert "ieee_float_artifact" in [r.get("kind") for r in row.get("create_new_risks") or []]


def test_a_double_into_real_is_still_a_narrowing_that_blocks(_isolated):
    """The chip is not the width guard: the collapse gate still refuses DOUBLE -> REAL."""
    import psycopg2

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src_table = f"d161_s_{uuid.uuid4().hex[:8]}"
    dst_table = f"d161_d_{uuid.uuid4().hex[:8]}"
    pg = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    pg.autocommit = True
    cfg = dict(
        kind="database", format="postgresql", schema="public", host="127.0.0.1", port=5432,
        database="dataflow", username="dataflow", password="dataflow",
    )
    try:
        with pg.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src_table}" (id int primary key, f double precision)')
            cur.execute(f'INSERT INTO "{src_table}" VALUES (1, 0.14285714285714285), (2, -1.5e-10)')
            cur.execute(f'CREATE TABLE "{dst_table}" (id int, f real)')
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(table=src_table, **cfg),
                destination=EndpointConfig(table=dst_table, **cfg),
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is False
        assert "collapse" in (result.error or "").lower()
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
        pg.close()
