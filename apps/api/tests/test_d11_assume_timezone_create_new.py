"""D11: a declared source zone makes the create-new carrier an instant, not NTZ.

``assume_timezone:X`` turns a naive column into an instant for every gate, but
the create-new stamp was invented from the zoneless source type: Snowflake got
``TIMESTAMP_NTZ`` and G3/G9 then blocked the run for the very polarity drop the
declaration had answered. Validate's stamp (decision_kernel.invent) and Map's
(mapping_pipeline, via the mapper's schemas) both read the declared type now.

Snowflake is emulated by fakesnow here; the Validate/Map cases involve no
warehouse at all.
"""
from __future__ import annotations

import sqlite3
import uuid

import pytest

from services.mapping_pipeline import run_mapping_pipeline
from services.preflight_service import run_file_preflight

_DESTS = ["snowflake", "postgresql", "redshift", "sqlserver", "oracle", "bigquery", "duckdb", "databricks"]


def _validate(dest: str, transform: str | None):
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0, "source_type": "integer"},
        {
            "source": "ts", "target": "ts", "confidence": 1.0, "source_type": "timestamp",
            **({"transform": transform} if transform else {}),
        },
    ]
    return run_file_preflight(
        columns=["id", "ts"],
        column_types={"id": "integer", "ts": "timestamp"},
        row_count=2,
        mappings=mappings,
        destination_connected=True,
        sample_rows=[{"id": 1, "ts": "2025-01-01 10:00:00"}, {"id": 2, "ts": "2025-01-02 10:00:00"}],
        source_kind="database",
        source_format="postgresql",
        source_table="t",
        destination_table="o",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type=dest,
        sync_mode="full_refresh_append",
    )


def _blocked(result) -> list[str]:
    return [g["id"] for g in result["gates"] if g["status"] == "block"]


def _stamp(result, target: str) -> str:
    return next(m["target_type"] for m in result["stamped_mappings"] if m["target"] == target)


@pytest.mark.parametrize("dest", _DESTS)
def test_validate_declared_zone_creates_an_instant_carrier(dest):
    result = _validate(dest, "assume_timezone:UTC")
    assert _blocked(result) == [], (dest, _stamp(result, "ts"))
    assert "NTZ" not in _stamp(result, "ts").upper()


def test_validate_without_a_declared_zone_still_creates_the_wall_clock_carrier():
    result = _validate("snowflake", None)
    assert _stamp(result, "ts") == "TIMESTAMP_NTZ"


def test_map_declared_zone_creates_an_instant_carrier():
    def stamp(transform: str) -> str:
        result = run_mapping_pipeline(
            source_columns=["id", "ts"],
            target_columns=[],
            source_schemas=[
                {"name": "id", "inferred_type": "INTEGER", "samples": ["1"]},
                {
                    "name": "ts", "inferred_type": "TIMESTAMP",
                    "native_type": "timestamp without time zone",
                    "samples": ["2025-01-01 10:00:00"],
                },
            ],
            file_format="postgresql",
            destination_db_type="snowflake",
            source_db_type="postgresql",
            destination_table_exists=False,
            use_llm=False,
            prior_mappings=[{
                "source": "ts", "target": "ts", "source_type": "TIMESTAMP",
                "create_new": True, "user_override": True, "reasoning": "operator",
                "confidence": 1.0, "assignment_strategy": "create_compatible_new",
                "transform": transform,
            }],
        )
        return next(m["target_type"] for m in result["mappings"] if m["source"] == "ts")

    assert "NTZ" not in stamp("assume_timezone:UTC").upper()
    assert stamp("none") == "TIMESTAMP_NTZ"


def test_execute_creates_a_tz_aware_snowflake_column_for_a_declared_zone(tmp_path, monkeypatch):
    pytest.importorskip("fakesnow")
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src = tmp_path / "s.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ts DATETIME)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, "2025-01-01 10:00:00"), (2, "2025-01-02 10:00:00")])
    conn.commit()
    conn.close()
    table = "d11_" + uuid.uuid4().hex[:8]
    dest = EndpointConfig(
        kind="database", format="snowflake", host="localhost", port=443,
        database="dataflow", username="test", password="test", schema="public", table=table,
    )
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=dest,
        sync_mode="full_refresh_append",
        validation_mode="balanced",
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "ts", "target": "ts", "confidence": 1.0, "transform": "assume_timezone:UTC"},
        ],
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error

    from connectors.snowflake_conn import get_connection

    live = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="public", warehouse="", connection_string="",
    )
    try:
        cur = live.cursor()
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            f"WHERE table_name = '{table.upper()}' AND column_name = 'TS'"
        )
        data_type = str(cur.fetchone()[0]).upper()
    finally:
        live.close()
    assert "NTZ" not in data_type and ("TZ" in data_type or "LTZ" in data_type), data_type
