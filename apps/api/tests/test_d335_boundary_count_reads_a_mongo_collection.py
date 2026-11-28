"""D335 x Mongo schedules: the post-write watermark count reads the collection.

A schedule builds a Mongo destination with ``collection`` set and ``table``
empty (services.schedule_runner._endpoint_from_connector). The boundary
duplicate gate read ``endpoint.table`` and so asked for collection ``""``:
pymongo raised InvalidName, read_target_sample turned that into
TargetSampleUnavailable, and every strict run with a tie at the watermark
failed "Exactly-once at the stored watermark is NOT proven" (train 2 CI:
test_r12_mongo_incremental_schedule_e2e, test_r14_mongo_d4_every_n_noop_schedule_e2e).
"""

from __future__ import annotations

from typing import Any

import pytest

from src.transfer.models import EndpointConfig
import src.transfer.stream_boundary_keyed as keyed


@pytest.fixture
def asked(monkeypatch) -> list[str]:
    tables: list[str] = []

    def fake_read(**kwargs: Any):
        tables.append(kwargs["table"])
        return iter(())

    monkeypatch.setattr(keyed, "read_candidates", fake_read)
    monkeypatch.setattr("src.transfer.adapters.resolve_connector_config", lambda ep, **k: {"database": "db"})
    return tables


def _tie(**extra: Any) -> dict[str, Any]:
    return {keyed.BOUNDARY_TIE_KEY: {
        "mode": "keyless", "cursor_column": "updated_at", "watermark": "2024-01-01T07:00:00",
        "types": {}, "match_columns": ["id"], "row_counts": {"x": 1}, **extra}}


def _mongo_schedule_endpoint() -> EndpointConfig:
    return EndpointConfig(kind="database", format="mongodb", database="db",
                          connection_string="mongodb://127.0.0.1:27017/db", collection="r12m_dst", table="")


def test_a_schedule_mongo_endpoint_is_counted_in_its_collection(asked) -> None:
    keyed.boundary_duplicates(dest_summary=_tie(), endpoint=_mongo_schedule_endpoint())
    assert asked == ["r12m_dst"]


def test_the_table_the_run_read_before_the_write_is_the_one_counted_after(asked) -> None:
    keyed.boundary_duplicates(dest_summary=_tie(table="r12m_dst"),
                              endpoint=EndpointConfig(kind="database", format="mongodb", database="db"))
    assert asked == ["r12m_dst"]
