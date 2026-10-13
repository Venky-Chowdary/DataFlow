"""DEF-C-019, DEF-C-010, DEF-ADLS-EMPTY-INCLUDE, DEF-REDIS-SRC-HEADERS.

A named miss is not a measured profile. Two uploads with one stem are both
named. Redis identity keeps a pending ``id``, and a prefix count is not dbsize.
Azure container listing does not send an empty ``include=``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _upload(name: str, columns: list[str], rows: int, file_type: str) -> object:
    from src.ai.training.universal_data_feeder import UniversalSchema

    return UniversalSchema(
        name=name,
        source="upload",
        columns=columns,
        samples={col: ["ACC-88421"] if col == "ACCT_NO" else ["v"] for col in columns},
        row_count=rows,
        file_type=file_type,
    )


def _tools(schemas: list) -> object:
    from src.ai.copilot.data_analyst import CopilotDataAnalyst
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    analyst = CopilotDataAnalyst()
    analyst.feeder.feed_all = lambda: list(schemas)
    tools.analyst = analyst
    return tools


_CSV_COLS = ["CUST_ID", "AMT", "TXN_DT", "ACCT_NO", "CCY", "REF_NO", "STS", "DESC"]
_TSV_COLS = ["CUST_ID", "AMT", "TXN_DT", "ACCT_NO", "CCY"]
_GENERIC = "declared types validated by G3 schema contract"


def test_unknown_dataset_is_not_a_measured_profile() -> None:
    tools = _tools([_upload("sample_payments", _CSV_COLS, 10, "csv")])
    out = tools._profile_quality_rules("not_a_dataset").output
    assert out["has_dataset"] is False
    assert out["rules"] == []
    assert out["pii_candidates"] == []
    assert out["column_count"] == 0
    assert "not found" in out["message"]
    assert _GENERIC not in " ".join(out["rules"])


def test_duplicate_stem_is_named_not_silently_widened() -> None:
    tools = _tools(
        [
            _upload("sample_payments", _CSV_COLS, 10, "csv"),
            _upload("sample_payments", _TSV_COLS, 4, "tsv"),
        ]
    )
    out = tools._profile_quality_rules("sample_payments").output
    assert out["has_dataset"] is False
    assert out["ambiguous"] is True
    assert out["column_count"] == 0
    assert out["rules"] == []
    kinds = {c["file_type"]: c["column_count"] for c in out["candidates"]}
    assert kinds == {"csv": 8, "tsv": 5}
    assert "sample_payments.csv" in out["message"]
    assert "sample_payments.tsv" in out["message"]


def test_named_file_profiles_account_number_as_pii() -> None:
    tools = _tools([_upload("sample_payments", _CSV_COLS, 10, "csv")])
    out = tools._profile_quality_rules("sample_payments.csv").output
    assert out["has_dataset"] is True
    assert out["column_count"] == 8
    assert "ACCT_NO" in out["pii_candidates"]
    assert any("ACCT_NO" in rule for rule in out["rules"])


def test_redis_db_index_is_the_database_not_a_key_prefix() -> None:
    from connectors.redis_reader import keys_for_prefix, resolve_key_pattern

    assert resolve_key_pattern("db0") == "*"
    assert resolve_key_pattern("DB12") == "*"
    assert resolve_key_pattern("qa19_products") == "qa19_products:*"
    owned = keys_for_prefix(
        ["qa19_products:1", "qa19_products:2", "other:1"],
        "qa6c_products",
    )
    assert owned == []
    assert keys_for_prefix(
        ["qa6c_products:1", "qa19:9"],
        "qa6c_products",
    ) == ["qa6c_products:1"]


def test_prefix_count_drops_unrelated_keys_when_scan_ignores_match(monkeypatch) -> None:
    from services.dest_precount import _redis_prefix_row_count

    class _Client:
        def scan(self, cursor=0, match=None, count=500):
            del match, count
            return 0, [b"qa19:1", b"qa19:2", b"qa6c_products:7"]

    monkeypatch.setattr(
        "connectors.redis_reader._redis_client",
        lambda cfg: _Client(),
    )
    assert _redis_prefix_row_count({"host": "localhost"}, prefix="qa6c_products") == 1
    assert _redis_prefix_row_count({"host": "localhost"}, prefix="") is None


def test_pending_id_mapping_stays_a_redis_column() -> None:
    from connectors.writer_common import resolve_target_columns
    from services.primary_key import infer_redis_conflict_columns

    mappings = [
        {
            "source": "id",
            "target": "id",
            "assignment_strategy": "pending_dest_schema",
            "target_type": "INTEGER",
        },
        {
            "source": "name",
            "target": "name",
            "assignment_strategy": "pending_dest_schema",
            "target_type": "VARCHAR",
        },
    ]
    dropped, _ = resolve_target_columns(
        mappings, {"id": "INTEGER", "name": "VARCHAR"}, preserve_case=True
    )
    assert dropped == []
    kept, _ = resolve_target_columns(
        mappings,
        {"id": "INTEGER", "name": "VARCHAR"},
        preserve_case=True,
        table_exists=False,
    )
    assert kept == ["id", "name"]
    assert infer_redis_conflict_columns(kept, mappings, ["id"]) == ["id"]


def test_redis_write_receives_primary_key_and_table_name(monkeypatch) -> None:
    from src.transfer.adapters import _write_destination_database
    from src.transfer.models import EndpointConfig

    captured: dict = {}

    def _write(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            ok=True,
            rows_written=1,
            table_name="qa6c_products",
            target_schema="db0",
            checksum="abc",
            driver="redis-py",
            rejected_rows=0,
            coerced_null_rows=0,
            rows_skipped=0,
            warnings=[],
            rejected_details=[],
            meta=None,
        )

    monkeypatch.setattr("connectors.redis_writer.write_mapped_rows", _write)
    endpoint = EndpointConfig(
        kind="database",
        format="redis",
        host="127.0.0.1",
        port=6379,
        database="0",
        table="qa6c_products",
    )
    _rows, _ddl, summary = _write_destination_database(
        endpoint,
        [{"id": "1", "name": "a"}],
        ["id", "name"],
        {"id": "integer", "name": "string"},
        [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
        write_mode="upsert",
        conflict_columns=["id"],
        sync_mode="incremental_upsert",
    )
    assert captured["conflict_columns"] == ["id"]
    assert captured["write_mode"] == "upsert"
    assert summary["table"] == "qa6c_products"
    assert summary["prefix"] == "qa6c_products"


def test_container_list_omits_empty_include() -> None:
    from azure.storage.blob._generated.operations._service_operations import (
        build_list_containers_segment_request,
    )
    from connectors.adls_common import list_service_containers

    empty = build_list_containers_segment_request(
        "http://127.0.0.1:10000/devstoreaccount1",
        version="2021-12-02",
        include=[],
    )
    assert "include=" in empty.url
    omitted = build_list_containers_segment_request(
        "http://127.0.0.1:10000/devstoreaccount1",
        version="2021-12-02",
        include=None,
    )
    assert "include=" not in omitted.url
    assert "comp=list" in omitted.url

    seen: dict = {}

    class _Service:
        def list_containers_segment(self, **kwargs):
            seen.update(kwargs)
            return {"ok": True}

    class _Inner:
        service = _Service()

    class _Client:
        _client = _Inner()

    list_service_containers(_Client(), maxresults=1)
    assert seen["include"] is None
    assert seen["maxresults"] == 1


def test_redis_inventory_lists_prefixes_not_db0(monkeypatch) -> None:
    import sys

    from connectors.redis_kv import test_redis

    class _Client:
        def ping(self):
            return True

        def scan(self, cursor=0, count=500):
            del count
            return 0, [b"qa19_products:1", b"qa19_products:2", b"barehash"]

        def close(self):
            return None

    class _RedisModule:
        Redis = staticmethod(lambda **_kwargs: _Client())

        @staticmethod
        def from_url(*_a, **_k):
            return _Client()

    monkeypatch.setitem(sys.modules, "redis", _RedisModule())
    result = test_redis(
        host="127.0.0.1",
        port=6379,
        database="0",
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
    )
    assert result.ok is True
    assert result.tables == ["qa19_products", "barehash"]
    assert "db0" not in result.tables
