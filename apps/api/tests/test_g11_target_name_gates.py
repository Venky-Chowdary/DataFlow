"""G11 / Document Modeler PR1: Validate and Execute refuse target names a
destination would silently change.

Before: every target went through SQL identifier sanitizing, cut at
PostgreSQL's 63 characters on every engine. A MongoDB target ``address.city``
landed as the top-level field ``address_city``; ``$x`` became ``_x``; a
100-character SQL Server column was cut to 63; two BigQuery targets ``Name``
and ``name`` became one column. Nothing at Validate said so.

Now each destination's own rule is checked at Validate (gate
``g23c_target_names``) and again at Execute before any write:

* MongoDB targets are field paths (``services.document_paths``): an empty
  segment, a leading ``$``, NUL, ``_id`` below the root, or a collision such as
  ``a`` and ``a.b`` is refused with the path's issue code;
* SQL engines refuse a name longer than the engine's real limit (PostgreSQL 63,
  Redshift 127, MySQL 64, SQL Server / Oracle 128, Snowflake 255, BigQuery 300;
  SQLite has none), naming the limit and the offending name;
* BigQuery refuses two targets that differ only in case;
* Elasticsearch keeps ``@timestamp`` and dotted object paths, and refuses a
  metadata field such as ``_source`` and a scalar/object collision.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _maps(*targets: str) -> list[dict]:
    return [
        {"source": f"c{i}", "target": t, "confidence": 1.0}
        for i, t in enumerate(targets)
    ]


def _name_gate(dest_type: str, *targets: str) -> dict | None:
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_overwrite",
        dest_type=dest_type,
        source_type="postgresql",
        source_kind="database",
        mappings=_maps(*targets),
        destination_table="target_table",
    )
    hits = [g for g in gates if g.get("id") == "g23c_target_names"]
    assert len(hits) <= 1, hits
    return hits[0] if hits else None


def _codes(gate: dict | None) -> list[str]:
    assert gate is not None, "expected the target-name gate to block"
    assert gate["status"] == "block", gate
    return [i["code"] for i in gate["details"]["issues"]]


# -- MongoDB field paths -----------------------------------------------------


def test_mongo_dotted_paths_pass_validate():
    assert _name_gate("mongodb", "_id", "name", "address.city", "address.zip", "meta.tags") is None


@pytest.mark.parametrize(
    ("target", "code"),
    [
        ("address..city", "target_path_empty_segment"),
        ("$where", "target_path_dollar_segment"),
        ("address.$city", "target_path_dollar_segment"),
        ("meta._id", "target_path_id_not_root"),
        ("bad\x00name", "target_path_nul"),
        (".lead", "target_path_empty_segment"),
    ],
)
def test_mongo_bad_path_refused_at_validate(target: str, code: str):
    gate = _name_gate("mongodb", "id", target)
    assert code in _codes(gate)
    assert repr(target) in gate["message"] or target in gate["message"]


def test_mongo_scalar_and_subdocument_collision_refused():
    gate = _name_gate("mongodb", "address", "address.city")
    issues = gate["details"]["issues"] if gate else []
    assert _codes(gate) == ["target_path_prefix_collision"], issues
    assert issues[0]["target"] == "address.city" and issues[0]["other"] == "address"


def test_mongo_alias_kinds_use_the_same_rule():
    assert "target_path_dollar_segment" in _codes(_name_gate("mongodb_atlas", "$x"))


# -- SQL engine limits -------------------------------------------------------


@pytest.mark.parametrize(
    ("dest", "limit"),
    [
        ("postgresql", 63),
        ("redshift", 127),
        ("mysql", 64),
        ("sqlserver", 128),
        ("oracle", 128),
        ("snowflake", 255),
        ("bigquery", 300),
    ],
)
def test_sql_name_at_limit_passes_and_one_over_is_refused(dest: str, limit: int):
    at_limit = "c" * limit
    assert _name_gate(dest, "id", at_limit) is None
    over = "d" * (limit + 1)
    gate = _name_gate(dest, "id", over)
    assert _codes(gate) == ["target_name_too_long"]
    assert f"at most {limit}" in gate["message"]
    assert over in gate["message"]


def test_mysql_64_character_name_is_kept_not_refused():
    """MySQL allows 64; the old universal cut at 63 renamed it silently."""
    assert _name_gate("mysql", "x" * 64) is None
    assert _codes(_name_gate("postgresql", "x" * 64)) == ["target_name_too_long"]


def test_sqlite_has_no_practical_limit():
    assert _name_gate("sqlite", "id", "s" * 500) is None


def test_postgres_limit_counts_bytes_after_sanitizing():
    # 'é' transliterates to 'e', so 63 accented letters fit the 63-byte limit.
    assert _name_gate("postgresql", "é" * 63) is None
    assert _codes(_name_gate("postgresql", "é" * 64)) == ["target_name_too_long"]


# -- BigQuery case ------------------------------------------------------------


def test_bigquery_case_only_collision_refused():
    gate = _name_gate("bigquery", "id", "Name", "name")
    assert _codes(gate) == ["target_name_case_collision"]
    assert "'Name'" in gate["message"] and "'name'" in gate["message"]


def test_bigquery_mixed_case_distinct_names_pass():
    assert _name_gate("bigquery", "CustomerID", "CustomerName", "createdAt") is None


# -- Elasticsearch -----------------------------------------------------------


def test_elasticsearch_keeps_at_timestamp_and_dotted_paths():
    assert _name_gate("elasticsearch", "@timestamp", "host.name", "host.ip", "_id") is None


def test_elasticsearch_refuses_metadata_field_and_collision():
    assert _codes(_name_gate("elasticsearch", "_source")) == ["target_name_reserved"]
    assert _codes(_name_gate("opensearch", "host", "host.name")) == [
        "target_path_prefix_collision"
    ]


# -- file exports: names are not refused for length or case -------------------


def test_file_export_keeps_long_and_mixed_case_headers():
    assert _name_gate("csv", "CustomerName", "customername", "h" * 400) is None


# -- Execute refuses the same names before any write --------------------------


def _request(dest_format: str, targets: list[str]):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        mappings=_maps(*targets),
        source=EndpointConfig(kind="database", format="sqlite", database="/nonexistent.db", table="t"),
        destination=EndpointConfig(
            kind="database", format=dest_format, database="d", table="t", collection="t",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )


@pytest.mark.parametrize(
    ("dest", "targets", "needle"),
    [
        ("mongodb", ["id", "address..city"], "empty part"),
        ("mongodb", ["id", "$x"], "'$'"),
        ("mongodb", ["a", "a.b"], "already holds a single value"),
        ("postgresql", ["id", "p" * 64], "at most 63"),
        ("mysql", ["id", "m" * 65], "at most 64"),
        ("bigquery", ["Name", "name"], "case"),
    ],
)
def test_execute_refuses_before_any_write(dest: str, targets: list[str], needle: str):
    from services.source_filter_gate import request_refusal

    reason = request_refusal(_request(dest, targets))
    assert needle in reason, reason


def test_execute_accepts_valid_mongo_paths():
    from services.source_filter_gate import request_refusal

    assert request_refusal(_request("mongodb", ["id", "address.city", "address.zip"])) == ""


def test_validate_api_map_items_are_read():
    """The Validate route hands the gates pydantic Map items, not dicts."""
    from services.preflight_transfer_policy import run_transfer_policy_gates
    from src.routers.preflight_router import MappingItem

    items = [
        MappingItem(source="id", target="id", confidence=1.0),
        MappingItem(source="city", target="address..city", confidence=1.0),
    ]
    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_overwrite", dest_type="mongodb", source_type="postgresql",
        source_kind="database", mappings=items, destination_table="t",
    )
    hit = [g for g in gates if g.get("id") == "g23c_target_names"]
    assert hit and _codes(hit[0]) == ["target_path_empty_segment"], gates
