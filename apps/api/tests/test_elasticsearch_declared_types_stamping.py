"""An Elasticsearch index declares its fields, so a reread must report them.

Reading an index without reporting its mapping left every field the bare
``string`` placeholder ``_schema_from_batch`` falls back to for sources with no
catalog. Map then saw a ``long`` destination column as text: the exact-name pair
``id -> id`` was demoted to 0.63 and held for review, although the mapper scores
that pair 0.99 in isolation and nothing about the route is lossy.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.elasticsearch_reader import index_native_types  # noqa: E402


class _ObjectApiResponse:
    """What the live client answers with — a body holder, not a dict."""

    def __init__(self, body: dict) -> None:
        self.body = body


def _client_with(properties: dict, *, index: str = "idx", wrap: bool = False):
    client = MagicMock()
    body = {index: {"mappings": {"properties": properties}}}
    client.indices.get_mapping.return_value = (
        _ObjectApiResponse(body) if wrap else body
    )
    return client


def test_declared_field_types_are_reported_as_carriers() -> None:
    client = _client_with(
        {"id": {"type": "long"}, "name": {"type": "text"}}
    )
    assert index_native_types(client, "idx", ["id", "name"]) == {
        "id": "BIGINT",
        "name": "TEXT",
    }


def test_platform_written_carrier_outranks_the_field_type() -> None:
    # scaled_float cannot express DECIMAL(12,4); meta.df_carrier can.
    client = _client_with(
        {
            "amount": {
                "type": "scaled_float",
                "scaling_factor": 10000,
                "meta": {"df_carrier": "DECIMAL(12,4)"},
            }
        }
    )
    assert index_native_types(client, "idx", ["amount"])["amount"] == "DECIMAL(12,4)"


def test_live_object_api_response_is_read_through_its_body() -> None:
    client = _client_with({"id": {"type": "long"}}, wrap=True)
    assert index_native_types(client, "idx", ["id"]) == {"id": "BIGINT"}


def test_unmapped_field_keeps_the_placeholder() -> None:
    client = _client_with({"id": {"type": "long"}})
    assert "dynamic" not in index_native_types(client, "idx", ["id", "dynamic"])


def test_probe_failure_reports_nothing_rather_than_inventing() -> None:
    client = MagicMock()
    client.indices.get_mapping.side_effect = RuntimeError("401 unauthorized")
    assert index_native_types(client, "idx", ["id"]) == {}


def test_read_batch_stamps_the_declared_types_on_the_first_page() -> None:
    from connectors import elasticsearch_reader as er

    client = MagicMock()
    client.count.return_value = {"count": 1}
    client.search.return_value = {
        "hits": {"hits": [{"_id": "1", "_source": {"id": 1}, "sort": [0]}]}
    }
    client.indices.get_mapping.return_value = {
        "idx": {"mappings": {"properties": {"id": {"type": "long"}}}}
    }
    with patch.object(er, "_client", return_value=client):
        batch, _ = er.read_index_batch(cfg={"host": "h"}, index="idx")
    assert (batch.meta or {}).get("native_types") == {"id": "BIGINT"}


def test_scrolled_page_does_not_re_read_the_mapping() -> None:
    from connectors import elasticsearch_reader as er

    client = MagicMock()
    client.count.return_value = {"count": 1}
    client.search.return_value = {"hits": {"hits": []}}
    with patch.object(er, "_client", return_value=client):
        batch, _ = er.read_index_batch(cfg={"host": "h"}, index="idx", search_after=[0])
    client.indices.get_mapping.assert_not_called()
    assert batch.meta is None


def test_declared_destination_keeps_an_exact_identity_pair_confident() -> None:
    """The defect, at the layer that showed it: 0.63 with the placeholder."""
    from services.mapping_pipeline import run_mapping_pipeline

    source = {"id": "INTEGER", "amount": "DECIMAL(12,2)"}
    source_schemas = [
        {"name": name, "inferred_type": carrier, "samples": ["1"]}
        for name, carrier in source.items()
    ]

    def _confidences(destination: dict[str, str]) -> dict[str, float]:
        result = run_mapping_pipeline(
            list(source),
            list(destination),
            source_schemas=source_schemas,
            target_schemas=[
                {"name": name, "inferred_type": carrier}
                for name, carrier in destination.items()
            ],
            use_llm=False,
            source_db_type="postgresql",
            destination_db_type="elasticsearch",
            destination_table_exists=True,
            source_types_authoritative=True,
        )
        return {
            str(m.get("source")): float(m.get("confidence") or 0)
            for m in result.get("mappings", [])
        }

    placeholder = _confidences({"id": "string", "amount": "string"})
    declared = _confidences({"id": "BIGINT", "amount": "DECIMAL(12,2)"})
    assert placeholder["id"] < 0.85
    assert declared["id"] == 0.99
    assert declared["amount"] == 0.99


def test_live_index_mapping_types_reach_the_engines_stamping_input() -> None:
    """End to end on a real index: the schema Map stamps is the declared one."""
    from tests.typed_fidelity_helpers import require_ports, uniq

    require_ports(9200)
    from elasticsearch import Elasticsearch

    from services.semantic_mapper import map_columns
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    index = uniq("es_declared")
    es = Elasticsearch("http://127.0.0.1:9200")
    try:
        es.indices.create(
            index=index,
            mappings={
                "properties": {
                    "id": {"type": "long"},
                    "amount": {
                        "type": "scaled_float",
                        "scaling_factor": 100,
                        "meta": {"df_carrier": "DECIMAL(12,2)"},
                    },
                    "name": {"type": "keyword"},
                }
            },
        )
        for i in range(1, 4):
            es.index(
                index=index,
                id=str(i),
                document={"id": i, "amount": i + 0.25, "name": f"n{i}"},
            )
        es.indices.refresh(index=index)

        info = introspect_endpoint(
            EndpointConfig(
                kind="database",
                format="elasticsearch",
                database=index,
                table=index,
                host="127.0.0.1",
                port=9200,
            )
        )
        schema = info.get("schema") or {}
        assert schema["id"] == "BIGINT"
        assert schema["amount"] == "DECIMAL(12,2)"

        source = {"id": "INTEGER", "amount": "DECIMAL(12,2)", "name": "VARCHAR(50)"}
        rows = map_columns(
            list(source),
            [c for c in (info.get("columns") or []) if not c.startswith("_")],
            source_schemas=[
                {"name": n, "type": t, "inferred_type": t} for n, t in source.items()
            ],
            target_schemas=[
                {"name": n, "type": t, "inferred_type": t} for n, t in schema.items()
            ],
            source_db_type="postgresql",
            destination_db_type="elasticsearch",
            destination_table_exists=True,
        )
        by_source = {str(r.get("source")): r for r in rows}
        assert float(by_source["id"]["confidence"]) == 0.99
        assert by_source["id"]["target"] == "id"
    finally:
        try:
            es.indices.delete(index=index, ignore_unavailable=True)
        finally:
            es.close()
