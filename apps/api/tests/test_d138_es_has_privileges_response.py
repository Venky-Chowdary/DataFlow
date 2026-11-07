"""D138: the ES privilege probe must read the real ``_has_privileges`` response.

elasticsearch-py 8 returns an ``ObjectApiResponse`` (not a ``dict``) whose body
keys ``index`` by index *name*. The probe read only dicts and a list shape, so
the ``elastic`` superuser was reported as lacking ``create_index`` on
'elasticsearch' (the engine name, not the index).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from services.destination_privilege_probe import (
    evaluate_elasticsearch_privileges,
    probe_destination_privileges,
)

_PRIVS = ["create", "create_doc", "create_index", "index", "manage", "write"]


def _api_response(body: dict):
    from elastic_transport import ApiResponseMeta, HttpHeaders, ObjectApiResponse

    meta = ApiResponseMeta(
        status=200, http_version="1.1", headers=HttpHeaders(), duration=0.0, node=MagicMock()
    )
    return ObjectApiResponse(body=body, meta=meta)


def _body(index: str, granted: set[str], *, all_requested: bool) -> dict:
    return {
        "username": "elastic",
        "has_all_requested": all_requested,
        "cluster": {},
        "index": {index: {p: p in granted for p in _PRIVS}},
        "application": {},
    }


def _probe(resp, *, exists: bool):
    client = MagicMock()
    client.indices.exists.return_value = exists
    client.security.has_privileges.return_value = resp
    with patch("connectors.elasticsearch_reader._client", return_value=client):
        return probe_destination_privileges(
            "elasticsearch", host="localhost", table="orders_d138", table_exists=exists
        )


def test_superuser_object_api_response_can_create_the_index():
    pytest.importorskip("elastic_transport")
    result = _probe(_api_response(_body("orders_d138", set(_PRIVS), all_requested=True)), exists=False)
    assert result.status == "ok", result.detail
    assert result.can_create_table is True and result.can_write is True


def test_name_keyed_index_privileges_are_read():
    pytest.importorskip("elastic_transport")
    # A writer role on an existing index: no manage/create_index, has index/write.
    granted = {"create", "create_doc", "index", "write"}
    result = _probe(_api_response(_body("orders_d138", granted, all_requested=False)), exists=True)
    assert result.status == "ok", result.detail
    assert result.can_write is True and result.can_create_table is False


def test_missing_create_index_names_the_target_index():
    pytest.importorskip("elastic_transport")
    result = _probe(_api_response(_body("orders_d138", {"index"}, all_requested=False)), exists=False)
    assert result.status == "denied"
    assert "orders_d138" in result.detail
    assert "'elasticsearch'" not in result.detail


def test_only_the_probed_index_entry_counts():
    body = {
        "has_all_requested": False,
        "index": {
            "other": {p: True for p in _PRIVS},
            "orders_d138": {p: False for p in _PRIVS},
        },
    }
    assert evaluate_elasticsearch_privileges(body, index="orders_d138", table_exists=True) == (
        False,
        False,
    )
