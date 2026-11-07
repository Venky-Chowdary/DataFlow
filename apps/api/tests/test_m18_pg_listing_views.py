"""M18 — connector object listing must include views with their real kind.

The PG probe queried only BASE TABLE, so every view was invisible to
list_connector_objects and the UI; and _operator_visible_objects labeled
whatever it did show a plain "table". The listing now covers views and each
object carries its kind.
"""

from __future__ import annotations

import inspect


def test_pg_probe_lists_views_and_tables():
    from connectors import postgresql

    src = inspect.getsource(postgresql.test_postgresql)
    assert "'VIEW'" in src, (
        "test_postgresql only queries BASE TABLE — views are invisible "
        "to list_connector_objects"
    )
    assert "table_types" in src, "probe must return per-object kinds"


def test_operator_visible_objects_uses_probe_kind():
    from src.transfer.endpoint_intelligence import _operator_visible_objects

    out = _operator_visible_objects(
        ["orders", "orders_v"], "table", {"orders_v": "view"}
    )
    kinds = {o["name"]: o["type"] for o in out}
    assert kinds == {"orders": "table", "orders_v": "view"}


def test_operator_visible_objects_without_map_defaults():
    from src.transfer.endpoint_intelligence import _operator_visible_objects

    out = _operator_visible_objects(["orders"], "table", None)
    assert out[0]["type"] == "table"
