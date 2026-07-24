"""Wave Z accuracy: SaaS streaming honesty, Stripe empty page, SQL count None."""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pytest
import responses


def test_salesforce_hubspot_excluded_from_offset_streaming():
    from src.transfer.stream import _STREAMING_TYPES

    assert "salesforce" not in _STREAMING_TYPES
    assert "hubspot" not in _STREAMING_TYPES
    assert "postgresql" in _STREAMING_TYPES


@responses.activate
def test_stripe_empty_page_with_has_more_fail_closed():
    import connectors.stripe as stripe

    responses.add(
        responses.GET,
        re.compile(r"https://api\.stripe\.com/v1/customers"),
        json={"data": [], "has_more": True},
        status=200,
    )
    with pytest.raises(RuntimeError, match="empty page"):
        stripe.read_object(cfg={"api_key": "sk_test_123"}, limit=10)


def test_generic_sql_count_failure_does_not_fabricate_total():
    from connectors.generic_sql import _count_table_raw

    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("count unsupported")
    assert _count_table_raw(conn, "t", None, dialect="postgresql") is None


def test_generic_sql_read_keeps_none_total_on_count_failure():
    from connectors.generic_sql import read_table_batch

    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = conn

    # Force reflection failure → raw path; count returns None.
    conn.execute.side_effect = [
        RuntimeError("reflect fail"),  # first attempt inside try reflection
    ]

    with patch("connectors.generic_sql._engine", return_value=engine), patch(
        "connectors.generic_sql.SQLALCHEMY_AVAILABLE", True
    ), patch(
        "connectors.generic_sql._read_table_raw",
        return_value=(["id"], [["1"], ["2"]]),
    ), patch(
        "connectors.generic_sql._count_table_raw",
        return_value=None,
    ), patch(
        "connectors.generic_sql.sa.MetaData"
    ) as meta_cls:
        # Make reflection raise so raw fallback is used.
        meta = MagicMock()
        meta.reflect.side_effect = RuntimeError("no catalog")
        meta_cls.return_value = meta
        batch = read_table_batch(
            host="localhost",
            port=5432,
            database="db",
            username="u",
            password="p",
            schema="public",
            connection_string="",
            ssl=False,
            table="t",
            offset=0,
            limit=2,
        )

    assert len(batch.rows) == 2
    assert batch.total_rows is None


@responses.activate
def test_hubspot_empty_continuation_page_fail_closed():
    import connectors.hubspot as hubspot

    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/properties/contacts"),
        json={"results": [{"name": "email", "type": "string"}]},
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://api\.hubapi\.com/crm/v3/objects/contacts"),
        json={
            "results": [],
            "paging": {"next": {"after": "cursor-x"}},
        },
        status=200,
    )
    with pytest.raises(RuntimeError, match="empty page"):
        hubspot.read_object(cfg={"api_key": "fake-token"}, limit=50)
