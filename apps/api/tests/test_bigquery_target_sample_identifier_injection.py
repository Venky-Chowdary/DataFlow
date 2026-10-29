"""BigQuery Gate-8 sample reads must quote every table-ref part, never splice it."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from services.copy_bigquery_common import bigquery_ident, bigquery_table_ref
from services.reconciliation import TargetSampleUnavailable
from services.target_sample import read_target_sample


def _run(dest: dict, *, schema: str, table: str, **kw) -> MagicMock:
    client = MagicMock()
    client.query.return_value.result.return_value = []
    with patch("connectors.bigquery_conn.get_client", return_value=client):
        read_target_sample("bigquery", dest, schema=schema, table_name=table, **kw)
    return client


def test_sample_sql_quotes_each_part() -> None:
    client = _run(
        {"database": "my-proj"}, schema="ds", table="orders", columns=["id"], sort_key="id"
    )
    sql = client.query.call_args.args[0]
    assert "FROM `my-proj`.`ds`.`orders` " in sql


def test_keyed_sample_sql_quotes_each_part() -> None:
    client = _run(
        {"database": "my-proj"},
        schema="ds",
        table="orders",
        columns=["id"],
        sort_key="id",
        key_values=[1],
    )
    assert "FROM `my-proj`.`ds`.`orders` WHERE" in client.query.call_args.args[0]


@pytest.mark.parametrize(
    "field",
    ["database", "schema", "table"],
)
@pytest.mark.parametrize(
    "payload",
    [
        "x` UNION ALL SELECT secret FROM other.t --",
        "x\\` UNION ALL SELECT secret FROM other.t --",
    ],
)
def test_backtick_or_backslash_in_any_part_never_reaches_query(field: str, payload: str) -> None:
    dest = {"database": payload if field == "database" else "proj"}
    schema = payload if field == "schema" else "ds"
    table = payload if field == "table" else "orders"
    client = MagicMock()
    with patch("connectors.bigquery_conn.get_client", return_value=client):
        with pytest.raises(TargetSampleUnavailable):
            read_target_sample(
                "bigquery", dest, schema=schema, table_name=table, columns=["id"], sort_key="id"
            )
    client.query.assert_not_called()
    client.list_rows.assert_not_called()


@pytest.mark.parametrize("bad", ["a`b", "a\\b", "a\\`b", "a;b", ""])
def test_bigquery_ident_refuses_unescapable_names(bad: str) -> None:
    with pytest.raises(ValueError):
        bigquery_ident(bad)


def test_bigquery_ident_keeps_legal_names() -> None:
    assert bigquery_ident("my-proj-123") == "`my-proj-123`"
    assert bigquery_ident("Order Lines") == "`Order Lines`"
    assert bigquery_table_ref("example.com:proj", "ds", "t") == "`example.com:proj`.`ds`.`t`"
