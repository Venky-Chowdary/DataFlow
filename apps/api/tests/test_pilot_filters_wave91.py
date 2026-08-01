"""Wave 91 — typed, bound WHERE predicates for Pilot analytics.

Rules under test:
* filter values are **bound parameters**, never interpolated into SQL;
* date windows are **half-open** ``[start, end)`` — ``BETWEEN`` would pull in the
  next day's midnight rows;
* the filtered column is never wrapped in a function (index-eligible predicates);
* a literal that cannot carry the column's type is **refused with a reason**,
  not sent to the driver;
* MongoDB gets the same predicates as ``$match`` with Extended JSON dates.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timezone

import pytest

from src.ai.copilot.aggregate_tools import parse_aggregation_request
from src.ai.copilot.predicates import (
    PredicateError,
    Predicate,
    describe,
    ground_filters,
    parse_filters,
    parse_time_window,
    temporal_window_predicate,
    to_mongo_match,
    to_sql,
)
from src.ai.copilot.query_tools import _quote_ident

_COLUMNS = [
    {"name": "id", "inferred_type": "INTEGER"},
    {"name": "status", "inferred_type": "TEXT"},
    {"name": "region", "inferred_type": "TEXT"},
    {"name": "email", "inferred_type": "TEXT"},
    {"name": "amount", "inferred_type": "NUMERIC"},
    {"name": "is_paid", "inferred_type": "BOOLEAN"},
    {"name": "ordered_at", "inferred_type": "DATE"},
]


def _resolve(target: str, names: list[str]) -> str:
    from src.ai.copilot.aggregate_tools import resolve_name

    return resolve_name(target, names)


def _type_of(columns, name: str) -> str:
    for col in columns:
        if col.get("name") == name:
            return str(col.get("inferred_type") or "")
    return ""


def _ground(where: str) -> list[Predicate]:
    parsed, _ = parse_filters(where)
    return ground_filters(parsed, _COLUMNS, _resolve, _type_of)


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "phrase,column,op,values",
    [
        ("where status = open", "status", "eq", ["open"]),
        ("where amount > 100", "amount", "gt", ["100"]),
        ("where amount >= 100", "amount", "gte", ["100"]),
        ("where email is null", "email", "is_null", []),
        ("where email is not null", "email", "not_null", []),
        ("where status in (open, closed)", "status", "in", ["open", "closed"]),
        ("where email contains acme", "email", "contains", ["acme"]),
        ("where amount over 250", "amount", "gt", ["250"]),
        ("where amount at least 10", "amount", "gte", ["10"]),
        ("where status != open", "status", "ne", ["open"]),
    ],
)
def test_filter_phrases_parse(phrase, column, op, values):
    parsed, _ = parse_filters(phrase)
    assert parsed, f"no filter parsed from {phrase!r}"
    assert parsed[0].column == column
    assert parsed[0].op == op
    assert parsed[0].raw_values == values


def test_filter_removal_leaves_the_question_intact():
    req = parse_aggregation_request("how many orders where status = open on Local Postgres")
    assert req is not None
    assert req.metric == "count"
    assert req.table == "orders"
    assert req.connector_name == "Local Postgres"
    assert req.where == "status = open"


def test_two_filters_are_both_kept():
    req = parse_aggregation_request(
        "count of orders where status = closed and amount > 50 on PG"
    )
    assert req is not None
    assert req.where == "status = closed and amount > 50"
    assert req.table == "orders"


def test_grouping_and_filter_coexist():
    req = parse_aggregation_request("count of orders by status where region = emea on PG")
    assert req is not None
    assert req.group_by == "status"
    assert req.where == "region = emea"
    assert req.table == "orders"


def test_question_without_filter_has_empty_where():
    req = parse_aggregation_request("count of orders on Local Postgres")
    assert req is not None
    assert req.where == ""


def test_non_analytics_prompt_is_still_ignored():
    assert parse_aggregation_request("list my connectors") is None


# --------------------------------------------------------------------------
# Time windows — half-open, no BETWEEN
# --------------------------------------------------------------------------


def test_calendar_year_is_half_open():
    window = parse_time_window("count of orders in 2024")
    assert window is not None
    start, end, _ = window
    assert start == date(2024, 1, 1)
    assert end == date(2025, 1, 1)  # exclusive, not 2024-12-31


def test_named_month_window():
    window = parse_time_window("revenue in March 2024")
    assert window is not None
    assert window[0] == date(2024, 3, 1)
    assert window[1] == date(2024, 4, 1)


def test_relative_days_window_is_anchored():
    now = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
    window = parse_time_window("orders in the last 7 days", now=now)
    assert window is not None
    assert window[0] == date(2026, 7, 25)
    assert window[1] == date(2026, 8, 1)  # exclusive end covers all of today


def test_named_windows():
    now = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
    assert parse_time_window("orders today", now=now)[:2] == (
        date(2026, 7, 31),
        date(2026, 8, 1),
    )
    assert parse_time_window("orders yesterday", now=now)[:2] == (
        date(2026, 7, 30),
        date(2026, 7, 31),
    )
    assert parse_time_window("orders last month", now=now)[:2] == (
        date(2026, 6, 1),
        date(2026, 7, 1),
    )
    assert parse_time_window("orders this year", now=now)[:2] == (
        date(2026, 1, 1),
        date(2027, 1, 1),
    )


def test_no_time_phrase_returns_none():
    assert parse_time_window("count of orders by status") is None


def test_window_binds_to_the_date_column():
    window = parse_time_window("in 2024")
    pred = temporal_window_predicate(window, _COLUMNS, _type_of)
    assert pred is not None
    assert pred.column == "ordered_at"
    assert pred.op == "range"
    assert pred.values == [date(2024, 1, 1), date(2025, 1, 1)]


def test_ambiguous_date_column_asks():
    columns = _COLUMNS + [{"name": "shipped_at", "inferred_type": "DATE"}]
    with pytest.raises(PredicateError) as err:
        temporal_window_predicate(parse_time_window("in 2024"), columns, _type_of)
    assert "Which date column" in str(err.value)


# --------------------------------------------------------------------------
# Type-aware grounding
# --------------------------------------------------------------------------


def test_numeric_literal_is_coerced_to_a_number():
    preds = _ground("amount > 100")
    assert preds[0].values == [100]
    assert isinstance(preds[0].values[0], int)


def test_numeric_column_refuses_text_literal():
    with pytest.raises(PredicateError) as err:
        _ground("amount > abc")
    assert "numeric" in str(err.value)


def test_boolean_column_accepts_words():
    assert _ground("is_paid = yes")[0].values == [True]
    assert _ground("is_paid = false")[0].values == [False]


def test_boolean_column_refuses_a_number_word():
    with pytest.raises(PredicateError):
        _ground("is_paid = maybe")


def test_date_column_refuses_a_non_date():
    with pytest.raises(PredicateError) as err:
        _ground("ordered_at = notadate")
    assert "date column" in str(err.value)


def test_date_equality_becomes_a_whole_day_range():
    preds = _ground("ordered_at = 2024-03-05")
    assert preds[0].op == "range"
    assert preds[0].values == [date(2024, 3, 5), date(2024, 3, 6)]


def test_unknown_column_is_reported_not_dropped():
    with pytest.raises(PredicateError) as err:
        _ground("nosuchcol = 1")
    assert "not in this table" in str(err.value)
    assert "status" in str(err.value)


def test_text_match_on_numeric_column_is_refused():
    with pytest.raises(PredicateError):
        _ground("amount contains 12")


# --------------------------------------------------------------------------
# SQL rendering — bound, half-open, index friendly
# --------------------------------------------------------------------------


def test_values_are_bound_never_inlined():
    sql, params = to_sql(_ground("status = open"), _quote_ident, "postgresql")
    assert sql == '"status" = :f0'
    assert params == {"f0": "open"}
    assert "open" not in sql


def test_injection_attempt_stays_a_literal():
    nasty = "x'; DROP TABLE orders; --"
    preds = [Predicate(column="status", op="eq", values=[nasty])]
    sql, params = to_sql(preds, _quote_ident, "postgresql")
    assert sql == '"status" = :f0'
    assert "DROP" not in sql
    assert params["f0"] == nasty


def test_range_renders_half_open_and_never_between():
    preds = _ground("ordered_at = 2024-03-05")
    sql, params = to_sql(preds, _quote_ident, "postgresql")
    assert sql == '("ordered_at" >= :f0 AND "ordered_at" < :f1)'
    assert "BETWEEN" not in sql.upper()
    assert params["f0"] == date(2024, 3, 5)


def test_filtered_column_is_not_wrapped_in_a_function():
    sql, _ = to_sql(_ground("ordered_at = 2024-03-05"), _quote_ident, "postgresql")
    for fn in ("DATE(", "CAST(", "LOWER(", "DATE_TRUNC("):
        assert fn not in sql.upper()


def test_null_checks_bind_nothing():
    sql, params = to_sql(_ground("email is null"), _quote_ident, "postgresql")
    assert sql == '"email" IS NULL'
    assert params == {}


def test_in_list_binds_each_value():
    sql, params = to_sql(_ground("status in (open, closed)"), _quote_ident, "postgresql")
    assert sql == '"status" IN (:f0, :f1)'
    assert params == {"f0": "open", "f1": "closed"}


def test_like_pattern_escapes_wildcards():
    preds = [Predicate(column="email", op="contains", values=["100%_raw"])]
    sql, params = to_sql(preds, _quote_ident, "postgresql")
    assert sql == '"email" LIKE :f0'
    assert params["f0"] == r"%100\%\_raw%"


def test_multiple_predicates_are_anded():
    sql, params = to_sql(
        _ground("status = closed and amount > 50"), _quote_ident, "postgresql"
    )
    assert sql == '"status" = :f0 AND "amount" > :f1'
    assert params == {"f0": "closed", "f1": 50}


def test_describe_is_operator_readable():
    text = describe(_ground("status = open and amount > 100"))
    assert "`status` = open" in text
    assert "`amount` > 100" in text


# --------------------------------------------------------------------------
# Mongo rendering — Extended JSON keeps dates typed
# --------------------------------------------------------------------------


def test_mongo_match_mirrors_sql_ops():
    match = to_mongo_match(_ground("status = open and amount > 100"))
    assert match["status"] == {"$eq": "open"}
    assert match["amount"] == {"$gt": 100}


def test_mongo_range_uses_extended_json_dates():
    match = to_mongo_match(_ground("ordered_at = 2024-03-05"))
    rng = match["ordered_at"]
    assert set(rng) == {"$gte", "$lt"}
    assert rng["$gte"] == {"$date": "2024-03-05T00:00:00Z"}
    # Plain JSON round-trip must keep the type wrapper intact.
    assert json.loads(json.dumps(rng))["$lt"] == {"$date": "2024-03-06T00:00:00Z"}


def test_mongo_null_and_in():
    assert to_mongo_match(_ground("email is null"))["email"] == {"$eq": None}
    assert to_mongo_match(_ground("email is not null"))["email"] == {"$ne": None}
    assert to_mongo_match(_ground("status in (open, closed)"))["status"] == {
        "$in": ["open", "closed"]
    }


def test_mongo_contains_is_an_escaped_regex():
    match = to_mongo_match([Predicate(column="email", op="contains", values=["a.b+c"])])
    assert match["email"]["$regex"] == r"a\.b\+c"


def test_router_parses_extended_json_dates_as_bson():
    pytest.importorskip("bson")
    from src.routers.query_router import _parse_mongodb_json

    parsed = _parse_mongodb_json('{"ordered_at": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}')
    value = parsed["ordered_at"]["$gte"]
    assert isinstance(value, datetime)
    assert value.year == 2024


def test_router_rewrites_named_params_for_pyformat():
    from src.routers.query_router import _to_pyformat

    sql = 'SELECT COUNT(*) FROM "t" WHERE "a" = :f0 AND "b" < :f10'
    out = _to_pyformat(sql, {"f0": 1, "f10": 2})
    assert out == 'SELECT COUNT(*) FROM "t" WHERE "a" = %(f0)s AND "b" < %(f10)s'


def test_pyformat_rewrite_leaves_casts_alone():
    from src.routers.query_router import _to_pyformat

    sql = "SELECT x::text FROM t WHERE a = :f0"
    assert _to_pyformat(sql, {"f0": 1}) == "SELECT x::text FROM t WHERE a = %(f0)s"


# --------------------------------------------------------------------------
# Live Postgres
# --------------------------------------------------------------------------


def _pg_reachable() -> bool:
    try:
        import psycopg2

        psycopg2.connect(
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ.get("PG_DB", "dataflow"),
            user=os.environ.get("PG_USER", "dataflow"),
            password=os.environ.get("PG_PASSWORD", "dataflow"),
            connect_timeout=2,
        ).close()
        return True
    except Exception:
        return False


def _mysql_reachable() -> bool:
    try:
        import pymysql

        pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            connect_timeout=2,
        ).close()
        return True
    except Exception:
        return False


def _mongo_reachable() -> bool:
    try:
        import pymongo

        cli = pymongo.MongoClient(
            os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
            serverSelectionTimeoutMS=2000,
        )
        cli.admin.command("ping")
        cli.close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _mysql_reachable(), reason="MySQL not reachable")
def test_live_mysql_filters_bind_and_match_control_sql():
    """The same predicate engine must be exact on MySQL, not just Postgres."""
    import pymysql
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import DataPilotTools

    table = f"w91my_{uuid.uuid4().hex[:6]}"
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE `{table}` (id INT AUTO_INCREMENT PRIMARY KEY, "
            "status VARCHAR(32), amount DECIMAL(10,2), ordered_at DATE)"
        )
        cur.executemany(
            f"INSERT INTO `{table}` (status, amount, ordered_at) VALUES (%s, %s, %s)",
            [
                ("open", 150, "2024-01-05"),
                ("open", 20, "2024-02-11"),
                ("closed", 300, "2024-02-14"),
                ("closed", 1, "2025-03-03"),
                (None, 400, "2025-03-21"),
            ],
        )
    name = f"W91MY{table[-6:]}"
    saved = create_connector(
        {
            "name": name,
            "type": "mysql",
            "host": "127.0.0.1",
            "port": 3306,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "dataflow",
        }
    )
    tools = DataPilotTools()

    def truth(sql: str):
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]

    try:
        cases = [
            (
                {"metric": "count", "where": "status = open"},
                f"SELECT COUNT(*) FROM `{table}` WHERE status = 'open'",
            ),
            (
                {"metric": "sum", "column": "amount", "where": "amount > 100"},
                f"SELECT SUM(amount) FROM `{table}` WHERE amount > 100",
            ),
            (
                {"metric": "count", "where": "in 2024"},
                f"SELECT COUNT(*) FROM `{table}` WHERE ordered_at >= '2024-01-01' "
                "AND ordered_at < '2025-01-01'",
            ),
            (
                {"metric": "count", "where": "status is null"},
                f"SELECT COUNT(*) FROM `{table}` WHERE status IS NULL",
            ),
        ]
        for kwargs, control in cases:
            result = tools.execute(
                "aggregate_data", {"connector_name": name, "table": table, **kwargs}
            )
            assert result.success, f"{kwargs}: {result.error}"
            assert float(result.output["value"]) == float(truth(control)), kwargs
            # MySQL identifiers must use backticks, and values stay bound.
            emitted = result.output["query"]
            assert f"`{table}`" in emitted, emitted
            if "is null" not in kwargs["where"]:
                assert ":f0" in emitted, emitted
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()
        delete_connector(saved.id)


@pytest.mark.skipif(not _mongo_reachable(), reason="MongoDB not reachable")
def test_live_mongo_filters_match_driver_counts():
    """Filtered $match must be exact, and date predicates must stay BSON Dates."""
    import pymongo
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import DataPilotTools

    cli = pymongo.MongoClient(
        os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=3000,
    )
    db = cli["dataflow_wave91"]
    coll = f"orders_{uuid.uuid4().hex[:6]}"
    docs = [
        {"status": "open", "amount": 150.0, "ordered_at": datetime(2024, 1, 5)},
        {"status": "open", "amount": 20.0, "ordered_at": datetime(2024, 2, 11)},
        {"status": "closed", "amount": 300.0, "ordered_at": datetime(2024, 2, 14)},
        {"status": "closed", "amount": 1.0, "ordered_at": datetime(2025, 3, 3)},
        {"amount": 400.0, "ordered_at": datetime(2025, 3, 21)},
    ]
    db[coll].insert_many(docs)
    name = f"W91MG{coll[-6:]}"
    saved = create_connector(
        {
            "name": name,
            "type": "mongodb",
            "host": "localhost",
            "port": 27017,
            "database": "dataflow_wave91",
        }
    )
    tools = DataPilotTools()
    try:
        count_open = tools.execute(
            "aggregate_data",
            {
                "connector_name": name,
                "table": coll,
                "metric": "count",
                "where": "status = open",
            },
        )
        assert count_open.success, count_open.error
        assert count_open.output["value"] == db[coll].count_documents({"status": "open"})

        total_big = tools.execute(
            "aggregate_data",
            {
                "connector_name": name,
                "table": coll,
                "metric": "sum",
                "column": "amount",
                "where": "amount > 100",
            },
        )
        assert total_big.success, total_big.error
        assert abs(float(total_big.output["value"]) - 850.0) < 1e-9

        # The date window must reach the server as a BSON Date, not a string.
        in_2024 = tools.execute(
            "aggregate_data",
            {
                "connector_name": name,
                "table": coll,
                "metric": "count",
                "where": "in 2024",
            },
        )
        assert in_2024.success, in_2024.error
        expected = db[coll].count_documents(
            {"ordered_at": {"$gte": datetime(2024, 1, 1), "$lt": datetime(2025, 1, 1)}}
        )
        assert in_2024.output["value"] == expected == 3
        assert "$date" in in_2024.output["query"]

        missing = tools.execute(
            "aggregate_data",
            {
                "connector_name": name,
                "table": coll,
                "metric": "count",
                "where": "status is null",
            },
        )
        assert missing.success, missing.error
        assert missing.output["value"] == 1

        grouped = tools.execute(
            "aggregate_data",
            {
                "connector_name": name,
                "table": coll,
                "metric": "sum",
                "column": "amount",
                "group_by": "status",
                "where": "amount > 10",
            },
        )
        assert grouped.success, grouped.error
        alias = grouped.output["metric_alias"]
        rows = {r.get("status"): float(r[alias]) for r in grouped.output["rows"]}
        assert rows["open"] == 170.0
        assert rows["closed"] == 300.0
    finally:
        db.drop_collection(coll)
        delete_connector(saved.id)
        cli.close()


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_filtered_aggregates_match_independent_sql():
    import psycopg2
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import DataPilotTools

    table = f"w91_{uuid.uuid4().hex[:6]}"
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "dataflow"),
        user=os.environ.get("PG_USER", "dataflow"),
        password=os.environ.get("PG_PASSWORD", "dataflow"),
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{table}" (id serial, status text, region text, '
            "email text, amount numeric(10,2), ordered_at date)"
        )
        cur.executemany(
            f'INSERT INTO "{table}" (status, region, email, amount, ordered_at) '
            "VALUES (%s, %s, %s, %s, %s)",
            [
                ("open", "emea", "a@acme.com", 150, "2024-01-05"),
                ("open", "emea", None, 20, "2024-02-11"),
                ("open", "apac", "c@zeta.com", 5, "2025-02-02"),
                ("closed", "emea", "d@acme.com", 300, "2024-02-14"),
                ("closed", "apac", None, 1, "2024-03-03"),
                (None, "emea", "f@acme.com", 400, "2025-03-21"),
            ],
        )
    name = f"W91PG{table[-6:]}"
    saved = create_connector(
        {
            "name": name,
            "type": "postgresql",
            "host": os.environ.get("PG_HOST", "localhost"),
            "port": int(os.environ.get("PG_PORT", "5432")),
            "database": os.environ.get("PG_DB", "dataflow"),
            "username": os.environ.get("PG_USER", "dataflow"),
            "password": os.environ.get("PG_PASSWORD", "dataflow"),
            "schema": "public",
        }
    )
    tools = DataPilotTools()

    def truth(sql: str):
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]

    cases = [
        (
            {"metric": "count", "where": "status = open"},
            f"SELECT COUNT(*) FROM \"{table}\" WHERE status = 'open'",
        ),
        (
            {"metric": "count", "where": "amount > 100"},
            f'SELECT COUNT(*) FROM "{table}" WHERE amount > 100',
        ),
        (
            {"metric": "count", "where": "email is null"},
            f'SELECT COUNT(*) FROM "{table}" WHERE email IS NULL',
        ),
        (
            {"metric": "count", "where": "in 2024"},
            f"SELECT COUNT(*) FROM \"{table}\" WHERE ordered_at >= '2024-01-01' "
            "AND ordered_at < '2025-01-01'",
        ),
        (
            {"metric": "sum", "column": "amount", "where": "status = open"},
            f"SELECT SUM(amount) FROM \"{table}\" WHERE status = 'open'",
        ),
        (
            {"metric": "count", "where": "status in (open, closed)"},
            f"SELECT COUNT(*) FROM \"{table}\" WHERE status IN ('open', 'closed')",
        ),
        (
            {"metric": "count", "where": "email contains acme"},
            f"SELECT COUNT(*) FROM \"{table}\" WHERE email LIKE '%acme%'",
        ),
        (
            {"metric": "count", "where": "status = closed and amount > 50"},
            f"SELECT COUNT(*) FROM \"{table}\" WHERE status = 'closed' AND amount > 50",
        ),
    ]
    try:
        for kwargs, control in cases:
            result = tools.execute(
                "aggregate_data", {"connector_name": name, "table": table, **kwargs}
            )
            assert result.success, f"{kwargs}: {result.error}"
            expected = truth(control)
            got = result.output["value"]
            assert float(got) == float(expected), f"{kwargs}: {got} != {expected}"
            # Literals must never appear in the emitted SQL. IS NULL is the one
            # predicate with nothing to bind.
            emitted = result.output["query"]
            if "is null" not in kwargs["where"]:
                assert ":f0" in emitted, emitted

        # A grouped + filtered aggregate still returns exact per-group totals.
        grouped = tools.execute(
            "aggregate_data",
            {
                "connector_name": name,
                "table": table,
                "metric": "sum",
                "column": "amount",
                "group_by": "region",
                "where": "status = open",
            },
        )
        assert grouped.success, grouped.error
        alias = grouped.output["metric_alias"]
        rows = {r["region"]: float(r[alias]) for r in grouped.output["rows"]}
        assert rows == {"emea": 170.0, "apac": 5.0}

        # An injection attempt is bound as data; the table survives.
        nasty = tools.execute(
            "aggregate_data",
            {
                "connector_name": name,
                "table": table,
                "metric": "count",
                "where": f"status = x'; DROP TABLE {table}; --",
            },
        )
        assert nasty.success, nasty.error
        assert nasty.output["value"] == 0
        assert truth(f'SELECT COUNT(*) FROM "{table}"') == 6
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()
        delete_connector(saved.id)
