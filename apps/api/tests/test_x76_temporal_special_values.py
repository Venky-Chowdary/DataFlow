"""X-76: PG temporal specials ('infinity', 'epoch', 'allballs') — exact or refuse.

Live evidence (live_616d_*): a PG TIMESTAMPTZ 'infinity' value on engines
without the special reached the driver as raw text — an opaque bind failure,
or worse, a silent clamp to the engine's max date. 'infinity' is a stored
value on PG-family (and DuckDB), not malformed input.

Fix: ``coerce_sql_temporal`` binds the literal on engines that accept it and
refuses plainly everywhere else — no silent clamp, no driver-level surprise.
"""

from __future__ import annotations

import os

import pytest

from connectors.sql_temporal import coerce_sql_temporal


_SPECIALS = ("infinity", "-infinity", "+infinity", "epoch", "allballs")


@pytest.mark.parametrize("token", _SPECIALS)
def test_special_binds_on_pg_and_duckdb(token):
    assert coerce_sql_temporal(token, "TIMESTAMPTZ", engine="postgresql") == token
    assert coerce_sql_temporal(token, "TIMESTAMP", engine="duckdb") == token


@pytest.mark.parametrize(
    "ddl,engine",
    [
        ("DATETIME2", "sqlserver"),
        ("DATETIME(6)", "mysql"),
        ("TIMESTAMP", "snowflake"),
        ("TIMESTAMP", "bigquery"),
        ("TIMESTAMPTZ", "oracle"),
    ],
)
def test_special_refuses_plainly_where_engine_lacks_it(ddl, engine):
    with pytest.raises(ValueError) as exc:
        coerce_sql_temporal("infinity", ddl, engine=engine)
    message = str(exc.value)
    assert "infinity" in message
    assert engine in message or "destination" in message
    assert "sentinel" in message or "TEXT" in message


def test_special_token_match_is_case_and_space_insensitive():
    with pytest.raises(ValueError):
        coerce_sql_temporal("  Infinity  ", "DATETIME2", engine="sqlserver")


def test_normal_datetimes_untouched():
    import datetime

    bound = coerce_sql_temporal(
        "2024-01-01 10:00:00", "DATETIME2", engine="sqlserver"
    )
    assert bound == datetime.datetime(2024, 1, 1, 10, 0)
    # 'now'-like status words inside ordinary text never reach here unless
    # the column is a temporal carrier — a TEXT column doesn't coerce.


def test_special_fingerprint_compares_under_text_carrier():
    # On a TEXT carrier (sqlite dest for a timestamptz source), 'infinity'
    # lands byte-exact and compares as literal text — never folded.
    from services.reconciliation import _fingerprint_with_ddl

    assert _fingerprint_with_ddl("infinity", "TEXT", "sqlite") == "infinity"
    assert (
        _fingerprint_with_ddl("infinity", "TEXT", "sqlite")
        != _fingerprint_with_ddl("-infinity", "TEXT", "sqlite")
    )
    assert (
        _fingerprint_with_ddl("infinity", "TIMESTAMP", "pg")
        != _fingerprint_with_ddl("2024-01-01 00:00:00", "TIMESTAMP", "pg")
    )


@pytest.mark.skipif(
    not os.environ.get("DATAFLOW_X76_LIVE"),
    reason="live engines (PG timestamptz source × 5 dests) — user-run matrix",
)
def test_x76_live_infinity_matrix():
    pytest.fail("harness leg: live_616d 'infinity'/'-infinity' clean+tamper")
