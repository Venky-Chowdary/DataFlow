"""A cached schema answer must never be the reason a write is refused."""

from __future__ import annotations

from typing import Any

from connectors.mysql_writer import _fetch_mysql_column_types
from services import reflection_cache


class FakeCursor:
    """Answers INFORMATION_SCHEMA.COLUMNS with the table's current shape."""

    def __init__(self, columns: dict[str, str]) -> None:
        self.columns = columns
        self.queries = 0
        self._rows: list[tuple[str, str]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.queries += 1
        self._rows = [(name, ctype) for name, ctype in self.columns.items()]

    def fetchall(self) -> list[tuple[str, str]]:
        return list(self._rows)


IDENTITY = "mysql://cache-staleness-test"
TABLE = "orders"


def _prime(value: dict[str, str]) -> None:
    reflection_cache.invalidate_by_identity(IDENTITY, "", TABLE)
    reflection_cache.put_by_identity(IDENTITY, "", TABLE, "mysql_col_types", value)


def test_cached_answer_is_reused_when_it_covers_the_mapped_columns():
    _prime({"id": "bigint", "email": "varchar(255)"})
    cur = FakeCursor({"id": "bigint", "email": "varchar(255)"})

    physical = _fetch_mysql_column_types(
        cur, TABLE, identity=IDENTITY, required_columns=["id", "email"]
    )

    assert physical == {"id": "bigint", "email": "varchar(255)"}
    assert cur.queries == 0


def test_stale_cache_missing_a_mapped_column_is_re_read_not_refused():
    # The operator recreated the destination between runs: the cache still
    # describes the old shape, and refusing on it would name a column that is
    # in fact present.
    _prime({"id": "bigint"})
    cur = FakeCursor({"id": "bigint", "email": "varchar(255)"})

    physical = _fetch_mysql_column_types(
        cur, TABLE, identity=IDENTITY, required_columns=["id", "email"]
    )

    assert physical == {"id": "bigint", "email": "varchar(255)"}
    assert cur.queries == 1


def test_folded_case_difference_does_not_force_a_re_read():
    # Coverage is judged with the same exact/lower/upper lookup the refusal
    # uses, so a cached answer that would satisfy the refusal is not re-read.
    _prime({"ID": "bigint", "EMAIL": "varchar(255)"})
    cur = FakeCursor({"ID": "bigint", "EMAIL": "varchar(255)"})

    _fetch_mysql_column_types(
        cur, TABLE, identity=IDENTITY, required_columns=["id", "email"]
    )

    assert cur.queries == 0


def test_column_genuinely_absent_still_reports_absent_after_re_read():
    # Re-reading must not invent the column: the refusal is correct here.
    _prime({"id": "bigint"})
    cur = FakeCursor({"id": "bigint"})

    physical = _fetch_mysql_column_types(
        cur, TABLE, identity=IDENTITY, required_columns=["id", "missing_col"]
    )

    assert physical == {"id": "bigint"}
    assert cur.queries == 1


def test_no_identity_always_reads_live():
    cur = FakeCursor({"id": "bigint"})

    _fetch_mysql_column_types(cur, TABLE, required_columns=["id"])

    assert cur.queries == 1
