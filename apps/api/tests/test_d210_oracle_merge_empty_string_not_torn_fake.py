"""D210: an Oracle upsert of '' writes NULL in the same MERGE as the rest of the row.

QA (MySQL -> Oracle, incremental_upsert strict): a source update set name to
'' and moved long_txt/updated_at; Oracle's read-back of id 12 kept name
'name_12' while LONG_TXT/UPDATED_AT changed (a torn row), and the retry went
green past it.

Oracle is not available locally. This drives the real write path for the
oracle dialect (``connectors.generic_sql._generic_bind_tuple_rows`` ->
``_upsert_batch`` -> ``merge_dialects._oracle_merge_upsert``) against a fake
connection that executes the staged rows and the generated MERGE with
Oracle's documented semantics: a zero-length character value is NULL
(Oracle SQL Language Reference, "Nulls"), and MERGE ... WHEN MATCHED THEN
UPDATE SET changes exactly the columns its SET list names. The row must
change as a whole: name NULL, long_txt and updated_at new. (Label: verified
against a faithful fake; the live Oracle tests are
test_r8_d210_oracle_empty_string_e2e / test_r10_d210_stale_row_repair_e2e.)
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from sqlalchemy.dialects import oracle


class _FakeOracle:
    """Just enough of an Oracle session for the MERGE upsert path."""

    def __init__(self, table: dict[int, dict]):
        self.table = table
        self.stage: list[dict] = []
        self.dialect = oracle.dialect()
        self.statements: list[str] = []

    @staticmethod
    def _oracle_value(value):
        return None if value == "" else value  # '' IS NULL in Oracle

    def execute(self, stmt, params=None):
        sql = str(stmt.compile(dialect=self.dialect)) if not isinstance(stmt, str) else stmt
        self.statements.append(sql)
        if "TEMPORARY TABLE" in sql or sql.startswith(("TRUNCATE", "DROP")):
            return None
        if sql.startswith("INSERT INTO"):
            self.stage.append({k: self._oracle_value(v) for k, v in dict(params).items()})
            return None
        if sql.startswith("MERGE INTO"):
            set_cols = re.findall(r't\."([^"]+)" = s\."\1"', sql.split("WHEN NOT MATCHED")[0])
            for row in self.stage:
                key = row["id"]
                if key in self.table:
                    for col in set_cols:
                        self.table[key][col] = row[col]
                else:
                    self.table[key] = dict(row)
            return None
        raise AssertionError(f"unexpected statement: {sql}")

    def rollback(self):
        return None


def test_oracle_merge_writes_null_for_empty_string_with_the_rest_of_the_row():
    from connectors.generic_sql import _generic_bind_tuple_rows, _upsert_batch

    cols = ["id", "name", "long_txt", "updated_at"]
    table_obj = sa.Table(
        "T_D210", sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(60)),
        sa.Column("long_txt", sa.String(4000)),
        sa.Column("updated_at", sa.String(40)),
    )
    dest = {12: {"id": 12, "name": "name_12", "long_txt": "old",
                 "updated_at": "2026-11-01 10:00:00"}}
    _rows, dicts, _nums = _generic_bind_tuple_rows(
        [(12, "", "new text", "2026-11-02 10:00:00")],
        [1],
        target_cols=cols,
        dest_types={"id": "NUMBER(10)", "name": "VARCHAR2(60)",
                    "long_txt": "VARCHAR2(4000)", "updated_at": "VARCHAR2(40)"},
        sa_col_types={c: table_obj.c[c].type for c in cols},
        dialect_name="oracle",
        db_type="oracle",
        rejected_details=[],
        policy="quarantine",
        mappings=[],
    )
    conn = _FakeOracle(dest)
    written = _upsert_batch(conn, table_obj, dicts, ["id"], cols, "oracle",
                            rejected_details=[], policy="quarantine")
    assert written == 1
    merge = [s for s in conn.statements if s.startswith("MERGE INTO")]
    assert len(merge) == 1, conn.statements
    # Every non-key column is in the one MERGE's SET list, name included.
    for col in ("name", "long_txt", "updated_at"):
        assert f't."{col}" = s."{col}"' in merge[0], merge[0]
    assert dest[12] == {"id": 12, "name": None, "long_txt": "new text",
                        "updated_at": "2026-11-02 10:00:00"}, dest[12]
