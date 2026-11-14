"""Re-review: warehouse stage encoders never confuse a text ``\\N`` with NULL.

- Snowflake: the CSV stage wrote NULL as ``\\N`` under QUOTE_ALL and COPY used
  ``NULL_IF = ('\\N')``, so (1, None) and (2, '\\N') were identical bytes and the
  text loaded NULL. A batch holding a text ``\\N`` now takes the exact bind
  (INSERT) path; every other batch keeps the proven stage encoding.
- Redshift: the TSV stage used PostgreSQL COPY TEXT escaping, but Redshift's
  ``COPY … DELIMITER '\\t' NULL AS '\\N'`` (no ESCAPE) takes a backslash
  literally, so a text ``\\N`` (staged ``\\\\N``) or a tab/newline loaded with a
  stray backslash. Such a batch takes the INSERT path; Parquet staging is exact.
- BigQuery loads newline-delimited JSON (``null`` vs ``"\\\\N"``): no collision.
"""

from __future__ import annotations

from pathlib import Path

import pytest

COLS = ["id", "note"]


class _Cur:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.many: list[tuple[str, list]] = []

    def execute(self, sql, *args):
        self.sql.append(str(sql))

    def executemany(self, sql, rows):
        self.many.append((str(sql), list(rows)))

    def fetchall(self):
        return [("f", "LOADED", 1, 300, 300, 0, None)]


def _rows(with_text_null: bool) -> list[tuple]:
    rows = [(i, None if i % 3 == 0 else ("" if i % 3 == 1 else f"n{i}")) for i in range(1, 301)]
    if with_text_null:
        rows[1] = (2, "\\N")
    return rows


def test_snowflake_stage_bytes_and_copy_options(tmp_path):
    from connectors.snowflake_writer import _copy_into_table, _write_temp_csv

    path = tmp_path / "stage.csv"
    _write_temp_csv(path, COLS, [(1, None), (2, ""), (3, "x")])
    assert path.read_bytes() == b'"id","note"\r\n"1","\\N"\r\n"2",""\r\n"3","x"\r\n'
    with pytest.raises(ValueError, match="spelled"):
        _write_temp_csv(path, COLS, [(1, "\\N")])
    cur = _Cur()
    _copy_into_table(cur, "T", str(path), COLS, ["NUMBER", "VARCHAR"])
    copy = next(s for s in cur.sql if "COPY INTO" in s)
    assert "FIELD_OPTIONALLY_ENCLOSED_BY = '\"'" in copy
    assert "NULL_IF = ('\\\\N')" in copy


@pytest.mark.parametrize("with_text_null, method", [(False, "copy_into"), (True, "insert")])
def test_snowflake_text_backslash_n_takes_the_bind_path(monkeypatch, with_text_null, method):
    from connectors import snowflake_writer

    monkeypatch.setattr(snowflake_writer, "_bind_rows_for_snowflake", lambda rows, *a, **k: list(rows))
    monkeypatch.setattr(snowflake_writer, "_copy_into_table", lambda *a, **k: 300)
    cur = _Cur()
    rows = _rows(with_text_null)
    got = snowflake_writer._load_rows_into_table(
        cur, "T", COLS, ["NUMBER", "VARCHAR"], rows, prefer_copy=True, conn=object(),
    )
    assert got == method
    if with_text_null:
        bound = [r for _sql, batch in cur.many for r in batch]
        assert bound[1] == (2, "\\N") and bound[2] == (3, None) and bound[0] == (1, "")
    assert snowflake_writer.snowflake_stage_copy_safe(rows) is (not with_text_null)


def test_redshift_stage_refuses_cells_redshift_would_load_with_a_backslash():
    from connectors.redshift_copy import (
        build_redshift_copy_sql,
        redshift_text_stage_safe,
        render_redshift_copy_text,
    )

    assert render_redshift_copy_text([(1, None, "x", "")]) == b"1\t\\N\tx\t\n"
    assert redshift_text_stage_safe([(1, None, "x", "", 2.5)])
    for value in ("\\N", "a\\b", "a\tb", "a\nb", "a\rb"):
        assert not redshift_text_stage_safe([(1, value)]), repr(value)
        with pytest.raises(ValueError, match="INSERT path"):
            render_redshift_copy_text([(1, value)])
    sql = build_redshift_copy_sql("s", "t", COLS, "s3://dw-stage/df/t.tsv", iam_role="arn:aws:iam::123456789012:role/r")
    assert "NULL AS '\\\\N'" in sql and "ESCAPE" not in sql


def test_bigquery_loads_json_so_null_and_text_backslash_n_differ():
    source = Path(__file__).resolve().parents[1].joinpath("connectors", "bigquery_writer.py").read_text()
    assert "NEWLINE_DELIMITED_JSON" in source and "load_table_from_json" in source
    import json

    assert json.dumps({"a": None}) != json.dumps({"a": "\\N"})
