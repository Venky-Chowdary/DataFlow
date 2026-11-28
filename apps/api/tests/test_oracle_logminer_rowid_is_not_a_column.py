"""LogMiner's ROWID in UPDATE/DELETE redo is the row address, not a column.

LogMiner writes ``... where "ID" = '1' and ROWID = 'AAAS...'`` into SQL_REDO.
Parsed as a column, ROWID reached the buffer-off CDC path as a source column
"added after this capture started" and stopped every buffer-off run on a LOB
or large-VARCHAR table (oracle-live CI on Oracle Database Free:
test_cdc_oracle_logminer_lob_live[buffer-off],
test_oracle_csf_large_varchar_exactly_once_survives_restart[buffer-off]).
The transaction-buffer path already dropped it; the parser now does, for both.
"""

from __future__ import annotations

from connectors.oracle_logminer import BEFORE_IMAGE_KEY, classify_sql_redo

_ADDR = "AAASaaAAMAAAADHAAA"


def test_update_redo_keeps_columns_and_drops_the_row_address() -> None:
    kind, row = classify_sql_redo(
        'update "DATAFLOW"."T" set "NOTE" = \'x\' where "ID" = \'1\' and "NOTE" = \'y\' '
        f"and ROWID = '{_ADDR}';",
        op="update", table="T",
    )
    assert kind == "ok"
    assert row["ID"] == "1" and row["NOTE"] == "x"
    assert "ROWID" not in row
    assert "ROWID" not in row[BEFORE_IMAGE_KEY] and row[BEFORE_IMAGE_KEY]["NOTE"] == "y"


def test_delete_redo_drops_the_row_address() -> None:
    kind, row = classify_sql_redo(
        f'delete from "DATAFLOW"."T" where "ID" = \'7\' and ROWID = \'{_ADDR}\';',
        op="delete", table="T",
    )
    assert (kind, row) == ("ok", {"ID": "7"})


def test_a_quoted_column_named_rowid_is_still_a_column() -> None:
    kind, row = classify_sql_redo(
        'update "DATAFLOW"."T" set "ROWID" = \'r2\' where "ID" = \'1\' '
        f"and ROWID = '{_ADDR}';",
        op="update", table="T",
    )
    assert kind == "ok" and row["ROWID"] == "r2" and row["ID"] == "1"


def test_a_redo_with_only_the_row_address_is_unparsed_not_an_empty_row() -> None:
    kind, row = classify_sql_redo(
        f"delete from \"DATAFLOW\".\"T\" where ROWID = '{_ADDR}';", op="delete", table="T",
    )
    assert kind == "unparsed", row
