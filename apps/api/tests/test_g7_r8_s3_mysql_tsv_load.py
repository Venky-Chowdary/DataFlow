"""G7 re-review B1b: the S3→MySQL LOAD DATA fast path reads TSV quotes and never rewrites backslashes.

Before: a TSV object loaded with ``FIELDS TERMINATED BY '\\t'`` and no
``ENCLOSED BY``, so a quoted TSV cell (``"a<TAB>b"``, a quoted leading-quote cell) landed with
its quotes and split on the tab. Both CSV and TSV loaded with
``ESCAPED BY '\\\\'``, so a value such as ``a\\b`` was rewritten by MySQL's
escape processing (``\\b`` is a backspace, ``\\N`` NULL, ``\\x`` → ``x``) —
under a COUNT(*)-only proof. Live LOAD DATA is unavailable here
(``local_infile=OFF``), so the statement and the bytes are asserted.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


class _Cursor:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql, *_a, **_k):
        self.sql.append(sql)

    def fetchall(self):
        return []


def _load(monkeypatch, path: Path, ext: str):
    import connectors.mysql_load_data as mld
    from services import copy_s3_mysql

    calls: list[dict] = []

    def _capture(cursor, **kwargs):
        calls.append(kwargs)
        cursor.execute(
            mld.build_load_data_sql(
                table_q="`t`",
                columns=kwargs["columns"],
                infile_sql="'/tmp/x'",
                field_terminator=kwargs["field_terminator"],
                optionally_enclosed=kwargs["optionally_enclosed"],
                ignore_lines=kwargs["ignore_lines"],
            )
        )

    monkeypatch.setattr(mld, "mysql_load_data_session_ready", lambda *_a, **_k: (True, ""))
    monkeypatch.setattr(mld, "execute_load_data_local", _capture)
    monkeypatch.setattr(mld, "fetch_load_data_warnings", lambda _cur: [])
    cur = _Cursor()
    copy_s3_mysql._load_delimited_into_mysql(
        object(), cur, path=str(path), table_q="`t`", columns=["id", "note"], ext=ext
    )
    return calls, cur.sql


def _object_store_tsv(rows: list[tuple[str, str]]) -> bytes:
    from connectors.object_store_common import serialize_object_store_export

    return serialize_object_store_export(
        key="out/data.tsv",
        mapped_rows=rows,
        target_cols=["id", "note"],
        dest_types={"id": "TEXT", "note": "TEXT"},
    ).read_all()


def test_tsv_load_is_quote_aware(monkeypatch, tmp_path):
    body = _object_store_tsv([("1", '5" pipe'), ("2", "a\tb"), ("3", '"lead'), ("4", "x\ny")])
    # What lands in S3: mid-field quote raw, tab/LF/leading-quote cells quoted.
    assert body.replace(b"\r\n", b"\n") == (
        b'id\tnote\n1\t5" pipe\n2\t"a\tb"\n3\t"""lead"\n4\t"x\ny"\n'
    )
    path = tmp_path / "src.tsv"
    path.write_bytes(body)
    calls, sql = _load(monkeypatch, path, "tsv")
    assert calls[0]["field_terminator"] == "\t"
    assert calls[0]["optionally_enclosed"] is True
    assert calls[0]["ignore_lines"] == 1
    # MySQL: a field that starts with the enclosure is read up to the closing
    # quote ("" -> "); a field that does not start with it keeps '"' literally.
    assert "FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"'" in sql[0]


def test_csv_load_statement_unchanged(monkeypatch, tmp_path):
    path = tmp_path / "src.csv"
    path.write_bytes(b'id,note\n1,"5"" pipe"\n')
    calls, sql = _load(monkeypatch, path, "csv")
    assert calls[0]["field_terminator"] == ","
    assert calls[0]["optionally_enclosed"] is True
    assert "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'" in sql[0]


@pytest.mark.parametrize("ext, body", [
    ("tsv", b"id\tnote\n1\ta\\b\n"),
    ("csv", b"id,note\n1,a\\b\n"),
    ("tsv", b"id\tnote\n1\t\\N\n"),
])
def test_backslash_object_declines_to_the_row_path(monkeypatch, tmp_path, ext, body):
    """ESCAPED BY '\\\\' would rewrite a\\b; the row path loads it exactly."""
    from services.copy_fast_path import FastPathUnavailable

    path = tmp_path / f"src.{ext}"
    path.write_bytes(body)
    with pytest.raises(FastPathUnavailable, match="backslash"):
        _load(monkeypatch, path, ext)
