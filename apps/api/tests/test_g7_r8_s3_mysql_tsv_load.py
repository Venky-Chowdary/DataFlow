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
                **({"line_terminator": kwargs["line_terminator"]} if "line_terminator" in kwargs else {}),
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


# ── K1: an unquoted NULL field would load as SQL NULL under ENCLOSED BY ──────


@pytest.mark.parametrize("ext, body", [
    ("tsv", b"id\tnote\n1\tNULL\n"),
    ("tsv", b"id\tnote\n1\tNULL"),
    ("tsv", b"id\tnote\nNULL\tx\n"),
    ("tsv", b"id\tnote\r\n1\tNULL\r\n"),
    ("csv", b"id,note\n1,NULL\n"),
    ("csv", b'id,note\n"1",NULL,\n'),
])
def test_unquoted_null_field_declines_to_the_row_path(monkeypatch, tmp_path, ext, body):
    """MySQL LOAD DATA with FIELDS ENCLOSED BY reads an unenclosed NULL as SQL NULL."""
    from services.copy_fast_path import FastPathUnavailable

    path = tmp_path / f"src.{ext}"
    path.write_bytes(body)
    with pytest.raises(FastPathUnavailable, match="NULL"):
        _load(monkeypatch, path, ext)


@pytest.mark.parametrize("ext, body", [
    ("tsv", b'id\tnote\n1\t"NULL"\n'),
    ("tsv", b"id\tnote\n1\tNULLx\n2\txNULL\n3\tnull\n"),
    ("csv", b'id,note\n1,"NULL"\n2,NULLx\n3,xNULL\n'),
])
def test_quoted_or_partial_null_still_loads(monkeypatch, tmp_path, ext, body):
    path = tmp_path / f"src.{ext}"
    path.write_bytes(body)
    calls, sql = _load(monkeypatch, path, ext)
    assert len(calls) == 1 and "LOAD DATA" in sql[0]


class _FakeConn:
    def __init__(self) -> None:
        self.cur = _Cursor()

    def cursor(self):
        return self.cur

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


@pytest.mark.parametrize("ext, body", [
    ("tsv", b"id\tnote\n1\tNULL\n"),
    ("csv", b"id,note\n1,NULL\n"),
])
def test_local_csv_tsv_to_mysql_declines_unquoted_null(monkeypatch, tmp_path, ext, body):
    """The local file → MySQL fast path shares the LOAD DATA loader."""
    import connectors.mysql_load_data as mld
    from services import copy_csv_local, copy_mysql_pg
    from services.copy_fast_path import FastPathUnavailable

    conn = _FakeConn()
    conn.cur.fetchone = lambda: (0,)
    monkeypatch.setattr(copy_mysql_pg, "_mysql_connect", lambda _cfg: conn)
    monkeypatch.setattr(mld, "mysql_load_data_session_ready", lambda *_a, **_k: (True, ""))
    loads: list[dict] = []
    monkeypatch.setattr(mld, "execute_load_data_local", lambda _c, **kw: loads.append(kw))
    path = tmp_path / f"mapped.{ext}"
    path.write_bytes(body)
    with pytest.raises(FastPathUnavailable, match="NULL"):
        copy_csv_local.copy_csv_to_mysql(
            mapped_path=str(path), ext=ext, dest_cfg={"host": "127.0.0.1"},
            dest_table="t", pairs=[("id", "id"), ("note", "note")],
            mysql_ddls=["BIGINT", "TEXT"], source_count=1, replace_destination=False,
        )
    assert loads == []


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 5, 7, 1 << 20])
def test_scan_finds_null_field_across_chunk_boundaries(tmp_path, chunk_size):
    from services.copy_s3_common import scan_delimited_file

    hit = tmp_path / "hit.tsv"
    hit.write_bytes(b"id\tnote\n11\tNULL\n22\tok\n")
    miss = tmp_path / "miss.tsv"
    miss.write_bytes(b'id\tnote\n11\t"NULL"\n22\tNULLx\n33\txNULL\n')
    assert scan_delimited_file(str(hit), "\t", chunk_size=chunk_size).unquoted_null is True
    assert scan_delimited_file(str(miss), "\t", chunk_size=chunk_size).unquoted_null is False



# ── line endings: the loader's LINES TERMINATED BY matches the file ──────────


@pytest.mark.parametrize("ext, body, terminator", [
    ("csv", b"id,note\r\n1,a\r\n2,b\r\n", "\\r\\n"),
    ("tsv", b"id\tnote\r\n1\ta\r\n2\tb", "\\r\\n"),
    ("csv", b"id,note\n1,a\n2,b\n", "\\n"),
    ("tsv", b"id\tnote\n1\ta\n", "\\n"),
])
def test_load_statement_matches_the_file_line_endings(monkeypatch, tmp_path, ext, body, terminator):
    """CRLF files loaded with LINES TERMINATED BY '\\n' kept a \\r on every last column."""
    path = tmp_path / f"src.{ext}"
    path.write_bytes(body)
    _calls, sql = _load(monkeypatch, path, ext)
    assert f"LINES TERMINATED BY '{terminator}'" in sql[0], sql[0]


@pytest.mark.parametrize("ext, body", [
    ("csv", b"id,note\r\n1,a\n2,b\r\n"),
    ("tsv", b"id\tnote\n1\ta\r\n"),
    ("csv", b"id,note\r1,a\r"),
    ("csv", b'id,note\r\n1,"a\nb"\r\n'),
])
def test_mixed_or_bare_cr_line_endings_decline(monkeypatch, tmp_path, ext, body):
    from services.copy_fast_path import FastPathUnavailable

    path = tmp_path / f"src.{ext}"
    path.write_bytes(body)
    with pytest.raises(FastPathUnavailable, match="line endings"):
        _load(monkeypatch, path, ext)


def test_crlf_null_field_is_still_declined(monkeypatch, tmp_path):
    from services.copy_fast_path import FastPathUnavailable

    path = tmp_path / "src.csv"
    path.write_bytes(b"id,note\r\n1,NULL\r\n")
    with pytest.raises(FastPathUnavailable, match="NULL"):
        _load(monkeypatch, path, "csv")


def test_row_path_reads_mixed_line_endings_exactly(tmp_path):
    from services.copy_s3_common import s3_iter_delimited_rows

    path = tmp_path / "mixed.csv"
    path.write_bytes(b"id,note\r\n1,a\n2,b\r\n3,c")
    assert list(s3_iter_delimited_rows(str(path), ",")) == [["1", "a"], ["2", "b"], ["3", "c"]]


@pytest.mark.parametrize("ext, body", [
    ("csv", b"id,note\r\n1,a\r\n"),
    ("tsv", b"id\tnote\n1\ta\n"),
])
def test_local_to_mysql_uses_the_detected_terminator(monkeypatch, tmp_path, ext, body):
    import connectors.mysql_load_data as mld
    from services import copy_csv_local, copy_mysql_pg

    conn = _FakeConn()
    rows = iter([(0,), (1,)])
    conn.cur.fetchone = lambda: next(rows)
    monkeypatch.setattr(copy_mysql_pg, "_mysql_connect", lambda _cfg: conn)
    monkeypatch.setattr(mld, "mysql_load_data_session_ready", lambda *_a, **_k: (True, ""))
    monkeypatch.setattr(mld, "fetch_load_data_warnings", lambda _cur: [])
    loads: list[dict] = []
    monkeypatch.setattr(mld, "execute_load_data_local", lambda _c, **kw: loads.append(kw))
    path = tmp_path / f"mapped.{ext}"
    path.write_bytes(body)
    copy_csv_local.copy_csv_to_mysql(
        mapped_path=str(path), ext=ext, dest_cfg={"host": "127.0.0.1"},
        dest_table="t", pairs=[("id", "id"), ("note", "note")],
        mysql_ddls=["BIGINT", "TEXT"], source_count=1, replace_destination=False,
    )
    expected = "\r\n" if b"\r\n" in body else "\n"
    assert loads and loads[0].get("line_terminator", "\n") == expected


# ── encoding: LOAD DATA reads utf8mb4 bytes ──────────────────────────────────


@pytest.mark.parametrize("body", [
    "id,note\n1,é\n".encode("utf-16"),
    "id,note\n1,é\n".encode("utf-16-be"),
    "id,note\n1,é\n".encode("utf-32"),
    b"id,note\n1,a\x00b\n",
])
def test_wide_encodings_and_nul_bytes_decline(monkeypatch, tmp_path, body):
    from services.copy_fast_path import FastPathUnavailable

    path = tmp_path / "src.csv"
    path.write_bytes(body)
    with pytest.raises(FastPathUnavailable, match="UTF-8"):
        _load(monkeypatch, path, "csv")


def test_utf8_bom_rides_on_the_skipped_header_line(monkeypatch, tmp_path):
    """Every loader file has a header row (IGNORE 1 LINES); a UTF-8 BOM is part of it."""
    path = tmp_path / "src.csv"
    path.write_bytes(b"\xef\xbb\xbfid,note\n1,a\n")
    calls, sql = _load(monkeypatch, path, "csv")
    assert calls[0]["ignore_lines"] == 1
    assert "IGNORE 1 LINES" in sql[0]
