"""D108 — appending to an existing S3 CSV must keep CR/CRLF inside quoted
fields byte-identical; the merge must not fold them to LF.

Two corruption points are pinned: ``_first_line`` must split the header
without rewriting the appended tail, and ``_parse_delimited`` must read
with newline="" so the csv module sees CR/CRLF inside quotes.
"""

from __future__ import annotations

from connectors.object_store_incremental import (
    _delimited_concat,
    _first_line,
    _parse_delimited,
)


def test_delimited_concat_preserves_cr_in_appended_rows():
    existing = b'id,v\n1,"a\rb"\n'
    new = b'id,v\n2,"a\rb"\n3,"a\r\nb"\n4,"a\r"\n5,"\r"\n6,"a\nb"\n7,plain\n'
    out = _delimited_concat(existing, new)
    assert out is not None
    # Appended rows are byte-identical to the writer output — CR, CRLF and a
    # bare-LF field all survive exactly.
    assert out == (
        b'id,v\n1,"a\rb"\n'
        b'2,"a\rb"\n3,"a\r\nb"\n4,"a\r"\n5,"\r"\n6,"a\nb"\n7,plain\n'
    )


def test_delimited_concat_crlf_headers_still_match():
    # CRLF-terminating existing object vs LF writer output — headers compare
    # on content, appended bytes stay verbatim.
    existing = b'id,v\r\n1,"a\rb"\r\n'
    new = b'id,v\n2,"x\ry"\n'
    out = _delimited_concat(existing, new)
    assert out is not None
    assert out == b'id,v\r\n1,"a\rb"\r\n2,"x\ry"\n'


def test_delimited_concat_mismatched_headers_decline():
    assert _delimited_concat(b'id,v\n1,a\n', b'id,x\n2,b\n') is None


def test_first_line_keeps_tail_verbatim():
    head, rest = _first_line(b'id,v\n1,"a\r\nb"\n2,"c\rd"\n')
    assert head == b"id,v"
    assert rest == b'1,"a\r\nb"\n2,"c\rd"\n'


def test_first_line_single_crlf_file():
    head, rest = _first_line(b'id,v\r\n1,a\r\n')
    assert head == b"id,v"
    assert rest == b"1,a\r\n"


def test_parse_delimited_reads_quoted_cr_exactly():
    body = b'id,v\n1,"a\rb"\n2,"a\r\nb"\n3,"a\r"\n4,"\r"\n5,"a\nb"\n6,plain\n'
    rows = _parse_delimited(body, ",", "t.csv")
    vals = [r["v"] for r in rows]
    assert vals == ["a\rb", "a\r\nb", "a\r", "\r", "a\nb", "plain"]


def test_parse_delimited_crlf_file_reads_quoted_cr():
    body = b'id,v\r\n1,"a\rb"\r\n2,"x\r\ny"\r\n'
    rows = _parse_delimited(body, ",", "t.csv")
    assert [r["v"] for r in rows] == ["a\rb", "x\r\ny"]
