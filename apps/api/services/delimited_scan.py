"""Field-aware scan of delimited bytes: quoted fields, record breaks, bare quotes.

A ``"`` opens a quoted field only at the start of a field (byte 0, or right
after the delimiter, CR or LF). Inside it the delimiter, CR and LF are data and
``""`` is one quote; the field ends at a ``"`` that is followed by the
delimiter, a line break or the end of the file (RFC 4180 section 2, rules
5-7). That is how Python's ``csv`` module, MySQL ``LOAD DATA ... OPTIONALLY
ENCLOSED BY '"'`` and PostgreSQL ``COPY (FORMAT csv)`` read a well-formed file.

A *bare* quote is any other ``"``: one in the middle of an unquoted field
(``5" pipe``), text after a closing quote (``"a"b``) or a quoted field that
never closes. Python's ``csv`` reads a mid-field quote literally; PostgreSQL's
CSV parser starts a quoted section there instead (``a"b"c`` loads as ``abc``
and ``5" pipe`` swallows the following rows), so a bulk COPY of such a file
must decline to the row path.

The scan walks quote characters only (``bytes.find``), so its Python cost is
per quote, not per byte, and it can be fed a file chunk by chunk.
"""

from __future__ import annotations

_QUOTE = b'"'


class QuoteScanner:
    """Streaming quote state over a delimited byte stream.

    ``feed`` chunks in order, then ``finish``. ``bare`` is true once a bare
    quote was seen. With ``record_spans=True`` the scanner keeps the
    ``[start, end)`` byte offsets of every quoted field (closing quote
    included), which :func:`record_breaks` and :func:`rewrite_record_breaks`
    use to leave quoted line breaks alone.
    """

    def __init__(self, delimiter: str | bytes = ",", *, record_spans: bool = False) -> None:
        delim = delimiter.encode("utf-8") if isinstance(delimiter, str) else bytes(delimiter)
        # A multi-byte delimiter is not a product dialect: every quote is bare.
        self._single = len(delim) == 1
        self._bounds = (delim, b"\r", b"\n")
        self._in_quote = False
        self._prev = b"\n"  # a virtual record break before byte 0
        self._pending = False  # the last byte fed was a quote inside a quoted field
        self._offset = 0
        self._open_at = -1
        self.bare = False
        self.spans: list[tuple[int, int]] | None = [] if record_spans else None

    def _close(self, end: int) -> None:
        self._in_quote = False
        if self.spans is not None:
            self.spans.append((self._open_at, end))

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        n = len(chunk)
        i = 0
        if self._pending:
            self._pending = False
            if chunk[:1] == _QUOTE:
                i = 1  # ``""``: one quote of data, the field is still open
            else:
                self._close(self._offset)
                if chunk[:1] not in self._bounds:
                    self.bare = True
        while True:
            j = chunk.find(_QUOTE, i)
            if j < 0:
                break
            if not self._in_quote:
                prev = chunk[j - 1 : j] if j > 0 else self._prev
                if self._single and prev in self._bounds:
                    self._in_quote = True
                    self._open_at = self._offset + j
                else:
                    # Mid-field: data to Python csv, a quote opener to PostgreSQL.
                    self.bare = True
                i = j + 1
                continue
            if j + 1 >= n:
                self._pending = True
                break
            nxt = chunk[j + 1 : j + 2]
            if nxt == _QUOTE:
                i = j + 2
                continue
            self._close(self._offset + j + 1)
            if nxt not in self._bounds:
                self.bare = True
            i = j + 1
        self._prev = chunk[-1:]
        self._offset += n

    def finish(self) -> bool:
        if self._pending:
            self._pending = False
            self._close(self._offset)
        elif self._in_quote:
            # A quoted field that never closes: the rest of the file is that field.
            self.bare = True
            self._close(self._offset)
        return self.bare


def _scan(body: bytes, delimiter: str | bytes) -> QuoteScanner:
    scanner = QuoteScanner(delimiter, record_spans=True)
    scanner.feed(body)
    scanner.finish()
    return scanner


def _outside_segments(body: bytes, spans: list[tuple[int, int]]):
    """Yield ``(segment, outside)`` pieces of ``body`` in order."""
    pos = 0
    for start, end in spans:
        if start > pos:
            yield body[pos:start], True
        yield body[start:end], False
        pos = end
    if pos < len(body):
        yield body[pos:], True


def record_breaks(body: bytes, delimiter: str | bytes = ",") -> tuple[int, int, int]:
    """``(crlf, bare_lf, bare_cr)`` record breaks outside quoted fields."""
    crlf = lf = cr = 0
    for segment, outside in _outside_segments(body, _scan(body, delimiter).spans or []):
        if not outside:
            continue
        pairs = segment.count(b"\r\n")
        crlf += pairs
        lf += segment.count(b"\n") - pairs
        cr += segment.count(b"\r") - pairs
    return crlf, lf, cr


def rewrite_record_breaks(body: bytes, terminator: bytes, delimiter: str | bytes = ",") -> bytes:
    """``body`` with every record break outside quoted fields spelled ``terminator``.

    A line break inside a quoted field is data and is copied byte for byte.
    """
    out: list[bytes] = []
    for segment, outside in _outside_segments(body, _scan(body, delimiter).spans or []):
        if outside:
            segment = segment.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
            if terminator != b"\n":
                segment = segment.replace(b"\n", terminator)
        out.append(segment)
    return b"".join(out)
