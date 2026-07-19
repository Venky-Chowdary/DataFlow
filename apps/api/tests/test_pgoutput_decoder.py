"""Unit tests for the binary pgoutput logical replication decoder."""

from __future__ import annotations

import struct

from connectors.pgoutput_decoder import PgOutputDecoder, changes_for_table


def _string(s: str) -> bytes:
    return s.encode("utf-8") + b"\x00"


def _relation_msg(
    oid: int = 12345,
    namespace: str = "public",
    relation: str = "orders",
    columns: list[str] | None = None,
) -> bytes:
    columns = columns or ["id", "amount"]
    body = b"R"
    body += struct.pack("!i", oid)
    body += _string(namespace) + _string(relation)
    body += struct.pack("!b", 0)  # replica identity
    body += struct.pack("!h", len(columns))
    for name in columns:
        body += struct.pack("!b", 0)  # flags
        body += _string(name)
        body += struct.pack("!i", 23)  # typid int4
        body += struct.pack("!i", -1)  # typmod
    return body


def _tuple(values: list[str | None]) -> bytes:
    body = struct.pack("!h", len(values))
    for v in values:
        if v is None:
            body += b"n"
        else:
            raw = v.encode("utf-8")
            body += b"t" + struct.pack("!i", len(raw)) + raw
    return body


def _insert_msg(oid: int = 12345, values: list[str | None] | None = None) -> bytes:
    values = values or ["1", "100.00"]
    return b"I" + struct.pack("!i", oid) + b"N" + _tuple(values)


def _delete_msg(oid: int = 12345, values: list[str | None] | None = None) -> bytes:
    values = values or ["1", None]
    return b"D" + struct.pack("!i", oid) + b"K" + _tuple(values)


def test_pgoutput_decode_insert_and_delete() -> None:
    decoder = PgOutputDecoder()
    assert decoder.feed(_relation_msg()) == []
    inserts = changes_for_table(decoder, _insert_msg(), schema="public", table="orders")
    assert len(inserts) == 1
    assert inserts[0].op == "insert"
    assert inserts[0].new_tuple == {"id": "1", "amount": "100.00"}

    deletes = changes_for_table(decoder, _delete_msg(), schema="public", table="orders")
    assert len(deletes) == 1
    assert deletes[0].op == "delete"
    assert deletes[0].old_tuple["id"] == "1"


def test_pgoutput_filters_other_tables() -> None:
    decoder = PgOutputDecoder()
    decoder.feed(_relation_msg(relation="other"))
    assert changes_for_table(decoder, _insert_msg(), schema="public", table="orders") == []
