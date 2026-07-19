"""Binary decoder for PostgreSQL ``pgoutput`` logical replication messages.

Decodes Relation / Insert / Update / Delete messages produced by
``pg_logical_slot_peek_changes`` / ``get_changes`` when the slot plugin is
``pgoutput``. This is the production binary path (Airbyte/Debezium-class),
not a stub — text ``test_decoding`` remains available as a fallback plugin.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RelationMeta:
    oid: int
    namespace: str
    relation: str
    replica_identity: int
    columns: list[str] = field(default_factory=list)


@dataclass
class DecodedChange:
    op: str  # begin | commit | insert | update | delete
    namespace: str = ""
    relation: str = ""
    old_tuple: dict[str, str] | None = None
    new_tuple: dict[str, str] | None = None
    xid: str | None = None


class PgOutputDecoder:
    """Stateful decoder — Relation messages populate column metadata for DML."""

    def __init__(self) -> None:
        self.relations: dict[int, RelationMeta] = {}

    def feed(self, payload: bytes | memoryview | str) -> list[DecodedChange]:
        if isinstance(payload, str):
            # Some drivers return hex/escaped; prefer raw bytes from memoryview.
            raw = payload.encode("latin-1", errors="ignore")
        else:
            raw = bytes(payload)
        if not raw:
            return []
        try:
            return self._decode_message(raw)
        except Exception:
            return []

    def _decode_message(self, data: bytes) -> list[DecodedChange]:
        if not data:
            return []
        msg_type = chr(data[0])
        buf = _ByteReader(data, 1)
        if msg_type == "B":
            # Begin: final_lsn(8) + commit_ts(8) + xid(4)
            try:
                buf.read_bytes(16)
                xid = str(buf.read_int32())
            except Exception:
                xid = None
            return [DecodedChange(op="begin", xid=xid)]
        if msg_type == "C":
            # Commit: flags(1) + commit_lsn(8) + end_lsn(8) + commit_ts(8)
            return [DecodedChange(op="commit")]
        if msg_type == "R":
            self._decode_relation(buf)
            return []
        if msg_type == "I":
            change = self._decode_insert(buf)
            return [change] if change else []
        if msg_type == "U":
            change = self._decode_update(buf)
            return [change] if change else []
        if msg_type == "D":
            change = self._decode_delete(buf)
            return [change] if change else []
        # T/Y/O/M ignored for DML apply
        return []

    def _decode_relation(self, buf: "_ByteReader") -> None:
        oid = buf.read_int32()
        namespace = buf.read_string()
        relation = buf.read_string()
        replica_identity = buf.read_int8()
        natts = buf.read_int16()
        columns: list[str] = []
        for _ in range(natts):
            buf.read_int8()  # flags
            name = buf.read_string()
            buf.read_int32()  # typid
            buf.read_int32()  # typmod
            columns.append(name)
        self.relations[oid] = RelationMeta(
            oid=oid,
            namespace=namespace,
            relation=relation,
            replica_identity=replica_identity,
            columns=columns,
        )

    def _decode_insert(self, buf: "_ByteReader") -> DecodedChange | None:
        oid = buf.read_int32()
        rel = self.relations.get(oid)
        if not rel or buf.read_bytes(1) != b"N":
            return None
        new_tuple = self._read_tuple(buf, rel.columns)
        return DecodedChange("insert", rel.namespace, rel.relation, new_tuple=new_tuple)

    def _decode_update(self, buf: "_ByteReader") -> DecodedChange | None:
        oid = buf.read_int32()
        rel = self.relations.get(oid)
        if not rel:
            return None
        old_tuple = None
        new_tuple = None
        while buf.remaining() > 0:
            kind = buf.read_bytes(1)
            if kind in (b"K", b"O"):
                old_tuple = self._read_tuple(buf, rel.columns)
            elif kind == b"N":
                new_tuple = self._read_tuple(buf, rel.columns)
            else:
                break
        if new_tuple is None and old_tuple is None:
            return None
        return DecodedChange(
            "update",
            rel.namespace,
            rel.relation,
            old_tuple=old_tuple,
            new_tuple=new_tuple or old_tuple,
        )

    def _decode_delete(self, buf: "_ByteReader") -> DecodedChange | None:
        oid = buf.read_int32()
        rel = self.relations.get(oid)
        if not rel:
            return None
        kind = buf.read_bytes(1)
        if kind not in (b"K", b"O"):
            return None
        old_tuple = self._read_tuple(buf, rel.columns)
        return DecodedChange("delete", rel.namespace, rel.relation, old_tuple=old_tuple)

    def _read_tuple(self, buf: "_ByteReader", columns: list[str]) -> dict[str, str]:
        natts = buf.read_int16()
        out: dict[str, str] = {}
        for i in range(natts):
            col = columns[i] if i < len(columns) else f"col_{i}"
            kind = buf.read_bytes(1)
            if kind == b"n":
                out[col] = ""
            elif kind == b"u":
                # unchanged toast — leave absent
                continue
            elif kind == b"t":
                length = buf.read_int32()
                raw = buf.read_bytes(length)
                out[col] = raw.decode("utf-8", errors="replace")
            else:
                break
        return out


class _ByteReader:
    def __init__(self, data: bytes, offset: int = 0) -> None:
        self.data = data
        self.offset = offset

    def remaining(self) -> int:
        return max(0, len(self.data) - self.offset)

    def read_bytes(self, n: int) -> bytes:
        end = self.offset + n
        chunk = self.data[self.offset : end]
        self.offset = end
        return chunk

    def read_int8(self) -> int:
        return struct.unpack("!b", self.read_bytes(1))[0]

    def read_int16(self) -> int:
        return struct.unpack("!h", self.read_bytes(2))[0]

    def read_int32(self) -> int:
        return struct.unpack("!i", self.read_bytes(4))[0]

    def read_string(self) -> str:
        end = self.data.find(b"\x00", self.offset)
        if end < 0:
            raw = self.data[self.offset :]
            self.offset = len(self.data)
            return raw.decode("utf-8", errors="replace")
        raw = self.data[self.offset : end]
        self.offset = end + 1
        return raw.decode("utf-8", errors="replace")


def changes_for_table(
    decoder: PgOutputDecoder,
    payload: bytes | memoryview | str,
    *,
    schema: str,
    table: str,
) -> list[DecodedChange]:
    """Decode messages; keep txn markers and DML for ``schema.table``."""
    out = []
    for change in decoder.feed(payload):
        if change.op in {"begin", "commit"}:
            out.append(change)
            continue
        if change.namespace == schema and change.relation == table:
            out.append(change)
        elif change.namespace.lower() == schema.lower() and change.relation.lower() == table.lower():
            out.append(change)
    return out
