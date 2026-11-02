"""Iceberg v3 deletion-vector dest-engine codec (Puffin + roaring).

Spec sources (not marketing):

* Iceberg table spec — content=3, ``content_offset`` +
  ``content_size_in_bytes`` required, at most one DV per data file
  per snapshot.
* Puffin spec — ``deletion-vector-v1`` blob: 4-byte big-endian
  length, magic ``D1 D3 39 64``, portable roaring, CRC-32 of
  magic+vector (big-endian). Length/CRC are big-endian for Delta
  Lake compatibility; roaring is little-endian.
* Roaring portable format — cookie 12346 (no runs) or 12347 (runs),
  array / bitmap / run containers.

This module is the dest-engine reader. Iceberg writes in this product
stay copy-on-write. A corrupt, truncated, or oversized vector is
unmeasured — never ``data_footer − record_count``.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

PUFFIN_MAGIC = b"PFA1"
DV_MAGIC = bytes([0xD1, 0xD3, 0x39, 0x64])
DV_BLOB_TYPE = "deletion-vector-v1"

SERIAL_COOKIE_NO_RUNCONTAINER = 12346
SERIAL_COOKIE = 12347
NO_OFFSET_THRESHOLD = 4
BITMAP_CONTAINER_BYTES = 8192
ARRAY_CONTAINER_MAX = 4096
MAX_CONTAINERS = 1 << 16
# Spec allows 2 GiB; dest-engine refuses a blob that would OOM the
# COUNT process. Larger DVs stay unmeasured (leftover MERGE will not
# invent deletes).
MAX_DV_BLOB_BYTES = 32 * 1024 * 1024
MAX_DV_POSITIONS = 8_388_608

# Iceberg MetadataColumns.ROW_POSITION field id (footer only).
_ROW_POSITION_FIELD_ID = 2147483545


class IcebergDeletionVectorError(Exception):
    """Fail-closed: the puffin / roaring payload cannot be applied honestly."""


@dataclass(frozen=True)
class DeletionVectorBlob:
    positions: frozenset[int]
    referenced_data_file: str | None = None
    cardinality: int | None = None


def deletion_vector_byte_range(ref: Mapping[str, Any]) -> tuple[int, int] | None:
    """Manifest ``content_offset`` + ``content_size_in_bytes``, or None."""
    offset = _optional_nonneg_int(
        ref.get("content_offset")
        if ref.get("content_offset") is not None
        else ref.get("content-offset")
    )
    size = _optional_nonneg_int(
        ref.get("content_size_in_bytes")
        if ref.get("content_size_in_bytes") is not None
        else ref.get("content-size-in-bytes")
    )
    if offset is None or size is None:
        return None
    if size < 12 or size > MAX_DV_BLOB_BYTES:
        raise IcebergDeletionVectorError(
            f"deletion-vector content_size_in_bytes out of range: {size}"
        )
    return offset, size


def referenced_data_file_from_ref(ref: Mapping[str, Any]) -> str:
    raw = (
        ref.get("referenced_data_file")
        if ref.get("referenced_data_file") not in (None, "")
        else ref.get("referenced-data-file")
    )
    path = str(raw or "").strip()
    if not path:
        raise IcebergDeletionVectorError("deletion vector missing referenced_data_file")
    return path


def record_count_from_ref(ref: Mapping[str, Any]) -> int | None:
    return _optional_nonneg_int(
        ref.get("record_count")
        if ref.get("record_count") is not None
        else ref.get("record-count")
    )


def read_deletion_vector(
    path: Path,
    *,
    offset: int,
    size: int,
    referenced_data_file: str | None = None,
    record_count: int | None = None,
) -> DeletionVectorBlob:
    """Read one ``deletion-vector-v1`` blob at ``[offset, offset+size)``."""
    if size < 12 or size > MAX_DV_BLOB_BYTES:
        raise IcebergDeletionVectorError(
            f"deletion-vector blob size out of range: {size}"
        )
    try:
        with path.open("rb") as handle:
            handle.seek(int(offset))
            blob = handle.read(int(size))
    except OSError as exc:
        raise IcebergDeletionVectorError(f"deletion-vector unreadable: {path}") from exc
    if len(blob) != int(size):
        raise IcebergDeletionVectorError("deletion-vector short read")
    positions = decode_deletion_vector_blob(blob)
    footer_ref, footer_card = _footer_blob_properties(path, offset, size)
    referenced = referenced_data_file or footer_ref
    if referenced_data_file and footer_ref and referenced_data_file != footer_ref:
        raise IcebergDeletionVectorError(
            "deletion-vector referenced-data-file disagrees with puffin footer"
        )
    cardinality = record_count if record_count is not None else footer_card
    if (
        record_count is not None
        and footer_card is not None
        and int(record_count) != int(footer_card)
    ):
        raise IcebergDeletionVectorError(
            "deletion-vector record_count disagrees with puffin cardinality"
        )
    if cardinality is not None and int(cardinality) != len(positions):
        raise IcebergDeletionVectorError(
            "deletion-vector cardinality does not match decoded positions"
        )
    return DeletionVectorBlob(
        positions=frozenset(positions),
        referenced_data_file=referenced,
        cardinality=cardinality,
    )


def decode_deletion_vector_blob(blob: bytes) -> set[int]:
    """Parse the Iceberg ``deletion-vector-v1`` payload (no puffin wrapper)."""
    if len(blob) < 12:
        raise IcebergDeletionVectorError("deletion-vector blob shorter than header")
    declared = struct.unpack_from(">I", blob, 0)[0]
    if blob[4:8] != DV_MAGIC:
        raise IcebergDeletionVectorError("deletion-vector magic mismatch")
    # length field + (magic+vector) + CRC-32
    if declared < 4 or 8 + declared != len(blob):
        raise IcebergDeletionVectorError("deletion-vector length exceeds blob")
    roaring = blob[8 : 4 + declared]
    crc_stored = struct.unpack_from(">I", blob, 4 + declared)[0]
    crc_actual = zlib.crc32(DV_MAGIC + roaring) & 0xFFFFFFFF
    if crc_stored != crc_actual:
        raise IcebergDeletionVectorError("deletion-vector CRC-32 mismatch")
    return decode_roaring64(roaring)


def encode_deletion_vector_blob(positions: set[int] | frozenset[int]) -> bytes:
    """Serialize a spec-compliant ``deletion-vector-v1`` blob."""
    roaring = encode_roaring64(positions)
    inner = DV_MAGIC + roaring
    crc = zlib.crc32(inner) & 0xFFFFFFFF
    return struct.pack(">I", len(inner)) + inner + struct.pack(">I", crc)


def encode_roaring64(positions: set[int] | frozenset[int]) -> bytes:
    by_key: dict[int, list[int]] = {}
    for raw in positions:
        pos = _require_position(raw)
        key = pos >> 32
        by_key.setdefault(key, []).append(pos & 0xFFFFFFFF)
    out = bytearray(struct.pack("<Q", len(by_key)))
    for key in sorted(by_key):
        out += struct.pack("<I", key)
        out += encode_roaring32(by_key[key])
    return bytes(out)


def decode_roaring64(data: bytes) -> set[int]:
    if len(data) < 8:
        raise IcebergDeletionVectorError("roaring64 header truncated")
    count = struct.unpack_from("<Q", data, 0)[0]
    if count > MAX_CONTAINERS:
        raise IcebergDeletionVectorError("roaring64 bitmap count too large")
    offset = 8
    out: set[int] = set()
    prev_key = -1
    for _ in range(count):
        if offset + 4 > len(data):
            raise IcebergDeletionVectorError("roaring64 key truncated")
        key = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        if key < prev_key:
            raise IcebergDeletionVectorError("roaring64 keys are not sorted")
        prev_key = key
        subs, offset = _decode_roaring32_at(data, offset)
        for sub in subs:
            pos = (key << 32) | sub
            if pos < 0 or pos >= (1 << 63):
                raise IcebergDeletionVectorError("deletion-vector position MSB set")
            out.add(pos)
            if len(out) > MAX_DV_POSITIONS:
                raise IcebergDeletionVectorError("deletion-vector exceeds dest-engine cap")
    if offset != len(data):
        raise IcebergDeletionVectorError("roaring64 trailing bytes")
    return out


def encode_roaring32(values: list[int], *, use_runs: bool = False) -> bytes:
    containers: dict[int, list[int]] = {}
    for raw in values:
        if raw < 0 or raw > 0xFFFFFFFF:
            raise IcebergDeletionVectorError("roaring32 value out of range")
        key = raw >> 16
        containers.setdefault(key, []).append(raw & 0xFFFF)
    keys = sorted(containers)
    size = len(keys)
    run_flags = [False] * size
    payloads: list[bytes] = []
    cards: list[int] = []
    for i, key in enumerate(keys):
        vals = sorted(set(containers[key]))
        cards.append(len(vals))
        if use_runs:
            run_flags[i] = True
            payloads.append(_encode_run_container(vals))
        elif len(vals) > ARRAY_CONTAINER_MAX:
            payloads.append(_encode_bitmap_container(vals))
        else:
            payloads.append(_encode_array_container(vals))
    if use_runs and size:
        cookie = SERIAL_COOKIE | ((size - 1) << 16)
        header = bytearray(struct.pack("<I", cookie))
        run_bytes = bytearray((size + 7) // 8)
        for i, flag in enumerate(run_flags):
            if flag:
                run_bytes[i // 8] |= 1 << (i % 8)
        header += run_bytes
    else:
        header = bytearray(struct.pack("<II", SERIAL_COOKIE_NO_RUNCONTAINER, size))
    for key, card in zip(keys, cards):
        header += struct.pack("<HH", key, card - 1)
    if size >= NO_OFFSET_THRESHOLD:
        cursor = len(header) + 4 * size
        for payload in payloads:
            header += struct.pack("<I", cursor)
            cursor += len(payload)
    return bytes(header) + b"".join(payloads)


def decode_roaring32(data: bytes) -> set[int]:
    values, consumed = _decode_roaring32_at(data, 0)
    if consumed != len(data):
        raise IcebergDeletionVectorError("roaring32 trailing bytes")
    return set(values)


def write_puffin_deletion_vector(
    path: Path,
    positions: set[int] | frozenset[int],
    *,
    referenced_data_file: str,
) -> tuple[int, int]:
    """Write a one-blob Puffin file. Returns ``(content_offset, content_size)``."""
    blob = encode_deletion_vector_blob(set(positions))
    offset = len(PUFFIN_MAGIC)
    footer_payload = json.dumps(
        {
            "blobs": [
                {
                    "type": DV_BLOB_TYPE,
                    "fields": [_ROW_POSITION_FIELD_ID],
                    "snapshot-id": -1,
                    "sequence-number": -1,
                    "offset": offset,
                    "length": len(blob),
                    "properties": {
                        "referenced-data-file": referenced_data_file,
                        "cardinality": str(len(set(positions))),
                    },
                }
            ],
            "properties": {"created-by": "datawrap dest-engine"},
        },
        separators=(",", ":"),
    ).encode("utf-8")
    footer = (
        PUFFIN_MAGIC
        + footer_payload
        + struct.pack("<i", len(footer_payload))
        + b"\x00\x00\x00\x00"
        + PUFFIN_MAGIC
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(PUFFIN_MAGIC + blob + footer)
    return offset, len(blob)


def _decode_roaring32_at(data: bytes, offset: int) -> tuple[list[int], int]:
    if offset + 4 > len(data):
        raise IcebergDeletionVectorError("roaring32 cookie truncated")
    cookie = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    has_runs = (cookie & 0xFFFF) == SERIAL_COOKIE
    if has_runs:
        size = (cookie >> 16) + 1
    elif cookie == SERIAL_COOKIE_NO_RUNCONTAINER:
        if offset + 4 > len(data):
            raise IcebergDeletionVectorError("roaring32 size truncated")
        size = struct.unpack_from("<I", data, offset)[0]
        offset += 4
    else:
        raise IcebergDeletionVectorError(f"roaring32 unknown cookie {cookie}")
    if size > MAX_CONTAINERS:
        raise IcebergDeletionVectorError("roaring32 container count too large")
    run_bits = b""
    if has_runs:
        nbits = (size + 7) // 8
        if offset + nbits > len(data):
            raise IcebergDeletionVectorError("roaring32 run bitmap truncated")
        run_bits = data[offset : offset + nbits]
        offset += nbits
    keys: list[int] = []
    cards: list[int] = []
    for _ in range(size):
        if offset + 4 > len(data):
            raise IcebergDeletionVectorError("roaring32 container header truncated")
        key, card_m1 = struct.unpack_from("<HH", data, offset)
        offset += 4
        keys.append(key)
        cards.append(card_m1 + 1)
    if size >= NO_OFFSET_THRESHOLD:
        skip = 4 * size
        if offset + skip > len(data):
            raise IcebergDeletionVectorError("roaring32 offset table truncated")
        offset += skip
    values: list[int] = []
    prev_key = -1
    for i, (key, card) in enumerate(zip(keys, cards)):
        if key <= prev_key and i:
            raise IcebergDeletionVectorError("roaring32 keys are not sorted")
        prev_key = key
        is_run = bool(has_runs and run_bits and (run_bits[i // 8] & (1 << (i % 8))))
        if is_run:
            decoded, offset = _decode_run_container(data, offset)
        elif card > ARRAY_CONTAINER_MAX:
            decoded, offset = _decode_bitmap_container(data, offset)
        else:
            decoded, offset = _decode_array_container(data, offset, card)
        if len(decoded) != card:
            raise IcebergDeletionVectorError("roaring32 container cardinality mismatch")
        high = key << 16
        values.extend(high | value for value in decoded)
    return values, offset


def _encode_array_container(values: list[int]) -> bytes:
    return b"".join(struct.pack("<H", value) for value in values)


def _decode_array_container(
    data: bytes, offset: int, card: int
) -> tuple[list[int], int]:
    need = 2 * card
    if offset + need > len(data):
        raise IcebergDeletionVectorError("roaring32 array container truncated")
    values: list[int] = []
    prev = -1
    for i in range(card):
        value = struct.unpack_from("<H", data, offset + 2 * i)[0]
        if value <= prev:
            raise IcebergDeletionVectorError("roaring32 array is not strictly increasing")
        prev = value
        values.append(value)
    return values, offset + need


def _encode_bitmap_container(values: list[int]) -> bytes:
    words = [0] * 1024
    for value in values:
        words[value >> 6] |= 1 << (value & 63)
    return b"".join(struct.pack("<Q", word) for word in words)


def _decode_bitmap_container(data: bytes, offset: int) -> tuple[list[int], int]:
    if offset + BITMAP_CONTAINER_BYTES > len(data):
        raise IcebergDeletionVectorError("roaring32 bitmap container truncated")
    values: list[int] = []
    for i in range(1024):
        word = struct.unpack_from("<Q", data, offset + 8 * i)[0]
        if not word:
            continue
        base = i * 64
        bit = 0
        while word:
            if word & 1:
                values.append(base + bit)
            word >>= 1
            bit += 1
    return values, offset + BITMAP_CONTAINER_BYTES


def _encode_run_container(values: list[int]) -> bytes:
    if not values:
        raise IcebergDeletionVectorError("empty run container")
    runs: list[tuple[int, int]] = []
    start = values[0]
    prev = values[0]
    for value in values[1:]:
        if value == prev + 1:
            prev = value
            continue
        runs.append((start, prev - start))
        start = value
        prev = value
    runs.append((start, prev - start))
    out = bytearray(struct.pack("<H", len(runs)))
    for start, length_m1 in runs:
        out += struct.pack("<HH", start, length_m1)
    return bytes(out)


def _decode_run_container(data: bytes, offset: int) -> tuple[list[int], int]:
    if offset + 2 > len(data):
        raise IcebergDeletionVectorError("roaring32 run header truncated")
    nruns = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    need = 4 * nruns
    if offset + need > len(data):
        raise IcebergDeletionVectorError("roaring32 run container truncated")
    values: list[int] = []
    prev_end = -1
    for i in range(nruns):
        start, length_m1 = struct.unpack_from("<HH", data, offset + 4 * i)
        end = start + length_m1
        if start <= prev_end:
            raise IcebergDeletionVectorError("roaring32 runs overlap or are unsorted")
        prev_end = end
        values.extend(range(start, end + 1))
    return values, offset + need


def _footer_blob_properties(
    path: Path, offset: int, size: int
) -> tuple[str | None, int | None]:
    """Best-effort puffin footer cross-check. Missing/compressed footer is OK."""
    try:
        raw = path.read_bytes()
    except OSError:
        return None, None
    if len(raw) < 16 or raw[:4] != PUFFIN_MAGIC or raw[-4:] != PUFFIN_MAGIC:
        return None, None
    flags = raw[-8:-4]
    if flags[0] & 1:
        return None, None
    payload_size = struct.unpack_from("<i", raw, len(raw) - 12)[0]
    start = len(raw) - 12 - payload_size - 4
    if payload_size < 2 or start < 4 or raw[start : start + 4] != PUFFIN_MAGIC:
        return None, None
    try:
        payload = json.loads(raw[start + 4 : start + 4 + payload_size].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None, None
    blobs = payload.get("blobs") if isinstance(payload, dict) else None
    if not isinstance(blobs, list):
        return None, None
    for blob in blobs:
        if not isinstance(blob, dict):
            continue
        if str(blob.get("type") or "") != DV_BLOB_TYPE:
            continue
        if int(blob.get("offset") or -1) != int(offset):
            continue
        if int(blob.get("length") or -1) != int(size):
            continue
        props = blob.get("properties") or {}
        if not isinstance(props, dict):
            return None, None
        referenced = str(props.get("referenced-data-file") or "").strip() or None
        card = _optional_nonneg_int(props.get("cardinality"))
        return referenced, card
    return None, None


def _require_position(value: int) -> int:
    try:
        pos = int(value)
    except (TypeError, ValueError) as exc:
        raise IcebergDeletionVectorError("deletion-vector position is not an integer") from exc
    if pos < 0 or pos >= (1 << 63):
        raise IcebergDeletionVectorError("deletion-vector position MSB set")
    return pos


def _optional_nonneg_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise IcebergDeletionVectorError("boolean integer field")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise IcebergDeletionVectorError(f"invalid integer field {value!r}") from exc
    if parsed < 0:
        raise IcebergDeletionVectorError(f"negative integer field {parsed}")
    return parsed
