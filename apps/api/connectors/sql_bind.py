"""Shared SQL bind normalization — Mongo/quarantine wire → driver-safe values.

MySQL, Postgres, and generic_sql writers must share one algorithm for:
- boolean string wire (``cell_to_string`` → ``"true"``/``"false"``)
- JSON empty / scalar / nested bind (MySQL 3140 class, JSONB dict bind)

Callers choose representation via ``bool_as_int`` / ``json_as_text``.
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any

# Canonical boolean wire only — SSOT with type_system / transform_engine.
# Informal yes/on/y/no/n invents truth; quarantine or operator transform owns those.
_TRUE_TOKENS = frozenset({"true", "t", "1"})
_FALSE_TOKENS = frozenset({"false", "f", "0"})


def _refuse_empty_specialty(text: str, label: str) -> str:
    """Fail-closed empty wire for typed specialty DDL (never invent SQL NULL).

    Explicit ``None`` / DF_MISSING stay caller-owned. Empty ``\"\"`` on upsert
    would wipe a present destination cell — quarantine or remap instead.
    """
    if not text:
        raise ValueError(
            f"empty string cannot coerce to {label} — "
            "refuse silent NULL invent (quarantine or remap upstream)"
        )
    return text


def coerce_inet_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``INET`` wire (host or host/prefix).

    Prefer ``ipaddress`` objects for psycopg adaptation; validate strings
    fail-closed (Postgres network types — never invent from ints/floats).
    """
    if value is None:
        return None
    from ipaddress import (
        IPv4Address,
        IPv4Interface,
        IPv6Address,
        IPv6Interface,
        ip_address,
        ip_interface,
    )

    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (IPv4Address, IPv4Interface, IPv6Address, IPv6Interface)):
        return value
    if isinstance(value, (bool, int, float, list, dict, bytes, bytearray, memoryview)):
        raise ValueError(
            f"inet wire cannot bind {type(value).__name__} — refuse invent into INET"
        )
    text = str(value).strip()
    if not text:
        raise ValueError(
            "empty string cannot coerce to INET — "
            "refuse silent NULL invent (quarantine or remap upstream)"
        )
    try:
        if "/" in text:
            return ip_interface(text)
        return ip_address(text)
    except ValueError as exc:
        raise ValueError(
            "inet wire is not a valid IPv4/IPv6 address — refuse invent into INET"
        ) from exc


def coerce_cidr_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``CIDR`` wire (network only).

    Host bits outside the prefix must not silently pass — ``ip_network`` with
    ``strict=True`` matches Postgres CIDR input checking.
    """
    if value is None:
        return None
    from ipaddress import IPv4Network, IPv6Network, ip_network

    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (IPv4Network, IPv6Network)):
        return value
    if isinstance(value, (bool, int, float, list, dict, bytes, bytearray, memoryview)):
        raise ValueError(
            f"cidr wire cannot bind {type(value).__name__} — refuse invent into CIDR"
        )
    text = str(value).strip()
    if not text:
        raise ValueError(
            "empty string cannot coerce to CIDR — "
            "refuse silent NULL invent (quarantine or remap upstream)"
        )
    try:
        return ip_network(text, strict=True)
    except ValueError as exc:
        raise ValueError(
            "cidr wire is not a valid network — refuse invent into CIDR"
        ) from exc


_MAC_RE = re.compile(
    r"^(?:"
    r"(?:[0-9A-Fa-f]{2}([-:]))(?:[0-9A-Fa-f]{2}\1){4}[0-9A-Fa-f]{2}"  # aa:bb:… or aa-bb-…
    r"|"
    r"(?:[0-9A-Fa-f]{4}\.){2}[0-9A-Fa-f]{4}"  # aabb.ccdd.eeff
    r"|"
    r"[0-9A-Fa-f]{12}"  # aabbccddeeff
    r"|"
    r"(?:[0-9A-Fa-f]{2}([-:]))(?:[0-9A-Fa-f]{2}\1){6}[0-9A-Fa-f]{2}"  # macaddr8
    r"|"
    r"[0-9A-Fa-f]{16}"  # 8-byte hex
    r")$"
)


def coerce_macaddr_wire(value: Any, *, eui64: bool = False) -> Any:
    """Normalize PostgreSQL ``MACADDR`` / ``MACADDR8`` wire fail-closed."""
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, list, dict)):
        raise ValueError(
            f"macaddr wire cannot bind {type(value).__name__} — refuse invent"
        )
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        need = 8 if eui64 else 6
        if len(raw) not in {6, 8} and len(raw) != need:
            raise ValueError("macaddr wire bytes length invalid — refuse invent")
        return ":".join(f"{b:02x}" for b in raw)
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'MACADDR')
    if not _MAC_RE.match(text):
        raise ValueError(
            "macaddr wire is not a valid MAC address — refuse invent into MACADDR"
        )
    return text


def coerce_hstore_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``HSTORE`` wire (Fivetran extension-type class).

    Accepts ``dict`` (→ compact JSON object for document sinks / psycopg) or the
    classic ``"k"=>"v"`` text form. Refuse invent from scalars/lists that are not
    key/value pair records.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel, json_default

    if is_missing_sentinel(value):
        return value
    if isinstance(value, dict):
        # Stringify keys; null values stay JSON null (hstore NULL).
        normalized = {str(k): v for k, v in value.items()}
        return json.dumps(
            normalized, ensure_ascii=False, separators=(",", ":"), default=json_default
        )
    if isinstance(value, list):
        if value and all(
            isinstance(item, dict) and set(item.keys()) >= {"key", "value"}
            for item in value
        ):
            as_obj = {str(item["key"]): item["value"] for item in value}
            return json.dumps(
                as_obj, ensure_ascii=False, separators=(",", ":"), default=json_default
            )
        raise ValueError(
            "hstore wire cannot bind arbitrary array — refuse invent into HSTORE"
        )
    if isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
        raise ValueError(
            f"hstore wire cannot bind {type(value).__name__} — refuse invent into HSTORE"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'HSTORE')
    # Already JSON object?
    if text.startswith("{") and text.endswith("}"):
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(
                "hstore wire JSON parse failed — refuse invent into HSTORE"
            ) from exc
        if not isinstance(parsed, dict):
            raise ValueError(
                "hstore wire JSON must be an object — refuse invent into HSTORE"
            )
        return text
    # Classic hstore literal: "k"=>"v", "k2"=>NULL
    if "=>" not in text:
        raise ValueError(
            "hstore wire is not JSON object or hstore literal — refuse invent"
        )
    out: dict[str, Any] = {}
    # Split on "," that are outside quotes — simplified tokenizer.
    parts: list[str] = []
    buf: list[str] = []
    in_q = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == '"' and (i == 0 or text[i - 1] != "\\"):
            in_q = not in_q
            buf.append(ch)
        elif ch == "," and not in_q:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    if buf:
        parts.append("".join(buf).strip())
    pair_re = re.compile(
        r'^"((?:\\.|[^"\\])*)"\s*=>\s*(NULL|"((?:\\.|[^"\\])*)")$',
        re.IGNORECASE,
    )
    for part in parts:
        if not part:
            continue
        m = pair_re.match(part)
        if not m:
            raise ValueError(
                f"hstore literal pair invalid: {part!r} — refuse invent into HSTORE"
            )
        key = m.group(1).replace('\\"', '"').replace("\\\\", "\\")
        if m.group(2).upper() == "NULL":
            out[key] = None
        else:
            out[key] = m.group(3).replace('\\"', '"').replace("\\\\", "\\")
    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


def _is_range_literal(text: str) -> bool:
    """True for Postgres range input forms (empty / [lo,hi) family)."""
    s = text.strip()
    if not s:
        return False
    if s.lower() == "empty":
        return True
    if len(s) < 3 or s[0] not in "[(" or s[-1] not in "])":
        return False
    # Find the comma that separates bounds (respect quoted segments).
    in_q = False
    for i, ch in enumerate(s[1:-1], start=1):
        if ch == '"' and s[i - 1] != "\\":
            in_q = not in_q
        elif ch == "," and not in_q:
            return True
    return False


def _is_multirange_literal(text: str) -> bool:
    s = text.strip()
    if not (s.startswith("{") and s.endswith("}")):
        return False
    body = s[1:-1].strip()
    if not body:
        return True
    # Split top-level commas between range literals.
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    in_q = False
    for i, ch in enumerate(body):
        if ch == '"' and (i == 0 or body[i - 1] != "\\"):
            in_q = not in_q
            buf.append(ch)
            continue
        if not in_q:
            if ch in "[(":
                depth += 1
            elif ch in "])":
                depth = max(0, depth - 1)
            elif ch == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())
    return all(_is_range_literal(p) for p in parts if p)


def coerce_range_wire(value: Any, *, multi: bool = False) -> Any:
    """Normalize PostgreSQL range / multirange wire (Postgres 8.17 class).

    Accepts canonical literals (``[1,10)``, ``empty``, ``{[1,2),[5,6)}``) or a
    dict ``{lower, upper, bounds}``. Refuse invent from bare scalars.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float)):
        raise ValueError(
            f"range wire cannot bind {type(value).__name__} — refuse invent into RANGE"
        )
    if isinstance(value, dict):
        lower = value.get("lower", value.get("start"))
        upper = value.get("upper", value.get("end"))
        bounds = str(value.get("bounds") or "[)").strip()
        if bounds not in {"[]", "[)", "(]", "()"}:
            raise ValueError(
                f"range bounds {bounds!r} invalid — refuse invent into RANGE"
            )
        lo = "" if lower is None else str(lower)
        hi = "" if upper is None else str(upper)

        def _q(bound: str) -> str:
            if bound == "":
                return ""
            if any(c in bound for c in '[],()"\\'):
                return '"' + bound.replace("\\", "\\\\").replace('"', '\\"') + '"'
            return bound

        literal = f"{bounds[0]}{_q(lo)},{_q(hi)}{bounds[1]}"
        if multi:
            return "{" + literal + "}"
        return literal
    if isinstance(value, list):
        if multi:
            parts = [coerce_range_wire(item, multi=False) for item in value]
            return "{" + ",".join(str(p) for p in parts) + "}"
        raise ValueError(
            "range wire cannot bind list — use multirange or refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'RANGE')
    if multi:
        if not _is_multirange_literal(text):
            raise ValueError(
                "multirange wire literal invalid — refuse invent into MULTIRANGE"
            )
        return text
    if not _is_range_literal(text):
        raise ValueError("range wire literal invalid — refuse invent into RANGE")
    return text


def coerce_jsonpath_wire(value: Any) -> str | None:
    """Normalize PostgreSQL ``jsonpath`` wire — path expression text.

    Refuse invent from bool/number/object (jsonpath is not JSON document invent).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value  # type: ignore[return-value]
    if isinstance(value, (bool, int, float, dict, list)):
        raise ValueError(
            f"jsonpath wire cannot bind {type(value).__name__} — refuse invent into JSONPATH"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'JSONPATH')
    return text


def coerce_xml_wire(value: Any) -> Any:
    """Normalize PostgreSQL / Oracle ``XML`` wire — well-formed document/content.

    Postgres accepts documents and content fragments (xmloption). Refuse invent
    from dict/list/scalars (Fivetran/HVR class: XML is not opaque VARCHAR invent).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            text = bytes(value).decode("utf-8")
        except Exception as exc:
            raise ValueError(
                "xml wire bytes are not UTF-8 — refuse invent into XML"
            ) from exc
        return coerce_xml_wire(text)
    if isinstance(value, (dict, list)):
        raise ValueError(
            "xml wire cannot bind object/array — refuse invent into XML "
            "(serialize to well-formed XML first)"
        )
    if isinstance(value, (bool, int, float)):
        raise ValueError(
            f"xml wire cannot bind {type(value).__name__} — refuse invent into XML"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'XML')
    if not (text.startswith("<") and ">" in text):
        raise ValueError(
            "xml wire is not well-formed markup — refuse invent into XML"
        )
    try:
        from defusedxml import ElementTree as ET

        try:
            ET.fromstring(text)  # nosec B314 — defusedxml, not stdlib ElementTree
        except ET.ParseError:
            # Content fragment (multiple top-level nodes) — PG xmloption=content.
            ET.fromstring(  # nosec B314 — defusedxml, not stdlib ElementTree
                f"<df_xml_root>{text}</df_xml_root>"
            )
    except Exception as exc:
        raise ValueError(
            "xml wire failed parse — refuse invent into XML"
        ) from exc
    return text


def coerce_citext_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``CITEXT`` — case-insensitive text carrier.

    Store the original casing (Postgres citext compares case-insensitively but
    preserves input). Refuse invent from bytes/objects.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bytes, bytearray, memoryview, dict, list, bool)):
        raise ValueError(
            f"citext wire cannot bind {type(value).__name__} — refuse invent into CITEXT"
        )
    if isinstance(value, (int, float)):
        # Numeric → text is lossless for display but loses typed numeric meaning;
        # allow only via explicit string wire from transforms.
        raise ValueError(
            "citext wire cannot bind number — refuse invent into CITEXT"
        )
    text = str(value)
    # CITEXT stores '' — never invent NULL (VARCHAR-class carrier).
    return text


def coerce_ltree_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``LTREE`` path (Fivetran extension-type class).

    Labels: ``A-Za-z0-9_-`` separated by ``.`` (C-locale subset — refuse invent
    of spaces/dots inside labels). SQL Server ``hierarchyid`` slash paths
    (``/1/2/3/``) are converted to ltree polarity (AWS DMS migration class).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, dict, list, bytes, bytearray, memoryview)):
        raise ValueError(
            f"ltree wire cannot bind {type(value).__name__} — refuse invent into LTREE"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'LTREE')
    if "/" in text:
        from services.type_system import hierarchyid_to_ltree_path

        text = hierarchyid_to_ltree_path(text)
        text = _refuse_empty_specialty(text, 'LTREE')
    labels = text.split(".")
    if not labels or any(not lab for lab in labels):
        raise ValueError("ltree path has empty label — refuse invent into LTREE")
    if len(labels) > 65535:
        raise ValueError("ltree path exceeds label limit — refuse invent into LTREE")
    label_re = re.compile(r"^[A-Za-z0-9_-]{1,1000}$")
    for lab in labels:
        if not label_re.match(lab):
            raise ValueError(
                f"ltree label {lab!r} invalid — refuse invent into LTREE"
            )
    return text


def coerce_hierarchyid_wire(value: Any, *, as_ltree: bool = False) -> Any:
    """Normalize SQL Server ``hierarchyid`` path wire (``/1/2/3/``).

    ``as_ltree=True`` emits PostgreSQL ltree polarity (dot labels). Invalid
    paths refuse invent — never silently stringify binary hierarchyids.
    """
    if value is None:
        return None
    from services.type_system import hierarchyid_to_ltree_path
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, dict, list)):
        raise ValueError(
            f"hierarchyid cannot bind {type(value).__name__} — refuse invent"
        )
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError(
            "hierarchyid binary wire unsupported — use ToString() path "
            "(/1/2/) — refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'HIERARCHYID')
    if as_ltree:
        out = hierarchyid_to_ltree_path(text)
        return _refuse_empty_specialty(out, 'HIERARCHYID')
    # Canonical slash form for SQL Server HIERARCHYID / NVARCHAR sinks.
    if text == "/":
        return "/"
    if "/" in text:
        parts = [p for p in text.strip("/").split("/") if p]
        if not parts:
            return "/"
        for lab in parts:
            if not re.fullmatch(r"[A-Za-z0-9_]+", lab):
                raise ValueError(
                    f"hierarchyid label {lab!r} invalid — refuse invent"
                )
        return "/" + "/".join(parts) + "/"
    # Dot path → slash (reverse-ETL from LTREE).
    if re.fullmatch(r"[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*", text):
        return "/" + text.replace(".", "/") + "/"
    raise ValueError(
        f"hierarchyid path invalid — refuse invent: {text[:64]!r}"
    )


def coerce_tsvector_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``TSVECTOR`` / ``TSQUERY`` text wire.

    Accept lexeme strings; refuse invent from objects/numbers (Airbyte string
    carrier for full-text types).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, dict, bytes, bytearray, memoryview)):
        raise ValueError(
            f"tsvector wire cannot bind {type(value).__name__} — refuse invent"
        )
    if isinstance(value, list):
        # Lexeme list → space-joined tsvector-ish text (operator may to_tsvector).
        parts = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    "tsvector list items must be non-empty strings — refuse invent"
                )
            parts.append(item.strip())
        return " ".join(parts)
    text = str(value).strip()
    return text  # empty tsvector is valid — never invent NULL


def coerce_point_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``POINT`` wire (Fivetran JSON / PG literal class).

    Accepts ``(x,y)``, ``x,y``, or ``{"x":..,"y":..}``. Refuse invent from
    scalars/3-tuples.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
        raise ValueError(
            f"point wire cannot bind {type(value).__name__} — refuse invent into POINT"
        )
    if isinstance(value, dict):
        if "x" not in value or "y" not in value:
            raise ValueError(
                "point object requires x and y — refuse invent into POINT"
            )
        try:
            x = float(value["x"])
            y = float(value["y"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "point x/y must be numeric — refuse invent into POINT"
            ) from exc
        return f"({x},{y})"
    if isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError(
                "point sequence must have length 2 — refuse invent into POINT"
            )
        try:
            x = float(value[0])
            y = float(value[1])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "point sequence values must be numeric — refuse invent into POINT"
            ) from exc
        return f"({x},{y})"
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'POINT')
    m = re.match(
        r"^\(?\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)\s*,\s*"
        r"([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)\s*\)?$",
        text,
    )
    if not m:
        raise ValueError(
            "point wire is not (x,y) literal — refuse invent into POINT"
        )
    return f"({m.group(1)},{m.group(2)})"


def coerce_box_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``BOX`` wire — two opposite corners.

    Accepts ``((x1,y1),(x2,y2))``, corner dicts, or a list of two points.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
        raise ValueError(
            f"box wire cannot bind {type(value).__name__} — refuse invent into BOX"
        )
    if isinstance(value, dict):
        # Fivetran-style or corner keys.
        if {"x1", "y1", "x2", "y2"} <= set(value.keys()):
            try:
                return (
                    f"(({float(value['x1'])},{float(value['y1'])}),"
                    f"({float(value['x2'])},{float(value['y2'])}))"
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "box corners must be numeric — refuse invent into BOX"
                ) from exc
        raise ValueError(
            "box object requires x1,y1,x2,y2 — refuse invent into BOX"
        )
    if isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError(
                "box sequence must be two points — refuse invent into BOX"
            )
        p1 = coerce_point_wire(value[0])
        p2 = coerce_point_wire(value[1])
        return f"({p1},{p2})"
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'BOX')
    # Extract four floats in order.
    nums = re.findall(
        r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?",
        text,
    )
    if len(nums) != 4:
        raise ValueError(
            "box wire must contain four coordinates — refuse invent into BOX"
        )
    return f"(({nums[0]},{nums[1]}),({nums[2]},{nums[3]}))"


def coerce_circle_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``CIRCLE`` wire — center + radius.

    Canonical output ``<(x,y),r>`` (Postgres default display form).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
        raise ValueError(
            f"circle wire cannot bind {type(value).__name__} — refuse invent into CIRCLE"
        )
    if isinstance(value, dict):
        center = value.get("center") or value.get("point")
        if center is None and "x" in value and "y" in value:
            center = {"x": value["x"], "y": value["y"]}
        if center is None or "r" not in value and "radius" not in value:
            raise ValueError(
                "circle object requires center/x,y and r/radius — refuse invent"
            )
        try:
            pt = coerce_point_wire(center)
            r = float(value.get("r", value.get("radius")))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "circle center/radius must be numeric — refuse invent into CIRCLE"
            ) from exc
        if r < 0:
            raise ValueError("circle radius cannot be negative — refuse invent")
        return f"<{pt},{r}>"
    if isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError(
                "circle sequence must be [center, radius] — refuse invent"
            )
        pt = coerce_point_wire(value[0])
        try:
            r = float(value[1])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "circle radius must be numeric — refuse invent into CIRCLE"
            ) from exc
        if r < 0:
            raise ValueError("circle radius cannot be negative — refuse invent")
        return f"<{pt},{r}>"
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'CIRCLE')
    nums = re.findall(
        r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?",
        text,
    )
    if len(nums) != 3:
        raise ValueError(
            "circle wire must contain x,y,r — refuse invent into CIRCLE"
        )
    r = float(nums[2])
    if r < 0:
        raise ValueError("circle radius cannot be negative — refuse invent")
    return f"<({nums[0]},{nums[1]}),{nums[2]}>"


def _extract_coord_pairs(text: str) -> list[tuple[str, str]]:
    """Pull ordered (x,y) numeric pairs from a geometric literal."""
    nums = re.findall(
        r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?",
        text,
    )
    if len(nums) < 2 or len(nums) % 2 != 0:
        return []
    return [(nums[i], nums[i + 1]) for i in range(0, len(nums), 2)]


def coerce_lseg_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``LSEG`` — two endpoints ``[(x1,y1),(x2,y2)]``."""
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
        raise ValueError(
            f"lseg wire cannot bind {type(value).__name__} — refuse invent into LSEG"
        )
    if isinstance(value, dict):
        if {"x1", "y1", "x2", "y2"} <= set(value.keys()):
            try:
                return (
                    f"[({float(value['x1'])},{float(value['y1'])}),"
                    f"({float(value['x2'])},{float(value['y2'])})]"
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "lseg endpoints must be numeric — refuse invent into LSEG"
                ) from exc
        raise ValueError("lseg object requires x1,y1,x2,y2 — refuse invent")
    if isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError("lseg sequence must be two points — refuse invent")
        p1 = coerce_point_wire(value[0])
        p2 = coerce_point_wire(value[1])
        return f"[{p1},{p2}]"
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'LSEG')
    pairs = _extract_coord_pairs(text)
    if len(pairs) != 2:
        raise ValueError("lseg wire needs two points — refuse invent into LSEG")
    (x1, y1), (x2, y2) = pairs
    return f"[({x1},{y1}),({x2},{y2})]"


def coerce_line_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``LINE`` — ``{A,B,C}`` or two distinct points."""
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, bytes, bytearray, memoryview)):
        raise ValueError(
            f"line wire cannot bind {type(value).__name__} — refuse invent into LINE"
        )
    if isinstance(value, dict):
        if {"a", "b", "c"} <= {k.lower() for k in value.keys()}:
            # Case-insensitive key pick.
            kv = {str(k).lower(): v for k, v in value.items()}
            try:
                a, b, c = float(kv["a"]), float(kv["b"]), float(kv["c"])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "line a,b,c must be numeric — refuse invent into LINE"
                ) from exc
            if a == 0 and b == 0:
                raise ValueError("line A and B cannot both be zero — refuse invent")
            return f"{{{a},{b},{c}}}"
        if {"x1", "y1", "x2", "y2"} <= set(value.keys()):
            # Two-point LINE input — same canonical as text path below.
            lit = coerce_lseg_wire(value)  # [(x1,y1),(x2,y2)]
            return f"({lit[1:-1]})"
        raise ValueError("line object requires a,b,c or two points — refuse invent")
    if isinstance(value, (list, tuple)):
        if len(value) == 3:
            try:
                a, b, c = float(value[0]), float(value[1]), float(value[2])
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "line coefficients must be numeric — refuse invent into LINE"
                ) from exc
            if a == 0 and b == 0:
                raise ValueError("line A and B cannot both be zero — refuse invent")
            return f"{{{a},{b},{c}}}"
        if len(value) == 2:
            lit = coerce_lseg_wire(value)
            return f"({lit[1:-1]})"  # ((x1,y1),(x2,y2))
        raise ValueError("line sequence must be 2 points or 3 coeffs — refuse invent")
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'LINE')
    if text.startswith("{") and text.endswith("}"):
        nums = re.findall(
            r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?",
            text,
        )
        if len(nums) != 3:
            raise ValueError("line {A,B,C} needs three coeffs — refuse invent")
        a, b = float(nums[0]), float(nums[1])
        if a == 0 and b == 0:
            raise ValueError("line A and B cannot both be zero — refuse invent")
        return f"{{{nums[0]},{nums[1]},{nums[2]}}}"
    pairs = _extract_coord_pairs(text)
    if len(pairs) != 2:
        raise ValueError("line wire needs two points or {{A,B,C}} — refuse invent")
    (x1, y1), (x2, y2) = pairs
    if x1 == x2 and y1 == y2:
        raise ValueError("line points must be distinct — refuse invent")
    return f"(({x1},{y1}),({x2},{y2}))"


def coerce_path_wire(value: Any, *, closed: bool | None = None) -> Any:
    """Normalize PostgreSQL ``PATH`` — open ``[…]`` or closed ``(…)`` point lists."""
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
        raise ValueError(
            f"path wire cannot bind {type(value).__name__} — refuse invent into PATH"
        )
    is_closed = closed
    points: list[str] = []
    if isinstance(value, dict):
        pts = value.get("points") or value.get("vertices")
        if not isinstance(pts, (list, tuple)) or len(pts) < 1:
            raise ValueError("path object requires points list — refuse invent")
        is_closed = bool(value.get("closed", True if closed is None else closed))
        points = [coerce_point_wire(p) for p in pts]
    elif isinstance(value, (list, tuple)):
        if len(value) < 1:
            raise ValueError("path needs at least one point — refuse invent")
        points = [coerce_point_wire(p) for p in value]
        is_closed = True if closed is None else closed
    else:
        text = str(value).strip()
        text = _refuse_empty_specialty(text, 'PATH')
        if is_closed is None:
            is_closed = not (text.startswith("[") and text.endswith("]"))
        pairs = _extract_coord_pairs(text)
        if len(pairs) < 1:
            raise ValueError("path wire has no points — refuse invent into PATH")
        points = [f"({x},{y})" for x, y in pairs]
    body = ",".join(points)
    return f"({body})" if is_closed else f"[{body}]"


def coerce_polygon_wire(value: Any) -> Any:
    """Normalize PostgreSQL ``POLYGON`` — closed vertex list ``((x,y),…)``."""
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, bytes, bytearray, memoryview)):
        raise ValueError(
            f"polygon wire cannot bind {type(value).__name__} — refuse invent into POLYGON"
        )
    if isinstance(value, dict):
        pts = value.get("points") or value.get("vertices")
        if not isinstance(pts, (list, tuple)) or len(pts) < 3:
            raise ValueError(
                "polygon requires ≥3 vertices — refuse invent into POLYGON"
            )
        points = [coerce_point_wire(p) for p in pts]
        return "(" + ",".join(points) + ")"
    if isinstance(value, (list, tuple)):
        if len(value) < 3:
            raise ValueError(
                "polygon requires ≥3 vertices — refuse invent into POLYGON"
            )
        points = [coerce_point_wire(p) for p in value]
        return "(" + ",".join(points) + ")"
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'POLYGON')
    pairs = _extract_coord_pairs(text)
    if len(pairs) < 3:
        raise ValueError(
            "polygon wire needs ≥3 vertices — refuse invent into POLYGON"
        )
    return "(" + ",".join(f"({x},{y})" for x, y in pairs) + ")"


def coerce_pg_lsn_wire(value: Any) -> str | None:
    """Normalize PostgreSQL ``pg_lsn`` to canonical ``%X/%08X`` (Debezium/WAL class).

    Accepts ``16/B374D848``, lower-case hex, and unsigned 64-bit integers
    (``(hi << 32) | lo``). Never invent an LSN from bool/float/garbage —
    CDC at-least-once guards depend on exact watermark fidelity.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, bool):
        raise ValueError("pg_lsn cannot bind bool — refuse invent")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("pg_lsn float must be integral — refuse invent")
        value = int(value)
    if isinstance(value, int):
        if value < 0 or value > 0xFFFFFFFFFFFFFFFF:
            raise ValueError("pg_lsn out of uint64 range — refuse invent")
        hi = (value >> 32) & 0xFFFFFFFF
        lo = value & 0xFFFFFFFF
        return f"{hi:X}/{lo:08X}"
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError(
            f"pg_lsn cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'PG_LSN')
    if "/" not in text:
        # Decimal uint64 string (some CDC serializers).
        if text.isdigit():
            return coerce_pg_lsn_wire(int(text))
        raise ValueError("pg_lsn wire needs hi/lo hex — refuse invent")
    hi_s, _, lo_s = text.partition("/")
    hi_s, lo_s = hi_s.strip(), lo_s.strip()
    if (
        not hi_s
        or not lo_s
        or len(hi_s) > 8
        or len(lo_s) > 8
        or any(c not in "0123456789abcdefABCDEF" for c in hi_s + lo_s)
    ):
        raise ValueError("pg_lsn wire is not valid hex/hex — refuse invent")
    hi = int(hi_s, 16)
    lo = int(lo_s, 16)
    return f"{hi:X}/{lo:08X}"


def coerce_oid_wire(value: Any) -> int | None:
    """Normalize PostgreSQL ``oid`` to unsigned 32-bit int (system catalog class).

    Refuse invent from bool, fractional float, or out-of-range values.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, bool):
        raise ValueError("oid cannot bind bool — refuse invent")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("oid float must be integral — refuse invent")
        value = int(value)
    if isinstance(value, int):
        if value < 0 or value > 0xFFFFFFFF:
            raise ValueError("oid out of uint32 range — refuse invent")
        return value
    if isinstance(value, (bytes, bytearray, memoryview, dict, list, tuple)):
        raise ValueError(
            f"oid cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'OID')
    if not text.isdigit():
        raise ValueError("oid wire must be unsigned decimal — refuse invent")
    n = int(text)
    if n > 0xFFFFFFFF:
        raise ValueError("oid out of uint32 range — refuse invent")
    return n


def coerce_tid_wire(value: Any) -> str | None:
    """Normalize PostgreSQL ``tid`` / ``ctid`` to ``(block,offset)``.

    Block is uint32; offset is uint16 (Npgsql / Postgres ItemPointer class).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, bool):
        raise ValueError("tid cannot bind bool — refuse invent")
    if isinstance(value, dict):
        block = value.get("block", value.get("block_number"))
        offset = value.get("offset", value.get("tuple_index", value.get("pos")))
        if block is None or offset is None:
            raise ValueError("tid object needs block+offset — refuse invent")
        return coerce_tid_wire((block, offset))
    if isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError("tid sequence must be (block, offset) — refuse invent")
        try:
            block_i = int(value[0])
            offset_i = int(value[1])
        except (TypeError, ValueError) as exc:
            raise ValueError("tid block/offset must be integers — refuse invent") from exc
        if block_i < 0 or block_i > 0xFFFFFFFF:
            raise ValueError("tid block out of uint32 range — refuse invent")
        if offset_i < 0 or offset_i > 0xFFFF:
            raise ValueError("tid offset out of uint16 range — refuse invent")
        return f"({block_i},{offset_i})"
    if isinstance(value, (bytes, bytearray, memoryview, int, float)):
        raise ValueError(
            f"tid cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'TID')
    m = re.fullmatch(
        r"\(\s*(\d+)\s*,\s*(\d+)\s*\)",
        text,
    )
    if not m:
        raise ValueError("tid wire must be (block,offset) — refuse invent")
    return coerce_tid_wire((int(m.group(1)), int(m.group(2))))


def coerce_xid_wire(value: Any, *, width64: bool = False) -> int | None:
    """Normalize PostgreSQL ``xid`` (uint32) or ``xid8`` (uint64) transaction ids."""
    label = "xid8" if width64 else "xid"
    max_v = 0xFFFFFFFFFFFFFFFF if width64 else 0xFFFFFFFF
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, bool):
        raise ValueError(f"{label} cannot bind bool — refuse invent")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{label} float must be integral — refuse invent")
        value = int(value)
    if isinstance(value, int):
        if value < 0 or value > max_v:
            raise ValueError(f"{label} out of range — refuse invent")
        return value
    if isinstance(value, (bytes, bytearray, memoryview, dict, list, tuple)):
        raise ValueError(
            f"{label} cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'XID')
    if not text.isdigit():
        raise ValueError(f"{label} wire must be unsigned decimal — refuse invent")
    n = int(text)
    if n > max_v:
        raise ValueError(f"{label} out of range — refuse invent")
    return n


def coerce_cid_wire(value: Any) -> int | None:
    """Normalize PostgreSQL ``cid`` (command identifier) as uint32."""
    # Same wire rules as oid/xid — reuse oid path with a clearer error label.
    try:
        return coerce_oid_wire(value)
    except ValueError as exc:
        raise ValueError(str(exc).replace("oid", "cid")) from exc


def coerce_txid_snapshot_wire(value: Any) -> str | None:
    """Normalize PostgreSQL ``txid_snapshot`` / ``pg_snapshot`` wire.

    Canonical text form is ``xmin:xmax:xip_list`` (InterDB / Postgres docs),
    e.g. ``10:20:10,14,15`` or ``100:100:`` (empty xip). Fail-closed — never
    invent a snapshot from bools or unstructured text (MVCC visibility class).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, bool):
        raise ValueError("txid_snapshot cannot bind bool — refuse invent")
    if isinstance(value, dict):
        xmin = value.get("xmin")
        xmax = value.get("xmax")
        xip = value.get("xip") or value.get("xip_list") or []
        if xmin is None or xmax is None:
            raise ValueError(
                "txid_snapshot object needs xmin+xmax — refuse invent"
            )
        if not isinstance(xip, (list, tuple)):
            raise ValueError("txid_snapshot xip must be a list — refuse invent")
        return coerce_txid_snapshot_wire(f"{xmin}:{xmax}:{','.join(str(x) for x in xip)}")
    if isinstance(value, (bytes, bytearray, memoryview, int, float, list, tuple)):
        raise ValueError(
            f"txid_snapshot cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'TXID_SNAPSHOT')
    parts = text.split(":")
    if len(parts) != 3:
        raise ValueError(
            "txid_snapshot wire must be xmin:xmax:xip_list — refuse invent"
        )
    xmin_s, xmax_s, xip_s = (p.strip() for p in parts)
    if not xmin_s.isdigit() or not xmax_s.isdigit():
        raise ValueError(
            "txid_snapshot xmin/xmax must be unsigned decimals — refuse invent"
        )
    xmin_i, xmax_i = int(xmin_s), int(xmax_s)
    if xmin_i > xmax_i:
        raise ValueError("txid_snapshot xmin cannot exceed xmax — refuse invent")
    xips: list[int] = []
    if xip_s:
        for tok in xip_s.split(","):
            tok = tok.strip()
            if not tok:
                continue
            if not tok.isdigit():
                raise ValueError(
                    "txid_snapshot xip entries must be unsigned decimals — refuse invent"
                )
            x = int(tok)
            if x < xmin_i or x >= xmax_i:
                raise ValueError(
                    "txid_snapshot xip must satisfy xmin <= xip < xmax — refuse invent"
                )
            xips.append(x)
    # Canonical: sorted unique xip (Postgres stores unique; order for Gate-8).
    xips = sorted(set(xips))
    xip_out = ",".join(str(x) for x in xips)
    return f"{xmin_i}:{xmax_i}:{xip_out}"


def coerce_enum_wire(value: Any, *, ddl_type: str) -> str | None:
    """Normalize MySQL ``ENUM`` wire — member string or 1-based ordinal (CDC).

    Index ``0`` / empty string is the MySQL error member — refuse invent (strict
    mode would reject; non-strict stores '' silently). Debezium often emits ints.
    """
    if value is None:
        return None
    from services.type_system import parse_enum_or_set_ordered_members
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    parsed = parse_enum_or_set_ordered_members(ddl_type)
    if parsed is None or parsed[0] != "ENUM":
        raise ValueError("coerce_enum_wire requires ENUM(...) ddl — refuse invent")
    _kind, members = parsed
    if not members:
        raise ValueError("ENUM domain is empty — refuse invent")
    member_set = frozenset(members)
    if isinstance(value, bool):
        raise ValueError("ENUM cannot bind bool — refuse invent")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("ENUM float must be integral ordinal — refuse invent")
        value = int(value)
    if isinstance(value, int):
        if value == 0:
            raise ValueError("ENUM index 0 is error member — refuse invent")
        if value < 1 or value > len(members):
            raise ValueError(
                f"ENUM ordinal {value} out of 1..{len(members)} — refuse invent"
            )
        return members[value - 1]
    if isinstance(value, (bytes, bytearray, memoryview, dict, list, tuple)):
        raise ValueError(
            f"ENUM cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    if not text:
        raise ValueError("ENUM empty string is error member — refuse invent")
    if text in member_set:
        return text
    # Quoted numeric without matching member → ordinal (MySQL ENUM literal rules).
    if text.isdigit():
        return coerce_enum_wire(int(text), ddl_type=ddl_type)
    raise ValueError(f"value not in ENUM domain — refuse invent: {text[:64]!r}")


def coerce_set_wire(
    value: Any,
    *,
    ddl_type: str,
    as_list: bool = False,
    joiner: str = ",",
) -> str | list[str] | None:
    """Normalize MySQL ``SET`` / SaaS multi-select wire.

    Canonical members are in definition order (Gate-8 stable). Invalid members
    refuse invent — MySQL IGNORE would drop them silently.

    ``as_list=True`` returns a Python list for PostgreSQL ``TEXT[]`` sinks
    (create-new SET→array polarity). MySQL keeps CSV string wire; HubSpot /
    Salesforce multipicklist use ``joiner=';'``.
    """
    if value is None:
        return None
    from services.type_system import parse_enum_or_set_ordered_members
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    parsed = parse_enum_or_set_ordered_members(ddl_type)
    if parsed is None or parsed[0] != "SET":
        raise ValueError("coerce_set_wire requires SET(...) ddl — refuse invent")
    _kind, members = parsed
    if not members:
        raise ValueError("SET domain is empty — refuse invent")
    member_set = frozenset(members)
    sep = joiner if joiner in {",", ";"} else ","

    def _emit(parts: list[str]) -> str | list[str]:
        return list(parts) if as_list else sep.join(parts)

    if isinstance(value, bool):
        raise ValueError("SET cannot bind bool — refuse invent")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError("SET float must be integral bitmask — refuse invent")
        value = int(value)
    if isinstance(value, int):
        if value < 0:
            raise ValueError("SET bitmask cannot be negative — refuse invent")
        max_mask = (1 << len(members)) - 1
        if value > max_mask:
            raise ValueError(
                f"SET bitmask {value} exceeds domain mask {max_mask} — refuse invent"
            )
        parts = [m for i, m in enumerate(members) if value & (1 << i)]
        return _emit(parts)
    if isinstance(value, (list, tuple)):
        parts = [str(p).strip() for p in value if str(p).strip()]
        bad = [p for p in parts if p not in member_set]
        if bad:
            raise ValueError(
                f"SET members not in domain — refuse invent: {bad[:3]!r}"
            )
        # Canonical definition order, unique.
        return _emit([m for m in members if m in parts])
    if isinstance(value, (bytes, bytearray, memoryview, dict)):
        raise ValueError(
            f"SET cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    if not text:
        return _emit([])  # empty SET is valid
    if text.isdigit() and text not in member_set:
        return coerce_set_wire(
            int(text), ddl_type=ddl_type, as_list=as_list, joiner=joiner
        )
    # Accept both MySQL CSV and HubSpot/Salesforce semicolon multi-select.
    import re

    parts = [p.strip() for p in re.split(r"[,;]", text) if p.strip()]
    bad = [p for p in parts if p not in member_set]
    if bad:
        raise ValueError(
            f"SET members not in domain — refuse invent: {bad[:3]!r}"
        )
    return _emit([m for m in members if m in parts])


def coerce_year_wire(value: Any) -> int | None:
    """Normalize MySQL ``YEAR`` to int 0 or 1901–2155 (string ``'0'`` → 2000)."""
    if value is None:
        return None
    from services.type_system import expand_mysql_year
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, str) and not value.strip():
        raise ValueError(
            "empty string cannot coerce to YEAR — "
            "refuse silent NULL invent (quarantine or remap upstream)"
        )
    n = expand_mysql_year(value)
    if n is None:
        raise ValueError(
            f"cannot coerce {value!r} to YEAR — refuse silent NULL invent"
        )
    if not (n == 0 or 1901 <= n <= 2155):
        raise ValueError(
            f"YEAR {n} outside 0 or 1901–2155 — refuse invent (MySQL would store 0000)"
        )
    return n


def coerce_boolean_wire(value: Any, *, as_int: bool = False) -> Any:
    """Normalize Mongo/CSV boolean wire. Unrecognized values pass through.

    Empty string refuses NULL invent on boolean sinks (upsert wipe). Informal
    ``yes``/``no`` pass through for quarantine — never invent True/False.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return (1 if value else 0) if as_int else value
    if isinstance(value, (int, float)) and value in (0, 1):
        bit = int(value)
        return bit if as_int else bool(bit)
    if isinstance(value, str):
        token = value.strip().lower()
        if not token:
            raise ValueError(
                "empty string cannot coerce to boolean — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            )
        if token in _TRUE_TOKENS:
            return 1 if as_int else True
        if token in _FALSE_TOKENS:
            return 0 if as_int else False
    return value


def coerce_integer_wire(
    value: Any,
    *,
    ddl_type: str | None = None,
    engine: str = "",
) -> Any:
    """Normalize integer-family bind (TINYINT…BIGINT). Digit strings → int.

    SQL Server ``TINYINT`` is unsigned 0–255 (Microsoft / pyodbc), not MySQL's
    TINYINT(1) boolean convention. Out-of-range and non-integral floats refuse
    invent — quarantine owns the row.
    """
    if value is None:
        return None
    from decimal import Decimal, InvalidOperation

    upper = (ddl_type or "INTEGER").upper().split("(", 1)[0].strip()
    eng = (engine or "").strip().lower()

    def _range_check(n: int) -> int:
        if upper == "TINYINT":
            # SQL Server / Azure SQL TINYINT is unsigned 0–255 (Microsoft docs /
            # pyodbc). MySQL signed TINYINT usually hits boolean wire first.
            unsigned = "unsigned" in (ddl_type or "").lower()
            if eng in {"sqlserver", "mssql", "azure_sql", "synapse"} or unsigned:
                lo, hi = 0, 255
            else:
                lo, hi = -128, 127
            if n < lo or n > hi:
                raise ValueError(
                    f"integer out of range for {ddl_type or upper}: {n} "
                    f"(refuse invent; expected {lo}..{hi})"
                )
        elif upper == "SMALLINT":
            if n < -32768 or n > 32767:
                raise ValueError(
                    f"integer out of range for {ddl_type or upper}: {n}"
                )
        elif upper == "MEDIUMINT":
            if n < -8388608 or n > 8388607:
                raise ValueError(
                    f"integer out of range for {ddl_type or upper}: {n}"
                )
        elif upper == "INT4":
            if n < -2147483648 or n > 2147483647:
                raise ValueError(
                    f"integer out of range for {ddl_type or upper}: {n}"
                )
        elif upper in {"INT", "INTEGER"}:
            # Bare INT/INTEGER is dialect-defined (SQLite holds 8 bytes,
            # BigQuery INT64, Snowflake/Oracle a decimal carrier). Ask the
            # storage-bounds SSOT instead of assuming the SQL-standard int4 —
            # a wrong bound refuses rows the destination stores natively.
            from services.numeric_fit import integer_storage_bounds

            bounds = integer_storage_bounds(upper, dest_db=eng)
            if bounds is not None and not (bounds[0] <= n <= bounds[1]):
                raise ValueError(
                    f"integer out of range for {ddl_type or upper}: {n}"
                )
        return n

    if isinstance(value, bool):
        # bool ⊂ int — map explicitly so True is never a silent 1 without intent
        # for boolean columns (those use coerce_boolean_wire). Integer sinks
        # accept 0/1 as the only safe bool→int polarity.
        return _range_check(1 if value else 0)
    if isinstance(value, int):
        return _range_check(value)
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise ValueError("refuse invent integer from non-finite float")
        if not value.is_integer():
            raise ValueError(
                f"refuse invent integer from fractional float {value!r}"
            )
        return _range_check(int(value))
    if isinstance(value, Decimal):
        try:
            if value != value.to_integral_value():
                raise ValueError(
                    f"refuse invent integer from fractional decimal {value!r}"
                )
            return _range_check(int(value))
        except (InvalidOperation, ValueError):
            raise
    if isinstance(value, str):
        token = value.strip()
        if not token:
            # Iceberg parity — never invent SQL NULL from "" on upsert wipe paths.
            # Quarantine / Risk Contract CAST_AND_CONTINUE is the only NULL invent.
            raise ValueError(
                f"empty string cannot coerce to integer for {ddl_type or upper} — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            )
        low = token.lower()
        # Digits 0/1 are valid integers — only refuse wordy bool tokens.
        if low in (_TRUE_TOKENS | _FALSE_TOKENS) - {"0", "1"}:
            raise ValueError(
                f"refuse invent integer from boolean token {value!r} "
                f"for {ddl_type or upper}"
            )
        try:
            if any(c in token for c in ".eE") and not re.fullmatch(
                r"[-+]?\d+", token
            ):
                dec = Decimal(token)
                if dec != dec.to_integral_value():
                    raise ValueError(
                        f"refuse invent integer from fractional {value!r}"
                    )
                return _range_check(int(dec))
            return _range_check(int(token, 10))
        except InvalidOperation as exc:
            raise ValueError(
                f"refuse invent integer from {value!r} for {ddl_type or upper}"
            ) from exc
        except ValueError as exc:
            msg = str(exc)
            if "out of range" in msg or msg.startswith("refuse invent"):
                raise
            raise ValueError(
                f"refuse invent integer from {value!r} for {ddl_type or upper}"
            ) from exc
    raise ValueError(
        f"refuse invent integer from {type(value).__name__} {value!r} "
        f"for {ddl_type or upper}"
    )


def coerce_sql_variant_wire(value: Any, *, as_json_envelope: bool = False) -> Any:
    """Normalize SQL Server ``sql_variant`` for create-new sinks.

    Native SQL Server keeps the scalar. PostgreSQL JSONB / Snowflake VARIANT
    get a typed envelope ``{"sql_variant_base": "...", "value": ...}`` so the
    base type is not silently wiped (AWS SCT VARCHAR-only gap).
    """
    if value is None:
        return None
    from decimal import Decimal
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value

    def _base_and_value(v: Any) -> tuple[str, Any]:
        if isinstance(v, bool):
            return "bit", v
        if isinstance(v, int):
            return "bigint", v
        if isinstance(v, float):
            return "float", v
        if isinstance(v, Decimal):
            return "decimal", str(v)
        if isinstance(v, (bytes, bytearray, memoryview)):
            return "varbinary", base64.b64encode(bytes(v)).decode("ascii")
        if isinstance(v, dict) and "sql_variant_base" in v and "value" in v:
            return str(v["sql_variant_base"]), v["value"]
        if isinstance(v, (list, tuple)):
            raise ValueError(
                "sql_variant cannot bind array — refuse invent"
            )
        text = str(v)
        return "nvarchar", text

    base, payload = _base_and_value(value)
    if not as_json_envelope:
        # SQL Server native / VARCHAR sinks — scalar wire only.
        if base == "varbinary":
            return base64.b64decode(payload) if isinstance(payload, str) else payload
        return payload
    # Native dict for JSONB/VARIANT adapters (psycopg / Snowflake).
    return {"sql_variant_base": base, "value": payload}


def coerce_rowid_wire(value: Any) -> str | None:
    """Normalize Oracle ``ROWID`` / ``UROWID`` to canonical string (18-char class)."""
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bool, int, float, dict, list, bytes, bytearray, memoryview)):
        raise ValueError(
            f"ROWID cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    text = _refuse_empty_specialty(text, 'ROWID')
    # Oracle extended ROWID is typically 18 chars; UROWID may be longer.
    if len(text) > 4000:
        raise ValueError("ROWID exceeds max length — refuse invent")
    if not re.fullmatch(r"[A-Za-z0-9+/=]+", text):
        raise ValueError(f"ROWID wire invalid — refuse invent: {text[:32]!r}")
    return text


def coerce_float_wire(value: Any, *, ddl_type: str | None = None) -> Any:
    """Normalize IEEE FLOAT/REAL/DOUBLE bind — digit strings → float.

    Non-numeric tokens refuse invent (never silent 0.0). Non-finite floats are
    kept as IEEE values when the source already produced them; string
    ``NaN``/``Infinity`` are accepted as explicit IEEE wire.
    """
    if value is None:
        return None
    from decimal import Decimal, InvalidOperation
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, bool):
        raise ValueError(
            f"float cannot bind bool — refuse invent into {ddl_type or 'FLOAT'}"
        )
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, Decimal):
        try:
            return float(value)
        except (InvalidOperation, OverflowError, ValueError) as exc:
            raise ValueError(
                f"refuse invent float from decimal {value!r}"
            ) from exc
    if isinstance(value, str):
        token = value.strip()
        if not token:
            raise ValueError(
                f"empty string cannot coerce to float for {ddl_type or 'FLOAT'} — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            )
        low = token.lower()
        if low in (_TRUE_TOKENS | _FALSE_TOKENS) - {"0", "1"}:
            raise ValueError(
                f"refuse invent float from boolean token {value!r}"
            )
        try:
            return float(token)
        except ValueError as exc:
            raise ValueError(
                f"refuse invent float from {value!r} for {ddl_type or 'FLOAT'}"
            ) from exc
    raise ValueError(
        f"unsupported float wire type: {type(value).__name__}"
    )


def coerce_json_wire(value: Any, *, as_text: bool = True) -> Any:
    """Normalize JSON/JSONB bind. Empty refuses NULL invent; invalid scalars wrap as JSON text."""
    if value is None:
        return None
    from services.value_serializer import json_default

    if isinstance(value, (dict, list, tuple)):
        if as_text:
            return json.dumps(
                value, ensure_ascii=False, separators=(",", ":"), default=json_default
            )
        return list(value) if isinstance(value, tuple) else value
    if isinstance(value, (bool, int, float)):
        return json.dumps(value, allow_nan=False) if as_text else value
    text = str(value).strip()
    if not text:
        # Never invent SQL NULL from "" on upsert wipe (MySQL 3140 avoidance is
        # quarantine/remap — not silent clear of a present JSON document).
        raise ValueError(
            "empty string cannot coerce to JSON — "
            "refuse silent NULL invent (quarantine or remap upstream)"
        )
    try:
        from services.value_serializer import json_loads_exact

        parsed = json_loads_exact(text)
    except Exception:
        # Lossless wrap so scalars still load into JSON columns.
        return json.dumps(text, ensure_ascii=False) if as_text else text
    if as_text:
        # Keep the original JSON text — json.dumps after loads would not
        # change polarity, but would rewrite whitespace/key order. The
        # engine text from col::text is the authority.
        return text
    return parsed


def coerce_binary_wire(value: Any) -> Any:
    """Normalize BYTEA/BLOB wire (base64 string → bytes).

    Invalid base64 must not silently UTF-8-encode into the blob — that invents
    bytes the operator never intended (Airbyte/Fivetran-class fail-closed).
    """
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except Exception as exc:
            raise ValueError(
                "binary wire is not valid base64 — refuse silent UTF-8 encode"
            ) from exc
    raise ValueError(
        f"unsupported binary wire type: {type(value).__name__}"
    )


def coerce_uuid_wire(value: Any, *, as_uuid: bool = False) -> Any:
    """Canonical UUID (lowercase hyphenated RFC 4122 string, or native UUID).

    Accepts ``uuid.UUID``, braced ``{…}``, ``urn:uuid:…``, and 32-hex forms
    (Fivetran HVR Compare / SQL Server UNIQUEIDENTIFIER class). Invalid wire
    raises ``ValueError`` — never invent a random UUID.

    ``as_uuid=True`` returns ``uuid.UUID`` for pyodbc SQL Server
    ``UNIQUEIDENTIFIER`` binds (string params often raise ODBC 8169).
    """
    import uuid as _uuid

    if value is None:
        return None
    if isinstance(value, _uuid.UUID):
        return value if as_uuid else str(value).lower()
    text = str(value).strip()
    if not text:
        raise ValueError("empty UUID wire")
    if text.startswith("{") and text.endswith("}"):
        text = text[1:-1]
    low = text.lower()
    if low.startswith("urn:uuid:"):
        text = text[9:]
    try:
        parsed = _uuid.UUID(text)
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError(f"invalid UUID wire: {text[:64]!r}") from exc
    return parsed if as_uuid else str(parsed).lower()


def coerce_rowversion_wire(value: Any) -> bytes | None:
    """Normalize SQL Server ``ROWVERSION`` / ``TIMESTAMP`` to exactly 8 bytes.

    Not a datetime — refuse ISO clock strings (Estuary/HVR BYTEA class).
    Accepts bytes, ``0x`` hex, 16-hex digits, or base64 encoding of 8 bytes.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) != 8:
            raise ValueError(
                f"ROWVERSION requires 8 bytes, got {len(raw)} — refuse invent"
            )
        return raw
    if isinstance(value, bool):
        raise ValueError("ROWVERSION cannot bind bool — refuse invent")
    if isinstance(value, int):
        if value < 0 or value > 0xFFFFFFFFFFFFFFFF:
            raise ValueError(
                f"ROWVERSION integer out of uint64 range — refuse invent: {value}"
            )
        return int(value).to_bytes(8, byteorder="big")
    if isinstance(value, (dict, list, tuple)):
        raise ValueError(
            f"ROWVERSION cannot bind {type(value).__name__} — refuse invent"
        )
    text = str(value).strip()
    if not text:
        raise ValueError("empty ROWVERSION wire — refuse invent")
    # Classic footgun: clock-looking strings must not become binary invent.
    if re.match(r"^\d{4}-\d{2}-\d{2}", text) or (
        "T" in text and re.search(r"\dT\d", text)
    ):
        raise ValueError(
            "ROWVERSION cannot bind datetime string — refuse invent "
            "(SQL Server TIMESTAMP is not a clock type)"
        )
    if text[:2].lower() == "0x":
        hexpart = text[2:]
        if len(hexpart) != 16 or not re.fullmatch(r"[0-9a-fA-F]{16}", hexpart):
            raise ValueError(
                f"ROWVERSION hex must be 16 digits — refuse invent: {text[:32]!r}"
            )
        return bytes.fromhex(hexpart)
    if re.fullmatch(r"[0-9a-fA-F]{16}", text):
        return bytes.fromhex(text)
    try:
        decoded = base64.b64decode(text, validate=True)
    except Exception as exc:
        raise ValueError(
            f"invalid ROWVERSION wire — refuse invent: {text[:32]!r}"
        ) from exc
    if len(decoded) != 8:
        raise ValueError(
            f"ROWVERSION base64 must decode to 8 bytes, got {len(decoded)}"
        )
    return decoded


def coerce_bitstring_wire(
    value: Any,
    *,
    width: int | None = None,
    varying: bool = False,
) -> str | None:
    """Normalize BIT/VARBIT wire to a ``0``/``1`` digit string.

    PostgreSQL bit strings are not opaque bytes — refuse base64 invent into
    ``BIT(n)``. Accepts ``1010``, ``B'1010'``, or raw bytes (MSB-first bits).
    """
    if value is None:
        return None
    bits: str
    if isinstance(value, (bytes, bytearray, memoryview)):
        bits = "".join(f"{b:08b}" for b in bytes(value))
    else:
        text = str(value).strip()
        text = _refuse_empty_specialty(text, 'BIT')
        if (text.startswith("B'") or text.startswith("b'")) and text.endswith("'"):
            text = text[2:-1]
        elif text.upper().startswith("0B"):
            text = text[2:]
        if not re.fullmatch(r"[01]+", text):
            raise ValueError(
                "bitstring wire must be 0/1 digits — refuse base64/UTF-8 invent into BIT"
            )
        bits = text
    if width is not None:
        if varying:
            if len(bits) > width:
                raise ValueError(
                    f"bitstring length {len(bits)} exceeds BIT VARYING({width})"
                )
        elif len(bits) != width:
            raise ValueError(
                f"bitstring length {len(bits)} does not match BIT({width})"
            )
    return bits


def coerce_array_wire(value: Any, *, engine: str = "", ddl_type: str = "") -> Any:
    """Normalize ARRAY / list wire for SQL bind.

    Postgres/Greenplum/Cockroach adapt Python ``list`` → ``ARRAY`` (psycopg).
    Dumping a JSON *string* into a typed ``text[]`` invents a one-element array
    of JSON text — fail-closed parse JSON arrays; refuse dict/object invent.
    Engines that map logical ARRAY → JSON/VARIANT keep ``coerce_json_wire``.

    When ``ddl_type`` is ``ARRAY<T>`` / ``T[]``, each element is normalized with
    the shared specialty SSOT (INET, UUID, …) — HVR-class typed array fidelity.
    """
    if value is None:
        return None
    eng = (engine or "").strip().lower()
    pg_native = eng in {
        "",
        "postgresql",
        "postgres",
        "pg",
        "greenplum",
        "cockroach",
        "cockroachdb",
        "timescaledb",
        "alloydb",
        "yugabytedb",
        "citus",
    } or eng.startswith("postgres")
    if not pg_native:
        return coerce_json_wire(value, as_text=True)

    def _elem_carrier() -> str:
        raw = (ddl_type or "").strip()
        if not raw:
            return ""
        upper = raw.upper()
        if upper.startswith("ARRAY<") and upper.endswith(">"):
            return raw[6:-1].strip()
        if upper.endswith("[]"):
            return raw[:-2].strip()
        return ""

    def _normalize_elems(items: list[Any]) -> list[Any]:
        carrier = _elem_carrier()
        if not carrier:
            return items
        out: list[Any] = []
        for item in items:
            if item is None:
                out.append(None)
            else:
                out.append(normalize_sql_bind_value(item, carrier, engine=eng))
        return out

    if isinstance(value, tuple):
        return _normalize_elems(list(value))
    if isinstance(value, list):
        return _normalize_elems(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("array wire cannot bind raw bytes — refuse invent")
    if isinstance(value, dict):
        raise ValueError(
            "array wire cannot bind object/dict — refuse invent into ARRAY"
        )
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(
                "empty string cannot coerce to ARRAY — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            )
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except Exception as exc:
                raise ValueError(
                    "array wire JSON parse failed — refuse invent into ARRAY"
                ) from exc
            if not isinstance(parsed, list):
                raise ValueError(
                    "array wire JSON must be a list — refuse invent into ARRAY"
                )
            return _normalize_elems(parsed)
        # Postgres literal form {a,b,c} is driver-specific; pass through only
        # when it already looks like an array literal (operator can cast).
        if text.startswith("{") and text.endswith("}"):
            return text
        raise ValueError(
            "array wire string is not a JSON list or PG literal — refuse invent"
        )
    # Scalars must not silently become one-element arrays (Airbyte honesty).
    raise ValueError(
        f"unsupported array wire type: {type(value).__name__} — refuse invent"
    )


def coerce_struct_wire(value: Any, *, engine: str = "", ddl_type: str = "") -> Any:
    """Normalize STRUCT/RECORD wire for SQL bind (Airbyte Destinations V2 class).

    Fielded objects bind as JSON text into JSON/VARIANT/SUPER document sinks.
    Lakehouse engines that declare ``STRUCT<a:T>`` still receive a JSON object
    string unless the driver registers a native struct adapter — never invent
    a STRUCT from a scalar or array.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel, json_default

    if is_missing_sentinel(value):
        return value
    if isinstance(value, list):
        raise ValueError(
            "struct wire cannot bind array/list — refuse invent into STRUCT"
        )
    if isinstance(value, (bool, int, float)):
        raise ValueError(
            f"struct wire cannot bind {type(value).__name__} — refuse invent into STRUCT"
        )
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("struct wire cannot bind raw bytes — refuse invent")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(
                "empty string cannot coerce to STRUCT — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            )
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except Exception as exc:
                raise ValueError(
                    "struct wire JSON parse failed — refuse invent into STRUCT"
                ) from exc
            if not isinstance(parsed, dict):
                raise ValueError(
                    "struct wire JSON must be an object — refuse invent into STRUCT"
                )
            return text
        raise ValueError(
            "struct wire string is not a JSON object — refuse invent into STRUCT"
        )
    if isinstance(value, dict):
        return json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), default=json_default
        )
    raise ValueError(
        f"unsupported struct wire type: {type(value).__name__} — refuse invent"
    )


def coerce_map_wire(value: Any, *, engine: str = "", ddl_type: str = "") -> Any:
    """Normalize MAP wire for SQL bind.

    Maps are string-keyed objects (Spark/Airbyte). Lists/scalars refuse invent.
    Document sinks get compact JSON text — same Destinations V2 pattern as STRUCT.
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel, json_default

    if is_missing_sentinel(value):
        return value
    if isinstance(value, list):
        # Some warehouses encode MAP as list of {key,value} pairs — accept only
        # that explicit shape; refuse inventing from arbitrary arrays.
        if value and all(
            isinstance(item, dict) and set(item.keys()) >= {"key", "value"}
            for item in value
        ):
            as_obj = {str(item["key"]): item["value"] for item in value}
            return json.dumps(
                as_obj, ensure_ascii=False, separators=(",", ":"), default=json_default
            )
        raise ValueError(
            "map wire cannot bind arbitrary array — refuse invent into MAP"
        )
    if isinstance(value, (bool, int, float)):
        raise ValueError(
            f"map wire cannot bind {type(value).__name__} — refuse invent into MAP"
        )
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(
                "empty string cannot coerce to MAP — "
                "refuse silent NULL invent (quarantine or remap upstream)"
            )
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
            except Exception as exc:
                raise ValueError(
                    "map wire JSON parse failed — refuse invent into MAP"
                ) from exc
            if not isinstance(parsed, dict):
                raise ValueError(
                    "map wire JSON must be an object — refuse invent into MAP"
                )
            return text
        raise ValueError(
            "map wire string is not a JSON object — refuse invent into MAP"
        )
    if isinstance(value, dict):
        return json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), default=json_default
        )
    raise ValueError(
        f"unsupported map wire type: {type(value).__name__} — refuse invent"
    )


def coerce_decimal_wire(value: Any, *, ddl_type: str = "", engine: str = "") -> Any:
    """Exact ``Decimal`` bind — never float64 round-trip invent.

    Values that cannot fit ``DECIMAL|NUMERIC|NUMBER(p,s)`` raise. PostgreSQL-
    family destinations round fractional excess (engine docs) — match
    ``fits_decimal(..., dest_db=)`` so quarantine and bind never disagree.
    Bare DECIMAL without (p,s) still returns an exact ``Decimal``.
    """
    if value is None:
        return None
    from decimal import (
        ROUND_HALF_UP,
        Context,
        Decimal,
        InvalidOperation,
        Overflow,
        localcontext,
    )

    from connectors.writer_common import (
        PG_DECIMAL_ROUND_DIALECTS,
        decimal_int_digits_and_scale,
        fits_decimal,
    )
    from services.type_system import parse_numeric_precision_scale
    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        return value
    from services.transform_engine import boolean_carrier_numeric_value

    _p, _s = parse_numeric_precision_scale(ddl_type)
    carrier_bool = boolean_carrier_numeric_value(value, _p, _s)
    if carrier_bool is not None:
        # NUMBER(1)/DECIMAL(1,0) is the boolean carrier on engines without a
        # native boolean — binding 1/0 there is total, not an invented number.
        return Decimal(carrier_bool)
    try:
        if isinstance(value, Decimal):
            d = value
        elif isinstance(value, bool):
            # bool is int subclass — refuse inventing 0/1 money from True/False.
            raise ValueError("decimal wire cannot bind bool — refuse invent")
        elif isinstance(value, int):
            d = Decimal(value)
        elif isinstance(value, float):
            if value != value or value in (float("inf"), float("-inf")):  # noqa: PLR0124
                raise ValueError("decimal wire refuses NaN/Inf")
            # str() avoids binary float expansion into spurious digits.
            d = Decimal(str(value))
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                raise ValueError(
                    f"empty string cannot coerce to decimal for {ddl_type or 'DECIMAL'} — "
                    "refuse silent NULL invent (quarantine or remap upstream)"
                )
            d = Decimal(text)
        else:
            raise ValueError(
                f"unsupported decimal wire type: {type(value).__name__}"
            )
        if not d.is_finite():
            raise ValueError("decimal wire refuses non-finite Decimal")
    except (InvalidOperation, Overflow, ValueError) as exc:
        msg = str(exc)
        # Preserve honest empty / bool / NaN refusals — do not wrap into generic parse.
        if "refuse" in msg.lower() or "empty string cannot coerce" in msg.lower():
            raise
        raise ValueError(
            f"decimal wire parse failed — refuse invent into {ddl_type or 'DECIMAL'}"
        ) from exc

    eng = (engine or "").strip().lower()
    dest_db = eng
    if eng in {"postgres", "pg", "postgresql+psycopg2"}:
        dest_db = "postgresql"
    precision, scale = parse_numeric_precision_scale(ddl_type)
    if precision is not None:
        s = 0 if scale is None else int(scale)
        if not fits_decimal(d, int(precision), s, dest_db=dest_db):
            raise ValueError(
                f"decimal overflow: value does not fit {ddl_type or f'DECIMAL({precision},{s})'} "
                "— refuse silent quantize"
            )
        # Apply the same PG scale round the engine would perform at INSERT.
        if dest_db in PG_DECIMAL_ROUND_DIALECTS:
            _, value_scale = decimal_int_digits_and_scale(d)
            if value_scale > s:
                with localcontext(
                    Context(prec=max(int(precision) + 16, 80), rounding=ROUND_HALF_UP)
                ):
                    d = d.quantize(Decimal(1).scaleb(-s))
    return d


def coerce_interval_wire(
    value: Any,
    *,
    ddl_type: str = "",
    engine: str = "",
) -> Any:
    """Normalize INTERVAL wire (ISO-8601 / SQL / timedelta) fail-closed.

    Family polarity (YM vs DS) must match destination DDL — never invent a cast
    across ANSI/Oracle/Snowflake families (Fivetran HVR Compare write-location).
    """
    from services.schema_inference import is_interval_wire, interval_wire_family
    from services.type_system import interval_family
    from services.value_serializer import format_bigquery_interval

    if value is None:
        return None
    if not is_interval_wire(value):
        raise ValueError(
            "interval wire is not ISO-8601/SQL duration — refuse invent into INTERVAL"
        )
    fam = interval_family(ddl_type)
    wire_fam = interval_wire_family(value)
    if fam and wire_fam and fam != wire_fam:
        raise ValueError(
            f"interval family mismatch: wire {wire_fam} cannot bind to {ddl_type}"
        )
    eng = (engine or "").strip().lower()
    if eng in {"bigquery", "bq"}:
        return format_bigquery_interval(value)
    try:
        from datetime import timedelta

        if isinstance(value, timedelta):
            return value
    except Exception:
        pass
    return str(value).strip()


def coerce_geography_wire(
    value: Any,
    *,
    ddl_type: str = "",
    engine: str = "",
) -> Any:
    """Normalize GEOGRAPHY/GEOMETRY wire (WKT/EWKT/GeoJSON/EWKB) fail-closed.

    SRID / polarity mismatches raise — never invent a reproject or cast.
    """
    import json

    from services.schema_inference import geography_wire_srid, is_geography_wire
    from services.type_system import parse_geography_srid

    del engine  # reserved for future engine-native EWKB objects
    if value is None:
        return None
    if not is_geography_wire(value):
        raise ValueError(
            "geography wire is not WKT/EWKT/GeoJSON/EWKB — refuse invent into GEOGRAPHY"
        )
    expected = parse_geography_srid(ddl_type)
    wire_srid = geography_wire_srid(value)
    if expected is not None and wire_srid is not None and int(expected) != int(wire_srid):
        raise ValueError(
            f"geography SRID mismatch: wire {wire_srid} cannot bind to {ddl_type}"
        )
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, dict):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value).strip()


def normalize_sql_bind_value(
    value: Any,
    ddl_type: str,
    *,
    engine: str = "",
) -> Any:
    """Route value through shared boolean/JSON/binary bind for ``ddl_type``.

    Temporal coercion stays in ``sql_temporal.coerce_sql_temporal`` — callers
    should apply that first (or via writer-specific helpers).
    """
    if value is None:
        return None
    from services.value_serializer import is_missing_sentinel

    # Sparse CDC sentinel must survive bind normalize — writers omit from SET.
    if is_missing_sentinel(value):
        return value
    from connectors.sql_temporal import coerce_sql_temporal, sql_base_type

    # ENUM/SET before sql_base_type — paren strip would drop member domain.
    from services.type_system import parse_enum_or_set_ordered_members

    eng = (engine or "").strip().lower()
    enum_set = parse_enum_or_set_ordered_members(ddl_type)
    if enum_set is not None:
        kind, _members = enum_set
        if kind == "ENUM":
            return coerce_enum_wire(value, ddl_type=ddl_type)
        # PostgreSQL create-new maps SET → TEXT[]; bind as list (psycopg array).
        pg_set_list = eng in {
            "postgresql",
            "postgres",
            "pg",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        } or eng.startswith("postgres")
        return coerce_set_wire(value, ddl_type=ddl_type, as_list=pg_set_list)

    temporal = coerce_sql_temporal(value, ddl_type, engine=eng)
    if temporal is not value:
        return temporal

    upper = sql_base_type(ddl_type)

    # MySQL YEAR before INTEGER collapse — string '0' → 2000 polarity.
    from services.type_system import is_year_carrier

    if is_year_carrier(ddl_type) or upper == "YEAR":
        return coerce_year_wire(value)
    # Oracle VARCHAR2/CHAR: zero-length string is stored as NULL (HVR write
    # coercion / Oracle semantics). Collapse only for true string carriers —
    # never silence specialty DDL (INET/CITEXT/TSVECTOR/…) that maps to
    # LOGICAL_STRING but must raise or keep '' under the honesty bar.
    if (
        isinstance(value, str)
        and value == ""
        and (eng in {"oracle", "oracledb", "oracle_autonomous"} or eng.startswith("oracle"))
    ):
        from services.type_system import (
            LOGICAL_STRING,
            LOGICAL_TEXT,
            normalize_logical_type,
            specialty_carrier_base,
        )

        if not specialty_carrier_base(ddl_type or upper) and (
            not upper
            or normalize_logical_type(ddl_type or upper) in {
                LOGICAL_STRING,
                LOGICAL_TEXT,
            }
        ):
            return None
    # BIT / VARBIT before BINARY — "BIT" must not fall through as boolean here
    # when width > 1 (caller passes BIT(32) etc.).
    from services.type_system import (
        is_bitstring_carrier,
        is_varying_bitstring_carrier,
        parse_bitstring_width,
    )

    if is_bitstring_carrier(ddl_type) or upper in {"BIT VARYING", "VARBIT"} or (
        upper == "BIT" and parse_bitstring_width(ddl_type) not in {None, 1}
    ):
        return coerce_bitstring_wire(
            value,
            width=parse_bitstring_width(ddl_type),
            varying=is_varying_bitstring_carrier(ddl_type) or upper in {"BIT VARYING", "VARBIT"},
        )
    # MySQL/SQL Server BIT(1) — boolean polarity (string '0' must stay 0, not True).
    if upper == "BIT" and parse_bitstring_width(ddl_type) in {None, 1}:
        return coerce_boolean_wire(value, as_int=eng in {"mysql", "mariadb", "tidb"})
    if upper == "ROWVERSION":
        return coerce_rowversion_wire(value)
    if upper == "SQL_VARIANT":
        json_env = eng in {
            "postgresql",
            "postgres",
            "pg",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
            "snowflake",
            "databricks",
        } or eng.startswith("postgres")
        envelope = coerce_sql_variant_wire(value, as_json_envelope=json_env)
        # The typed envelope is a dict; it still has to reach the driver as JSON
        # text for the same reason JSON/JSONB does above.
        if isinstance(envelope, (dict, list)):
            return coerce_json_wire(envelope, as_text=True)
        return envelope
    if upper in {"ROWID", "UROWID"}:
        return coerce_rowid_wire(value)
    if upper == "HIERARCHYID":
        pg_ltree = eng in {
            "postgresql",
            "postgres",
            "pg",
            "cockroachdb",
            "timescaledb",
            "alloydb",
            "yugabytedb",
            "citus",
            "supabase",
            "greenplum",
        } or eng.startswith("postgres")
        return coerce_hierarchyid_wire(value, as_ltree=pg_ltree)
    if upper in {"BINARY", "BLOB", "LONGBLOB", "VARBINARY", "BYTEA"}:
        return coerce_binary_wire(value)
    if upper in {"UUID", "UNIQUEIDENTIFIER", "GUID"}:
        # pyodbc UNIQUEIDENTIFIER prefers native uuid.UUID (ODBC 8169 on bad strings).
        mssql_uuid = eng in {
            "sqlserver",
            "mssql",
            "azure_sql",
            "synapse",
            "azure_synapse",
        }
        return coerce_uuid_wire(value, as_uuid=mssql_uuid)
    if upper in {"PG_LSN", "LSN"}:
        return coerce_pg_lsn_wire(value)
    if upper == "OID":
        return coerce_oid_wire(value)
    if upper in {"TID", "CTID"}:
        return coerce_tid_wire(value)
    if upper == "XID8":
        return coerce_xid_wire(value, width64=True)
    if upper == "XID":
        return coerce_xid_wire(value, width64=False)
    if upper == "CID":
        return coerce_cid_wire(value)
    if upper in {"TXID_SNAPSHOT", "PG_SNAPSHOT"}:
        return coerce_txid_snapshot_wire(value)
    if upper in {"INET", "IPV4", "IPV6", "IP"}:
        return coerce_inet_wire(value)
    if upper in {"CIDR"}:
        return coerce_cidr_wire(value)
    if upper in {"MACADDR", "MACADDR8"}:
        return coerce_macaddr_wire(value, eui64=upper == "MACADDR8")
    if upper in {"XML", "XMLTYPE"}:
        return coerce_xml_wire(value)
    if upper == "JSONPATH":
        return coerce_jsonpath_wire(value)
    if upper == "CITEXT":
        return coerce_citext_wire(value)
    if upper == "LTREE":
        return coerce_ltree_wire(value)
    if upper in {"TSVECTOR", "TSQUERY"}:
        return coerce_tsvector_wire(value)
    if upper == "POINT":
        return coerce_point_wire(value)
    if upper == "BOX":
        return coerce_box_wire(value)
    if upper == "CIRCLE":
        return coerce_circle_wire(value)
    if upper == "LSEG":
        return coerce_lseg_wire(value)
    if upper == "LINE":
        return coerce_line_wire(value)
    if upper == "PATH":
        return coerce_path_wire(value)
    if upper == "POLYGON":
        return coerce_polygon_wire(value)
    if upper == "HSTORE":
        return coerce_hstore_wire(value)
    if "MULTIRANGE" in upper:
        return coerce_range_wire(value, multi=True)
    if upper.endswith("RANGE") and upper != "RANGE":  # int4range, daterange, …
        return coerce_range_wire(value, multi=False)
    if upper == "RANGE":
        return coerce_range_wire(value, multi=False)
    if upper == "ARRAY" or upper.endswith("[]") or (
        (upper.startswith("ARRAY<") or upper.startswith("LIST<")) and upper.endswith(">")
    ) or (
        (upper.startswith("ARRAY(") or upper.startswith("LIST(") or upper.startswith("NESTED("))
        and upper.endswith(")")
    ):
        return coerce_array_wire(value, engine=eng, ddl_type=ddl_type or upper)
    if upper == "STRUCT" or upper.startswith(
        ("STRUCT<", "RECORD<", "STRUCT(", "ROW(", "OBJECT(", "TUPLE(")
    ):
        return coerce_struct_wire(value, engine=eng, ddl_type=ddl_type or upper)
    if upper == "MAP" or upper.startswith("MAP<") or upper.startswith("MAP("):
        return coerce_map_wire(value, engine=eng, ddl_type=ddl_type or upper)
    if upper in {"JSON", "JSONB", "VARIANT", "OBJECT", "SUPER"}:
        # JSON text is the portable wire for every engine here. Handing psycopg2 a
        # native dict/list raises "can't adapt type 'dict'" and aborted the whole
        # transfer (only psycopg3 adapts dicts), while Postgres casts an
        # unknown-typed text parameter straight into json/jsonb. Read-back is
        # unaffected — psycopg2 still parses jsonb into native dict/list.
        # SUPER (Redshift) shares empty→NULL refuse with VARIANT/OBJECT.
        return coerce_json_wire(value, as_text=True)
    if upper in {"BOOLEAN", "BOOL"}:
        return coerce_boolean_wire(value, as_int=eng in {"mysql", "mariadb"})
    if upper == "TINYINT" and eng in {"mysql", "mariadb", "tidb"}:
        # MySQL TINYINT(1) convention — same as BOOLEAN wire (0/1 int).
        return coerce_boolean_wire(value, as_int=True)
    if upper in {
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "INT",
        "INTEGER",
        "BIGINT",
        "INT2",
        "INT4",
        "INT8",
        "SERIAL",
        "BIGSERIAL",
        "SMALLSERIAL",
    }:
        # SQL Server TINYINT stays numeric 0–255 (pyodbc/Microsoft) — never bool.
        return coerce_integer_wire(value, ddl_type=ddl_type or upper, engine=eng)
    if upper in {
        "FLOAT",
        "FLOAT4",
        "FLOAT8",
        "FLOAT16",
        "FLOAT32",
        "FLOAT64",
        "HALF",
        "HALFFLOAT",
        "REAL",
        "DOUBLE",
        "DOUBLE PRECISION",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
    } or upper.startswith("FLOAT("):
        return coerce_float_wire(value, ddl_type=ddl_type or upper)
    if upper in {
        "DECIMAL",
        "NUMERIC",
        "NUMBER",
        "MONEY",
        "SMALLMONEY",
        "BIGNUMERIC",
        "BIGDECIMAL",
        "CURRENCY",
    } or upper.startswith(("DECIMAL(", "NUMERIC(", "NUMBER(", "BIGNUMERIC(")):
        return coerce_decimal_wire(value, ddl_type=ddl_type or upper, engine=eng)
    from services.type_system import (
        LOGICAL_GEOGRAPHY,
        LOGICAL_INTERVAL,
        LOGICAL_MAP,
        LOGICAL_STRUCT,
        normalize_logical_type,
    )

    logical = normalize_logical_type(ddl_type or upper)
    if logical == LOGICAL_STRUCT:
        return coerce_struct_wire(value, engine=eng, ddl_type=ddl_type or upper)
    if logical == LOGICAL_MAP:
        return coerce_map_wire(value, engine=eng, ddl_type=ddl_type or upper)
    if logical == LOGICAL_INTERVAL or upper.startswith("INTERVAL"):
        return coerce_interval_wire(value, ddl_type=ddl_type or upper, engine=eng)
    if logical == LOGICAL_GEOGRAPHY or upper in {
        "GEOGRAPHY",
        "GEOMETRY",
        "SDO_GEOMETRY",
        "GEOGRAPHY(POINT)",
        "GEOMETRY(POINT)",
    }:
        return coerce_geography_wire(value, ddl_type=ddl_type or upper, engine=eng)
    return value
