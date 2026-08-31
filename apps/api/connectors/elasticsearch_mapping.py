"""Explicit Elasticsearch index mappings for a declared destination shape.

A created index must declare its fields. Left to dynamic mapping, the first
document decides: a ``DECIMAL`` the writer sends as a precision-preserving
string is dynamic-mapped ``text`` (analyzed, not aggregatable), and the next run
reads that ``text`` back as the destination carrier and refuses its own output as
a fidelity collapse.

Each declared property also carries ``meta.df_carrier`` — the carrier the field
holds — so a later reread recovers it instead of re-deriving one from the
Elasticsearch field type, which cannot express ``DECIMAL(12,4)`` vs ``DECIMAL``,
or a JSON document stored as a term vs prose.
"""

from __future__ import annotations

from services.type_system import parse_numeric_precision_scale

# Elasticsearch mapping metadata limits (ES >= 7.6): at most 5 entries, keys of
# at most 20 characters, string values of at most 50 characters.
_META_KEY = "df_carrier"
_META_VALUE_MAX = 50

# scaled_float keeps doc_values in a long (value * scaling_factor). Beyond 18
# total digits that product leaves the exact long range, so the declared scale
# would silently round — such a column stays an exact keyword term.
_SCALED_FLOAT_MAX_DIGITS = 18

# Field types Elasticsearch accepts in a mapping. A carrier whose invented DDL
# is not one of these (an unmapped logical falling back to a SQL spelling) is
# left undeclared rather than sent as an invalid mapping that would fail the
# whole index creation.
_ES_FIELD_TYPES = frozenset(
    {
        "text",
        "keyword",
        "long",
        "integer",
        "short",
        "byte",
        "double",
        "float",
        "half_float",
        "scaled_float",
        "boolean",
        "date",
        "date_nanos",
        "binary",
        "ip",
        "object",
        "nested",
        "flattened",
        "geo_point",
        "geo_shape",
        "dense_vector",
        "version",
    }
)

# Logicals the writer wires as a JSON *document string* (one Elasticsearch field
# cannot hold mixed object/array shapes), so the declared field must be textual.
_DOCUMENT_LOGICALS = frozenset({"json", "array", "struct", "map"})

_TEXTUAL_ES_TYPES = frozenset({"text", "keyword", "constant_keyword", "wildcard"})

_DECIMAL_LOGICALS = frozenset({"decimal", "numeric"})


def is_es_field_type(token: str) -> bool:
    """True when ``token`` is an Elasticsearch field type, not a SQL carrier.

    Several names collide (``date``, ``text``, ``long``, ``binary``): a mapping
    always spells its field type lowercase, while a SQL carrier is spelled in
    the upper case the dialects and Map use, so only the lowercase spelling is
    read as a field type. Reading SQL ``DATE`` as the ES ``date`` field type
    would widen a calendar date into an instant.
    """
    stripped = str(token or "").strip()
    return bool(stripped) and stripped.islower() and stripped in _ES_FIELD_TYPES


def carrier_for_es_field_type(es_type: str) -> str:
    """Carrier an Elasticsearch field type holds.

    ``date`` is an *instant* (epoch millis with an optional format), not a
    calendar date: binding it as SQL ``DATE`` truncates the time of day off
    every value the field can hold.
    """
    from services.schema_introspect import _es_mapping_type

    token = str(es_type or "").strip().lower()
    if token == "date":
        return "TIMESTAMP"
    return _es_mapping_type(token)


def _text_property() -> dict[str, object]:
    """Analyzed text with the ``.keyword`` subfield dynamic mapping would add."""
    return {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    }


def _decimal_property(carrier: str) -> tuple[dict[str, object], str]:
    """``DECIMAL(p,s)`` → fixed-point ``scaled_float`` when exact, else keyword.

    A keyword field holds the writer's exact decimal text, so either shape keeps
    the value; only ``scaled_float`` also makes the column comparable and
    aggregatable, which is why it is preferred when the scale is known and the
    scaled value stays inside the backing long.
    """
    precision, scale = parse_numeric_precision_scale(carrier)
    declared = f"DECIMAL({precision},{scale})" if precision and scale is not None else "DECIMAL"
    if precision is None or scale is None or scale < 0:
        # Unknown scale — no scaling factor can be chosen without inventing one.
        return {"type": "keyword"}, declared
    if precision > _SCALED_FLOAT_MAX_DIGITS:
        return {"type": "keyword"}, declared
    return {"type": "scaled_float", "scaling_factor": 10**scale}, declared


def es_property_for_carrier(carrier: str, es_type: str = "") -> dict[str, object]:
    """Declared Elasticsearch property for one column.

    ``carrier`` is the logical/source carrier the column holds (``DECIMAL(12,4)``,
    ``TIMESTAMPTZ``, ``JSON``); ``es_type`` is the field type Map already invented
    for this destination, when there is one. The carrier decides precision, scale
    and temporal polarity — an Elasticsearch field type cannot express them — and
    ``es_type`` keeps the destination shape Map approved.
    """
    from services.type_system import ddl_type, normalize_logical_type

    text = str(carrier or "").strip()
    token = str(es_type or "").strip().lower()
    if not text and not token:
        return {}
    logical = normalize_logical_type(text) if text else ""
    if not token or not is_es_field_type(token):
        token = str(ddl_type("elasticsearch", text or token) or "").strip().lower()
    if token not in _ES_FIELD_TYPES:
        return {}
    if logical in _DECIMAL_LOGICALS and token in _TEXTUAL_ES_TYPES | {
        "scaled_float"
    }:
        prop, declared = _decimal_property(text)
    elif logical in _DOCUMENT_LOGICALS or (
        not logical and token in {"object", "nested", "flattened"}
    ):
        # The writer stores documents as JSON text: one field cannot hold mixed
        # object and array shapes, and an object container declared without its
        # children would pin a shape this transfer cannot prove.
        prop = _text_property()
        declared = "ARRAY<JSON>" if logical == "array" else "JSON"
    elif token == "text":
        prop = _text_property()
        declared = text or carrier_for_es_field_type(token)
    elif token in {"object", "nested", "flattened"}:
        return {}
    else:
        prop = {"type": token}
        declared = _declared_for_token(token, text)
    prop["meta"] = {_META_KEY: declared[:_META_VALUE_MAX]}
    return prop


def _declared_for_token(token: str, carrier: str) -> str:
    """Carrier to record for a field declared as ``token``.

    The source carrier is kept when the field really holds it (an ES ``date``
    holds a timestamp and a ``keyword`` holds exact text, so ``TIMESTAMPTZ``
    survives); otherwise the field type's own carrier is recorded, because that
    is what a reader can rely on.
    """
    from services.type_system import normalize_logical_type

    field_carrier = carrier_for_es_field_type(token)
    if not carrier:
        return field_carrier
    logical = normalize_logical_type(carrier)
    if token in {"date", "date_nanos"} and logical in {"date", "datetime"}:
        return carrier
    if token in _TEXTUAL_ES_TYPES:
        # An exact-text field holds the carrier's own wire form (this is how a
        # decimal too wide for scaled_float is stored), so the carrier survives.
        return carrier
    if normalize_logical_type(field_carrier) == logical:
        return carrier
    return field_carrier


def es_index_properties(
    target_cols: list[str],
    carriers: dict[str, str],
    es_types: dict[str, str] | None = None,
) -> dict[str, dict[str, object]]:
    """Mappings ``properties`` body for the fields this transfer writes.

    ``_id`` is document identity, not a field. A column with neither a carrier
    nor an invented field type is left out so dynamic mapping still decides it —
    declaring a guessed type would be an invention.
    """
    props: dict[str, dict[str, object]] = {}
    for col in target_cols or []:
        name = str(col or "")
        if not name or name == "_id":
            continue
        carrier = _lookup(carriers, name)
        prop = es_property_for_carrier(carrier, _lookup(es_types or {}, name))
        if prop:
            props[name] = prop
    return props


def _lookup(values: dict[str, str], name: str) -> str:
    hit = (
        values.get(name)
        or values.get(name.lower())
        or values.get(name.upper())
        or ""
    )
    return str(hit or "").strip()


def carrier_from_es_property(info: object) -> str:
    """Carrier declared by a previous run, from mapping ``meta`` — else ``""``."""
    if not isinstance(info, dict):
        return ""
    meta = info.get("meta")
    if not isinstance(meta, dict):
        return ""
    declared = meta.get(_META_KEY)
    if not isinstance(declared, str):
        return ""
    return declared.strip()
