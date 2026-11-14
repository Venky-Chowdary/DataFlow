"""Text-ordered MongoDB cursor fields (backlog C6, leading-zero text keys).

MongoDB compares two strings by their bytes, so a string cursor field is
ordered as text, exactly like a SQL text column. The SQL route already refuses
a text cursor whose values text order misplaces (:mod:`services.cursor_order`):
numbers of more than one width (``'9'`` vs ``'10'``, ``'007'`` vs ``'0010'``),
calendar dates, mixed offsets. The MongoDB route never asked, so a string field
holding ``'1'``, ``'9'`` and later ``'10'`` read ``'10'`` as behind the watermark
``'9'``: the document was skipped forever and every run reported success.

The same shape rules now apply to a MongoDB field that stores strings. A field
that stores ObjectIds, numbers or dates is ordered by MongoDB by value (an
ObjectId by its 12 bytes, which its 24-hex text spells in the same order) and
is not judged here, even when an ObjectId's hex happens to look like a number.
"""

from __future__ import annotations

import re
from typing import Any

#: Values sampled to judge the shape, besides the field's own MIN and MAX.
_SAMPLE = 1000


def _for_mongo(reason: str) -> str:
    return reason.replace("is a text column", "is a string field").replace(
        "(or cast it in a view)", "(or store the values as one)"
    )


def probe_mongo_text_cursor(cfg: dict[str, Any], collection: str, field: str) -> str:
    """See :func:`_probe`; the refusal speaks of a MongoDB string field."""
    return _for_mongo(_probe(cfg, collection, field))


def _probe(cfg: dict[str, Any], collection: str, field: str) -> str:
    """Why this collection's string cursor cannot bound an incremental read, or ``""``.

    Raises when the collection cannot be read (the caller reports it unmeasured).
    """
    from connectors.mongodb_reader import _connection_string, _mongo_client, stored_cursor_bson_kind
    from services.cursor_order import SHAPE_PADDED_NUMBER, text_cursor_refusal, text_cursor_shape

    database = str(cfg.get("database") or "")
    if not (database and collection and field):
        return ""
    coll = _mongo_client(_connection_string(cfg))[database][collection]
    try:
        kind = stored_cursor_bson_kind(coll, field)
    except ValueError as mixed:
        # More than one BSON type: no single watermark bounds the read.
        return str(mixed)
    if kind != "string":
        return ""
    typed = {field: {"$type": "string"}}
    values = [d.get(field) for d in coll.find(typed, {field: 1, "_id": 0}).limit(_SAMPLE)]
    for direction in (1, -1):
        edge = coll.find_one(typed, {field: 1, "_id": 0}, sort=[(field, direction)])
        if edge:
            values.append(edge.get(field))
    texts = [str(v).strip() for v in values if isinstance(v, str) and str(v).strip()]
    if not texts:
        return ""
    shape = text_cursor_shape(texts)
    reason = text_cursor_refusal(field, shape)
    if reason or shape.kind != SHAPE_PADDED_NUMBER:
        return reason
    # One width of digits so far: any string of another width is a value text
    # order misplaces ('0999' < '1000' holds only at one width).
    width = len(texts[0])
    odd = coll.find_one(
        {field: {"$type": "string", "$not": re.compile(rf"^[0-9]{{{width}}}$")}},
        {field: 1, "_id": 0},
    )
    if odd and isinstance(odd.get(field), str):
        return text_cursor_refusal(field, text_cursor_shape([*texts, odd[field]]))
    return ""
