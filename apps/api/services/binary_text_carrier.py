"""DATA-25: say where bytes are stored as base64 text.

A Redis value is text (the writer stores each row as a JSON document), so a
binary source column (PostgreSQL ``bytea``, MongoDB BinData, ...) lands as the
base64 text of its bytes. That is a faithful encoding, not the bytes, and a
read back gives text. DataFlow labels it rather than storing raw bytes: Map
shows the note on the mapping and the Trust Report carries it per column.
"""

from __future__ import annotations

from collections.abc import Mapping

#: Destinations whose stored value is text, so bytes land base64-encoded.
TEXT_VALUE_STORES = frozenset({"redis"})
BASE64_TEXT_LABEL = "stored as base64 text"


def base64_text_note(source_type: str, dest_engine: str) -> str:
    """The Map / Trust Report label for one column, or ``""``."""
    from services.type_system import normalize_logical_type

    engine = (dest_engine or "").strip().lower()
    if engine not in TEXT_VALUE_STORES or normalize_logical_type(source_type or "") != "binary":
        return ""
    return f"{BASE64_TEXT_LABEL} ({engine.capitalize()} values are text, so the bytes are base64-encoded)"


def representation_notes(source_types: Mapping[str, str] | None, dest_engine: str) -> list[str]:
    """``"<column>: stored as base64 text (...)"`` for each binary column."""
    notes = []
    for column, source_type in (source_types or {}).items():
        note = base64_text_note(str(source_type or ""), dest_engine)
        if note:
            notes.append(f"{column}: {note}")
    return notes
