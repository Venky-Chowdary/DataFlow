"""Tabular YAML ingest — sequence of flat mappings, scalars as text.

YAML 1.1 implicit types (``yes``/``on``/``n`` as booleans, unquoted integers)
are a silent-loss vector: a CSV-shaped ``flag: yes`` must stay the characters
``yes`` so schema inference and the writer decide the type, not PyYAML.
This module never calls ``construct_object``. It walks composer events and
keeps every scalar's original text.

Nested cell values, aliases, and mixed wrapper documents are refused rather
than flattened. COUNT is the walk of ``iter_yaml_dicts`` — never a prefix.
Export (``dump_yaml_records``) is the inverse: a sequence of flat mappings
with every scalar double-quoted so YAML 1.1 cannot coerce ``yes`` into a
boolean. An empty population is ``[]``, still YAML.
"""

from __future__ import annotations

import io
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

__all__ = [
    "YAMLTabularError",
    "count_yaml_records",
    "dump_yaml_records",
    "iter_yaml_dicts",
    "yaml_quote",
]


class YAMLTabularError(ValueError):
    """YAML that cannot be a tabular source. The message names the fix."""


def _is_path(content: Any) -> bool:
    if isinstance(content, Path):
        return True
    if isinstance(content, (bytes, bytearray)):
        return False
    if isinstance(content, str) and os.path.isfile(content):
        return True
    return False


def _open_text(content: bytes | str | Path, encoding: str) -> tuple[Any, Any]:
    if _is_path(content):
        handle = open(os.fspath(content), encoding=encoding, newline="")
        return handle, handle.close
    if isinstance(content, (bytes, bytearray)):
        try:
            text = bytes(content).decode(encoding)
        except UnicodeDecodeError as extra:
            raise YAMLTabularError(
                f"YAML is not valid {encoding} ({extra}); refuse silent byte replacement"
            ) from extra
        return io.StringIO(text), None
    if isinstance(content, str):
        return io.StringIO(content), None
    read = getattr(content, "read", None)
    if callable(read):
        if isinstance(getattr(content, "encoding", None), str):
            return content, None
        wrapper = io.TextIOWrapper(content, encoding=encoding, newline="")
        return wrapper, None
    raise YAMLTabularError("YAML COUNT expects bytes, str, Path, or a readable stream")


def iter_yaml_dicts(
    content: bytes | str | Path,
    *,
    encoding: str = "utf-8",
) -> Iterator[dict[str, Any]]:
    """Yield one flat mapping per YAML record.

    Accepted documents: a sequence of mappings; a single mapping (one row);
    a mapping whose only value is a sequence of mappings (a wrapper such as
    ``records: [...]``). Empty sequence / empty file is zero rows.
    """
    try:
        import yaml
        from yaml.events import (
            AliasEvent,
            DocumentEndEvent,
            DocumentStartEvent,
            MappingEndEvent,
            MappingStartEvent,
            ScalarEvent,
            SequenceEndEvent,
            SequenceStartEvent,
            StreamEndEvent,
            StreamStartEvent,
        )
    except ImportError as exc:
        raise YAMLTabularError(
            "YAML ingest requires PyYAML (pip install PyYAML)"
        ) from exc

    closer = None
    loader = None
    try:
        stream, closer = _open_text(content, encoding)
        loader = yaml.SafeLoader(stream)
        while loader.check_event():
            event = loader.peek_event()
            if isinstance(event, StreamStartEvent):
                loader.get_event()
                continue
            if isinstance(event, StreamEndEvent):
                loader.get_event()
                break
            if isinstance(event, DocumentStartEvent):
                loader.get_event()
                yield from _iter_document(loader, yaml_events=(
                    AliasEvent,
                    DocumentEndEvent,
                    MappingEndEvent,
                    MappingStartEvent,
                    ScalarEvent,
                    SequenceEndEvent,
                    SequenceStartEvent,
                ))
                continue
            if isinstance(event, DocumentEndEvent):
                loader.get_event()
                continue
            raise YAMLTabularError(
                f"YAML document is unmeasured ({type(event).__name__})"
            )
    except YAMLTabularError:
        raise
    except Exception as exc:
        raise YAMLTabularError(f"YAML is malformed ({exc})") from exc
    finally:
        if loader is not None:
            try:
                loader.dispose()
            except Exception:
                pass
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def count_yaml_records(
    content: bytes | str | Path,
    *,
    encoding: str = "utf-8",
) -> int | None:
    """Dest-engine record COUNT of tabular YAML. ``None`` when unmeasured."""
    try:
        return sum(1 for _ in iter_yaml_dicts(content, encoding=encoding))
    except YAMLTabularError:
        return None
    except (OSError, UnicodeDecodeError, UnicodeEncodeError, TypeError):
        return None
    except Exception:
        return None


def _iter_document(loader: Any, yaml_events: tuple[type, ...]) -> Iterator[dict[str, Any]]:
    (
        AliasEvent,
        DocumentEndEvent,
        MappingEndEvent,
        MappingStartEvent,
        ScalarEvent,
        SequenceEndEvent,
        SequenceStartEvent,
    ) = yaml_events
    if not loader.check_event():
        return
    peeked = loader.peek_event()
    if isinstance(peeked, DocumentEndEvent):
        return
    if isinstance(peeked, SequenceStartEvent):
        loader.get_event()
        yield from _iter_sequence_of_mappings(loader, yaml_events)
        return
    if isinstance(peeked, MappingStartEvent):
        loader.get_event()
        yield from _iter_document_mapping(loader, yaml_events)
        return
    if isinstance(peeked, ScalarEvent):
        raise YAMLTabularError(
            "YAML root is a scalar; a tabular source is a sequence of mappings"
        )
    if isinstance(peeked, AliasEvent):
        raise YAMLTabularError("YAML aliases are unmeasured; expand the document")
    raise YAMLTabularError(
        f"YAML root {type(peeked).__name__} cannot be streamed as rows"
    )


def _iter_sequence_of_mappings(
    loader: Any, yaml_events: tuple[type, ...]
) -> Iterator[dict[str, Any]]:
    (
        AliasEvent,
        _DocumentEndEvent,
        _MappingEndEvent,
        MappingStartEvent,
        ScalarEvent,
        SequenceEndEvent,
        SequenceStartEvent,
    ) = yaml_events
    while loader.check_event() and not isinstance(loader.peek_event(), SequenceEndEvent):
        peeked = loader.peek_event()
        if isinstance(peeked, MappingStartEvent):
            loader.get_event()
            yield _read_flat_mapping(loader, yaml_events)
            continue
        if isinstance(peeked, ScalarEvent):
            raise YAMLTabularError(
                "YAML sequence contains a scalar; every record must be a mapping"
            )
        if isinstance(peeked, SequenceStartEvent):
            raise YAMLTabularError(
                "YAML sequence contains a nested list; refuse silent flatten"
            )
        if isinstance(peeked, AliasEvent):
            raise YAMLTabularError("YAML aliases are unmeasured; expand the document")
        raise YAMLTabularError(
            f"YAML sequence item {type(peeked).__name__} is not a mapping"
        )
    if loader.check_event() and isinstance(loader.peek_event(), SequenceEndEvent):
        loader.get_event()


def _iter_document_mapping(
    loader: Any, yaml_events: tuple[type, ...]
) -> Iterator[dict[str, Any]]:
    """A document that is one mapping: one row, or a single list wrapper."""
    (
        AliasEvent,
        _DocumentEndEvent,
        MappingEndEvent,
        MappingStartEvent,
        ScalarEvent,
        SequenceEndEvent,
        SequenceStartEvent,
    ) = yaml_events
    scalars: list[tuple[str, str]] = []
    sequences: list[list[dict[str, Any]]] = []
    while loader.check_event() and not isinstance(loader.peek_event(), MappingEndEvent):
        key_event = loader.get_event()
        if isinstance(key_event, AliasEvent):
            raise YAMLTabularError("YAML aliases are unmeasured; expand the document")
        if not isinstance(key_event, ScalarEvent):
            raise YAMLTabularError("YAML mapping keys must be scalars")
        key = str(key_event.value or "")
        if not key:
            raise YAMLTabularError("YAML mapping has an empty key")
        peeked = loader.peek_event()
        if isinstance(peeked, ScalarEvent):
            scalars.append((key, _scalar_text(loader.get_event())))
            continue
        if isinstance(peeked, SequenceStartEvent):
            loader.get_event()
            sequences.append(list(_iter_sequence_of_mappings(loader, yaml_events)))
            continue
        if isinstance(peeked, MappingStartEvent):
            raise YAMLTabularError(
                f"YAML field {key!r} is a nested mapping; refuse silent flatten"
            )
        if isinstance(peeked, AliasEvent):
            raise YAMLTabularError("YAML aliases are unmeasured; expand the document")
        raise YAMLTabularError(
            f"YAML field {key!r} is not a scalar or record list"
        )
    if loader.check_event() and isinstance(loader.peek_event(), MappingEndEvent):
        loader.get_event()
    if sequences and scalars:
        raise YAMLTabularError(
            "YAML wrapper mixed scalar fields with a record list; "
            "use a sequence of mappings"
        )
    if len(sequences) == 1:
        yield from sequences[0]
        return
    if sequences:
        raise YAMLTabularError(
            "YAML document has multiple nested lists; refuse ambiguous records"
        )
    yield dict(scalars)


def _read_flat_mapping(loader: Any, yaml_events: tuple[type, ...]) -> dict[str, Any]:
    (
        AliasEvent,
        _DocumentEndEvent,
        MappingEndEvent,
        MappingStartEvent,
        ScalarEvent,
        _SequenceEndEvent,
        SequenceStartEvent,
    ) = yaml_events
    rec: dict[str, Any] = {}
    while loader.check_event() and not isinstance(loader.peek_event(), MappingEndEvent):
        key_event = loader.get_event()
        if isinstance(key_event, AliasEvent):
            raise YAMLTabularError("YAML aliases are unmeasured; expand the document")
        if not isinstance(key_event, ScalarEvent):
            raise YAMLTabularError("YAML mapping keys must be scalars")
        key = str(key_event.value or "")
        if not key:
            raise YAMLTabularError("YAML mapping has an empty key")
        if key in rec:
            raise YAMLTabularError(
                f"YAML mapping repeats key {key!r}; refuse silent overwrite"
            )
        peeked = loader.peek_event()
        if isinstance(peeked, ScalarEvent):
            rec[key] = _scalar_text(loader.get_event())
            continue
        if isinstance(peeked, (MappingStartEvent, SequenceStartEvent)):
            raise YAMLTabularError(
                f"YAML field {key!r} is nested; refuse silent flatten"
            )
        if isinstance(peeked, AliasEvent):
            raise YAMLTabularError("YAML aliases are unmeasured; expand the document")
        raise YAMLTabularError(
            f"YAML field {key!r} is not a scalar"
        )
    if loader.check_event() and isinstance(loader.peek_event(), MappingEndEvent):
        loader.get_event()
    return rec


def _scalar_text(event: Any) -> str:
    """Keep the characters YAML wrote. Null-style empty scalars stay ``''``."""
    raw = event.value
    if raw is None:
        return ""
    return str(raw)


def yaml_quote(text: str) -> str:
    """Double-quoted YAML scalar. Always quoted so YAML 1.1 cannot coerce.

    ``yes`` / ``NO`` / ``on`` / ``007`` / ``1.50`` stay characters. PyYAML's
    default dump would emit a bool or a float and the next ingest would
    not see what this run wrote.
    """
    escaped = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        raise YAMLTabularError(
            "YAML export refuses nested cell values; flatten before write"
        )
    return str(value)


def dump_yaml_records(
    records: list[dict[str, Any]],
    columns: list[str] | None = None,
    *,
    encoding: str = "utf-8",
) -> bytes:
    """Write a sequence of flat mappings — the inverse of ``iter_yaml_dicts``.

    Empty population is the YAML sequence ``[]``, never a JSON array under a
    ``.yaml`` name and never an empty file that COUNT cannot measure.
    """
    if not records:
        return b"[]\n"
    cols = list(columns or [])
    if not cols:
        seen: set[str] = set()
        for rec in records:
            for key in rec.keys():
                name = str(key)
                if name not in seen:
                    seen.add(name)
                    cols.append(name)
    if not cols:
        return b"[]\n"
    lines: list[str] = []
    for rec in records:
        first = True
        for col in cols:
            rendered = f"{yaml_quote(str(col))}: {yaml_quote(_cell_text(rec.get(col)))}"
            if first:
                lines.append(f"- {rendered}")
                first = False
            else:
                lines.append(f"  {rendered}")
    return ("\n".join(lines) + "\n").encode(encoding)
