"""Fixed-width tabular ingest — declared layout only, never guessed.

A delimiter-free record is unparseable without column widths. Guessing from
spaces is how a COBOL amount field silently eats the next column. This module
is the single owner of layout resolution and the row walk:

1. operator ``read_options.fixed_width_layout``
2. sidecar ``<file>.layout.json`` next to a path source
3. first-line ``#layout: name:width,...`` in the file itself

Mismatch between two declarations fails closed. A file with none of them
refuses with the control the operator must fill in. COUNT is the walk of
``iter_fixed_width_dicts`` — never ``wc -l``.
"""

from __future__ import annotations

import io
import json
import os
import re
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

__all__ = [
    "FixedWidthError",
    "FixedWidthLayout",
    "count_fixed_width_records",
    "iter_fixed_width_dicts",
    "layout_from_payload",
    "layout_header_line",
    "parse_layout_header",
    "resolve_fixed_width_layout",
]

LAYOUT_HEADER_PREFIX = "#layout:"
_HEADER_PAIR = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(\d+)$")

FixedWidthLayout = tuple[tuple[str, int], ...]


class FixedWidthError(ValueError):
    """Fixed-width source that cannot be parsed. The message names the fix."""


def layout_from_payload(raw: object) -> FixedWidthLayout:
    """Accept the shapes an API body, sidecar, or test naturally sends."""
    if raw is None or raw == "" or raw == ():
        return ()
    if isinstance(raw, str):
        return parse_layout_header(raw)
    if isinstance(raw, Mapping):
        columns = raw.get("columns") or raw.get("fields") or raw.get("layout")
        if columns is None:
            raise FixedWidthError(
                "fixed_width_layout object must have a columns list of "
                "{name, width} entries"
            )
        return layout_from_payload(columns)
    if not isinstance(raw, Sequence) or isinstance(raw, (bytes, bytearray)):
        raise FixedWidthError(
            f"fixed_width_layout must be a list of {{name, width}}, got {type(raw).__name__}"
        )
    out: list[tuple[str, int]] = []
    seen: set[str] = set()
    for item in raw:
        if isinstance(item, Mapping):
            name = str(item.get("name") or item.get("column") or "").strip()
            width_raw = item.get("width") or item.get("length")
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)) and len(item) == 2:
            name = str(item[0]).strip()
            width_raw = item[1]
        else:
            raise FixedWidthError(
                "each layout entry must be {name, width} or [name, width]"
            )
        if not name:
            raise FixedWidthError("layout column name is empty")
        if name in seen:
            raise FixedWidthError(f"layout repeats column {name!r}")
        try:
            width = int(width_raw)
        except (TypeError, ValueError) as exc:
            raise FixedWidthError(
                f"layout width for {name!r} must be an integer, got {width_raw!r}"
            ) from exc
        if width < 1:
            raise FixedWidthError(f"layout width for {name!r} must be >= 1, got {width}")
        seen.add(name)
        out.append((name, width))
    return tuple(out)


def parse_layout_header(text: str) -> FixedWidthLayout:
    """Parse ``#layout: id:8,amount:16`` or the same body without the prefix."""
    body = text.strip()
    if body.lower().startswith(LAYOUT_HEADER_PREFIX):
        body = body[len(LAYOUT_HEADER_PREFIX) :].strip()
    if not body:
        return ()
    parts = [p.strip() for p in body.split(",") if p.strip()]
    out: list[tuple[str, int]] = []
    seen: set[str] = set()
    for part in parts:
        match = _HEADER_PAIR.match(part)
        if match is None:
            raise FixedWidthError(
                f"layout pair {part!r} is not name:width "
                f"(example: {LAYOUT_HEADER_PREFIX} id:8,amount:16)"
            )
        name = match.group(1)
        width = int(match.group(2))
        if width < 1:
            raise FixedWidthError(f"layout width for {name!r} must be >= 1")
        if name in seen:
            raise FixedWidthError(f"layout repeats column {name!r}")
        seen.add(name)
        out.append((name, width))
    return tuple(out)


def layout_header_line(layout: Sequence[tuple[str, int]]) -> str:
    body = ",".join(f"{name}:{width}" for name, width in layout)
    return f"{LAYOUT_HEADER_PREFIX} {body}"


def _sidecar_path(content: bytes | str | Path) -> Path | None:
    if isinstance(content, Path):
        return Path(str(content) + ".layout.json")
    if isinstance(content, str) and os.path.isfile(content):
        return Path(content + ".layout.json")
    return None


def load_sidecar_layout(content: bytes | str | Path) -> FixedWidthLayout:
    path = _sidecar_path(content)
    if path is None or not path.is_file():
        return ()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FixedWidthError(
            f"fixed-width sidecar {path.name} is not readable JSON ({exc})"
        ) from exc
    return layout_from_payload(payload)


def _open_text(content: bytes | str | Path, encoding: str) -> tuple[Any, Any]:
    if isinstance(content, Path) or (
        isinstance(content, str) and os.path.isfile(content)
    ):
        handle = open(os.fspath(content), encoding=encoding, newline="")
        return handle, handle.close
    if isinstance(content, (bytes, bytearray)):
        try:
            text = bytes(content).decode(encoding)
        except UnicodeDecodeError as exc:
            raise FixedWidthError(
                f"fixed-width file is not valid {encoding} ({exc}); "
                "refuse silent byte replacement"
            ) from exc
        return io.StringIO(text), None
    if isinstance(content, str):
        return io.StringIO(content), None
    raise FixedWidthError("fixed-width COUNT expects bytes, str, or Path")


def _first_line(content: bytes | str | Path, encoding: str) -> str:
    stream, closer = _open_text(content, encoding)
    try:
        line = stream.readline()
        return line
    finally:
        if closer is not None:
            closer()


def resolve_fixed_width_layout(
    content: bytes | str | Path,
    declared: Sequence[tuple[str, int]] | None = None,
    *,
    encoding: str = "utf-8",
) -> tuple[FixedWidthLayout, bool]:
    """Return ``(layout, skip_header_line)``.

    ``skip_header_line`` is true when the first line of the file is the
    ``#layout:`` declaration (whether or not an operator layout was also given).
    """
    from_options = tuple(declared) if declared else ()
    sidecar = load_sidecar_layout(content)
    header_layout: FixedWidthLayout = ()
    header_present = False
    first = _first_line(content, encoding)
    if first.lstrip().lower().startswith(LAYOUT_HEADER_PREFIX):
        header_present = True
        header_layout = parse_layout_header(first)

    present = [(name, layout) for name, layout in (
        ("read_options", from_options),
        ("sidecar", sidecar),
        ("#layout header", header_layout),
    ) if layout]

    if not present:
        raise FixedWidthError(
            "Fixed-width files need a declared layout — set read_options."
            "fixed_width_layout, add a sidecar .layout.json, or start the file "
            f"with {LAYOUT_HEADER_PREFIX} id:8,amount:16"
        )

    canonical = present[0][1]
    for name, layout in present[1:]:
        if layout != canonical:
            raise FixedWidthError(
                f"fixed-width layout from {present[0][0]} disagrees with {name}"
            )
    return canonical, header_present


def iter_fixed_width_dicts(
    content: bytes | str | Path,
    layout: Sequence[tuple[str, int]] | None = None,
    *,
    encoding: str = "utf-8",
) -> Iterator[dict[str, Any]]:
    resolved, skip_header = resolve_fixed_width_layout(
        content, layout, encoding=encoding
    )
    expected = sum(width for _name, width in resolved)
    stream, closer = _open_text(content, encoding)
    try:
        line_no = 0
        for raw in stream:
            line_no += 1
            if skip_header and line_no == 1:
                continue
            line = raw.rstrip("\n\r")
            if not line.strip():
                continue
            if len(line) != expected:
                raise FixedWidthError(
                    f"line {line_no} is {len(line)} characters; the layout "
                    f"requires exactly {expected}"
                )
            rec: dict[str, Any] = {}
            pos = 0
            for name, width in resolved:
                rec[name] = line[pos : pos + width].rstrip(" ")
                pos += width
            yield rec
    finally:
        if closer is not None:
            closer()


def count_fixed_width_records(
    content: bytes | str | Path,
    layout: Sequence[tuple[str, int]] | None = None,
    *,
    encoding: str = "utf-8",
) -> int | None:
    try:
        return sum(
            1
            for _ in iter_fixed_width_dicts(
                content, layout, encoding=encoding
            )
        )
    except FixedWidthError:
        return None
    except (OSError, UnicodeDecodeError, UnicodeEncodeError, TypeError):
        return None
    except Exception:
        return None
