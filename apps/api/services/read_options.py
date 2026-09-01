"""Source read options — the window a tabular source is read through.

A real client workbook rarely starts with the header on row 1 of the first
sheet. It has a title, a blank line, a header, then the data, and the sheet the
operator cares about is the third one. Without these options such a file cannot
be ingested at all; with them the window is declared once and every consumer —
ingest, preview, source COUNT, Gate-8 cell checksum — reads the same rows.

That last property is the reason this is a value object with a hash rather than
a bag of keyword arguments: the population Validate profiles and the population
the writer sends must be the same set of rows, and the only way to guarantee
that is for one declaration to reach both.
"""

from __future__ import annotations

import codecs
import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping

__all__ = ["ReadOptions", "ReadOptionsError", "parse_read_options_payload"]

# A workbook cannot have a header on row 1_000_001; a value that large is a
# typo or a probe, and skipping that far is indistinguishable from an empty
# read. Refuse instead of returning zero rows and calling it success.
_MAX_ROW_OFFSET = 1_000_000


class ReadOptionsError(ValueError):
    """Read options that cannot be honoured. The message names the fix."""


@dataclass(frozen=True, slots=True)
class ReadOptions:
    """Which sheet, which header row, and which data rows to leave out.

    ``sheet``       sheet name; empty means the workbook's active sheet.
    ``sheet_index`` 0-based sheet position; ``-1`` means unset. Ignored when
                    ``sheet`` is given, so a name always wins over a position.
    ``header_row``  1-based *physical* row carrying the column names. Rows
                    above it are preamble and are not data. ``0`` means the
                    sheet has no header and names are synthesized ``col_0…``.
    ``skip_rows``   value-bearing data rows to drop from the head, after the
                    header. Blank/formatting-only rows are never counted here,
                    so the number means what the operator sees.
    ``skip_footer`` value-bearing data rows to drop from the tail — the totals
                    row a spreadsheet almost always carries.
    ``encoding``    text codec for delimited files; empty means sniff (BOM →
                    utf-8-sig, else utf-8, else latin-1).
    ``delimiter``   single-character field separator for delimited files; empty
                    means sniff. ``\\t`` and ``tab`` are accepted spellings.
    ``fixed_width_layout``
                    declared ``(name, width)`` pairs for a fixed-width source.
                    Empty means the reader looks for a ``#layout:`` header or
                    a sidecar ``.layout.json``. Guessing widths is forbidden.
    """

    sheet: str = ""
    sheet_index: int = -1
    header_row: int = 1
    skip_rows: int = 0
    skip_footer: int = 0
    encoding: str = ""
    delimiter: str = ""
    fixed_width_layout: tuple[tuple[str, int], ...] = ()

    def __post_init__(self) -> None:
        if self.encoding:
            try:
                codecs.lookup(self.encoding)
            except LookupError as exc:
                raise ReadOptionsError(
                    f"'{self.encoding}' is not a known text encoding"
                ) from exc
        if len(self.delimiter) > 1:
            raise ReadOptionsError(
                f"delimiter must be a single character, got {self.delimiter!r}"
            )
        if self.fixed_width_layout and not isinstance(self.fixed_width_layout, tuple):
            object.__setattr__(
                self, "fixed_width_layout", _as_fixed_width_layout(self.fixed_width_layout)
            )
        if self.sheet_index < -1:
            raise ReadOptionsError(
                f"sheet_index must be 0-based or -1 for unset, got {self.sheet_index}"
            )
        for name, value in (
            ("header_row", self.header_row),
            ("skip_rows", self.skip_rows),
            ("skip_footer", self.skip_footer),
        ):
            if value < 0:
                raise ReadOptionsError(f"{name} cannot be negative, got {value}")
            if value > _MAX_ROW_OFFSET:
                raise ReadOptionsError(
                    f"{name}={value} exceeds the {_MAX_ROW_OFFSET} row limit — "
                    "an offset that large reads an empty population"
                )

    @property
    def is_default(self) -> bool:
        """True when this window is exactly today's behaviour (row 1, active sheet)."""
        return self == ReadOptions()

    @property
    def has_header(self) -> bool:
        return self.header_row > 0

    @property
    def sheet_window(self) -> "ReadOptions":
        """Only the workbook-shaped fields, for refusing them on other sources."""
        return ReadOptions(
            sheet=self.sheet,
            sheet_index=self.sheet_index,
            header_row=self.header_row,
            skip_rows=self.skip_rows,
            skip_footer=self.skip_footer,
        )

    @property
    def selects_sheet(self) -> bool:
        """True when a specific worksheet was named — meaningless off a workbook."""
        return bool(self.sheet) or self.sheet_index >= 0

    @property
    def text_encoding(self) -> str | None:
        """Declared codec, or ``None`` to let the reader sniff."""
        return self.encoding or None

    @property
    def field_delimiter(self) -> str | None:
        """Declared separator, or ``None`` to let the reader sniff."""
        return self.delimiter or None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_wire(self) -> dict[str, Any]:
        """Only the non-default fields — a plan diff should not be noise."""
        default = ReadOptions().to_dict()
        return {
            key: value
            for key, value in self.to_dict().items()
            if value != default[key]
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> "ReadOptions":
        """Build from an API payload, accepting the aliases a UI naturally sends."""
        if not data:
            return cls()
        if isinstance(data, cls):
            return data

        sheet_raw = data.get("sheet", data.get("sheet_name", ""))
        sheet = ""
        sheet_index = _as_int(data.get("sheet_index", -1), "sheet_index", default=-1)
        # A UI select can only carry one value, so a numeric ``sheet`` is a
        # position. Resolving it here keeps the ambiguity out of the readers.
        if isinstance(sheet_raw, bool):
            raise ReadOptionsError("sheet must be a name or a 0-based index")
        elif isinstance(sheet_raw, int):
            sheet_index = sheet_raw
        elif sheet_raw is not None:
            sheet = str(sheet_raw).strip()

        return cls(
            sheet=sheet,
            sheet_index=sheet_index,
            header_row=_as_int(data.get("header_row", 1), "header_row", default=1),
            skip_rows=_as_int(data.get("skip_rows", 0), "skip_rows", default=0),
            skip_footer=_as_int(data.get("skip_footer", 0), "skip_footer", default=0),
            encoding=str(data.get("encoding") or "").strip(),
            delimiter=_as_delimiter(data.get("delimiter")),
            fixed_width_layout=_as_fixed_width_layout(
                data.get("fixed_width_layout", data.get("colspecs"))
            ),
        )

    @property
    def options_hash(self) -> str:
        """Stable identity of this window, for the decision artifact and proof pack."""
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def describe(self) -> str:
        """One line an operator can read back in an audit trail."""
        parts: list[str] = []
        if self.sheet:
            parts.append(f"sheet '{self.sheet}'")
        elif self.sheet_index >= 0:
            parts.append(f"sheet #{self.sheet_index}")
        else:
            parts.append("active sheet")
        parts.append(
            f"header row {self.header_row}" if self.has_header else "no header (synthesized names)"
        )
        if self.skip_rows:
            parts.append(f"skip first {self.skip_rows} data row(s)")
        if self.skip_footer:
            parts.append(f"skip last {self.skip_footer} data row(s)")
        if self.encoding:
            parts.append(f"encoding {self.encoding}")
        if self.delimiter:
            parts.append(f"delimiter {self.delimiter!r}")
        if self.fixed_width_layout:
            parts.append(
                "fixed-width "
                + ",".join(f"{n}:{w}" for n, w in self.fixed_width_layout)
            )
        return ", ".join(parts)


def parse_read_options_payload(raw: Mapping[str, Any] | str | None) -> ReadOptions:
    """Build read options from a JSON body object or a multipart JSON string.

    One parser for both API shapes, so a form-encoded transfer and a JSON
    transfer cannot disagree about what the declared window means.
    """
    if raw is None or isinstance(raw, ReadOptions):
        return ReadOptions.from_dict(raw)
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return ReadOptions()
        try:
            parsed = json.loads(text)
        except ValueError as exc:
            raise ReadOptionsError(f"read_options is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ReadOptionsError("read_options must be a JSON object")
        return ReadOptions.from_dict(parsed)
    return ReadOptions.from_dict(raw)


# A UI select cannot carry a raw tab, and a JSON body escapes it inconsistently;
# accept the spellings an operator or a form actually sends.
_DELIMITER_WORDS = {
    "tab": "\t",
    "\\t": "\t",
    "comma": ",",
    "semicolon": ";",
    "pipe": "|",
}


def _as_delimiter(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ReadOptionsError(f"delimiter must be a single character, got {value!r}")
    if value in ("\t", " "):
        return value
    text = value.strip()
    if not text:
        return ""
    folded = _DELIMITER_WORDS.get(text.casefold())
    if folded is not None:
        return folded
    if len(text) != 1:
        raise ReadOptionsError(
            f"delimiter must be a single character, got {value!r}"
        )
    return text


def _as_fixed_width_layout(value: Any) -> tuple[tuple[str, int], ...]:
    if value is None or value == "" or value == ():
        return ()
    try:
        from services.fixed_width_layout import layout_from_payload
    except ImportError:
        from src.services.fixed_width_layout import layout_from_payload  # type: ignore
    try:
        return layout_from_payload(value)
    except ValueError as exc:
        raise ReadOptionsError(str(exc)) from exc


def _as_int(value: Any, field: str, *, default: int) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise ReadOptionsError(f"{field} must be an integer, got a boolean")
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ReadOptionsError(f"{field} must be an integer, got {value!r}") from exc
