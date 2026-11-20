"""What SQL Server's ``cdc.ddl_history`` says changed since the last baseline (D392).

``cdc.ddl_history`` keeps one row per DDL statement on a CDC-enabled table for
as long as the capture instance exists, so "the history is not empty" is not
"there is new DDL". A row is new when its ``ddl_lsn`` is past the ``ddl_lsn``
stored with the last schema snapshot. ``ALTER TABLE ... ADD`` does not change
``cdc.captured_columns`` of the existing capture instance, so the DDL text is
the only place the added column is named.

Docs: https://learn.microsoft.com/sql/relational-databases/system-tables/cdc-ddl-history-transact-sql
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: Words that follow ``ADD`` without naming a column.
_NOT_A_COLUMN = frozenset({
    "CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "DEFAULT", "INDEX",
    "PERIOD", "COLUMN_SET", "FOR",
})
_IDENT = r'(\[[^\]]+\]|"[^"]+"|[A-Za-z_@#][\w@#$]*)'


@dataclass(frozen=True)
class DdlHistory:
    """Rows newest first; ``readable`` is False when the catalog refused the read."""

    rows: tuple[tuple[str, str], ...] = ()
    readable: bool = True

    @property
    def latest_lsn(self) -> str:
        return max((lsn for _cmd, lsn in self.rows), default="")

    def commands_after(self, watermark: str) -> list[str]:
        """DDL recorded past ``watermark`` (hex ``ddl_lsn``), oldest first."""
        return [cmd for cmd, lsn in sorted(self.rows, key=lambda r: r[1]) if lsn > watermark]


def _lsn_hex(value: Any) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    text = str(value or "").strip().lower()
    return text[2:] if text.startswith("0x") else text


def read_ddl_history(cur: Any, capture_instance: str) -> DdlHistory:
    """This capture instance's ``cdc.ddl_history`` rows as (command, lsn hex)."""
    try:
        cur.execute(
            """
            SELECT h.ddl_command, h.ddl_lsn
            FROM cdc.ddl_history h
            JOIN cdc.change_tables ct ON ct.object_id = h.object_id
            WHERE ct.capture_instance = %s
            ORDER BY h.ddl_lsn DESC
            """,
            (capture_instance,),
        )
        rows = tuple(
            (str(row[0]), _lsn_hex(row[1]))
            for row in (cur.fetchall() or [])
            if row and row[0]
        )
    except Exception as exc:  # noqa: BLE001 - classified: unreadable, not "no DDL"
        logger.warning(
            "SQL Server CDC could not read cdc.ddl_history for %s; DDL since the "
            "last baseline is checked again on the next run: %s",
            capture_instance,
            exc,
        )
        return DdlHistory(readable=False)
    return DdlHistory(rows=rows)


def _unquote(name: str) -> str:
    name = name.strip()
    if len(name) >= 2 and name[0] in '["' and name[-1] in ']"':
        return name[1:-1]
    return name


def _split_top_level(text: str) -> list[str]:
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(text):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return parts


def columns_in_ddl(commands: list[str]) -> list[str]:
    """Column names an ``ALTER TABLE`` adds, drops or alters, in order, once each."""
    found: list[str] = []

    def _take(name: str) -> None:
        clean = _unquote(name)
        if clean and clean.upper() not in _NOT_A_COLUMN and clean not in found:
            found.append(clean)

    for command in commands or []:
        text = str(command or "")
        for match in re.finditer(r"\bALTER\s+COLUMN\s+" + _IDENT, text, re.IGNORECASE):
            _take(match.group(1))
        drop = re.search(r"\bDROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?(.*)$", text, re.IGNORECASE | re.DOTALL)
        if drop:
            for part in _split_top_level(drop.group(1)):
                head = re.match(r"\s*" + _IDENT, part)
                if head:
                    _take(head.group(1))
        add = re.search(r"\bADD\s+(?!CONSTRAINT\b)(.*)$", text, re.IGNORECASE | re.DOTALL)
        if add and not re.match(r"\s*COLUMN\s*$", add.group(1), re.IGNORECASE):
            for part in _split_top_level(add.group(1)):
                head = re.match(r"\s*(?:COLUMN\s+)?" + _IDENT, part, re.IGNORECASE)
                if head:
                    _take(head.group(1))
    return found
