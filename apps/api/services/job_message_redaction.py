"""Failed-job text never carries statements, bound values, row images or driver dumps (D405, R11).

Every writer of a job's operator-facing text (the engine's failure handler,
the background runner, verification polls, resume, replay, the exit guard,
notifications, lineage) used to put ``str(exc)`` on the record. A SQLAlchemy
error carries the whole statement and every bound parameter of the failed
batch (customer rows); pymssql carries its DB-Lib dump, including the host it
dialled; PostgreSQL names the failing row; a MongoDB bulk error repeats the
document it refused. Workspace viewers, the audit trail and e-mails read
that text.

Fixing each writer would miss the next one, so the job store applies this at
its write sink (``update_job_status`` / ``update_job_fields`` on both
backends) and the notification and lineage builders apply it too. Only the
operator-facing text keys are touched (``error``, ``message``, ``raw``, any
``*_error`` / ``*_message``, and strings in their lists); quarantined row
payloads (``row``, ``record``, ``document`` …) are stored data, not messages,
and are left alone whatever their columns are called.

What is kept: the classified cause, driver error codes, column and key names
and a key value named by a duplicate-key message (the "key range" the
operator needs). What is withheld: the SQL text, its parameters, a failing
row image, a refused document and the raw DB-Lib dump. The original text
goes to the server log (statement shape and dump kept; parameters, row
images and the statement's quoted literals withheld)
under a reference that the stored message names.
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any

logger = logging.getLogger("datawrap.restricted.job_errors")

#: Operator-facing text fields, at any nesting depth of a job update.
TEXT_KEYS = frozenset({
    "error", "message", "operator_error", "raw", "error_message", "reason",
    "detail", "note", "verification_message", "failure", "last_error",
})
#: Any key that names operator text by suffix (``conservation_error``) or as a list.
_TEXT_SUFFIXES = ("_error", "_errors", "_message", "_messages")
_TEXT_LISTS = frozenset({"errors", "messages", "warnings", "reasons"})
#: Stored row payloads (a quarantined row, a refused document): data, never
#: messages, whatever their columns are called (an error-log table has an
#: ``error`` column). Their subtrees are not rewritten.
_PAYLOAD_KEYS = frozenset({
    "row", "rows", "record", "records", "document", "documents", "row_data", "sample_rows",
})
_MAX_DEPTH = 5
_TRIGGERS = (
    "[SQL:", "[parameters:", "DB-Lib error", "Failing row contains",
    "sqlalche.me", "'op': {", '"op": {',
)
_OPENERS = {"[": "]", "(": ")", "{": "}"}
_CLOSERS = frozenset(_OPENERS.values())
_BACKGROUND = re.compile(r"\s*\(Background on this error at: [^)]*\)")
_PYMSSQL = re.compile(r"\((\d{2,6}), b(['\"])(.*?)(?<!\\)\2\)", re.S)
_DBLIB_HEADER = re.compile(r"DB-Lib error message \d+, severity \d+:", re.S)
_DBLIB_TAIL = re.compile(r"\s*DB-Lib error message \d+, severity \d+:.*", re.S)
_TRAILING_HOST = re.compile(r"\s*\([^()\s]*\)\s*$")
_REFERENCE = re.compile(r"\bReference [0-9a-zA-Z_-]{6,}")
#: A quoted SQL literal ('' and backslash escapes inside) in a logged statement.
_SQL_LITERAL = re.compile(r"'(?:[^'\\]|\\.|'')*'?")


def _balanced_end(text: str, start: int) -> int:
    """Index just past the bracket group opening at ``text[start]``, or -1.

    Quoted runs (``'…'`` / ``"…"`` with backslash escapes, as Python reprs and
    SQL literals write them) are skipped, so a bracket inside a value does not
    end the group early.
    """
    depth = 0
    quote = ""
    i = start
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                quote = ""
        elif ch in ("'", '"'):
            quote = ch
        elif ch in _OPENERS:
            depth += 1
        elif ch in _CLOSERS:
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return -1


def _withhold_group(text: str, marker: str, replacement: str, *, stop: tuple[str, ...] = ()) -> str:
    """Replace each ``marker`` group (``marker`` starts at its opening bracket)."""
    out: list[str] = []
    pos = 0
    while True:
        at = text.find(marker, pos)
        if at < 0:
            out.append(text[pos:])
            return "".join(out)
        out.append(text[pos:at])
        end = _balanced_end(text, at)
        if end < 0:
            # Truncated or unbalanced: withhold up to the next known section.
            cut = [text.find(s, at + len(marker)) for s in stop]
            cut = [c for c in cut if c > at]
            end = min(cut) if cut else len(text)
        out.append(replacement)
        pos = end


def _pymssql_cause(match: re.Match[str]) -> str:
    code, body = match.group(1), match.group(3).replace("\\n", "\n")
    head, _, rest = body.partition("DB-Lib error message")
    cause = head.strip()
    if not cause and rest:
        lines = [ln.strip() for ln in _DBLIB_HEADER.sub("", "DB-Lib error message" + rest).splitlines()]
        cause = next((ln for ln in lines if ln), "")
    cause = _TRAILING_HOST.sub("", cause).strip().rstrip(".")
    return f"SQL Server error {code}: {cause}." if cause else f"SQL Server error {code}."


def _needs_redaction(text: str) -> bool:
    return any(token in text for token in _TRIGGERS)


def redact_driver_text(text: str) -> str:
    """``text`` without SQL, bound values, row images or DB-Lib dumps."""
    if not isinstance(text, str) or not _needs_redaction(text):
        return text
    out = _withhold_group(text, "[parameters:", "[parameters: withheld]", stop=("(Background",))
    out = _withhold_group(out, "[SQL:", "[SQL: withheld]", stop=("[parameters:", "(Background"))
    out = _BACKGROUND.sub("", out)
    out = _PYMSSQL.sub(_pymssql_cause, out)
    out = _DBLIB_TAIL.sub("", out)
    out = _withhold_row_images(out)
    return out.strip()


def _withhold_row_images(text: str) -> str:
    out = text
    for marker in ("Failing row contains (",):
        out = _withhold_after(out, marker, "Failing row contains (withheld)")
    for marker in ("'op': {", '"op": {'):
        out = _withhold_after(out, marker, marker[:-1] + "{withheld}")
    return out


def _withhold_after(text: str, marker: str, replacement: str) -> str:
    """Like :func:`_withhold_group` for a marker whose group opens at its last char."""
    out: list[str] = []
    pos = 0
    while True:
        at = text.find(marker, pos)
        if at < 0:
            out.append(text[pos:])
            return "".join(out)
        out.append(text[pos:at])
        end = _balanced_end(text, at + len(marker) - 1)
        out.append(replacement)
        pos = len(text) if end < 0 else end


def _for_log(text: str) -> str:
    """The restricted log copy: statement shape and dump kept, row values withheld.

    Built SQL inlines values (``WHERE email IN ('ada@...')``), so the quoted
    literals of a logged statement are withheld too (review m2).
    """
    out = _withhold_group(text, "[parameters:", "[parameters: withheld]", stop=("(Background",))
    return _withhold_sql_literals(_withhold_row_images(out))[:4000]


def _withhold_sql_literals(text: str) -> str:
    out: list[str] = []
    pos = 0
    while (at := text.find("[SQL:", pos)) >= 0:
        end = _balanced_end(text, at)
        end = len(text) if end < 0 else end
        out.append(text[pos:at])
        out.append(_SQL_LITERAL.sub("'?'", text[at:end]))
        pos = end
    out.append(text[pos:])
    return "".join(out)


def _is_text_key(field: str) -> bool:
    return field in TEXT_KEYS or field in _TEXT_LISTS or field.endswith(_TEXT_SUFFIXES)


def _redact(value: Any, depth: int, ref: list[str], field: str) -> Any:
    if isinstance(value, str):
        if not _is_text_key(field) or not _needs_redaction(value):
            return value
        if not ref:
            ref.append(uuid.uuid4().hex[:12])
        logger.warning("job text withheld reference=%s field=%s: %s", ref[0], field, _for_log(value))
        clean = redact_driver_text(value)
        if not _REFERENCE.search(clean):
            clean = f"{clean.rstrip()} Reference {ref[0]}."
        return clean
    if depth >= _MAX_DEPTH:
        return value
    if isinstance(value, dict):
        out = None
        for key, item in value.items():
            name = str(key).rsplit(".", 1)[-1]
            if name in _PAYLOAD_KEYS:
                continue
            new = _redact(item, depth + 1, ref, name)
            if new is not item:
                out = dict(value) if out is None else out
                out[key] = new
        return value if out is None else out
    if isinstance(value, list):
        items = None
        for i, item in enumerate(value):
            if not isinstance(item, (dict, list, str)):
                continue
            # A string in a list is judged by the list's own key (``errors``).
            new = _redact(item, depth + 1, ref, field)
            if new is not item:
                items = list(value) if items is None else items
                items[i] = new
        return value if items is None else items
    return value


def redact_job_update(fields: dict[str, Any]) -> dict[str, Any]:
    """A job update (status kwargs or a field patch) safe to store and show.

    Dotted keys (``"verification.message"``) are judged by their last segment.
    Returns ``fields`` itself when nothing needed withholding.
    """
    if not isinstance(fields, dict) or not fields:
        return fields
    ref: list[str] = []
    return _redact(fields, 0, ref, "")


__all__ = ["TEXT_KEYS", "redact_driver_text", "redact_job_update"]
