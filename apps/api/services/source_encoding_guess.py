"""Make a guessed text encoding visible, and stop the guesses that cannot be right.

A CSV with no byte-order mark that is not valid UTF-8 declares nothing about
its encoding. DataFlow reads it as Windows-1252 (the code page Windows and
Excel write), which is exact for a Western export and silently wrong for a
Cyrillic, Greek or Central-European one: cp1251 'Привет' reads back as the
perfectly printable 'Ïðèâåò'. A guess the operator never sees is the silent
corruption QA rejects, so:

* the guess is reported - preflight carries one warning naming the encoding
  and a few decoded values, and the job records ``detected_encoding`` and
  ``encoding_guessed``;
* a guess the bytes contradict is refused, asking for the encoding to be
  declared: decoded C1 controls (U+0080-U+009F), bytes Windows-1252 leaves
  undefined (the latin-1 fallback), symbols from U+00A1-U+00BF between two
  letters (cp1250's 'ż'/'ł'/'ą' read as '¿'/'³'/'¹'), or text whose letters are
  mostly accented Latin-1 letters (another alphabet read through
  Windows-1252 - Western text is mostly ASCII letters with a few accents);
* a declared encoding (the source read option) is never a guess: no warning,
  and the decode is strict.
"""

from __future__ import annotations

import logging
import os
from typing import Any

GATE_ID = "g1_source_encoding"
_PREFIX = 65536
_GUESS_ENCODINGS = {"cp1252", "latin-1"}
#: Letters in U+00C0..U+00FF: share of all letters above which the text is
#: another alphabet read through Windows-1252, and the count that makes the
#: share meaningful.
_HIGH_LETTER_SHARE = 0.4
_HIGH_LETTER_MIN = 8


def assess_text_encoding(prefix: bytes, declared: str = "") -> dict[str, Any]:
    """What encoding the bytes were read as, whether it was a guess, and if it holds."""
    from services.csv_profiler import detect_encoding

    declared = (declared or "").strip()
    if declared:
        return {"detected_encoding": declared, "encoding_guessed": False,
                "declared": True, "implausible": False, "reason": "", "samples": []}
    detected = detect_encoding(prefix)
    guessed = detected in _GUESS_ENCODINGS
    report: dict[str, Any] = {"detected_encoding": detected, "encoding_guessed": guessed,
                              "declared": False, "implausible": False, "reason": "",
                              "samples": []}
    if not guessed:
        return report
    text = prefix.decode(detected, errors="replace")
    report["samples"] = _samples(text)
    report["reason"] = _implausible_reason(text, detected)
    report["implausible"] = bool(report["reason"])
    return report


def assess_file_encoding(path_or_bytes: Any, declared: str = "") -> dict[str, Any] | None:
    """:func:`assess_text_encoding` for a CSV path or payload; None when unreadable."""
    try:
        if isinstance(path_or_bytes, (bytes, bytearray)):
            prefix = bytes(path_or_bytes[:_PREFIX])
        elif path_or_bytes and os.path.isfile(str(path_or_bytes)):
            from src.transfer.file_stream import _first_bytes

            prefix = _first_bytes(str(path_or_bytes), _PREFIX)
        else:
            return None
    except Exception:  # noqa: BLE001 - an unreadable file is G1's finding, not this gate's
        return None
    return assess_text_encoding(prefix, declared)


def _implausible_reason(text: str, detected: str) -> str:
    if detected == "latin-1":
        return ("it has bytes Windows-1252 does not define (0x81, 0x8D, 0x8F, 0x90, "
                "0x9D), so it is not a Windows-1252 file")
    c1 = sorted({f"U+{ord(ch):04X}" for ch in text if 0x80 <= ord(ch) <= 0x9F})
    if c1:
        return f"it decodes to control characters ({', '.join(c1[:5])})"
    inside = sorted({text[i] for i in range(1, len(text) - 1)
                     if 0xA1 <= ord(text[i]) <= 0xBF
                     and text[i - 1].isalpha() and text[i + 1].isalpha()})
    if inside:
        # '¿', '³', '¹' between two letters: Central-European letters (cp1250
        # 'ż', 'ł', 'ą') read through Windows-1252, never Western spelling.
        return ("it decodes to symbols inside words ("
                + ", ".join(repr(ch) for ch in inside[:5])
                + "), the letters of another code page read as Windows-1252")
    letters = [ch for ch in text if ch.isalpha()]
    high = [ch for ch in letters if 0xC0 <= ord(ch) <= 0xFF]
    if len(high) >= _HIGH_LETTER_MIN and len(high) / max(len(letters), 1) > _HIGH_LETTER_SHARE:
        return (f"{len(high)} of its {len(letters)} letters decode to accented Latin "
                "letters, which is another alphabet (Cyrillic, Greek, Central "
                "European...) read through the wrong code page")
    return ""


def _samples(text: str, limit: int = 5) -> list[str]:
    """Decoded values that carry non-ASCII text: what the operator should check."""
    out: list[str] = []
    for line in text.splitlines()[1:]:
        for cell in line.split(","):
            cell = cell.strip().strip('"')
            if cell and not cell.isascii() and cell not in out:
                out.append(cell[:60])
                if len(out) >= limit:
                    return out
    return out


def encoding_gate(report: dict[str, Any] | None) -> dict[str, Any] | None:
    """Preflight gate for a guessed encoding: warn when it holds, block when not."""
    if not report or not report.get("encoding_guessed"):
        return None
    shown = ", ".join(repr(s) for s in report.get("samples") or []) or "no non-ASCII values"
    if report.get("implausible"):
        return {
            "id": GATE_ID, "status": "block", "duration_ms": 0,
            "message": (
                "The file declares no encoding and is not UTF-8. Read as "
                f"Windows-1252 it cannot be right: {report['reason']} "
                f"(decoded: {shown}). Declare the file's encoding (source read "
                "option 'encoding', e.g. cp1251, cp1250, iso-8859-7) and re-run."
            ),
            "details": dict(report),
        }
    return {
        "id": GATE_ID, "status": "warn", "duration_ms": 0,
        "message": (
            "The file declares no encoding and is not UTF-8; it was read as "
            f"Windows-1252 ({report.get('detected_encoding')}). Check these decoded "
            f"values: {shown}. If they are wrong, declare the encoding (source read "
            "option 'encoding')."
        ),
        "details": dict(report),
    }


def apply_encoding_gate(out: dict[str, Any], report: dict[str, Any] | None) -> None:
    """Add the gate (and, when it blocks, the blocker) to a preflight result."""
    gate = encoding_gate(report)
    if report:
        out["source_encoding"] = dict(report)
    if gate is None:
        return
    out["gates"] = [g for g in out.get("gates") or [] if g.get("id") != GATE_ID] + [gate]
    if gate["status"] == "block":
        out["blockers"] = [*(out.get("blockers") or []),
                           {"id": GATE_ID, "message": gate["message"],
                            "details": gate["details"]}]
        out["passed"] = False
    else:
        out["warnings"] = [*(out.get("warnings") or []), gate["message"]]


def preflight_file_encoding(source_format: str, source_config: Any) -> dict[str, Any] | None:
    """The encoding report for a CSV/TSV source preflight can read by path."""
    if str(source_format or "").strip().lower() not in {"csv", "tsv"}:
        return None
    if not isinstance(source_config, dict):
        return None
    path = str(source_config.get("path") or source_config.get("source_path") or "")
    if not path:
        return None
    declared = str(source_config.get("encoding") or "")
    return assess_file_encoding(path, declared)


def record_file_encoding(
    mongo: Any, job_id: str, content: Any, filename: str, read_options: Any
) -> dict[str, Any] | None:
    """Assess the encoding Execute reads a CSV/TSV with; stamp it on the job."""
    from services.file_parser import FileParser

    try:
        from src.transfer.file_stream import _first_bytes

        prefix = _first_bytes(content, _PREFIX)
    except Exception:  # noqa: BLE001 - unreadable content fails the read itself
        return None
    if FileParser.detect_file_type(filename or "", prefix or None) not in {"csv", "tsv"}:
        return None
    report = assess_text_encoding(prefix, str(getattr(read_options, "encoding", "") or ""))
    try:
        mongo.update_job_fields(job_id, {
            "detected_encoding": report["detected_encoding"],
            "encoding_guessed": bool(report["encoding_guessed"]),
            "source_encoding": dict(report),
        })
    except Exception:  # noqa: BLE001 - bookkeeping must not fail the run
        logging.getLogger(__name__).debug("encoding stamp on job failed", exc_info=True)
    return report
