"""A refusal's envelope names its real cause and the action that helps (D424).

Every 4xx used to render as ``platform.request_refused`` / "Request refused" /
"Check the values on the form, then try again." / ``retriable: false``, with
the cause only in ``detail.error``: a plan that timed out in preflight, a DNS
refusal, a Resume refused because the checkpoint is not safe (where Retry is
the remedy) and a tool error from ``/mcp/tools/call`` all told the operator to
re-edit a form that was already valid.

:func:`refusal_message` reads the cause out of a dict ``detail`` (``message``,
``error``, ``detail``, ``reason`` and any ``reasons``); :func:`refusal_envelope_fields`
picks the envelope's code from it: a known driver/connection cause by the
error taxonomy, a timeout (retriable), a 409 whose remedy is Retry, any other
409 (state, not input), else the generic refusal.
"""

from __future__ import annotations

import re
from typing import Any

_TEXT_KEYS = ("message", "error", "detail", "reason")
_TIMED_OUT = re.compile(
    r"did not (?:finish|answer) within|timed out|took longer than|deadline exceeded|\btime limit\b", re.I
)
_USE_RETRY = re.compile(r"\buse retry\b|\bretry from (?:the )?start\b", re.I)


def refusal_message(raw: Any, status: int) -> str:
    """The cause text of an HTTP error ``detail`` (str, or a dict carrying one)."""
    if isinstance(raw, str) and raw.strip():
        return raw
    if isinstance(raw, dict):
        texts = [str(raw.get(k)).strip() for k in _TEXT_KEYS if isinstance(raw.get(k), str) and raw.get(k).strip()]
        # A sentence beats a code-like token ("error": "data_residency").
        text = next((t for t in texts if " " in t), texts[0] if texts else str(raw.get("code") or ""))
        reasons = raw.get("reasons")
        if isinstance(reasons, list):
            extra = [str(r).strip() for r in reasons if isinstance(r, str) and r.strip() and str(r).strip() not in text]
            if extra:
                text = f"{text.rstrip('.')}: {'; '.join(extra)}" if text else "; ".join(extra)
        if text:
            return text
    return "Request refused" if status < 500 else "Something went wrong"


def refusal_envelope_fields(status: int, message: str) -> dict[str, Any]:
    """``code``, ``title``, ``action`` and ``retriable`` for a 4xx (not 404) refusal of ``message``."""
    from services.error_taxonomy import TEMPLATES, classify

    text = str(message or "")
    if status not in (401, 403):
        found = classify(text, fallback=False)
        if found is not None and found.code in TEMPLATES and found.code != "unclassified":
            return {"code": found.code, "title": found.title, "action": dict(found.action),
                    "retriable": found.retriable}
    if _TIMED_OUT.search(text):
        code = "platform.step_timed_out"
    elif status == 409:
        code = "platform.use_retry" if _USE_RETRY.search(text) else "platform.state_conflict"
    else:
        code = "platform.request_refused"
    tpl = TEMPLATES[code]
    return {"code": code, "title": tpl.title, "action": {"label": tpl.action, "ui": tpl.ui, "api": tpl.api},
            "retriable": tpl.retriable}
