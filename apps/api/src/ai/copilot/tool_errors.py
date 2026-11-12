"""Operator-safe text for a failed Pilot tool, for every surface that shows one.

A tool error reaches the operator three ways: the chat answer, the
``tools_used[].summary`` in the chat response, and the MCP tool result. The
MCP path masked it; the chat path copied ``str(exc)`` verbatim, so a driver
error carrying ``mongodb://user:password@internal-host`` was printed in the
chat (audit H6). Both now pass through ``DataPilotTools.execute``:

* an error a tool *reports* keeps its sentence, with URI credentials and
  secret query values masked by the same ``mask_secrets_in_text`` MCP uses;
* an exception a tool did *not* handle is a defect, so its text is not shown
  at all — the operator gets a safe sentence and a reference
  (``services.safe_errors.public_error``), and the raw error goes to the log.
"""

from __future__ import annotations

import logging

from services.safe_errors import public_error
from services.secret_config import mask_secrets_in_text

_logger = logging.getLogger(__name__)


def safe_tool_error(text: object) -> str:
    """A tool-reported error with embedded credentials masked."""
    return mask_secrets_in_text(str(text or ""))


def unhandled_tool_error(tool: str, exc: BaseException) -> str:
    """The sentence shown for an exception a tool let escape."""
    detail, _reference = public_error(
        exc,
        event="pilot.tool.exception",
        public_message=(
            f"I hit a server-side error ({type(exc).__name__}) and could not complete that. "
            "Nothing was read or changed by this step; the detail is in the server log."
        ),
        status_code=500,
        logger=_logger,
        tool=tool,
    )
    return safe_tool_error(detail)
