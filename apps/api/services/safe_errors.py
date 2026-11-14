"""Canonical safe error details for public API boundaries."""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from fastapi import HTTPException
from services import tracing
from services.log_redaction import redact_log_text
from services.ops_events import emit_event

_LOG = logging.getLogger("dataflow.errors")
EXPOSE_ENV = "DATAFLOW_EXPOSE_ERROR_DETAIL"


def expose_error_detail() -> bool:
    return os.getenv(EXPOSE_ENV, "").strip() == "1"


def public_error(
    exc: BaseException,
    *,
    event: str,
    public_message: str,
    status_code: int = 500,
    logger: logging.Logger | None = None,
    **safe_fields: Any,
) -> tuple[str, str]:
    cid = tracing.get_correlation_id() or uuid.uuid4().hex
    emit_event(
        logger or _LOG,
        event,
        outcome="errored",
        level=logging.ERROR if status_code >= 500 else logging.WARNING,
        error=exc,
        correlation_id=cid,
        http_status=status_code,
        **safe_fields,
    )
    detail = f"{public_message} Reference: {cid}"
    if expose_error_detail():
        detail += f" Detail: {redact_log_text(str(exc))[:500]}"
    return detail, cid


class EnvelopeHTTPException(HTTPException):
    """An HTTP error that renders the error envelope.

    ``{"error": {code, message, action{label, ui, api}, object, retriable,
    reference}, "detail": "<message> Reference: <cid>"}``. ``detail`` stays a
    string for every client written against ``{detail}``; the app-level
    handler in ``src.main`` adds ``error``. Without that handler (a bare
    FastAPI app in a test) it renders as a plain ``HTTPException``.
    """

    def __init__(self, status_code: int, envelope: dict[str, Any], headers: dict[str, str] | None = None) -> None:
        super().__init__(status_code=status_code, detail=envelope.get("detail"), headers=headers)
        self.envelope = envelope


def _platform_envelope(public_message: str, cid: str, detail: str, status_code: int) -> dict[str, Any]:
    from services.error_taxonomy import TEMPLATES

    code = "platform.gateway_timeout" if status_code == 504 else "platform.internal"
    tpl = TEMPLATES[code]
    return {
        "error": {
            "code": code,
            "title": tpl.title,
            "message": public_message,
            "action": {"label": tpl.action, "ui": tpl.ui, "api": tpl.api},
            "object": {},
            "retriable": tpl.retriable,
            "reference": cid,
        },
        "detail": detail,
    }


def safe_http_exception(
    exc: BaseException,
    *,
    event: str,
    public_message: str,
    status_code: int = 500,
    logger: logging.Logger | None = None,
    **safe_fields: Any,
) -> HTTPException:
    detail, cid = public_error(
        exc,
        event=event,
        public_message=public_message,
        status_code=status_code,
        logger=logger,
        **safe_fields,
    )
    return EnvelopeHTTPException(
        status_code,
        _platform_envelope(public_message, cid, detail, status_code),
        headers={"X-Correlation-ID": cid},
    )


def classified_http_exception(
    exc: BaseException,
    *,
    side: str,
    phase: str,
    engine: str = "",
    object: dict[str, Any] | None = None,  # noqa: A002 — the envelope's field name
    status_code: int = 400,
    event: str,
    logger: logging.Logger | None = None,
    unclassified_prefix: str = "",
) -> HTTPException:
    """A driver failure as the taxonomy's message, one action and a reference.

    Driver text reaches the user only as ``driver_sentence`` inside the
    ``unclassified`` copy. The raw exception goes to the log event.
    """
    from services.error_taxonomy import classify, driver_sentence

    cid = tracing.get_correlation_id() or uuid.uuid4().hex
    emit_event(
        logger or _LOG,
        event,
        outcome="failed",
        level=logging.WARNING,
        error=exc,
        correlation_id=cid,
        http_status=status_code,
    )
    classified = classify(exc, side=side, phase=phase, engine=engine, object=object)
    assert classified is not None
    envelope = classified.envelope(cid)
    if classified.code == "unclassified" and unclassified_prefix:
        sentence = driver_sentence(exc, limit=100_000)[:500]
        envelope["detail"] = f"{unclassified_prefix}: {sentence} Reference: {cid}"
    return EnvelopeHTTPException(status_code, envelope, headers={"X-Correlation-ID": cid})


def user_query_error(
    exc: BaseException,
    *,
    public_message: str,
    status_code: int = 400,
    event: str = "query.user_query.failed",
    logger: logging.Logger | None = None,
    engine: str = "",
) -> HTTPException:
    """A Query-page failure, classified like every other surface.

    A connection failure (a rejected password, a refused port, an unknown
    database) reads exactly as it does on Test (ERR-11, ERR-20); a query error
    names its cause and the next action. Only a failure the taxonomy cannot
    name shows the driver's own sentence — never SQL, a caret line, a driver
    class or a sqlalche.me link (UX-37).
    """
    return classified_http_exception(
        exc,
        side="query",
        phase="query",
        engine=engine,
        status_code=status_code,
        event=event,
        logger=logger,
        unclassified_prefix=public_message,
    )


def coded_http_exception(
    code: str,
    *,
    status_code: int = 400,
    object: dict[str, Any] | None = None,  # noqa: A002 — the envelope's field name
    side: str = "",
) -> HTTPException:
    """A refusal Datawrap itself decides, worded by the taxonomy's template table."""
    from services.error_taxonomy import DatawrapError, classify

    cid = tracing.get_correlation_id() or uuid.uuid4().hex
    classified = classify(DatawrapError(code, object=object), side=side)
    assert classified is not None
    return EnvelopeHTTPException(status_code, classified.envelope(cid), headers={"X-Correlation-ID": cid})


def connection_http_exception(
    driver: str,
    exc: BaseException,
    *,
    status_code: int = 400,
    side: str = "connection",
    phase: str = "connect",
    auth_mode: str = "",
    event: str = "connector.connection.failed",
    prefix: str = "",
) -> HTTPException:
    """A connection failure in Test's words (``humanize_connection_error``), with a reference.

    ERR-11 / ERR-21: the Query page and the schema browser no longer show a
    driver's install hint or raw text for a failed connect.
    """
    from services.error_taxonomy import classify
    from src.transfer.connector_registry import humanize_connection_error

    cid = tracing.get_correlation_id() or uuid.uuid4().hex
    emit_event(_LOG, event, outcome="failed", level=logging.WARNING, error=exc,
               correlation_id=cid, http_status=status_code)
    message = humanize_connection_error(driver, exc, auth_mode=auth_mode, side=side, phase=phase)
    classified = classify(exc, side=side, phase=phase, engine=driver)
    assert classified is not None
    envelope = classified.envelope(cid)
    text = f"{prefix}{message}" if prefix else str(message)
    envelope["detail"] = f"{text} Reference: {cid}"
    if str(message) != classified.text:
        envelope["error"]["message"] = str(message)
        envelope["error"]["action"] = {"label": "", "ui": "open_connection", "api": ""}
    return EnvelopeHTTPException(status_code, envelope, headers={"X-Correlation-ID": cid})


def failure_fields(message: Any, *, engine: str = "", side: str = "connection", phase: str = "connect") -> dict[str, Any]:
    """``{error: envelope, reference}`` for a ``success: false`` body (ERR-40, UX-7).

    ``message`` is the copy already shown (Test's ``humanize_connection_error``);
    the envelope adds its code, one action and the support reference, so the
    page can show the reference as its own chip.
    """
    from services.error_taxonomy import classify

    cid = tracing.get_correlation_id() or uuid.uuid4().hex
    code = str(getattr(message, "code", "") or "")
    classified = classify(str(message or ""), side=side, phase=phase, engine=engine, fallback=False)
    envelope = classified.envelope(cid)["error"] if classified is not None else {
        "code": code or "unclassified", "message": str(message or ""), "action": None,
        "object": {}, "retriable": False, "reference": cid,
    }
    envelope["message"] = str(message or envelope.get("message") or "")
    return {"error": envelope, "reference": cid}


def run_failure_http_exception(
    exc: BaseException,
    *,
    status_code: int = 422,
    event: str,
    side: str = "",
    phase: str = "",
) -> HTTPException:
    """A failed run answered over HTTP in the job record's words (``humanize_transfer_failure``)."""
    from services.error_handling import humanize_transfer_failure

    cid = tracing.get_correlation_id() or uuid.uuid4().hex
    emit_event(_LOG, event, outcome="failed", level=logging.WARNING, error=exc,
               correlation_id=cid, http_status=status_code)
    human = humanize_transfer_failure(exc, side=side, phase=phase)
    text = " ".join(p for p in (str(human.get("message") or ""), str(human.get("fix") or "")) if p)
    envelope = {
        "error": {
            "code": human.get("code"), "title": human.get("title"), "message": human.get("message"),
            "action": human.get("action") or {"label": human.get("fix") or "", "ui": "", "api": ""},
            "object": human.get("object") or {}, "retriable": bool(human.get("retriable")),
            "reference": cid,
        },
        "detail": f"{text} Reference: {cid}",
    }
    if human.get("legacy_code"):
        envelope["error"]["legacy_code"] = human["legacy_code"]  # deprecated, one release
    return EnvelopeHTTPException(status_code, envelope, headers={"X-Correlation-ID": cid})
