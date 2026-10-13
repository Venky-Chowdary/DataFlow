"""Canonical safe error details for public API boundaries."""

from __future__ import annotations

import logging
import os
import re
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
    return HTTPException(
        status_code=status_code,
        detail=detail,
        headers={"X-Correlation-ID": cid},
    )


def user_query_error(
    exc: BaseException,
    *,
    public_message: str,
    status_code: int = 400,
    event: str = "query.user_query.failed",
    logger: logging.Logger | None = None,
) -> HTTPException:
    """Expose errors from the caller's own query text after canonical redaction.

    Use only when the caller already has query permission on the connector.
    Never use this for connection or configuration errors.
    """
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
    # SQLAlchemy appends the statement, the caret, and a documentation link.
    # The operator needs the driver's sentence (QA: raw errors echo the SQL).
    sentence = re.split(
        r"(?:\n\s*)?(?:LINE \d+:|\[SQL:|\(Background on this error)",
        str(exc),
        maxsplit=1,
    )[0]
    detail = f"{public_message}: {redact_log_text(sentence)[:500]} Reference: {cid}"
    return HTTPException(
        status_code=status_code,
        detail=detail,
        headers={"X-Correlation-ID": cid},
    )
