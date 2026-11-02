"""Wait for the serialized mail lane to deliver into ``ConsoleEmailSender.OUTBOX``.

Mail delivery is async (``signup_service._MAIL_LANE``); tests that read
``ConsoleEmailSender.OUTBOX`` right after an HTTP 202 race the lane. Poll
instead of asserting at t=0, and filter by recipient so stray timer-fired
retry mails from earlier tests can't corrupt positional asserts.
"""
from __future__ import annotations

import time

from services import mail_outbox
from services.email_sender import ConsoleEmailSender


def drain_mail_lane(*, timeout: float = 10.0) -> None:
    """Poll the outbox until no row is ``queued`` or ``sending`` — i.e. the
    serialized lane has finished every delivery the test just enqueued.
    Use before positional ``OUTBOX[-1]`` asserts, which race the async lane."""
    deadline = time.monotonic() + timeout
    while True:
        c = mail_outbox.counts()
        if c.get("queued", 0) == 0 and c.get("sending", 0) == 0:
            return
        if time.monotonic() >= deadline:
            return
        time.sleep(0.02)


def await_outbox(
        to: str | None = None, *, min_count: int = 1,
        timeout: float = 5.0) -> list:
    """Poll until at least ``min_count`` captured mails (optionally addressed
    to ``to``) exist, then return all matches. A shorter list means the mails
    never arrived — the assert that follows should fail."""
    deadline = time.monotonic() + timeout
    while True:
        hits = [m for m in ConsoleEmailSender.OUTBOX if to is None or m.to == to]
        if len(hits) >= min_count or time.monotonic() >= deadline:
            return hits
        time.sleep(0.02)
