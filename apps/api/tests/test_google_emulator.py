"""Fail-fast Google emulator retry — create-new must not sleep on 404/500."""

from __future__ import annotations

from connectors.google_emulator import (
    looks_like_google_emulator,
    google_emulator_retry,
    google_emulator_timeout,
)


def test_looks_like_google_emulator_localhost() -> None:
    assert looks_like_google_emulator(host="localhost", port=4443) is True
    assert looks_like_google_emulator(host="127.0.0.1", port=9050) is True
    assert looks_like_google_emulator(host="storage.googleapis.com", port=443) is False


def test_emulator_retry_never_retries() -> None:
    retry = google_emulator_retry()
    assert retry.deadline == 8.0
    assert retry._predicate(RuntimeError("404")) is False
    assert google_emulator_timeout() == 8.0
