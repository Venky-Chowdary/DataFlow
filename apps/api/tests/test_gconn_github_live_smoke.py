from __future__ import annotations

import os
from collections.abc import Mapping

import pytest
import requests

from connectors.sdk import RecordBatch
from connectors.sdk.github import GitHubSource


@pytest.mark.live
def test_github_live_smoke_reads_one_manifest_page_or_skips_with_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        pytest.skip("GITHUB_TOKEN is not set")

    try:
        probe = requests.get("https://api.github.com", timeout=2)
    except requests.RequestException as exc:
        pytest.skip(f"api.github.com two-second probe failed ({type(exc).__name__})")
    if not probe.ok:
        pytest.skip(f"api.github.com two-second probe returned HTTP {probe.status_code}")

    source = GitHubSource({"access_token": token})
    try:
        batches = list(source.read("repositories", limit=1))
    except Exception:
        assert token not in caplog.text
        raise

    assert batches
    assert all(isinstance(batch, RecordBatch) for batch in batches)
    assert all(
        isinstance(record, Mapping)
        for batch in batches
        for record in batch.records
    )
    assert token not in caplog.text
