"""Unit tests for the Railway CORS regex used in main.py."""

from __future__ import annotations

import re

import pytest


@pytest.mark.parametrize(
    "origin,should_match",
    [
        ("https://dataflowweb-production.up.railway.app", True),
        ("https://dataflow-api-production-722b.up.railway.app", True),
        ("https://my-service-123.up.railway.app", True),
        ("http://dataflowweb-production.up.railway.app", False),
        ("https://evil.up.railway.app.attacker.com", False),
        ("https://attacker.com", False),
        ("https://up.railway.app", False),
    ],
)
def test_railway_cors_regex_anchors_end(origin: str, should_match: bool):
    """The default Railway CORS regex must exactly match one *.up.railway.app subdomain."""
    regex = r"https://[a-zA-Z0-9_-]+\.up\.railway\.app$"
    assert bool(re.match(regex, origin)) is should_match
