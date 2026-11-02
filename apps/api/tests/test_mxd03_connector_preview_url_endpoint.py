"""QA MXD03 + MX1-04 — the create_connector preview misreported URL-style endpoints.

QA saw ``{"type": "restapi", "host": "(from URL)", "port": 5432,
"has_password": true}`` for a password-less https URL, 5432/3306 for SQL URLs
that named another port, ``port: 443`` for an s3 endpoint ending in
``:40447``, and 27017 for a ``mongodb://…:36057`` URL. The preview took the
port the draft *defaulted* to and counted any connection string as a password.
"""

from __future__ import annotations

import uuid

import pytest

from src.ai.copilot.tools import DataPilotTools


def _preview(**kwargs):
    res = DataPilotTools()._create_connector(
        name=f"mxd03-{uuid.uuid4().hex[:8]}", test_first=False, **kwargs
    )
    assert res.success, res.error
    return res.output["preview"]


@pytest.mark.parametrize(
    "kwargs, host, port, has_password",
    [
        ({"type": "rest_api", "connection_string": "https://jsonplaceholder.typicode.com"},
         "jsonplaceholder.typicode.com", None, False),
        ({"type": "generic_sql", "connection_string": "sqlite:////tmp/mxd03.db"},
         "(from URL)", None, False),
        ({"type": "mysql", "connection_string": "mysql://qa:pw@db.example.com:3310/shop"},
         "db.example.com", 3310, True),
        ({"type": "postgresql", "connection_string": "postgresql://qa@pg.example.com:6543/app"},
         "pg.example.com", 6543, False),
        ({"type": "mongodb", "connection_string": "mongodb://u:pw@mg.example.com:36057/db"},
         "mg.example.com", 36057, True),
        ({"type": "s3", "host": "http://example.invalid:40447", "username": "a", "database": "bk"},
         "http://example.invalid:40447", 40447, False),
    ],
)
def test_preview_reports_the_url_endpoint(kwargs, host, port, has_password):
    p = _preview(**kwargs)
    assert p["host"] == host
    assert p["port"] == port
    assert p["has_password"] is has_password


def test_field_form_preview_unchanged():
    p = _preview(type="postgresql", host="pg.example.com", port=5433, database="app", username="u", password="s3")
    assert (p["host"], p["port"], p["has_password"]) == ("pg.example.com", 5433, True)
