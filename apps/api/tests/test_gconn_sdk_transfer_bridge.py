from __future__ import annotations

import base64
import json

from connectors.sdk.transfer_bridge import read_object
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def test_sdk_transfer_bridge_reads_one_page_and_encodes_resume_state() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/repos/acme/widgets/issues",
            responses=[
                FixtureResponse(
                    body=[{"id": 1, "title": "first"}],
                    headers={"Link": f"<{fixture.base_url}/repos/acme/widgets/issues?page=2>; rel=\"next\""},
                ),
                FixtureResponse(body=[{"id": 2, "title": "second"}]),
            ],
        )
        cfg = {
            "type": "github",
            "base_url": fixture.base_url,
            "owner": "acme",
            "repo": "widgets",
            "access_token": "synthetic-token",
        }
        first = read_object(cfg=cfg, object="issues", limit=100)

        assert first.headers[:2] == ["id", "number"] or "id" in first.headers
        assert first.rows[0][first.headers.index("title")] == "first"
        assert first.meta["sdk_done"] is False
        token = first.meta["sdk_state"]
        assert token and "=" not in token
        decoded = json.loads(
            base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        )
        assert decoded["page_token"].endswith("page=2")
        assert len(fixture.request_log) == 1

        second = read_object(
            cfg=cfg,
            object="issues",
            limit=100,
            sdk_state=token,
        )

        assert second.rows[0][second.headers.index("title")] == "second"
        assert second.meta["sdk_done"] is True
        assert len(fixture.request_log) == 2
