from __future__ import annotations

import requests

from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def test_fixture_server_records_requests_and_serves_response_sequence() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/sequence",
            responses=[
                FixtureResponse(
                    status=503,
                    body={"error": "busy"},
                    headers={"Retry-After": "2"},
                ),
                FixtureResponse(status=200, body={"ok": True}),
            ],
        )

        first = requests.get(f"{fixture.base_url}/sequence", timeout=2)
        second = requests.get(f"{fixture.base_url}/sequence", timeout=2)

        assert first.status_code == 503
        assert first.headers["Retry-After"] == "2"
        assert second.json() == {"ok": True}
        assert [entry.path for entry in fixture.request_log] == [
            "/sequence",
            "/sequence",
        ]


def test_fixture_server_can_drop_a_connection() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/drop",
            responses=[FixtureResponse(drop_connection=True)],
        )

        try:
            requests.get(f"{fixture.base_url}/drop", timeout=2)
        except requests.ConnectionError:
            pass
        else:
            raise AssertionError("expected the fixture to drop the connection")

        assert len(fixture.request_log) == 1
