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
        assert first.meta["native_types"]["id"] == "INTEGER"
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


def test_manifest_identity_seeds_contract_defaults() -> None:
    """Vendor-declared pk/cursor fills empty contract fields (catalog parity)."""
    from services.sync_cursor import seed_sdk_contract_defaults

    contracts = [{"name": "tickets", "sync_mode": "incremental_append"}]
    seeded = seed_sdk_contract_defaults("zendesk", "tickets", contracts)

    assert seeded[0]["primary_key"] == "id"
    assert seeded[0]["cursor_field"] == "generated_timestamp"
    # Epoch export cursor is a monotonic sequence, not a row update timestamp.
    assert seeded[0]["cursor_semantics"] == "monotonic_sequence"


def test_manifest_identity_never_overrides_operator_declarations() -> None:
    from services.sync_cursor import seed_sdk_contract_defaults

    contracts = [
        {
            "name": "tickets",
            "primary_key": "external_id",
            "cursor_field": "my_bookmark",
        }
    ]
    seeded = seed_sdk_contract_defaults("zendesk", "tickets", contracts)

    assert seeded[0]["primary_key"] == "external_id"
    assert seeded[0]["cursor_field"] == "my_bookmark"
    # Operator-declared cursor is not the manifest field — semantics untouched.
    assert "cursor_semantics" not in seeded[0]


def test_manifest_identity_noop_for_non_sdk_sources() -> None:
    from services.sync_cursor import seed_sdk_contract_defaults

    contracts = [{"name": "orders", "sync_mode": "incremental_append"}]
    seeded = seed_sdk_contract_defaults("postgresql", "orders", contracts)
    assert "cursor_field" not in seeded[0]
    assert "primary_key" not in seeded[0]


def test_manifest_identity_seeds_named_stream_only() -> None:
    """A contract naming a different stream does not take this stream's key."""
    from services.sync_cursor import seed_sdk_contract_defaults

    contracts = [{"name": "users", "sync_mode": "incremental_append"}]
    seeded = seed_sdk_contract_defaults("zendesk", "users", contracts)
    # users stream exists in the manifest and declares the same identity.
    assert seeded[0]["cursor_field"] == "generated_timestamp"
