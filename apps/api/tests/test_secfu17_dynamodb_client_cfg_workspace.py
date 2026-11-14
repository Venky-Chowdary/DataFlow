"""Follow-up review 1, L4: the DynamoDB client config keeps the destination's workspace.

``dynamodb_writer.dynamodb_client_cfg`` (one derivation for the write and
Gate-8's read-back, security follow-up 5) dropped ``workspace_id``, so
``aws_common.boto3_client``'s pre-dial check of a custom endpoint read no
workspace's egress allowlist from the config: a private DynamoDB endpoint the
workspace's admin allowlisted was refused at read-back unless a dial scope
happened to name the workspace. The config now carries the resolved
destination's workspace when it has one. The signing region is unchanged
(the region label in ``host``, or the region in an ``amazonaws.com`` endpoint,
else us-east-1), the same for the write and the read-back.
"""

from __future__ import annotations

import pytest

from connectors.aws_common import boto3_client, resolve_region
from connectors.dynamodb_writer import dynamodb_client_cfg
from services import egress_guard


@pytest.fixture
def allowlists(monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        monkeypatch.delenv(f"{prefix}_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.setattr(
        egress_guard, "_workspace_egress_entries",
        lambda ws: ["10.20.0.0/16"] if ws == "ws-ddb" else [],
    )
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )


_DEST = {"type": "dynamodb", "host": "http://10.20.0.5:8000", "port": 8000, "username": "AKIATENANT",
         "password": "s", "workspace_id": "ws-ddb"}


def test_the_client_config_carries_the_destinations_workspace():
    assert dynamodb_client_cfg(dict(_DEST))["workspace_id"] == "ws-ddb"
    assert dynamodb_client_cfg({k: v for k, v in _DEST.items() if k != "workspace_id"})["workspace_id"] == ""


def test_the_read_back_client_uses_the_workspaces_egress_allowlist(allowlists):
    client = boto3_client("dynamodb", dynamodb_client_cfg(dict(_DEST)))
    assert client.meta.endpoint_url == "http://10.20.0.5:8000"
    with pytest.raises(egress_guard.EgressRefused):
        boto3_client("dynamodb", dynamodb_client_cfg({**_DEST, "workspace_id": "ws-other"}))


@pytest.mark.parametrize(
    "host, region",
    [("eu-west-1", "eu-west-1"), ("https://dynamodb.ap-south-1.amazonaws.com", "ap-south-1"), ("", "us-east-1")],
)
def test_the_signing_region_is_the_hosts_for_write_and_read_back(host, region):
    assert resolve_region(dynamodb_client_cfg({"host": host, "endpoint_url": ""})) == region
