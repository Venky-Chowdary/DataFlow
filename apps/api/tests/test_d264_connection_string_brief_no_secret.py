"""D264: a Key=Value connection string is not a URL; its secrets never reach a brief."""

import json

import pytest

from src.ai.copilot.lifecycle_tools import _connector_brief

AZ_KEY = "ZmFrZWtleWZha2VrZXlmYWtla2V5ZmFrZWtleQ=="


@pytest.mark.parametrize(
    "conn, host, port, secret",
    [
        (
            {"type": "adls", "connection_string": "DefaultEndpointsProtocol=https;AccountName=acct1;"
             f"AccountKey={AZ_KEY};EndpointSuffix=core.windows.net"},
            "acct1.blob.core.windows.net", 0, AZ_KEY,
        ),
        (
            {"type": "azure_blob", "connection_string": f"AccountName=acct2;AccountKey={AZ_KEY}"},
            "acct2.blob.core.windows.net", 0, AZ_KEY,
        ),
        (
            {"type": "adls", "connection_string": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
             f"AccountKey={AZ_KEY};BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"},
            "127.0.0.1", 10000, AZ_KEY,
        ),
        (
            {"type": "adls", "connection_string": "BlobEndpoint=https://acct3.blob.core.windows.net/;"
             "SharedAccessSignature=sv=2022-11-02&ss=b&sig=SECRETSIG%3D"},
            "acct3.blob.core.windows.net", 0, "SECRETSIG",
        ),
        (
            {"type": "mssql", "port": 1433, "connection_string":
             "Driver={ODBC Driver 18 for SQL Server};Server=tcp:db1.example.com,14330;Uid=sa;Pwd={S3c;ret!};"},
            "db1.example.com", 14330, "S3c;ret!",
        ),
        (
            {"type": "mssql", "port": 1433, "connection_string":
             "Data Source=db2.example.com;Initial Catalog=x;User ID=sa;Password=Pw0rd!"},
            "db2.example.com", 1433, "Pw0rd!",
        ),
        (
            {"type": "mongodb", "connection_string": "mongodb://u:p4ss@h1:27017/db"},
            "h1", 27017, "p4ss",
        ),
    ],
)
def test_brief_derives_the_dialed_host_and_never_echoes_secrets(conn, host, port, secret):
    brief = _connector_brief({"id": "c1", **conn})
    assert (brief["effective_host"], brief["effective_port"]) == (host, port)
    text = json.dumps(brief)
    assert secret not in text
    assert "AccountKey" not in text and "Pwd" not in text and "Password" not in text


def test_string_naming_no_host_keeps_the_stored_host():
    brief = _connector_brief({"id": "c1", "type": "adls", "host": "acct9", "port": 0,
                              "connection_string": f"AccountKey={AZ_KEY}"})
    assert brief["effective_host"] == "acct9"
    assert AZ_KEY not in json.dumps(brief)
