"""D253 — update_connector confirm echoed an ADLS connection_string with its
full AccountKey. Every connector response (get, list, update, confirm) and
error text goes through ``mask_secrets_in_text``, which masked only URI
userinfo and URL query values; ``Key=Value;`` secret segments passed through.
"""
from __future__ import annotations

import pytest

from services.secret_config import mask_secrets_in_text

_AZ_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

CASES = [
    (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey={_AZ_KEY};BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
        _AZ_KEY,
        ["AccountName=devstoreaccount1", "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"],
    ),
    (
        "BlobEndpoint=https://acct.blob.core.windows.net/;"
        "SharedAccessSignature=sv=2022-11-02&ss=b&sig=AbCdEf%2Bsecret",
        "AbCdEf%2Bsecret",
        ["BlobEndpoint=https://acct.blob.core.windows.net/"],
    ),
    (
        "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=Root;"
        "SharedAccessKey=k3yValue=",
        "k3yValue",
        ["SharedAccessKeyName=Root"],
    ),
    ("Driver={ODBC Driver 18};Server=h;UID=sa;PWD={S3c;ret!};", "S3c", ["UID=sa", "Server=h"]),
    ("Server=h;User Id=sa;Password=Pa55word;Database=d", "Pa55word", ["User Id=sa", "Database=d"]),
    ("jdbc:sqlserver://h:1433;user=sa;password=JdbcPw9", "JdbcPw9", ["user=sa"]),
    ("host=h user=u password=LibPqPw dbname=d", "LibPqPw", ["user=u", "dbname=d"]),
]


@pytest.mark.parametrize("raw,secret,kept", CASES)
def test_secret_segment_is_masked_and_the_rest_kept(raw, secret, kept):
    out = mask_secrets_in_text(raw)
    assert secret not in out, out
    for part in kept:
        assert part in out, out


def test_connector_response_never_echoes_the_account_key():
    from services.connector_store import SavedConnector, mask_connector
    from src.routers.saved_connectors_router import _is_masked_value, _to_ui

    raw = CASES[0][0]
    conn = SavedConnector.from_dict(
        {"id": "c1", "name": "az", "type": "adls", "role": "source", "connection_string": raw},
        secrets_encrypted=False,
    )
    for view in (mask_connector(conn), _to_ui(conn)):
        assert _AZ_KEY not in str(view)
        # The UI sends the masked value back on save; it must read as "unchanged".
        assert _is_masked_value(view["connection_string"])


def test_non_secret_text_is_untouched():
    for text in ("rows=5; batch=3", "mongodb://h:27017/db", "Server=h;Database=d;"):
        assert mask_secrets_in_text(text) == text
