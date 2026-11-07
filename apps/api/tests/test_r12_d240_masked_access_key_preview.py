"""D240: the create preview shows a masked access key id, never a blank.

S3-family connectors keep the access key id in ``username``. The preview
blanked it so the key was not echoed, which read as "nothing saved".
"""

from __future__ import annotations

import uuid

import pytest

KEY = "AKIAIOSFODNN7EXAMPLE"


def _preview(ctype: str, username: str) -> dict:
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute(
        "create_connector",
        {
            "name": f"d240-{uuid.uuid4().hex[:8]}",
            "type": ctype,
            "host": "http://moto.d240.test:4566",
            "database": "qaenv",
            "username": username,
            "password": "d240-secret-value",
            "test_first": False,
        },
    )
    assert staged.success, staged.error
    return staged.output["preview"]


@pytest.mark.parametrize("ctype", ["s3", "minio", "gcs", "azure_blob_storage"])
def test_access_key_is_masked_not_blank(ctype: str) -> None:
    preview = _preview(ctype, KEY)
    assert preview["username"] == "AKIA…LE"
    assert KEY not in repr(preview)


def test_short_key_reveals_nothing_but_presence() -> None:
    preview = _preview("s3", "abc12")
    assert preview["username"] == "•••"


def test_no_key_stays_blank() -> None:
    assert _preview("s3", "")["username"] == ""


def test_database_user_is_shown_as_is() -> None:
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute(
        "create_connector",
        {"name": f"d240-{uuid.uuid4().hex[:8]}", "type": "postgresql", "host": "pg.d240.test",
         "port": 5432, "database": "qa", "username": "app_user", "password": "x", "test_first": False},
    )
    assert staged.success, staged.error
    assert staged.output["preview"]["username"] == "app_user"
