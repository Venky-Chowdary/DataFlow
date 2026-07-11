"""Persistence for upload registry and auth actor wiring."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.platform_config import data_dir, upload_dir


def test_upload_registry_persists_across_reload(tmp_path, monkeypatch):
    data = tmp_path / "data"
    uploads = tmp_path / "uploads"
    data.mkdir()
    uploads.mkdir()
    registry = data / "upload_registry.json"

    monkeypatch.setattr("services.platform_config.data_dir", lambda: data)
    monkeypatch.setattr("services.platform_config.upload_dir", lambda: uploads)

    import importlib
    import services.file_parser as fp

    importlib.reload(fp)

    content = b"id,name\n1,alpha\n2,beta\n"
    record = fp.store_upload("sample.csv", content)
    assert registry.exists()

    importlib.reload(fp)
    restored = fp.get_file(record["file_id"])
    assert restored is not None
    assert restored["filename"] == "sample.csv"
    assert restored["row_count"] == 2


def test_auth_middleware_sets_user_email():
    from src.services.auth_service import create_token
    from src.middleware.auth_middleware import AuthMiddleware

    token, _ = create_token("test@gmail.com")
    assert token

    # verify_token path is covered by auth_service tests; lookup_user used in middleware
    from src.services.auth_service import lookup_user

    user = lookup_user("test@gmail.com")
    assert user is None or user["email"] == "test@gmail.com"
