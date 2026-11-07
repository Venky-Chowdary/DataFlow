"""Destination probe metadata must not carry plaintext credentials.

Preflight results are serialised into gate details, Decision Artifacts and the
Validate response, so anything the probe config exposes through ``repr``/``str``
or a log line lands in operator-visible proof — and in log aggregation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.secret_config import RedactedConfig, redact_config  # noqa: E402

_CFG = {
    "type": "postgresql",
    "host": "db.internal",
    "username": "svc_migrate",
    "password": "hunter2",
    "connection_string": "postgresql://svc:hunter2@db.internal/app",
    "api_key": "sk-live-123",
}


def test_repr_and_str_never_show_secrets():
    cfg = RedactedConfig(_CFG)
    for rendered in (repr(cfg), str(cfg), f"{cfg}", "%s" % (cfg,)):
        assert "hunter2" not in rendered
        assert "sk-live-123" not in rendered
        assert "db.internal" in rendered


def test_execution_paths_still_read_the_real_secret():
    cfg = RedactedConfig(_CFG)
    assert cfg["password"] == "hunter2"
    assert cfg["connection_string"].endswith("@db.internal/app")


def test_redact_config_keeps_shape_and_empty_values():
    out = redact_config({**_CFG, "password": "", "schema": "public"})
    assert out["password"] == ""
    assert out["schema"] == "public"
    assert out["api_key"] == "***"
    assert set(out) == set(_CFG) | {"schema"}


def test_public_metadata_serialises_redacted():
    body = json.dumps({"probe": RedactedConfig(_CFG).redacted()})
    assert "hunter2" not in body
    assert "sk-live-123" not in body
