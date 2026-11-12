"""Security review (low): DATAWRAP_X / DATAFLOW_X precedence is safe and visible.

For the flags converted to the brand reader, an *empty* ``DATAWRAP_X`` (a
blank line left in an environment block) used to override a set
``DATAFLOW_X`` -- turning off audit legal hold, rate limits, the SFTP host-key
policy, HSTS or residency strict mode. Empty now counts as unset. When both
names are set to different values the documented precedence holds (the new
``DATAWRAP_`` name wins) and the app says so at startup, naming the flag and
never either value.
"""

from __future__ import annotations

import inspect
import logging

import pytest

from services.brand_env import getenv_brand, getenv_brand_bool


@pytest.fixture
def clean(monkeypatch):
    import os

    for name in list(os.environ):
        if name.startswith(("DATAWRAP_", "DATAFLOW_")):
            monkeypatch.delenv(name)
    return monkeypatch


@pytest.mark.parametrize("blank", ["", "   "])
def test_an_empty_new_name_does_not_override_a_set_legacy_name(clean, blank):
    clean.setenv("DATAFLOW_RESIDENCY_STRICT", "1")
    clean.setenv("DATAWRAP_RESIDENCY_STRICT", blank)
    assert getenv_brand("RESIDENCY_STRICT") == "1"
    assert getenv_brand_bool("RESIDENCY_STRICT") is True
    from services import data_residency

    assert data_residency._strict() is True


def test_an_empty_new_name_alone_is_still_read_as_empty(clean):
    clean.setenv("DATAWRAP_AUDIT_RETENTION_DAYS", "")
    assert getenv_brand("AUDIT_RETENTION_DAYS", "30") == ""


def test_when_both_are_set_the_new_name_wins_and_startup_names_the_flag(clean, caplog):
    from services.brand_env import warn_brand_flag_conflicts

    clean.setenv("DATAFLOW_RESIDENCY_STRICT", "1")
    clean.setenv("DATAWRAP_RESIDENCY_STRICT", "0")
    clean.setenv("DATAFLOW_AUTH_SECRET", "legacy-secret-value-123")
    clean.setenv("DATAWRAP_AUTH_SECRET", "new-secret-value-456")
    clean.setenv("DATAFLOW_HSTS", "1")
    clean.setenv("DATAWRAP_HSTS", " 1 ")  # the same value: no warning
    clean.setenv("DATAFLOW_PUBLIC_RATE_LIMIT", "1")
    clean.setenv("DATAWRAP_PUBLIC_RATE_LIMIT", "")  # empty: no conflict, the legacy name applies
    assert getenv_brand("RESIDENCY_STRICT") == "0"

    with caplog.at_level(logging.WARNING):
        conflicts = warn_brand_flag_conflicts(logging.getLogger("datawrap.config"))
    assert conflicts == ["AUTH_SECRET", "RESIDENCY_STRICT"]
    text = caplog.text
    assert "DATAWRAP_RESIDENCY_STRICT" in text and "DATAFLOW_RESIDENCY_STRICT" in text
    assert "DATAWRAP_AUTH_SECRET" in text
    for secret in ("legacy-secret-value-123", "new-secret-value-456"):
        assert secret not in text
    assert "HSTS" not in text and "PUBLIC_RATE_LIMIT" not in text


def test_the_api_warns_at_startup():
    from src import main

    assert "warn_brand_flag_conflicts" in inspect.getsource(main.lifespan)
