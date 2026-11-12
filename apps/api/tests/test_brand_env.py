"""Brand env dual-read: DATAWRAP_* preferred, DATAFLOW_* fallback."""

from __future__ import annotations


from services.brand_env import getenv_brand, getenv_brand_str


def test_prefers_datawrap(monkeypatch):
    monkeypatch.setenv("DATAWRAP_AUTH_SECRET", "wrap")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "legacy")
    assert getenv_brand("AUTH_SECRET") == "wrap"
    assert getenv_brand("DATAFLOW_AUTH_SECRET") == "wrap"
    assert getenv_brand_str("AUTH_SECRET") == "wrap"


def test_falls_back_to_dataflow(monkeypatch):
    monkeypatch.delenv("DATAWRAP_AUTH_SECRET", raising=False)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "legacy")
    assert getenv_brand("AUTH_SECRET") == "legacy"


def test_unset_returns_default(monkeypatch):
    monkeypatch.delenv("DATAWRAP_ENV", raising=False)
    monkeypatch.delenv("DATAFLOW_ENV", raising=False)
    assert getenv_brand("ENV", "development") == "development"
    assert getenv_brand("ENV") is None


def test_empty_datawrap_does_not_shadow_a_set_legacy_name(monkeypatch):
    """An empty DATAWRAP_* is unset when DATAFLOW_* has a value (security review:
    a blank line in an environment block turned security flags off). To clear
    a flag, clear both names."""
    monkeypatch.setenv("DATAWRAP_TRAINING", "")
    monkeypatch.setenv("DATAFLOW_TRAINING", "on")
    assert getenv_brand("TRAINING") == "on"
    monkeypatch.setenv("DATAFLOW_TRAINING", "")
    assert getenv_brand("TRAINING") == ""
