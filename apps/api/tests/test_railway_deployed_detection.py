"""Production-detection env contract.

is_production() must key off markers Railway only sets inside a deployed
service (RAILWAY_ENVIRONMENT_NAME / RAILWAY_DEPLOYMENT_ID / RAILWAY_REPLICA_ID)
or an explicit ENV declaration — never off CLI-shell vars like RAILWAY_TOKEN,
RAILWAY_ENVIRONMENT or RAILWAY_SERVICE_ID, which `railway run`, linked dev
shells and CI agents with the Railway CLI installed all export.
"""

import pytest


def _clean_env(monkeypatch):
    for key in (
        "RAILWAY_ENVIRONMENT",
        "RAILWAY_SERVICE_ID",
        "RAILWAY_PROJECT_ID",
        "RAILWAY_TOKEN",
        "RAILWAY_ENVIRONMENT_NAME",
        "RAILWAY_DEPLOYMENT_ID",
        "RAILWAY_REPLICA_ID",
        "DATAWRAP_ENV",
        "DATAFLOW_ENV",
        "ENVIRONMENT",
        "DATAWRAP_ASSUME_PRODUCTION",
        "DATAFLOW_ASSUME_PRODUCTION",
        "DATAWRAP_REQUIRE_AUTH",
        "DATAFLOW_REQUIRE_AUTH",
    ):
        monkeypatch.delenv(key, raising=False)


def test_railway_cli_token_alone_is_not_production(monkeypatch):
    """A dev box / CI agent that merely has `railway login` credentials."""
    _clean_env(monkeypatch)
    monkeypatch.setenv("RAILWAY_TOKEN", "cli-token-abc")
    from services import platform_config

    assert platform_config.is_production() is False


def test_railway_run_linked_shell_is_not_production(monkeypatch):
    """`railway run` injects RAILWAY_ENVIRONMENT + RAILWAY_SERVICE_ID."""
    _clean_env(monkeypatch)
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.setenv("RAILWAY_SERVICE_ID", "svc-123")
    from services import platform_config

    assert platform_config.is_production() is False


@pytest.mark.parametrize(
    "marker",
    ["RAILWAY_ENVIRONMENT_NAME", "RAILWAY_DEPLOYMENT_ID", "RAILWAY_REPLICA_ID"],
)
def test_deployed_railway_service_is_production(monkeypatch, marker):
    """Deploy-time markers Railway injects only into a running service."""
    _clean_env(monkeypatch)
    monkeypatch.setenv(marker, "production" if "NAME" in marker else "dep-1")
    from services import platform_config

    assert platform_config.is_production() is True


def test_explicit_env_test_beats_railway_deploy_markers(monkeypatch):
    """DATAWRAP_ENV=test pins non-production even if deploy markers leak."""
    _clean_env(monkeypatch)
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_NAME", "production")
    monkeypatch.setenv("RAILWAY_TOKEN", "cli-token-abc")
    from services import platform_config

    assert platform_config.is_production() is False


def test_explicit_env_production_still_wins(monkeypatch):
    _clean_env(monkeypatch)
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    from services import platform_config

    assert platform_config.is_production() is True


def test_is_railway_still_covers_linked_shells(monkeypatch):
    """is_railway (host-hint/volume layer) keeps accepting linked-shell vars —
    only the production gate was tightened."""
    _clean_env(monkeypatch)
    monkeypatch.setenv("RAILWAY_SERVICE_ID", "svc-123")
    from services import platform_config

    assert platform_config.is_railway() is True
    assert platform_config.is_railway_deployed() is False
