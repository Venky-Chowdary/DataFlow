"""Ops review round 2, Medium 10: compose must let workers finish their drain hand-over.

Only the API service got a stop grace period. The batch worker, the CDC
worker and the scheduler (which also runs a worker fleet) kept Docker's
10 s default, but a stopping worker hands its in-flight jobs over at most
``WORKER_DRAIN_SECONDS`` (25 s) after SIGTERM, so Docker killed it first and
the jobs waited for their lease to expire instead.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from services.worker_fleet import worker_drain_seconds

COMPOSE = Path(__file__).resolve().parents[3] / "docker-compose.prod.yml"
DOCKER_DEFAULT_SECONDS = 10.0
MARGIN_SECONDS = 10.0


def _seconds(value: object) -> float:
    if value is None:
        return DOCKER_DEFAULT_SECONDS
    total = 0.0
    for amount, unit in re.findall(r"(\d+(?:\.\d+)?)(h|ms|m|s|us)", str(value)):
        total += float(amount) * {"h": 3600, "m": 60, "s": 1, "ms": 0.001, "us": 0.000001}[unit]
    return total


def _runs_a_worker_fleet(service: dict) -> bool:
    env = service.get("environment") or {}
    command = " ".join(service.get("command") or [])
    return "start-worker" in command or str(env.get("DATAWRAP_WORKER_FLEET", "")) == "1"


def test_every_service_that_runs_transfers_outlives_its_drain():
    services = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]
    fleets = {name: svc for name, svc in services.items() if _runs_a_worker_fleet(svc)}
    assert {"worker", "cdc-worker", "scheduler"} <= set(fleets)
    need = worker_drain_seconds() + MARGIN_SECONDS
    short = {name: svc.get("stop_grace_period") for name, svc in fleets.items()
             if _seconds(svc.get("stop_grace_period")) < need}
    assert short == {}, f"these services are killed before their {worker_drain_seconds():.0f} s drain: {short}"
