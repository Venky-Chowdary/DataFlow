"""Bring up the local engine fleet and print the env every live suite reads.

Live suites resolve credentials through ``apps/api/tests/helpers/live_env.py``,
which reads the tool-standard variables (``PGHOST``/``PGUSER``/…, ``MYSQL_*``)
before falling back to a default that does not match this repository's compose
file. So a perfectly healthy fleet still produced 20+ ``skip``s — the suite
looked for ``postgres``/``admin`` while compose serves ``dataflow``/``dataflow``.

This script is the one place that knows both halves: it starts the services
``docker-compose.yml`` declares, waits for the ones that expose a healthcheck,
and emits the exact exports for the shell that will run the tests. It never
prints a value it did not read from the compose file, and it invents no
credentials.

    python scripts/local_engines.py                 # start + print exports
    python scripts/local_engines.py --print-only    # exports for a running fleet
    python scripts/local_engines.py --shell bash    # POSIX form
    python scripts/local_engines.py --with-search   # add Elasticsearch (JVM)

``--check`` exits non-zero when a declared service is not reachable, so a
matrix run can refuse to grade itself green against a fleet that is not up.
"""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"

#: Services started by default: every engine a local suite can prove a real
#: transfer against without cloud credentials. Elasticsearch is opt-in because a
#: JVM node is the heaviest member of the fleet.
CORE_SERVICES = ("postgres", "mysql", "mysql-init", "mongodb", "mongo-init", "redis")
SEARCH_SERVICES = ("elasticsearch",)

#: Environment the live resolver reads, with the values ``docker-compose.yml``
#: actually serves. Kept in this table rather than duplicated per suite so a
#: compose change has exactly one place to follow.
FLEET_ENV: dict[str, str] = {
    "PGHOST": "127.0.0.1",
    "PGPORT": "5432",
    "PGDATABASE": "dataflow",
    "PGUSER": "dataflow",
    "PGPASSWORD": "dataflow",
    "MYSQL_HOST": "127.0.0.1",
    "MYSQL_PORT": "3306",
    "MYSQL_DATABASE": "dataflow",
    "MYSQL_USER": "dataflow",
    "MYSQL_PASSWORD": "dataflow",
    "P2_MONGO_URI": "mongodb://127.0.0.1:27017",
    "REDIS_URL": "redis://127.0.0.1:6379/0",
    "ES_URL": "http://127.0.0.1:9200",
}

#: ``(label, host, port)`` — the ports compose publishes for the fleet.
PORTS: tuple[tuple[str, str, int], ...] = (
    ("postgresql", "127.0.0.1", 5432),
    ("mysql", "127.0.0.1", 3306),
    ("mongodb", "127.0.0.1", 27017),
    ("redis", "127.0.0.1", 6379),
)
SEARCH_PORTS: tuple[tuple[str, str, int], ...] = (("elasticsearch", "127.0.0.1", 9200),)


def _compose(args: list[str]) -> int:
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    return subprocess.call(cmd, cwd=str(REPO_ROOT))


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait(targets: tuple[tuple[str, str, int], ...], seconds: int) -> list[str]:
    """Return the labels still unreachable after ``seconds``."""
    deadline = time.time() + seconds
    # Probe once before consulting the deadline: with ``seconds == 0`` a
    # deadline-first loop reports a fleet that is already up as unreachable.
    pending = [t for t in targets if not _port_open(t[1], t[2])]
    while pending and time.time() < deadline:
        time.sleep(2)
        pending = [t for t in pending if not _port_open(t[1], t[2])]
    return [t[0] for t in pending]


def _emit(shell: str) -> None:
    for key, value in FLEET_ENV.items():
        if shell == "bash":
            print(f'export {key}="{value}"')
        elif shell == "json":
            continue
        else:
            print(f'$env:{key} = "{value}"')
    if shell == "json":
        print(json.dumps(FLEET_ENV, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--print-only", action="store_true", help="do not start anything"
    )
    parser.add_argument(
        "--with-search", action="store_true", help="also start Elasticsearch"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero when a fleet port is unreachable",
    )
    parser.add_argument(
        "--shell", choices=("powershell", "bash", "json"), default="powershell"
    )
    parser.add_argument(
        "--timeout", type=int, default=180, help="seconds to wait for ports"
    )
    args = parser.parse_args()

    targets = PORTS + (SEARCH_PORTS if args.with_search else ())

    if not args.print_only:
        rc = _compose(["up", "-d", *CORE_SERVICES])
        if rc != 0:
            print("docker compose up failed for the core fleet", file=sys.stderr)
            return rc
        if args.with_search:
            rc = _compose(["--profile", "search", "up", "-d", *SEARCH_SERVICES])
            if rc != 0:
                print("docker compose up failed for elasticsearch", file=sys.stderr)
                return rc

    missing = _wait(targets, 0 if args.print_only else args.timeout)
    for label, host, port in targets:
        state = "unreachable" if label in missing else "up"
        print(f"# {label:<14} {host}:{port} {state}", file=sys.stderr)

    _emit(args.shell)

    if missing and (args.check or not args.print_only):
        print(
            "# fleet incomplete: " + ", ".join(missing) + " - live suites will skip",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
