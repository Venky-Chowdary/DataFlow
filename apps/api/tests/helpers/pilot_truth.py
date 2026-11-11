"""Seeded workspace state and store truth for the Pilot ground-truth suite.

A case in this suite is: seed real jobs / pipelines / connectors, ask Pilot a
question through ``DataPilotAgent.chat``, and require the facts in the answer
to equal what the stores say. The expected numbers are always read back from
the stores (``job_truth``), never typed into a test, so a case cannot pass on
a substring that happens to be in the answer.

Everything runs offline against the in-memory job store and the file-backed
schedule / connector stores the test session already isolates.
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

#: The instant every windowed case is asked at. 02:30 UTC is the same calendar
#: day in UTC, the *next* local day east of it and the *previous* one west of
#: it, so "today" differs by time zone and a UTC-only answer is caught.
FROZEN_NOW = datetime(2026, 11, 11, 2, 30, tzinfo=timezone.utc)


_CASE = [0]


def mongo():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def reset_workspace(monkeypatch=None) -> None:
    """Empty the job ledger and every per-process Pilot cache."""
    from services import mongodb_service
    from src.ai.copilot import context_builder

    if monkeypatch is not None:
        monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
        # A fresh in-memory ledger per case, restored when the case ends.
        monkeypatch.setattr(mongodb_service, "_mongodb_service", None)
        # ...and a connector file of its own, inside the session's data dir.
        _CASE[0] += 1
        case_dir = Path(os.environ["DATAFLOW_DATA_DIR"]) / f"pilot-truth-{os.getpid()}-{_CASE[0]}"
        case_dir.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(case_dir / "connectors.json"))
        monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.schedule_store as schedule_store

    schedule_store._save_all([])
    store = mongo()
    assert type(store).__name__ == "MemoryMongoDBService", "truth suite needs DATAFLOW_JOB_STORE=memory"
    store._jobs.clear()

    context_builder.reset_context_cooldowns()
    if monkeypatch is not None:
        for key in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
            monkeypatch.setenv(key, "")
        monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:9")
        monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
        monkeypatch.setenv("DATAFLOW_SEED_DEMO", "0")


def freeze_clock(monkeypatch, now: datetime = FROZEN_NOW, zone: str = "UTC") -> None:
    """Pin Pilot's clock and the workspace time zone for a windowed case."""
    try:
        from src.ai.copilot import job_window
    except ImportError:  # code that predates windowed job reads has no clock to pin
        return
    monkeypatch.setattr(job_window, "utc_now", lambda: now)
    monkeypatch.setattr(job_window, "workspace_zone_name", lambda: zone)


def seed_job(status: str = "completed", *, minutes_ago: float = 0, at: datetime | None = None,
             now: datetime | None = None, **extra: Any) -> str:
    """One job in the ledger with an exact ``created_at`` and status."""
    store = mongo()
    created = at or ((now or datetime.now(timezone.utc)) - timedelta(minutes=minutes_ago))
    data = {
        "source_name": extra.pop("source_name", "orders"),
        "source_type": extra.pop("source_type", "postgresql"),
        "destination_type": extra.pop("destination_type", "snowflake"),
        "destination_collection": extra.pop("destination_collection", "orders_wh"),
        "status": status,
        "records_processed": extra.pop("records_processed", 100),
    }
    data.update(extra)
    job_id = store.create_transfer_job(data)
    row = store._jobs[job_id]
    row.update(extra)
    row.update(created_at=created, updated_at=created, status=status)
    return job_id


def seed_schedules(count: int, *, enabled_every: int = 2, workspace_id: str = "") -> list[Any]:
    """``count`` saved pipelines; every ``enabled_every``-th one is enabled."""
    import services.schedule_store as ss

    now = datetime.now(timezone.utc)
    rows = []
    for i in range(count):
        sched = ss.PipelineSchedule(
            id=f"sched-{i:03d}",
            name=f"nightly-{i:02d}",
            source_connector_id="c-src",
            source_table="orders",
            dest_connector_id="c-dst",
            dest_table="orders_wh",
            interval="daily",
            enabled=(i % enabled_every == 0),
        )
        sched.workspace_id = workspace_id
        sched.created_at = (now - timedelta(minutes=i)).isoformat()
        sched.next_run_at = (now + timedelta(hours=1, minutes=i)).isoformat()
        rows.append(sched)
    ss._save_all(rows)
    return rows


def seed_sqlite_connector(directory: Path, *, name: str = "CRM SQLite", table: str = "customers",
                          rows: int = 7, workspace_id: str = "") -> dict[str, Any]:
    """A real SQLite connector whose table holds recognisable PII cells."""
    from services.connector_store import create_connector

    db = Path(directory) / (name.replace(" ", "_") + ".db")
    with sqlite3.connect(db) as conn:
        conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" '
            "(id INTEGER PRIMARY KEY, email TEXT, ssn TEXT, note TEXT, amount REAL)"
        )
        conn.executemany(
            f'INSERT INTO "{table}" VALUES (?,?,?,?,?)',
            [(i, pii_email(i), pii_ssn(i), f"note {i}", 10.0 * i) for i in range(1, rows + 1)],
        )
        conn.commit()
    payload = {"name": name, "type": "sqlite", "role": "source", "database": str(db),
               "connection_string": str(db)}
    if workspace_id:
        payload["workspace_id"] = workspace_id
    return create_connector(payload)


def pii_email(i: int) -> str:
    return f"person{i}@example.com"


def pii_ssn(i: int) -> str:
    return f"123-45-{6000 + i}"


def agent():
    from src.ai.copilot.pilot_agent import DataPilotAgent

    return DataPilotAgent()


_SESSION = [0]


def ask(pilot, question: str, history: list[dict] | None = None, **data_context: Any):
    """One chat turn in its own session unless ``pilot_session_id`` is given."""
    _SESSION[0] += 1
    context = {"pilot_session_id": f"truth-{_SESSION[0]}"}
    context.update(data_context)
    return pilot.chat(question, history or [], data_context=context)


def grounded(response) -> bool:
    from src.ai.copilot.pilot_agent import carries_evidence

    return carries_evidence(response)


# ---- truth, read straight from the stores ---------------------------------


def job_truth(**filters: Any) -> dict[str, Any]:
    """``count_jobs`` for the whole ledger, or for a status / created window."""
    return mongo().count_jobs(**filters)


def job_ids(**filters: Any) -> list[str]:
    return [str(j.get("_id") or j.get("id")) for j in mongo().list_jobs(limit=100, **filters)]


# ---- answer parsing --------------------------------------------------------

_BOLD_INT = re.compile(r"\*\*(\d[\d,]*)(?=\*\*|\s)")
_JOB_ID = re.compile(r"`([0-9a-f]{24})`")


def bold_ints(answer: str) -> list[int]:
    """Every number Pilot stated as a fact (bold integers), in order."""
    return [int(m.replace(",", "")) for m in _BOLD_INT.findall(answer or "")]


def listed_job_ids(answer: str) -> list[str]:
    return _JOB_ID.findall(answer or "")


def says_unreadable(answer: str) -> bool:
    """The outage sentence: the store could not be read (never "you have none")."""
    return bool(re.search(r"could(?: not|n't) (?:read|load|reach)", answer or "", re.I))
