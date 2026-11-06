"""D280: the reconcile pulse must not promise a lock bound it is already past.

QA (scheduled Timescale -> new SQL Server, 30 rows): the job sat at
"Reconciling data (Ns) - checksum scan still running for 30 rows. No
destination row has been read yet; a metadata lock fails within 120s. The job
has not stalled. Checksum 0%" for 467 s, with no DataFlow request on SQL
Server and only idle sessions on Timescale. The sentence promised a 120 s
bound for 8 minutes and named a lock that did not exist.

Now the bound is the configured lock wait, and once the wait outlasts it the
pulse says the destination has not answered the checksum read.
"""

from __future__ import annotations

import threading
import time

from src.transfer.reconcile_heartbeat import reconcile_phase_heartbeat


class _Store:
    def __init__(self) -> None:
        self.messages: list[str] = []
        self.lock = threading.Lock()

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        with self.lock:
            self.messages.append(str(kwargs.get("message") or ""))
        return True


def test_the_wait_sentence_follows_the_configured_bound_and_stops_promising_past_it(monkeypatch):
    monkeypatch.setenv("DATAFLOW_DEST_LOCK_WAIT_SECONDS", "1")
    store = _Store()
    with reconcile_phase_heartbeat(store, "j280", processed=30, total=30, interval_s=0.4):
        time.sleep(3.2)
    pulses = [m for m in store.messages if m.startswith("Reconciling data (")]
    assert pulses, store.messages
    assert not any("120s" in m or "metadata lock" in m for m in pulses), pulses
    early = [m for m in pulses if m.startswith(("Reconciling data (0s)", "Reconciling data (1s)"))]
    late = [m for m in pulses if m.startswith(("Reconciling data (2s)", "Reconciling data (3s)"))]
    assert early and all("ends within 1s" in m for m in early), early
    assert late, pulses
    for m in late:
        assert "ends within" not in m, m
        assert "has not answered the checksum read" in m and "longer than the 1s" in m, m
