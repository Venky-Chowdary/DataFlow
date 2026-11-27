"""Child process for the kill-and-resume test (``tests/test_kill_resume_live.py``).

``python tests/helpers/kill_resume_child.py SPEC.json`` runs one transfer
through ``UniversalTransferEngine().execute_tracked`` and writes the result to
``SPEC['result_path']``. The parent SIGKILLs this process mid-run, then starts
it again with ``resume: true``.

The job store is the in-memory one, made durable the way MongoDB is: every
call that may change it rewrites a pickle of its state (write, fsync, rename)
before returning, and a new process loads that state. A SIGKILL therefore
loses exactly what a killed worker loses in production — nothing that the
store had acknowledged. The sync-cursor store is a file already.
"""

from __future__ import annotations

import json
import os
import pickle
import sys
import threading
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_API_ROOT))

os.environ["DATAFLOW_JOB_STORE"] = "memory"
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")

import services.mongodb_service as mongo_mod  # noqa: E402

_STATE_FIELDS = ("_connectors", "_jobs", "_claims")
_READ_ONLY_PREFIXES = ("get_", "list_", "test_", "count_", "find_", "is_", "has_")


class DurableMemoryStore(mongo_mod.MemoryMongoDBService):
    """The in-memory job store, persisted after every mutating call."""

    def __init__(self, path: Path) -> None:
        super().__init__()
        self._path = path
        self._persist_lock = threading.Lock()
        if path.exists():
            with path.open("rb") as fh:
                state = pickle.load(fh)  # noqa: S301 - this test's own file
            for name in _STATE_FIELDS:
                setattr(self, name, state.get(name, {}))

    def _persist(self) -> None:
        with self._persist_lock:
            state = {name: getattr(self, name) for name in _STATE_FIELDS}
            tmp = self._path.with_suffix(".tmp")
            with tmp.open("wb") as fh:
                pickle.dump(state, fh)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._path)

    def __getattribute__(self, name: str):
        attr = super().__getattribute__(name)
        if (
            name.startswith("_")
            or name.startswith(_READ_ONLY_PREFIXES)
            or not callable(attr)
        ):
            return attr

        def durable(*args, **kwargs):
            out = attr(*args, **kwargs)
            self._persist()
            return out

        return durable


def main() -> int:
    spec = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    store = DurableMemoryStore(Path(spec["store_path"]))
    mongo_mod._mongodb_service = store

    import services.sync_cursor as sc
    import src.transfer.stream as stream_mod

    sc.STORE_PATH = Path(spec["cursor_path"])
    sc._mongo_cursors = lambda: None
    stream_mod.CHUNK_SIZE = int(spec["chunk_size"])

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(**spec["source"]),
        destination=EndpointConfig(**spec["destination"]),
        sync_mode=spec["mode"],
        stream_contracts=spec["contracts"],
        skip_preflight=True,
        validation_mode="strict",
        source_filter=spec.get("source_filter") or {},
    )
    job_id = spec["job_id"]
    resume = bool(spec.get("resume"))
    if resume:
        store.update_job_status(job_id, "pending", message="resume after kill",
                                allow_terminal_exit=True)
    else:
        store.update_job_status(job_id, "pending", transfer_request={})
    Path(spec["started_path"]).write_text("started", encoding="utf-8")
    result = UniversalTransferEngine().execute_tracked(request, job_id, resume=resume)
    recon = result.reconciliation or {}
    Path(spec["result_path"]).write_text(json.dumps({
        "success": bool(result.success),
        "error": str(result.error or "")[:2000],
        "records_transferred": int(result.records_transferred or 0),
        "reconciliation_passed": recon.get("passed"),
        "target_rows": recon.get("target_rows"),
    }), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
