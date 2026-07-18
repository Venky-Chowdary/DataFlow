"""Job registry with checkpoint progress and reconciliation."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from services.value_serializer import json_default

logger = logging.getLogger(__name__)


def _job_record_from_dict(d: dict[str, Any]) -> JobRecord:
    """Hydrate a JobRecord from a plain dictionary."""
    return JobRecord(
        job_id=d["job_id"],
        status=d["status"],
        operation=d["operation"],
        source=d["source"],
        destination=d["destination"],
        rows_processed=d.get("rows_processed", 0),
        total_rows=d.get("total_rows", 0),
        current_chunk=d.get("current_chunk", 0),
        total_chunks=d.get("total_chunks", 0),
        table_name=d.get("table_name", ""),
        driver=d.get("driver", ""),
        reconciliation=d.get("reconciliation"),
        checkpoints=d.get("checkpoints", []),
        rejected_details=d.get("rejected_details", []),
        workflow_phase=d.get("workflow_phase", "queued"),
        created_at=d.get("created_at", datetime.now(timezone.utc).isoformat()),
        message=d.get("message", ""),
    )


@dataclass
class JobRecord:
    job_id: str
    status: str
    operation: str
    source: str
    destination: str
    rows_processed: int = 0
    total_rows: int = 0
    current_chunk: int = 0
    total_chunks: int = 0
    table_name: str = ""
    driver: str = ""
    reconciliation: dict[str, Any] | None = None
    checkpoints: list[dict[str, Any]] = field(default_factory=list)
    rejected_details: list[dict[str, Any]] = field(default_factory=list)
    workflow_phase: str = "queued"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    message: str = ""


class MemoryJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = Lock()

    @property
    def backend(self) -> str:
        return "memory"

    def create(
        self,
        *,
        operation: str,
        source: str,
        destination: str,
        total_rows: int = 0,
    ) -> JobRecord:
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        record = JobRecord(
            job_id=job_id,
            status="queued",
            operation=operation,
            source=source,
            destination=destination,
            total_rows=total_rows,
            message="Queued",
        )
        with self._lock:
            self._jobs[job_id] = record
        return record

    def get(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def add_checkpoint(self, job_id: str, *, chunk: int, total: int, rows: int) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.checkpoints.append(
                    {"chunk": chunk, "total": total, "rows": rows, "at": datetime.now(timezone.utc).isoformat()}
                )

    def add_rejected_rows(self, job_id: str, details: list[dict[str, Any]]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job and details:
                job.rejected_details.extend(details[:200])

    def set_workflow_phase(self, job_id: str, phase: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.workflow_phase = phase

    def set_message(self, job_id: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.message = message

    def set_running(self, job_id: str, *, total_rows: int, table_name: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = "running"
                job.total_rows = total_rows
                job.table_name = table_name
                job.message = "Transfer in progress"
                job.workflow_phase = "transfer"

    def update_progress(self, job_id: str, *, current_chunk: int, total_chunks: int, rows_processed: int) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.current_chunk = current_chunk
                job.total_chunks = total_chunks
                job.rows_processed = rows_processed

    def complete(
        self,
        job_id: str,
        rows: int,
        *,
        reconciliation: dict | None = None,
        table_name: str = "",
        driver: str = "",
    ) -> JobRecord | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            job.status = "completed"
            job.rows_processed = rows
            job.total_rows = rows
            job.reconciliation = reconciliation
            job.workflow_phase = "completed"
            if table_name:
                job.table_name = table_name
            if driver:
                job.driver = driver
            job.message = reconciliation.get("message", "Completed") if reconciliation else "Completed"
            return job

    def fail(self, job_id: str, error: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.status = "failed"
                job.message = error
                job.workflow_phase = "failed"

    def list_recent(self, limit: int = 20) -> list[dict]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
        return [asdict(j) for j in jobs[:limit]]

    def stats(self) -> dict:
        with self._lock:
            jobs = list(self._jobs.values())
        completed = sum(1 for j in jobs if j.status == "completed")
        running = sum(1 for j in jobs if j.status in {"queued", "running"})
        failed = sum(1 for j in jobs if j.status in {"failed", "blocked"})
        return {
            "total_jobs": len(jobs),
            "completed": completed,
            "active": running,
            "failed": failed,
            "rows_transferred": sum(j.rows_processed for j in jobs),
        }


class JsonFileJobStore(MemoryJobStore):
    """In-memory job store that persists state to a JSON file for resilience.

    Useful in single-node deployments where PostgreSQL is not available and
    the process may restart. Writes are atomic (temp file + rename) and lock
    protected so concurrent in-process mutations stay consistent.
    """

    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._load()

    @property
    def backend(self) -> str:
        return "json_file"

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            if not isinstance(raw, list):
                return
            with self._lock:
                for item in raw:
                    try:
                        job = _job_record_from_dict(item)
                        self._jobs[job.job_id] = job
                    except Exception:
                        pass
        except Exception:
            pass

    def _persist(self) -> None:
        try:
            with self._lock:
                snapshot = [asdict(job) for job in self._jobs.values()]
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(json.dumps(snapshot, indent=2, default=json_default), encoding="utf-8")
            tmp.replace(self._path)
        except Exception:
            pass

    def create(self, **kwargs: Any) -> JobRecord:
        record = super().create(**kwargs)
        self._persist()
        return record

    def get(self, job_id: str) -> JobRecord | None:
        return super().get(job_id)

    def add_checkpoint(self, job_id: str, *, chunk: int, total: int, rows: int) -> None:
        super().add_checkpoint(job_id, chunk=chunk, total=total, rows=rows)
        self._persist()

    def set_workflow_phase(self, job_id: str, phase: str) -> None:
        super().set_workflow_phase(job_id, phase)
        self._persist()

    def set_message(self, job_id: str, message: str) -> None:
        super().set_message(job_id, message)
        self._persist()

    def set_running(self, job_id: str, *, total_rows: int, table_name: str) -> None:
        super().set_running(job_id, total_rows=total_rows, table_name=table_name)
        self._persist()

    def update_progress(self, job_id: str, *, current_chunk: int, total_chunks: int, rows_processed: int) -> None:
        super().update_progress(job_id, current_chunk=current_chunk, total_chunks=total_chunks, rows_processed=rows_processed)
        self._persist()

    def complete(
        self,
        job_id: str,
        rows: int,
        *,
        reconciliation: dict | None = None,
        table_name: str = "",
        driver: str = "",
    ) -> JobRecord | None:
        record = super().complete(job_id, rows, reconciliation=reconciliation, table_name=table_name, driver=driver)
        self._persist()
        return record

    def fail(self, job_id: str, error: str) -> None:
        super().fail(job_id, error)
        self._persist()


class PostgresJobStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @property
    def backend(self) -> str:
        return "postgres"

    def _connect(self):
        import psycopg2

        return psycopg2.connect(self._dsn, connect_timeout=5)

    def _row_to_record(self, row: tuple, checkpoints: list[dict]) -> JobRecord:
        (
            job_id,
            status,
            operation,
            source,
            destination,
            rows_processed,
            total_rows,
            current_chunk,
            total_chunks,
            table_name,
            driver,
            reconciliation,
            workflow_phase,
            created_at,
            message,
        ) = row
        recon = reconciliation
        if isinstance(recon, str):
            recon = json.loads(recon)
        return JobRecord(
            job_id=job_id,
            status=status,
            operation=operation,
            source=source,
            destination=destination,
            rows_processed=rows_processed,
            total_rows=total_rows,
            current_chunk=current_chunk,
            total_chunks=total_chunks,
            table_name=table_name or "",
            driver=driver or "",
            reconciliation=recon,
            checkpoints=checkpoints,
            workflow_phase=workflow_phase,
            created_at=created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
            message=message or "",
        )

    def _fetch_checkpoints(self, cur, job_id: str) -> list[dict]:
        cur.execute(
            """
            SELECT chunk, total, rows, created_at
            FROM job_checkpoints
            WHERE job_id = %s
            ORDER BY id
            """,
            (job_id,),
        )
        return [
            {
                "chunk": r[0],
                "total": r[1],
                "rows": r[2],
                "at": r[3].isoformat() if hasattr(r[3], "isoformat") else str(r[3]),
            }
            for r in cur.fetchall()
        ]

    def create(
        self,
        *,
        operation: str,
        source: str,
        destination: str,
        total_rows: int = 0,
    ) -> JobRecord:
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO jobs (job_id, status, operation, source, destination, total_rows, message)
                        VALUES (%s, 'queued', %s, %s, %s, %s, 'Queued')
                        RETURNING job_id, status, operation, source, destination,
                                  rows_processed, total_rows, current_chunk, total_chunks,
                                  table_name, driver, reconciliation, workflow_phase, created_at, message
                        """,
                        (job_id, operation, source, destination, total_rows),
                    )
                    row = cur.fetchone()
            return self._row_to_record(row, [])
        finally:
            conn.close()

    def get(self, job_id: str) -> JobRecord | None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_id, status, operation, source, destination,
                           rows_processed, total_rows, current_chunk, total_chunks,
                           table_name, driver, reconciliation, workflow_phase, created_at, message
                    FROM jobs WHERE job_id = %s
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                checkpoints = self._fetch_checkpoints(cur, job_id)
            return self._row_to_record(row, checkpoints)
        finally:
            conn.close()

    def add_checkpoint(self, job_id: str, *, chunk: int, total: int, rows: int) -> None:
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO job_checkpoints (job_id, chunk, total, rows)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (job_id, chunk, total, rows),
                    )
        finally:
            conn.close()

    def set_workflow_phase(self, job_id: str, phase: str) -> None:
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE jobs SET workflow_phase = %s WHERE job_id = %s", (phase, job_id))
        finally:
            conn.close()

    def set_message(self, job_id: str, message: str) -> None:
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE jobs SET message = %s WHERE job_id = %s", (message, job_id))
        finally:
            conn.close()

    def set_running(self, job_id: str, *, total_rows: int, table_name: str) -> None:
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'running', total_rows = %s, table_name = %s,
                            message = 'Transfer in progress', workflow_phase = 'transfer'
                        WHERE job_id = %s
                        """,
                        (total_rows, table_name, job_id),
                    )
        finally:
            conn.close()

    def update_progress(self, job_id: str, *, current_chunk: int, total_chunks: int, rows_processed: int) -> None:
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE jobs
                        SET current_chunk = %s, total_chunks = %s, rows_processed = %s
                        WHERE job_id = %s
                        """,
                        (current_chunk, total_chunks, rows_processed, job_id),
                    )
        finally:
            conn.close()

    def complete(
        self,
        job_id: str,
        rows: int,
        *,
        reconciliation: dict | None = None,
        table_name: str = "",
        driver: str = "",
    ) -> JobRecord | None:
        message = reconciliation.get("message", "Completed") if reconciliation else "Completed"
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'completed', rows_processed = %s, total_rows = %s,
                            reconciliation = %s, workflow_phase = 'completed', message = %s,
                            table_name = CASE WHEN %s <> '' THEN %s ELSE table_name END,
                            driver = CASE WHEN %s <> '' THEN %s ELSE driver END
                        WHERE job_id = %s
                        RETURNING job_id, status, operation, source, destination,
                                  rows_processed, total_rows, current_chunk, total_chunks,
                                  table_name, driver, reconciliation, workflow_phase, created_at, message
                        """,
                        (
                            rows,
                            rows,
                            json.dumps(reconciliation) if reconciliation else None,
                            message,
                            table_name,
                            table_name,
                            driver,
                            driver,
                            job_id,
                        ),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    checkpoints = self._fetch_checkpoints(cur, job_id)
            return self._row_to_record(row, checkpoints)
        finally:
            conn.close()

    def fail(self, job_id: str, error: str) -> None:
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE jobs SET status = 'failed', message = %s, workflow_phase = 'failed'
                        WHERE job_id = %s
                        """,
                        (error, job_id),
                    )
        finally:
            conn.close()

    def list_recent(self, limit: int = 20) -> list[dict]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_id, status, operation, source, destination,
                           rows_processed, total_rows, current_chunk, total_chunks,
                           table_name, driver, reconciliation, workflow_phase, created_at, message
                    FROM jobs
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
                result = []
                for row in rows:
                    checkpoints = self._fetch_checkpoints(cur, row[0])
                    result.append(asdict(self._row_to_record(row, checkpoints)))
                return result
        finally:
            conn.close()

    def stats(self) -> dict:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM jobs")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM jobs WHERE status = 'completed'")
                completed = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')")
                active = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('failed', 'blocked')")
                failed = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(SUM(rows_processed), 0) FROM jobs")
                rows_transferred = cur.fetchone()[0]
            return {
                "total_jobs": total,
                "completed": completed,
                "active": active,
                "failed": failed,
                "rows_transferred": int(rows_transferred),
            }
        finally:
            conn.close()


def _seed_demo_jobs(store: MemoryJobStore) -> None:
    demos = [
        ("upload", "payments_q4.csv", "postgresql://prod/analytics", 2100000),
        ("migration", "postgresql://legacy/hr", "postgresql://cloud/hr_v2", 890000),
    ]
    for op, src, dst, rows in demos:
        rec = store.create(operation=op, source=src, destination=dst, total_rows=rows)
        store.complete(rec.job_id, rows, reconciliation={"passed": True, "message": "Demo job"})


def create_job_store() -> MemoryJobStore | PostgresJobStore | JsonFileJobStore:
    from services import db
    from services.config import get_settings, settings
    from services.platform_config import data_dir

    backend = settings.job_store_backend.lower()
    persist = get_settings().job_store_persist
    path = data_dir() / "jobs.json"

    if backend in ("file", "json"):
        store: MemoryJobStore | PostgresJobStore | JsonFileJobStore = JsonFileJobStore(path)
        logger.info("Job store: json-file (%s)", path)
    elif backend == "memory":
        if persist:
            store = JsonFileJobStore(path)
            logger.info("Job store: memory with file persistence (%s)", path)
        else:
            store = MemoryJobStore()
            logger.info("Job store: in-memory")
    elif backend == "postgres":
        db.init_schema(settings.database_url)
        store = PostgresJobStore(settings.database_url)
        logger.info("Job store: PostgreSQL")
    else:
        if db.ping(settings.database_url):
            try:
                db.init_schema(settings.database_url)
                store = PostgresJobStore(settings.database_url)
                logger.info("Job store: PostgreSQL (auto)")
            except Exception as exc:
                logger.warning("PostgreSQL unavailable, using file: %s", exc)
                store = JsonFileJobStore(path)
        else:
            if persist:
                store = JsonFileJobStore(path)
                logger.info("Job store: json-file (auto, Postgres unreachable)")
            else:
                store = MemoryJobStore()
                logger.info("Job store: in-memory (Postgres unreachable)")

    if isinstance(store, (MemoryJobStore, JsonFileJobStore)) and store.stats()["total_jobs"] == 0:
        if get_settings().seed_demo_jobs:
            _seed_demo_jobs(store)

    return store


job_store = create_job_store()
