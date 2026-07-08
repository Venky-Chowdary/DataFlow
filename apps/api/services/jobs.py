"""Job registry with checkpoint progress and reconciliation."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)


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

    def set_workflow_phase(self, job_id: str, phase: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.workflow_phase = phase

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


def create_job_store() -> MemoryJobStore | PostgresJobStore:
    from services.config import settings
    from services import db

    backend = settings.job_store_backend.lower()
    if backend == "memory":
        store: MemoryJobStore | PostgresJobStore = MemoryJobStore()
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
                logger.warning("PostgreSQL unavailable, using memory: %s", exc)
                store = MemoryJobStore()
        else:
            store = MemoryJobStore()
            logger.info("Job store: in-memory (Postgres unreachable)")

    if isinstance(store, MemoryJobStore) and store.stats()["total_jobs"] == 0:
        from services.config import get_settings
        if get_settings().seed_demo_jobs:
            _seed_demo_jobs(store)

    return store


job_store = create_job_store()
