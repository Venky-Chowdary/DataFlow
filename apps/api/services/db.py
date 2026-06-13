"""PostgreSQL schema bootstrap for platform metadata."""

from __future__ import annotations

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    operation TEXT NOT NULL,
    source TEXT NOT NULL,
    destination TEXT NOT NULL,
    rows_processed INT NOT NULL DEFAULT 0,
    total_rows INT NOT NULL DEFAULT 0,
    current_chunk INT NOT NULL DEFAULT 0,
    total_chunks INT NOT NULL DEFAULT 0,
    table_name TEXT NOT NULL DEFAULT '',
    driver TEXT NOT NULL DEFAULT '',
    reconciliation JSONB,
    workflow_phase TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    message TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS job_checkpoints (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    chunk INT NOT NULL,
    total INT NOT NULL,
    rows INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_checkpoints_job_id ON job_checkpoints (job_id);
"""


def init_schema(dsn: str) -> None:
    import psycopg2

    conn = psycopg2.connect(dsn, connect_timeout=5)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
    finally:
        conn.close()


def ping(dsn: str) -> bool:
    try:
        import psycopg2

        conn = psycopg2.connect(dsn, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False
