"""D381 — the certificate, the parity report and the job message agree.

QA job 6afbac50 (PostgreSQL -> MySQL overwrite, no transforms): 390/390 cells
exact, parity "proven", message "Row fidelity verified", certificate
NOT PROVEN with "source re-read identity/hash alignment is required" and
"connector_versions lack a captured release". The PR made parity quote the
certificate and say "unverifiable", so a run whose two full populations were
re-read and digested equal was reported as unverifiable (R9: a verifiable run
must not become unverifiable).

Root cause: the engine-copy route earns full_checksum at verify time (both
populations re-read and digested with one algorithm) but stamped neither the
connector releases nor the identity alignment, which the row path stamps.
Overwrite replaces the destination with exactly what this run wrote, so a
matching verify-time digest of the whole source and the whole destination is
that alignment.

Live: MCP start_transfer -> confirm, PostgreSQL -> MySQL, then the three
surfaces over HTTP.
"""

from __future__ import annotations

import uuid

import pytest

import services.config  # noqa: F401,E402 - settings parse before the harness blanks env

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r
from tests.test_d347_propagate_columns_mirror_scd2 import client  # noqa: F401
from tests.test_r10_fidelity_run2_e2e import _start

pytestmark = pytest.mark.timeout(600)


def test_a_verified_overwrite_is_proven_on_every_surface(client):  # noqa: F811
    token = uuid.uuid4().hex[:8]
    src, dst = f"d381s_{token}", f"d381d_{token}"
    r.pg(
        f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, name varchar(20), '
        "amt numeric(10,2), ts timestamp)"
    )
    r.pg(
        f'INSERT INTO public."{src}" SELECT g, %s || g, g * 1.5, '  # nosec B608
        "timestamp '2026-01-01' + g * interval '1 minute' FROM generate_series(1, 30) g",
        ("n",),
    )
    src_id = r.connector("source", r.pg_cfg())
    dst_id = r.connector("destination", r.mysql_family("mysql"))
    try:
        job = _start(
            client, source_connector_id=src_id, source_table=src,
            dest_connector_id=dst_id, dest_table=dst,
            sync_mode="full_refresh_overwrite", validation_mode="strict",
        )
        job_id = str(job.get("id") or job.get("_id") or job.get("job_id"))
        cert = client.get(f"/api/v1/transfer/{job_id}/certificate").json()
        parity = client.get(f"/api/v1/jobs/{job_id}/parity-report").json()
    finally:
        h.drop_table("postgresql", src)
        r.mysql_execute("mysql", f"DROP TABLE IF EXISTS `{dst}`")

    assert str(job.get("status")) == "completed", job.get("error")
    assert "digests match (full_checksum)" in str(job.get("message") or "")
    verdict = cert.get("verdict") or {}
    assert verdict.get("migration_proven") is True, verdict
    assert verdict.get("headline") == "MIGRATION PROVEN", verdict
    assert parity.get("verdict") == "proven", parity.get("verdict_reason")


def test_a_mismatched_or_appended_digest_earns_no_alignment() -> None:
    """Not weakened: alignment is stamped only where it is implied."""
    from src.transfer.reconcile_step import engine_copy_identity_alignment

    assert engine_copy_identity_alignment(
        {"checksum_match": True}, {"sync_mode": "full_refresh_overwrite"}
    )["identity_hash_aligned"] is True
    # A mismatch proves nothing.
    assert engine_copy_identity_alignment(
        {"checksum_match": False}, {"sync_mode": "full_refresh_overwrite"}
    ) is None
    # An append into rows that were already there: the destination population
    # is not only this run's write.
    assert engine_copy_identity_alignment(
        {"checksum_match": True, "target_rows_before": 5},
        {"sync_mode": "incremental_append"},
    ) is None
    assert engine_copy_identity_alignment(
        {"checksum_match": True, "target_rows_before": 0},
        {"sync_mode": "incremental_append"},
    )["identity_hash_aligned"] is True
