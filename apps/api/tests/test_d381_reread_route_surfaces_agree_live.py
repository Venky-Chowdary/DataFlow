"""D381 — certificate, parity report and job message agree on the re-read route.

The engine-copy overwrite was fixed earlier (``test_d381_trust_surfaces_agree_live``).
A PG → MySQL overwrite with a value transform takes the row path instead: it
re-reads the source on purpose and does not fingerprint the write pass, so its
identity alignment was *unmeasured* (``None``). The job said "Row fidelity
verified — source and target checksums match", the parity report
"unverifiable", the certificate "NOT PROVEN — source re-read identity/hash
alignment is required". When the run replaced the destination with its write
and the whole-population digests of the independent source re-read and the
destination match, that is the alignment, as on the engine-copy route.

Not weakened: a mirror through a staging table whose source pass was an
engine copy (count only) re-reads *staging*, not the source; it earns no
alignment and every surface keeps saying it is not proven.

Live: MCP start_transfer -> confirm, PostgreSQL -> MySQL, three surfaces over HTTP.
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


def _surfaces(client, **kwargs):  # noqa: F811
    token = uuid.uuid4().hex[:8]
    src, dst = f"d381r_{token}", f"d381rd_{token}"
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
            dest_connector_id=dst_id, dest_table=dst, validation_mode="strict", **kwargs,
        )
        job_id = str(job.get("id") or job.get("_id") or job.get("job_id"))
        cert = client.get(f"/api/v1/transfer/{job_id}/certificate").json()
        parity = client.get(f"/api/v1/jobs/{job_id}/parity-report").json()
        from services.mongodb_service import get_mongodb_service

        recon = (get_mongodb_service().get_job(job_id) or {}).get("reconciliation") or {}
        upper = r.mysql_execute("mysql", f"SELECT name FROM `{dst}` WHERE id = 7")  # nosec B608
    finally:
        h.drop_table("postgresql", src)
        r.mysql_execute("mysql", f"DROP TABLE IF EXISTS `{dst}`")
    return job, cert.get("verdict") or {}, parity, recon, upper


def test_a_verified_transformed_overwrite_is_proven_on_every_surface(client):  # noqa: F811
    job, verdict, parity, recon, upper = _surfaces(
        client, sync_mode="full_refresh_overwrite", column_transforms={"name": "upper"},
    )
    assert str(job.get("status")) == "completed", job.get("error")
    assert upper and upper[0][0] == "N7", upper  # the transform landed
    assert "checksums match" in str(job.get("message") or "")
    assert recon.get("identity_hash_aligned") is True, recon.get("identity_alignment")
    assert verdict.get("migration_proven") is True, verdict
    assert verdict.get("headline") == "MIGRATION PROVEN", verdict
    assert parity.get("verdict") == "proven", parity.get("verdict_reason")


def test_a_staged_mirror_without_a_source_reread_is_not_proven_anywhere(client):  # noqa: F811
    job, verdict, parity, recon, _upper = _surfaces(
        client, sync_mode="full_refresh_mirror", primary_key="id",
    )
    assert str(job.get("status")) == "completed", job.get("error")
    assert recon.get("identity_hash_aligned") is not True, recon.get("identity_alignment")
    assert verdict.get("migration_proven") is not True, verdict
    assert parity.get("verdict") != "proven", parity.get("verdict_reason")
