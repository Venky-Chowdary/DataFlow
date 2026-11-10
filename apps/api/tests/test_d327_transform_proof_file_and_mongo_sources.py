"""D327 — the transform proof for file and document sources, in plain words.

QA: an SFTP CSV → SQL Server job with six column transforms ended "Row fidelity
verified … (transform proof unverifiable — expected-output recompute failed:
SQLAlchemy dialect/driver for 'sftp' is not available …)": the proof built a
SQLAlchemy engine for a file source, never proved the transforms, and put the
driver text in the job message. A Mongo source reported "source changed during
run — 0 of 0 keys changed in source during run; 0 proven" on an untouched
collection.

Now an SFTP / object-store file is re-read through the engine's own batch
reader (the reader the write used) and the recipe is proven against it; a
source the proof cannot re-read says so plainly; no driver/dialect text and no
"0 of 0 keys" reach the message.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)

# Six transforms on a five-row file, as QA ran it: two pipelines, four singles.
_TRANSFORMS = {
    "name": ["trim", "upper"],
    "email": ["trim", "lower"],
    "status": "replace:active:ACT",
    "note": "default:NA",
    "code": "upper",
    "city": "trim",
}
_COLS = ["id", "name", "email", "status", "note", "code", "city"]
_CSV = (
    "id,name,email,status,note,code,city\n"
    "1,  ada ,  Ada@X.io ,active,x,ab,  Paris \n"
    "2,bob,BOB@y.io,inactive,,cd,Lyon\n"
    "3,Zoë,zoe@z.io,active,y,ef, Köln\n"
    "4,  ,e@e.io,active,,gh,Nice\n"
    "5,eve,EVE@w.io,closed,z,ij,Rome \n"
)


def _request(source, dest, cols=_COLS, transforms=_TRANSFORMS, **extra) -> TransferRequest:
    from services.column_transform_chain import compile_column_chains, split_column_transforms
    from services.shape_models import ShapeRecipe

    singles, chains = split_column_transforms(transforms)
    recipe, refusal = compile_column_chains(chains, cols)
    assert not refusal, refusal
    mappings = []
    for c in cols:
        m = {"source": c, "target": c, "approved": True, "confidence": 0.99}
        if c in singles:
            m.update(transform=singles[c], transform_override=True)
        mappings.append(m)
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=mappings,
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=ShapeRecipe.parse(recipe).recipe_hash,
        skip_preflight=True,
        **extra,
    )


@pytest.fixture()
def fake_jobs(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d327" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _sqlite(path: Path) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table="t")


def _sftp_source(local_sftp, name: str) -> EndpointConfig:
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    Path(local_sftp.local_path(f"/{name}")).write_text(_CSV, encoding="utf-8")
    cfg = local_sftp.endpoint_config(f"/{name}")
    return EndpointConfig(
        kind="database",
        format="sftp",
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        table=cfg["table"],
        extra={"host_key": cfg["host_key"]},
    )


def _no_internal_text(result) -> str:
    recon = result.reconciliation or {}
    blob = f"{recon.get('message') or ''} {result.error or ''}"
    for leaked in ("SQLAlchemy", "dialect/driver", "0 of 0 keys", "Traceback"):
        assert leaked not in blob, blob
    return blob


def test_sftp_csv_transforms_are_proven_from_the_file(tmp_path, fake_jobs, local_sftp):
    src = _sftp_source(local_sftp, f"d327_{uuid.uuid4().hex[:6]}.csv")
    result = _run(fake_jobs, _request(src, _sqlite(tmp_path / "d.sqlite")))

    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert proof.get("rows") == 5, proof
    assert "transform output verified" in _no_internal_text(result)


def test_sftp_destination_without_the_recipe_output_fails(
    tmp_path, fake_jobs, local_sftp, monkeypatch
):
    """The file proof is a real check: a wrong destination row is caught."""
    import src.transfer.reconcile_step as rs

    src = _sftp_source(local_sftp, f"d327_{uuid.uuid4().hex[:6]}.csv")
    dst = tmp_path / "d.sqlite"
    original = rs._transform_applied_proof

    def _untransformed_row_then_prove(**kwargs):
        # Row 1 lands exactly as the file holds it: the recipe did not run.
        conn = sqlite3.connect(str(dst))
        with conn:
            conn.execute(
                "UPDATE t SET name = '  ada ', email = '  Ada@X.io ', "
                "status = 'active', note = 'x', code = 'ab', city = '  Paris ' "
                "WHERE id = 1"
            )
        conn.close()
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _untransformed_row_then_prove)
    result = _run(fake_jobs, _request(src, _sqlite(dst)))

    assert not result.success
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "failed", proof
    _no_internal_text(result)


def test_keyless_file_divergence_does_not_claim_the_source_changed(
    tmp_path, fake_jobs, local_sftp, monkeypatch
):
    """Without a key the proof cannot tell drift from a wrong value: it says so."""
    import src.transfer.reconcile_step as rs

    src = _sftp_source(local_sftp, f"d327_{uuid.uuid4().hex[:6]}.csv")
    dst = tmp_path / "d.sqlite"
    original = rs._transform_applied_proof

    def _tamper_then_prove(**kwargs):
        conn = sqlite3.connect(str(dst))
        with conn:
            conn.execute("UPDATE t SET name = 'ada' WHERE id = 1")
        conn.close()
        return original(**kwargs)

    monkeypatch.setattr(rs, "_transform_applied_proof", _tamper_then_prove)
    result = _run(fake_jobs, _request(src, _sqlite(dst)))

    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "unverifiable", proof
    reason = str(proof.get("reason"))
    assert "source changed during run" not in reason, proof
    assert "not proven" in reason, proof
    _no_internal_text(result)


def test_mongo_source_transforms_are_proven(tmp_path, fake_jobs):
    pymongo = pytest.importorskip("pymongo")
    from tests.typed_fidelity_helpers import mongo_endpoint, reachable

    if not reachable("localhost", 27017):
        pytest.skip("MongoDB not running on :27017")
    name = f"d327_{uuid.uuid4().hex[:8]}"
    coll = pymongo.MongoClient("localhost", 27017, serverSelectionTimeoutMS=3000)["dataflow"][name]
    coll.insert_many(
        [
            {"_id": 1, "name": "  Ada ", "status": "active", "note": ""},
            {"_id": 2, "name": "bob", "status": "inactive", "note": None},
            {"_id": 3, "name": "Zoë", "status": "active", "note": "x"},
        ]
    )
    try:
        result = _run(
            fake_jobs,
            _request(
                mongo_endpoint(name),
                _sqlite(tmp_path / "d.sqlite"),
                cols=["_id", "name", "status", "note"],
                transforms={
                    "name": ["trim", "upper"],
                    "status": "replace:active:ACT",
                    "note": "default:NA",
                },
            ),
        )
    finally:
        coll.drop()
    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "verified", proof
    assert "source changed during run" not in _no_internal_text(result)


def test_keyless_count_divergence_names_no_phantom_key_counts(tmp_path, fake_jobs, monkeypatch):
    """No pinned keys → no "0 of 0 keys changed in source during run"."""
    import src.transfer.reconcile_step as rs

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (code TEXT, name TEXT)")  # no key at all
        conn.executemany("INSERT INTO t VALUES (?, ?)", [("a", " x "), ("b", "y")])
    conn.close()

    def _reread_with_an_extra_row(**_kw):
        yield [{"code": "a", "name": " x "}, {"code": "b", "name": "y"}, {"code": "c", "name": "z"}]

    monkeypatch.setattr(rs, "_proof_source_batches", _reread_with_an_extra_row)
    result = _run(
        fake_jobs,
        _request(_sqlite(src), _sqlite(dst), cols=["code", "name"], transforms={"name": ["trim", "upper"]}),
    )
    assert result.success, result.error  # an unproven recipe never rolls back correct data
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "unverifiable", proof
    reason = str(proof.get("reason"))
    assert "0 of 0" not in reason and "keys changed" not in reason, proof
    assert "gives 3 rows" in reason and "holds 2" in reason, proof


def test_a_failed_reread_is_reported_without_driver_text(tmp_path, fake_jobs, monkeypatch):
    import src.transfer.reconcile_step as rs

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, " x "), (2, "y")])
    conn.close()

    def _broken(**_kw):
        raise RuntimeError(
            "SQLAlchemy dialect/driver for 'sftp' is not available (tried 'sftp')."
        )
        yield  # pragma: no cover

    monkeypatch.setattr(rs, "_proof_source_batches", _broken)
    result = _run(
        fake_jobs,
        _request(_sqlite(src), _sqlite(dst), cols=["id", "name"], transforms={"name": ["trim", "upper"]}),
    )
    assert result.success, result.error
    proof = (result.reconciliation or {}).get("transform_applied") or {}
    assert proof.get("status") == "unverifiable", proof
    assert "not proven" in str(proof.get("reason")), proof
    _no_internal_text(result)


@pytest.mark.parametrize(
    "endpoint,src_type,what",
    [
        (EndpointConfig(kind="file", format="csv"), "csv", "uploaded or local file"),
        (EndpointConfig(kind="database", format="dynamodb"), "dynamodb", "dynamodb document"),
        (EndpointConfig(kind="database", format="kafka"), "kafka", "kafka document or stream"),
    ],
)
def test_sources_the_proof_cannot_reread_say_so(endpoint, src_type, what):
    from src.transfer.proof_api_source import proof_unavailable_reason

    reason = proof_unavailable_reason(endpoint, src_type) or ""
    assert reason.startswith("transform proof not available for"), reason
    assert what.split()[0] in reason and "not proven" in reason, reason


def test_sftp_and_object_files_reread_through_the_writer_reader():
    from src.transfer.proof_api_source import is_reader_source, proof_unavailable_reason

    for kind in ("sftp", "s3", "gcs", "adls"):
        assert is_reader_source(kind)
        assert proof_unavailable_reason(EndpointConfig(kind="database", format=kind), kind) is None
    assert not is_reader_source("postgresql")
