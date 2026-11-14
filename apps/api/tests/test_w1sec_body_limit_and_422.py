"""SEC-8: JSON bodies have a size limit, and a 422 never echoes the input.

A 50 MB unauthenticated body was parsed in full before its 422, a 60 MB plan
was stored, a 10 MB login body came back as a 10 MB 422, and
``POST /preflight/run`` echoed ``source.password`` in its validation error.
"""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from tests.tenancy_world import tenancy_world  # noqa: F401

PW = "w1secPW_422_s3ntinel"


def test_unauthenticated_oversized_body_is_413(tenancy_world):  # noqa: F811
    anon = TestClient(tenancy_world.app)
    blob = b'{"email":"' + b"a" * 2_000_000 + b'","password":"x"}'
    for path in ("/api/v1/auth/login", "/api/v1/contact-sales"):
        r = anon.post(path, content=blob, headers={"Content-Type": "application/json"})
        assert r.status_code == 413, (path, r.status_code, r.text[:200])
        assert len(r.content) < 1000


def test_authenticated_oversized_body_is_413_and_nothing_is_stored(tenancy_world):  # noqa: F811
    from services import transfer_plan_store

    blob = json.dumps({"name": "w1sec-big", "source_columns": ["id"], "target_columns": ["id"],
                       "sample_rows": [{"id": "x" * 1000} for _ in range(10_000)]})  # ~10 MB
    r = tenancy_world.a.post("/api/v1/transfer/plans", content=blob, headers={"Content-Type": "application/json"})
    assert r.status_code == 413, r.text[:200]
    with tenancy_world.as_workspace(tenancy_world.workspace_a):
        assert not [p for p in transfer_plan_store.list_plans(limit=500) if p.name == "w1sec-big"]
    # A normal body still goes through.
    ok = tenancy_world.a.post("/api/v1/transfer/plans", json={"name": "w1sec-small", "source_columns": ["id"],
                                                              "target_columns": ["id"]})
    assert ok.status_code == 200, ok.text[:200]


def test_a_chunked_body_is_limited_too(tenancy_world):  # noqa: F811
    anon = TestClient(tenancy_world.app)

    def gen():
        yield b'{"email":"'
        for _ in range(40):
            yield b"a" * 65536
        yield b'","password":"x"}'

    r = anon.post("/api/v1/auth/login", content=gen(), headers={"Content-Type": "application/json"})
    assert r.status_code == 413, r.text[:200]


def test_validation_errors_never_echo_the_input(tenancy_world):  # noqa: F811
    anon = TestClient(tenancy_world.app)
    r = anon.post("/api/v1/auth/login", json={"email": ["not", "a", "string"], "password": PW})
    assert r.status_code == 422 and PW not in r.text and '"input"' not in r.text, r.text[:300]
    src = {"kind": "database", "format": "postgresql", "host": "h.example.com", "password": PW,
           "port": "not-a-port-" + PW}
    r = tenancy_world.a.post("/api/v1/preflight/run", json={"source": src, "mappings": "nope"})
    assert r.status_code == 422, r.text[:300]
    assert PW not in r.text and '"input"' not in r.text, r.text[:300]
    assert r.json()["detail"][0]["loc"], r.text[:300]
