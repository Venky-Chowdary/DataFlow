"""Model/index artifacts must not be able to execute arbitrary code."""

from __future__ import annotations

import pickle
from pathlib import Path

import pytest

from services.safe_pickle import UnsafePickleError, file_digest, load_restricted


class _Evil:
    def __reduce__(self):
        import os

        return (os.system, ("echo pwned",))


def test_blocked_module_is_refused(tmp_path: Path):
    art = tmp_path / "evil.pkl"
    art.write_bytes(pickle.dumps(_Evil()))
    with pytest.raises(UnsafePickleError):
        load_restricted(art, allowed_modules={"ml"})


def test_plain_container_loads_without_any_allowed_module(tmp_path: Path):
    art = tmp_path / "index.pkl"
    payload = {"cust_id": "customer_id", "amt": "amount"}
    art.write_bytes(pickle.dumps(payload))
    assert load_restricted(art, allowed_modules=frozenset()) == payload


def test_digest_sidecar_mismatch_fails_closed(tmp_path: Path):
    art = tmp_path / "index.pkl"
    art.write_bytes(pickle.dumps({"a": "b"}))
    (tmp_path / "index.pkl.sha256").write_text("0" * 64)
    with pytest.raises(UnsafePickleError) as exc:
        load_restricted(art, allowed_modules=frozenset())
    assert "digest mismatch" in str(exc.value)


def test_matching_digest_sidecar_loads(tmp_path: Path):
    art = tmp_path / "index.pkl"
    art.write_bytes(pickle.dumps({"a": "b"}))
    (tmp_path / "index.pkl.sha256").write_text(file_digest(art))
    assert load_restricted(art, allowed_modules=frozenset()) == {"a": "b"}


def test_require_digest_refuses_unpinned_artifact(tmp_path: Path):
    art = tmp_path / "index.pkl"
    art.write_bytes(pickle.dumps({"a": "b"}))
    with pytest.raises(UnsafePickleError):
        load_restricted(art, allowed_modules=frozenset(), require_digest=True)


def test_no_executable_pickle_artifacts_are_shipped():
    """The repo must not ship .pkl model artifacts — JSON vocabularies only."""
    root = Path(__file__).resolve().parents[3]
    shipped = [
        p
        for p in (root / "packages").rglob("*.pkl")
        if ".venv" not in p.parts and "node_modules" not in p.parts
    ]
    assert shipped == [], f"executable pickle artifacts shipped: {shipped}"


def test_digest_helper_is_stable(tmp_path: Path):
    art = tmp_path / "a.pkl"
    art.write_bytes(pickle.dumps({"a": "b"}))
    assert file_digest(art) == file_digest(art)
