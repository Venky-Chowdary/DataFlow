"""Audit findings 2 + 7 — a fingerprint rule change must not refuse appends.

The append continuity ledger stores a digest of the destination after each
verified run and compares it before the next append. Where the engine has no
in-engine digest that stored digest is the Gate-8 Python fingerprint, so a
change of fingerprint rules makes every stored baseline differ from a fresh
read of an *unchanged* table: "the destination changed since the last verified
run", for every table, on the first run after the upgrade.

A baseline of a retired fingerprint version is therefore never compared with a
fresh digest. The row count (version independent) is still enforced; the
digest is re-read and recorded under the current version, audited.
"""

from __future__ import annotations

from typing import Any

import pytest

from services import append_continuity as ac
from services import continuity_digest as cd
from services.fingerprint_rules import FINGERPRINT_VERSION
from tests.test_x97_append_continuity import (  # noqa: F401 — fixture re-export
    _add_source_rows,
    _Dest,
    _key,
    _runner,
    _source,
    fake_store,
)


def _audit_spy(monkeypatch) -> list[dict[str, Any]]:
    import services.audit_log as audit_log

    events: list[dict[str, Any]] = []
    real = audit_log.append_audit_event

    def spy(**kw):
        if str(kw.get("action", "")).startswith("append_continuity."):
            events.append(kw)
            return kw
        return real(**kw)

    monkeypatch.setattr(audit_log, "append_audit_event", spy)
    return events


def _store_retired_baseline(dest: _Dest, *, with_algo: bool) -> dict[str, Any]:
    """What a build before fingerprint version 2 left in the store."""
    key = _key(dest)
    rows = int(ac.load_baseline(key)["rows"])
    fp: dict[str, Any] = {"rows": rows, "checksum": "f" * 64}
    if with_algo:
        fp["algo"] = "gate8_python_v0"
    ac.save_baseline(key, fp, "pre-upgrade-job")
    stored = ac.load_baseline(key)
    assert stored["algo"] == ac.LEGACY_ALGO == "gate8_python_v0"
    assert ac.is_retired_algo(stored["algo"])
    return stored


@pytest.fixture
def python_digest_engine(monkeypatch):
    """An engine without an in-engine digest keeps the Gate-8 Python baseline."""
    monkeypatch.setattr(cd, "SUPPORTED", frozenset(cd.SUPPORTED - {"sqlite"}))


def test_gate8_algo_name_carries_the_fingerprint_version():
    assert ac.GATE8_ALGO == f"gate8_python_fp{FINGERPRINT_VERSION}"
    assert not ac.is_retired_algo(ac.GATE8_ALGO)
    assert not ac.is_retired_algo(cd.ALGO)
    assert ac.is_retired_algo(None) and ac.is_retired_algo("gate8_python_v0")
    assert ac.is_retired_algo("gate8_python_fp1")


@pytest.mark.parametrize("with_algo", [True, False])
def test_retired_baseline_on_unchanged_destination_is_rebaselined_not_refused(
    with_algo, tmp_path, fake_store, monkeypatch, python_digest_engine
):
    events = _audit_spy(monkeypatch)
    dest = _Dest("sqlite", tmp_path)
    src = _source(tmp_path, pk=True)
    run = _runner(fake_store, src, dest, pk=True)
    assert run().success
    _store_retired_baseline(dest, with_algo=with_algo)
    _add_source_rows(src, 11, 13)

    second = run()

    assert second.success, second.error
    base = ac.load_baseline(_key(dest))
    assert base["algo"] == ac.GATE8_ALGO and base["rows"] == 12
    assert base["fingerprint_version"] == FINGERPRINT_VERSION
    moved = [e for e in events if e["action"] == "append_continuity.fingerprint_version_rebaselined"]
    assert len(moved) == 1
    details = moved[0]["details"]
    assert details["from_algo"] == "gate8_python_v0"
    assert details["to_algo"] == ac.GATE8_ALGO
    assert details["values_reproven"] is False and details["verified_rows"] == 10
    # The run after that compares digest with digest again, under one version.
    _add_source_rows(src, 13, 15)
    third = run()
    assert third.success, third.error
    assert len([e for e in events if "fingerprint_version" in e["action"]]) == 1


def test_retired_baseline_still_refuses_a_changed_row_count(
    tmp_path, fake_store, monkeypatch, python_digest_engine
):
    events = _audit_spy(monkeypatch)
    dest = _Dest("sqlite", tmp_path)
    src = _source(tmp_path, pk=True)
    run = _runner(fake_store, src, dest, pk=True)
    assert run().success
    stored = _store_retired_baseline(dest, with_algo=True)
    dest.tamper_one("delete")
    before = dest.snapshot()
    _add_source_rows(src, 11, 13)

    second = run()

    assert not second.success
    assert second.error_details["mismatch"] == "row_count"
    assert dest.snapshot() == before
    assert ac.load_baseline(_key(dest)) == stored
    assert not events


def test_retired_baseline_moves_to_the_engine_digest_where_one_exists(
    tmp_path, fake_store, monkeypatch
):
    events = _audit_spy(monkeypatch)
    dest = _Dest("sqlite", tmp_path)
    src = _source(tmp_path, pk=True)
    run = _runner(fake_store, src, dest, pk=True)
    assert run().success
    _store_retired_baseline(dest, with_algo=True)
    _add_source_rows(src, 11, 13)
    second = run()
    assert second.success, second.error
    assert ac.load_baseline(_key(dest))["algo"] == cd.ALGO
    assert [e["action"] for e in events] == [
        "append_continuity.fingerprint_version_rebaselined"
    ]


def test_current_python_baseline_still_catches_a_same_count_value_change(
    tmp_path, fake_store, python_digest_engine
):
    dest = _Dest("sqlite", tmp_path)
    src = _source(tmp_path, pk=True)
    run = _runner(fake_store, src, dest, pk=True)
    assert run().success
    assert ac.load_baseline(_key(dest))["algo"] == ac.GATE8_ALGO
    dest.tamper_one("cell")
    _add_source_rows(src, 11, 13)
    second = run()
    assert not second.success
    assert second.error_details["mismatch"] == "checksum"
