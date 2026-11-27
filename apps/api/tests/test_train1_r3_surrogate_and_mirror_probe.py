"""Review 3 item 7.

(a) ``surrogate_approved`` came from the run's general ``ddl_changes_acknowledged``,
so a run acknowledging any DDL change let a ``_dw_row_id`` column on someone
else's table pass as Datawrap's approved surrogate. It is now "the live
``_dw_row_id`` is on a table Datawrap created" (the writer adds it only at
CREATE, after its own approval).

(b) The mirror key-staging probe read a cursor it could not open as "this
server does not force a PRIMARY KEY"; it is now unreadable, like the probe.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.dest_require_primary_key import PrimaryKeyRequirementUnreadable
from services.mirror_engine import _forces_primary_key
from src.transfer import engine_identity

_MAPS = [
    {"source": "id", "target": "id", "source_type": "BIGINT", "target_type": "BIGINT", "create_new": True},
    {"source": "v", "target": "v", "source_type": "TEXT", "target_type": "TEXT", "create_new": True},
]


def _run(monkeypatch, *, managed: bool, acknowledged: bool):
    from services.decision_kernel.execute_gate import build_artifact_from_mappings

    monkeypatch.setattr("src.transfer.engine_schema_propagation._destination_is_managed",
                        lambda request: managed)
    art = build_artifact_from_mappings(
        _MAPS, dest_db="postgresql", source_db="postgresql", source_fingerprint="",
        dest_fingerprint="", sync_mode="incremental_append", route_id="validate:postgresql",
        artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00",
    )
    request = SimpleNamespace(ddl_changes_acknowledged=acknowledged, workspace_id="",
                              destination=SimpleNamespace(connector_id="", table="t"),
                              approved_ddl_identity_hash="", schema_policy="")
    live = {"id": "bigint", "v": "text", "_dw_row_id": "bigint"}
    return engine_identity._enforce_decision_artifact_once(
        None, _MAPS, dest_db="postgresql", approved_decision_artifact_hash=art.content_hash,
        decision_artifact=art.to_dict(), sync_mode="incremental_append",
        destination_column_types=live, destination_table_exists=True,
        source_format="postgresql", plan_created_destination=True, request=request,
    )


def test_a_dw_row_id_on_someone_elses_table_is_drift_even_when_ddl_was_acknowledged(monkeypatch):
    err, art = _run(monkeypatch, managed=False, acknowledged=True)
    assert err and "drifted" in err.lower(), err
    assert art is None


def test_the_surrogate_on_datawraps_own_table_runs_without_a_new_acknowledgement(monkeypatch):
    err, art = _run(monkeypatch, managed=True, acknowledged=False)
    assert err is None, err
    assert art is not None


def test_a_cursor_that_cannot_open_is_unreadable_not_unforced():
    class _Raw:
        def cursor(self):
            raise OSError("connection reset")

    with pytest.raises(PrimaryKeyRequirementUnreadable):
        _forces_primary_key(SimpleNamespace(connection=_Raw()))
