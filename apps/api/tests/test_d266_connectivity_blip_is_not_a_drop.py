"""D266 — a transient connection failure must not be read as a dropped table.

Prod repro: Oracle -> Timescale incremental_append rerun during a tunnel renewal
failed "the table no longer exists ... It was dropped, or this run points at a
different database" while the table sat there with its 6 rows; the identical
rerun a minute later completed.

Root cause: ``resolve_object_identity`` returns ``exists=False, resolved=False``
when the catalog cannot be read, and ``dest_object_missing`` returned
``not ident.exists`` — so "could not ask" became "gone".
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import pytest  # noqa: E402

from services import append_continuity as ac  # noqa: E402
from services.preflight_cursor_gate import dest_object_missing  # noqa: E402

_BASELINE = {"rows": 6, "checksum": "c" * 64, "algo": "engine_v1", "job_id": "6ae98f40", "recorded_at": "t"}
_REFUSED_PG = {
    "type": "postgresql", "host": "127.0.0.1", "port": 1, "database": "x",
    "username": "u", "password": "p",
}


def test_connection_refused_is_unknown_not_missing():
    # Port 1 refuses at once: the catalog is unreadable, the table is not "gone".
    assert dest_object_missing("postgresql", _REFUSED_PG, "pv_ora_ts_ia") is None


def test_unresolved_identity_is_unknown(monkeypatch):
    import connectors.generic_sql as generic_sql
    import services.sql_object_identity as soi

    monkeypatch.setattr(generic_sql, "_engine", lambda cfg: object())
    monkeypatch.setattr(
        soi, "resolve_object_identity",
        lambda engine, table, schema=None, **kw: soi.ObjectIdentity(schema, table, False, resolved=False),
    )
    assert dest_object_missing("postgresql", _REFUSED_PG, "t") is None


def test_catalog_says_absent_is_still_missing(monkeypatch):
    import connectors.generic_sql as generic_sql
    import services.sql_object_identity as soi

    monkeypatch.setattr(generic_sql, "_engine", lambda cfg: object())
    monkeypatch.setattr(
        soi, "resolve_object_identity",
        lambda engine, table, schema=None, **kw: soi.ObjectIdentity(schema, table, False),
    )
    assert dest_object_missing("postgresql", _REFUSED_PG, "t") is True


def test_blip_wording_is_unreadable_not_dropped(monkeypatch):
    """End to end through the continuity gate: an unreadable destination keeps
    the honest "could not be re-read" refusal and never claims a drop."""
    monkeypatch.setattr(ac, "_collection", lambda: None)
    monkeypatch.setattr(ac, "load_baseline", lambda key: dict(_BASELINE))
    monkeypatch.setattr(ac, "fingerprint", lambda target, algo=None: (_ for _ in ()).throw(ConnectionError("tunnel down")))
    target = ac.ContinuityTarget("postgresql", dict(_REFUSED_PG), "public", "pv_ora_ts_ia", "k")
    monkeypatch.setattr(ac, "resolve_target", lambda endpoint: target)
    monkeypatch.setattr(ac, "effective_sync_mode", lambda request: "incremental_append")
    violation = ac.check_before_write(object())
    assert violation is not None
    assert violation.details["mismatch"] == "unreadable"
    assert "no longer exists" not in violation.message
    assert "It was dropped" not in violation.message
    assert "could not be re-read" in violation.message


@pytest.mark.parametrize("probe", ["upsert_ddl_gate", "null_cursor_probe"])
def test_sibling_probes_do_not_read_unresolved_as_absent(monkeypatch, probe):
    import connectors.generic_sql as generic_sql
    import services.sql_object_identity as soi

    monkeypatch.setattr(generic_sql, "_engine", lambda cfg: object())
    monkeypatch.setattr(
        soi, "resolve_object_identity",
        lambda engine, table, schema=None, **kw: soi.ObjectIdentity(schema, table, False, resolved=False),
    )
    if probe == "upsert_ddl_gate":
        from services.upsert_ddl_gate import destination_unique_covering

        assert destination_unique_covering(dict(_REFUSED_PG), "t", ["id"], "postgresql") is None
    else:
        from services.null_cursor_probe import _sql_null_count

        with pytest.raises(ValueError, match="could not be read"):
            _sql_null_count(dict(_REFUSED_PG), "t", "updated_at")


def test_object_identity_is_tri_state():
    from services.sql_object_identity import ObjectIdentity

    unreadable = ObjectIdentity(None, "t", False, resolved=False)
    assert unreadable.catalog_unreadable and not unreadable.known_missing and not unreadable.known_present
    absent = ObjectIdentity(None, "t", False)
    assert absent.known_missing and not absent.catalog_unreadable
    present = ObjectIdentity(None, "t", True)
    assert present.known_present and not present.known_missing


def test_oracle_read_back_does_not_fold_the_name_on_an_unreadable_catalog(monkeypatch):
    import services.sql_object_identity as soi
    from services.reconciliation_oracle import OracleCatalogUnreadable, oracle_catalog_identity

    monkeypatch.setattr(
        soi, "resolve_object_identity",
        lambda conn, table, schema=None, columns=None: soi.ObjectIdentity(schema, table, False, resolved=False),
    )
    with pytest.raises(OracleCatalogUnreadable):
        oracle_catalog_identity(object(), "USERS", "APP", ["ID"])
    monkeypatch.setattr(
        soi, "resolve_object_identity",
        lambda conn, table, schema=None, columns=None: soi.ObjectIdentity(schema, table, False),
    )
    assert oracle_catalog_identity(object(), "USERS", "APP", ["ID"]) == (None, None, {})


def test_oracle_sample_on_an_unreadable_catalog_is_unavailable_not_missing(monkeypatch):
    """F11: the ladder's read-back treats the blip as 'sample unavailable, keep rows'."""
    import contextlib

    import connectors.generic_sql as gs
    import services.reconciliation_oracle as ro
    from services.reconciliation import TargetSampleUnavailable
    from services.target_sample import read_target_sample

    class _Engine:
        def connect(self):
            return contextlib.nullcontext(object())

    monkeypatch.setattr(gs, "get_sqlalchemy_engine", lambda cfg: _Engine())

    def _unreadable(*a, **k):
        raise ro.OracleCatalogUnreadable("ORA-12541 no listener")

    monkeypatch.setattr(ro, "oracle_catalog_identity", _unreadable)
    dest = {"type": "oracle", "host": "h", "port": 1521, "database": "x", "username": "u", "password": "p", "schema": "APP"}
    with pytest.raises(TargetSampleUnavailable) as exc:
        read_target_sample("oracle", dest, schema="APP", table_name="USERS", columns=["ID"], limit=5)
    assert getattr(exc.value, "transient", False) is True


def test_remaining_identity_consumers_use_the_tri_state():
    import inspect

    import services.code_crosswalk as cc
    import services.destination_key_collision_probe as dk

    for mod in (cc, dk):
        src = inspect.getsource(mod)
        assert "ident.exists" not in src, mod.__name__
        assert "known_present" in src, mod.__name__
