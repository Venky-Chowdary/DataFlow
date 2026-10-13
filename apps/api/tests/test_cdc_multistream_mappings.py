"""Unit test: multi-stream CDC prefers per-stream mappings from contracts."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.sync_cursor import SyncContract
from src.transfer.cdc_transfer import _run_cdc_multi_stream


def test_multi_stream_uses_per_stream_mappings() -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.table = "a,b"
    source.collection = ""
    destination = MagicMock()
    destination.format = "postgresql"
    destination.table = "out"
    destination.collection = ""

    shared = [{"source": "id", "target": "id"}]
    stream_a = [{"source": "id", "target": "id_a"}, {"source": "x", "target": "x"}]
    stream_b = [{"source": "id", "target": "id_b"}]
    contracts = [
        {"name": "a", "selected": True, "sync_mode": "cdc", "primary_key": "id", "mappings": stream_a},
        {"name": "b", "selected": True, "sync_mode": "cdc", "primary_key": "id", "mappings": stream_b},
    ]
    selected = [
        SyncContract(name="a", sync_mode="cdc", primary_key="id"),
        SyncContract(name="b", sync_mode="cdc", primary_key="id"),
    ]

    seen: list[list] = []
    inherited_flags: list[bool] = []

    def _fake_single(src, dest, mappings, *args, **kwargs):
        seen.append(list(mappings))
        inherited_flags.append(bool(kwargs.get("mappings_inherited")))
        return 1, [], {"cdc": {}}, ["id"]

    with patch("src.transfer.cdc_transfer._run_cdc_single_stream", side_effect=_fake_single), \
         patch(
             "src.transfer.cdc_transfer._run_cdc_shared_multi_table",
             side_effect=RuntimeError("force sequential path for mapping unit test"),
         ):
        _run_cdc_multi_stream(
            source,
            destination,
            shared,
            {},
            None,
            sync_mode="cdc",
            stream_contracts=contracts,
            selected=selected,
            job_id="j1",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    assert len(seen) == 2
    assert seen[0] == stream_a
    assert seen[1] == stream_b
    assert inherited_flags == [False, False]


def test_undeclared_cdc_stream_does_not_inherit_another_tables_map() -> None:
    source = MagicMock()
    source.format = "postgresql"
    source.table = "customers,orders"
    source.collection = ""
    destination = MagicMock()
    destination.format = "postgresql"
    destination.table = "out"
    destination.collection = ""

    shared = [
        {"source": "id", "target": "id"},
        {"source": "email", "target": "customer_email"},
    ]
    contracts = [
        {"name": "customers", "selected": True, "sync_mode": "cdc", "primary_key": "id"},
        {"name": "orders", "selected": True, "sync_mode": "cdc", "primary_key": "id"},
    ]
    selected = [
        SyncContract(name="customers", sync_mode="cdc", primary_key="id"),
        SyncContract(name="orders", sync_mode="cdc", primary_key="id"),
    ]
    inherited_flags: list[bool] = []

    def _fake_single(src, dest, mappings, *args, **kwargs):
        inherited_flags.append(bool(kwargs.get("mappings_inherited")))
        assert mappings == shared
        return 1, [], {"cdc": {}}, ["id"]

    with patch("src.transfer.cdc_transfer._run_cdc_single_stream", side_effect=_fake_single), \
         patch(
             "src.transfer.cdc_transfer._run_cdc_shared_multi_table",
             side_effect=RuntimeError("force sequential path for mapping unit test"),
         ):
        _run_cdc_multi_stream(
            source,
            destination,
            shared,
            {"id": "integer", "email": "string"},
            None,
            sync_mode="cdc",
            stream_contracts=contracts,
            selected=selected,
            job_id="j2",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    assert inherited_flags == [True, True]


def _install_cdc_write_stubs(monkeypatch, captured: dict) -> None:
    def fake_write_batch(*args, **kwargs):
        captured["mappings"] = args[6] if len(args) > 6 else kwargs.get("mappings")
        captured["conflict_columns"] = kwargs.get("conflict_columns")
        captured["headers"] = args[4] if len(args) > 4 else None
        return 1, "checksum", {}

    monkeypatch.setattr("src.transfer.cdc_transfer._write_batch", fake_write_batch)
    monkeypatch.setattr("src.transfer.cdc_transfer.with_retry", lambda fn, **_k: fn())
    monkeypatch.setattr(
        "src.transfer.cdc_transfer.classify_replay_safety",
        lambda **_k: type("S", (), {"allows_retry": lambda *_a, **_k: True})(),
    )


def test_inherited_cdc_batch_writes_this_tables_columns(monkeypatch) -> None:
    """customers.id,email must not drop orders.amount, and must not rename the key."""
    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import _apply_change_batch

    captured: dict = {}
    _install_cdc_write_stubs(monkeypatch, captured)
    lock: dict = {}
    customers = [
        {"source": "id", "target": "customer_id"},
        {"source": "email", "target": "customer_email"},
    ]
    change = ChangeBatch(
        inserts=[{"id": "7", "customer_id": "7", "amount": "12.50"}],
        updates=[],
        deletes=[],
        resume_token={"file": "binlog.000001", "pos": 4},
    )
    _rows, _checksum, summary, _deleted = _apply_change_batch(
        "sqlite",
        type("E", (), {"format": "sqlite", "table": "orders", "extra": {}})(),
        {"database": ":memory:"},
        "orders",
        change,
        customers,
        {"id": "integer", "email": "string"},
        ["id", "email"],
        "customer_id",
        0,
        1,
        mappings_inherited=True,
        pk_source_cols=["id"],
        align_lock=lock,
    )
    sources = {row["source"] for row in captured["mappings"] if row.get("source") != "_df_lsn"}
    assert sources == {"id", "customer_id", "amount"}
    assert all(
        row["source"] == row["target"]
        for row in captured["mappings"]
        if row.get("source") != "_df_lsn"
    )
    assert captured["conflict_columns"] == ["id"]
    assert any("amount" in str(line) for line in (summary.get("warnings") or []))

    deleted_with: dict = {}

    def fake_delete(**kwargs):
        deleted_with["primary_key_column"] = kwargs.get("primary_key_column")
        return 1

    monkeypatch.setattr("src.transfer.cdc_transfer.delete_by_primary_keys", fake_delete)
    _apply_change_batch(
        "sqlite",
        type("E", (), {"format": "sqlite", "table": "orders", "extra": {}})(),
        {"database": ":memory:"},
        "orders",
        ChangeBatch(inserts=[], updates=[], deletes=["7"], resume_token={"pos": 8}),
        customers,
        {"id": "integer", "email": "string"},
        ["id", "email"],
        "customer_id",
        1,
        2,
        mappings_inherited=True,
        pk_source_cols=["id"],
        align_lock=lock,
    )
    assert deleted_with["primary_key_column"] == ["id"]


def test_inherited_cdc_exactly_once_uses_this_tables_key(monkeypatch) -> None:
    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import _apply_change_batch

    captured: dict = {}

    def fake_eos(**kwargs):
        captured["mappings"] = kwargs.get("mappings")
        captured["pk_target_cols"] = kwargs.get("pk_target_cols")
        captured["headers"] = kwargs.get("headers")
        return 1, "checksum", {}, 0

    monkeypatch.setattr(
        "connectors.cdc_eos_sql.apply_change_batch_exactly_once",
        fake_eos,
    )
    _apply_change_batch(
        "sqlite",
        type("E", (), {"format": "sqlite", "table": "orders", "extra": {}})(),
        {"database": ":memory:"},
        "orders",
        ChangeBatch(
            inserts=[{"id": "1", "amount": "4"}],
            updates=[],
            deletes=[],
            resume_token={"lsn": "0/16B3748"},
        ),
        [
            {"source": "id", "target": "customer_id"},
            {"source": "email", "target": "customer_email"},
        ],
        {"id": "integer", "email": "string"},
        ["id", "email"],
        "customer_id",
        0,
        1,
        delivery_guarantee="exactly_once",
        mappings_inherited=True,
        pk_source_cols=["id"],
        align_lock={},
    )
    assert captured["pk_target_cols"] == ["id"]
    assert {row["source"] for row in captured["mappings"]} == {"id", "amount"}
    assert "amount" in captured["headers"]
    assert "email" not in captured["headers"]
