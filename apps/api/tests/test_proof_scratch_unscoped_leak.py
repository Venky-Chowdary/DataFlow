"""A proof store opened outside the wrapper must not leak into the shared temp dir.

``/tmp/df_pinmap_*.db`` files of 85-250 MB were found on a QA host. The Gate-8
proof bound a private scratch dir only in its wrapper
(``_transform_applied_proof``); the proof body itself, or a store opened
anywhere else, fell back to ``mkstemp`` in the SHARED temp dir. There only
``__del__`` removed it, and ``__del__`` never runs while an exception's
traceback still holds the frame (a failed job keeps its exception), nor
reliably at interpreter exit.
"""

from __future__ import annotations

import gc
import os
import tempfile
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.reconcile_step as rs  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

_PREFIXES = ("df_", "dataflow_")


class _Cancelled(BaseException):
    """A job cancel / interrupt: not an Exception, so nothing swallows it."""


def _leftovers(root: Path) -> set[str]:
    return {p.name for p in root.iterdir() if p.name.startswith(_PREFIXES)}


@pytest.fixture()
def private_tmp(tmp_path: Path, monkeypatch):
    root = tmp_path / "tmp"
    root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(root))
    assert tempfile.gettempdir() == str(root)
    return root


def _proof_kwargs(
    tmp_path: Path, sync_mode: str = "incremental_upsert", summary: dict | None = None
) -> dict:
    src = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "s.db"), table="t"
    )
    dst = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "d.db"), table="t"
    )
    return {
        "shape_recipe": {"steps": [{"op": "trim", "column": "name"}]},
        # A keyed run: the proof opens the run-key set (df_scope_) and the
        # pin store (df_pinmap_) before it reads anything. An incremental
        # append that recorded its cursor window keeps every version per key
        # (R6, D359): it scopes the read-back by that window and opens no
        # run-key set.
        "dest_summary": {
            "shape_recipe_hash": "h",
            "sync_mode": sync_mode,
            "primary_key_columns": ["id"],
            **(summary or {}),
        },
        "source_endpoint": src,
        "endpoint": dst,
        "columns": ["id", "name"],
        "mappings": [
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
        "source_schema": {"id": "INTEGER", "name": "TEXT"},
        "recon": {},
    }


_WINDOW = {"incremental_window_cursor": "id", "incremental_window_max": "3"}


@pytest.mark.parametrize(
    ("sync_mode", "summary", "stores"),
    [
        ("incremental_upsert", {}, {"pinmap", "scope"}),
        # The append proofs (the mode the leak was first found on) must leave
        # nothing behind either, with and without a recorded cursor window.
        ("full_refresh_append", {}, {"pinmap", "scope"}),
        ("incremental_append", _WINDOW, {"pinmap"}),
    ],
)
def test_proof_body_called_outside_the_wrapper_leaves_nothing_when_cancelled(
    private_tmp, tmp_path, monkeypatch, sync_mode, summary, stores
):
    import services.reconciliation as recon_mod

    opened: list[str] = []
    real_pin, real_scope = rs._PinStore.__init__, rs._DiskKeySet.__init__

    def pin_init(self):
        real_pin(self)
        opened.append(self.path)

    def scope_init(self):
        real_scope(self)
        opened.append(self.path)

    monkeypatch.setattr(rs._PinStore, "__init__", pin_init)
    monkeypatch.setattr(rs._DiskKeySet, "__init__", scope_init)

    def cancelled(*_a, **_kw):
        raise _Cancelled("job cancelled mid-proof")

    monkeypatch.setattr(recon_mod, "canonical_checksum_from_iter", cancelled)

    with pytest.raises(_Cancelled) as held:
        rs._transform_applied_proof_impl(**_proof_kwargs(tmp_path, sync_mode, summary))

    # The stores this mode uses were really opened by the proof path under test...
    assert {Path(p).name.split("_")[1] for p in opened} == stores, opened
    # ...and the failed run's traceback still references their frame, so
    # __del__ has not run. Nothing may be left in the shared temp dir.
    assert held.value is not None
    assert not any(os.path.exists(p) for p in opened), opened
    assert _leftovers(private_tmp) == set()


def test_a_bare_store_never_sits_in_the_shared_temp_dir(private_tmp):
    """Opened with no proof scope at all: its own private dir, removed on close
    and on collection — never a file in the shared temp dir."""
    pin, scope = rs._PinStore(), rs._DiskKeySet()
    for store in (pin, scope):
        assert Path(store.path).parent != private_tmp, store.path
        assert Path(store.path).parent.name.startswith("df_proof_")
    pin.close()
    assert not Path(pin.path).parent.exists()
    path = Path(scope.path)
    del scope, store  # the loop variable is a reference too
    gc.collect()
    assert not path.parent.exists()
    assert _leftovers(private_tmp) == set()
