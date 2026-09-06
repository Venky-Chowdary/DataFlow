"""A host without named pipes declines the FIFO COPY routes, it does not fail the job.

``os.mkfifo`` is POSIX-only. Learning that inside the copy raised OSError after
the destination had already been recreated, so the row writer never ran and a
MySQL destination was unusable on Windows rather than merely slower.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest  # noqa: E402

from services.copy_fast_path import (  # noqa: E402
    FastPathUnavailable,
    fifo_streaming_supported,
    require_fifo_streaming,
)


def test_fifo_support_follows_the_platform() -> None:
    assert fifo_streaming_supported() is hasattr(os, "mkfifo")


def test_a_host_with_named_pipes_does_not_decline() -> None:
    if not fifo_streaming_supported():
        pytest.skip("host has no os.mkfifo")
    require_fifo_streaming("PG→MySQL")


def test_a_host_without_named_pipes_declines_and_names_the_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(os, "mkfifo", raising=False)
    assert fifo_streaming_supported() is False
    with pytest.raises(FastPathUnavailable) as err:
        require_fifo_streaming("PG→MySQL")
    # The decline reason is what the operator reads in dest_summary.
    assert "PG→MySQL" in str(err.value)
    assert "mkfifo" in str(err.value)


@pytest.mark.parametrize(
    "route",
    [
        ("services.copy_pg_mysql", "copy_postgres_to_mysql"),
        ("services.copy_mysql_pg", "copy_mysql_to_postgres"),
    ],
)
def test_route_declines_before_touching_the_destination(
    route: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """No connection, no DDL: the guard runs before anything is written."""
    import importlib

    module = importlib.import_module(route[0])
    fn = getattr(module, route[1])
    monkeypatch.delattr(os, "mkfifo", raising=False)

    def _explode(*_a: object, **_k: object) -> None:
        raise AssertionError("the route connected before declining")

    monkeypatch.setattr(module, "_mysql_connect", _explode, raising=False)
    monkeypatch.setattr(module, "_pg_connect", _explode, raising=False)

    kwargs: dict[str, object] = {
        "source_cfg": {"host": "127.0.0.1", "database": "dataflow"},
        "source_table": "t",
        "dest_cfg": {"host": "127.0.0.1", "database": "dataflow"},
        "dest_table": "t_copy",
        "pairs": [("id", "id")],
        "replace_destination": True,
    }
    if route[1] == "copy_postgres_to_mysql":
        kwargs["source_schema"] = "public"
        kwargs["mysql_ddls"] = ["BIGINT"]
    else:
        kwargs["dest_schema"] = "public"
        kwargs["pg_ddls"] = ["BIGINT"]

    with pytest.raises(FastPathUnavailable, match="mkfifo"):
        fn(**kwargs)


def test_same_instance_mysql_insert_select_is_not_declined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """INSERT…SELECT never opens a pipe, so the guard must leave it alone."""
    import services.copy_mysql_mysql as mod

    monkeypatch.delattr(os, "mkfifo", raising=False)
    monkeypatch.setattr(mod, "mysql_mysql_insert_select_enabled", lambda: True)
    reached: list[str] = []

    def _connect(cfg: dict[str, object]) -> object:
        reached.append("connect")
        raise RuntimeError("stop here")

    monkeypatch.setattr(mod, "_mysql_connect", _connect)
    with pytest.raises(RuntimeError, match="stop here"):
        mod.copy_mysql_to_mysql(
            source_cfg={"host": "127.0.0.1", "port": 3306, "database": "a"},
            source_table="t",
            dest_cfg={"host": "127.0.0.1", "port": 3306, "database": "b"},
            dest_table="t",
            pairs=[("id", "id")],
            mysql_ddls=["BIGINT"],
            replace_destination=True,
        )
    assert reached == ["connect"]
