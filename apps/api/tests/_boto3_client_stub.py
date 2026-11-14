"""Stand in for ``connectors.aws_common.boto3_client`` without leaking it.

Several modules bind the factory at import (``from connectors.aws_common
import boto3_client``). Patching only ``aws_common`` missed every module that
already held the real one, and a module *first imported* while the patch was
in place copied the stand-in into its own namespace, where monkeypatch's undo
never reaches it. Every later test in the process then dialled the stand-in:
``test_s3_moto_chunk_safety`` failed its Gate-8 S3 read-back with
``object_store_list_unknowable`` whenever it ran after the residency review
tests (T-F3).

``stub_boto3_client`` patches every binding, and on teardown resets any module
still holding a stand-in — including one imported mid-test — to the real
factory.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from typing import Any

import pytest


def _bindings(value: Any) -> list[Any]:
    """Every loaded module whose own namespace binds ``boto3_client`` to ``value``."""
    return [
        mod
        for mod in list(sys.modules.values())
        if getattr(mod, "__dict__", {}).get("boto3_client") is value
    ]


@pytest.fixture
def stub_boto3_client() -> Iterator[Callable[[Callable[..., Any]], None]]:
    """``install(stand_in)`` replaces the AWS client factory everywhere it is bound.

    Every binding it replaces held the real factory, so teardown restores the
    real factory to every module holding any stand-in — no saved value can
    put a stand-in back.
    """
    import connectors.aws_common as aws_common

    real = aws_common.boto3_client
    installed: list[Callable[..., Any]] = []

    def install(stand_in: Callable[..., Any]) -> None:
        current = installed[-1] if installed else real
        installed.append(stand_in)
        for mod in _bindings(current):
            setattr(mod, "boto3_client", stand_in)

    try:
        yield install
    finally:
        for stand_in in installed:
            for mod in _bindings(stand_in):
                setattr(mod, "boto3_client", real)
