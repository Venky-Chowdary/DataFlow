"""Namespace existence checks fail closed on catalog errors."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

pytest.importorskip("pyiceberg")

from pyiceberg.exceptions import (  # noqa: E402
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
)

from connectors.iceberg_catalog import (  # noqa: E402
    IcebergCatalogError,
    _namespace_exists,
    ensure_namespace,
)


class _FakeCatalog:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.created: list[tuple[str, ...]] = []

    def load_namespace_properties(self, namespace: tuple[str, ...]) -> dict[str, str]:
        if self.error is not None:
            raise self.error
        return {}

    def create_namespace(self, namespace: tuple[str, ...]) -> None:
        self.created.append(namespace)
        raise NamespaceAlreadyExistsError("another writer created it first")


def test_namespace_exists_only_swallows_no_such_namespace(caplog) -> None:
    assert _namespace_exists(_FakeCatalog(), ("default",)) is True
    assert (
        _namespace_exists(_FakeCatalog(NoSuchNamespaceError("missing")), ("default",))
        is False
    )

    for exc in (RuntimeError("catalog down"), PermissionError("denied")):
        caplog.clear()
        with caplog.at_level(logging.ERROR, logger="connectors.iceberg_catalog"):
            with pytest.raises(IcebergCatalogError):
                _namespace_exists(_FakeCatalog(exc), ("analytics", "raw"))
        assert "analytics.raw" in caplog.text
        assert type(exc).__name__ in caplog.text


def test_ensure_namespace_tolerates_a_creation_race() -> None:
    catalog = _FakeCatalog(NoSuchNamespaceError("missing"))
    ensure_namespace(catalog, ("analytics", "raw"))
    assert catalog.created == [("analytics",), ("analytics", "raw")]
