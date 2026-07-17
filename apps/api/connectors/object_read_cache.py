"""In-process cache for parsed S3/GCS objects — one download+parse per transfer job."""

from __future__ import annotations

from typing import Callable

_CACHE: dict[str, tuple[list[dict], list[str]]] = {}


def clear_object_cache() -> None:
    _CACHE.clear()


def get_or_parse(
    cache_key: str,
    loader: Callable[[], tuple[list[dict], list[str], dict[str, str]]],
) -> tuple[list[dict], list[str], int]:
    if cache_key not in _CACHE:
        records, columns, _schema = loader()
        _CACHE[cache_key] = (records, columns)
    records, columns = _CACHE[cache_key]
    return records, columns, len(records)
