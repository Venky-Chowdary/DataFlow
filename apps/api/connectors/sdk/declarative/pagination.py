from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urljoin, urlsplit, urlunsplit

from connectors.sdk.declarative.errors import PaginationError, ResponseShapeError
from connectors.sdk.declarative.requester import HttpRequester
from services.json_tabular import _dig_path, extract_json_records
from services.secret_config import redact_url

PaginatorType = Literal["none", "cursor", "offset", "page", "link_header"]


@dataclass(frozen=True)
class PaginatorSpec:
    type: PaginatorType = "none"
    page_size: int = 100
    page_size_param: str = "limit"
    max_pages: int = 10_000
    offset_param: str = "offset"
    start_offset: int = 0
    page_param: str = "page"
    start_index: int = 1
    cursor_param: str = "cursor"
    cursor_path: str = ""
    cursor_header: str = ""
    initial_token: str | None = None

    def __post_init__(self) -> None:
        if self.type not in {"none", "cursor", "offset", "page", "link_header"}:
            raise ValueError(f"unsupported paginator type {self.type!r}")
        if self.page_size < 1:
            raise ValueError("page_size must be positive")
        if self.max_pages < 1:
            raise ValueError("max_pages must be positive")
        if self.start_offset < 0 or self.start_index < 0:
            raise ValueError("pagination start values cannot be negative")
        if self.type == "cursor":
            if not self.cursor_param:
                raise ValueError("cursor paginator requires cursor_param")
            if bool(self.cursor_path) == bool(self.cursor_header):
                raise ValueError("cursor paginator requires exactly one token path or header")
        if self.type == "offset" and not self.offset_param:
            raise ValueError("offset paginator requires offset_param")
        if self.type == "page" and not self.page_param:
            raise ValueError("page paginator requires page_param")


@dataclass(frozen=True)
class Page:
    records: list[dict[str, Any]]
    headers: Mapping[str, str]
    request_id: str
    url: str


def _extract_records(
    payload: Any,
    records_path: str | None,
    *,
    request_id: str,
    stream: str,
    status: int | None,
) -> list[dict[str, Any]]:
    try:
        path = (records_path or "").strip()
        if path:
            if path in {"$", "root", "item"} and isinstance(payload, list):
                rows = payload
            else:
                rows = _dig_path(payload, path)
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise ValueError(f"records_path={path!r} did not resolve to an array of objects")
            return rows
        return extract_json_records(payload)
    except ValueError:
        raise ResponseShapeError(
            f"Response records do not match records_path={records_path!r}",
            request_id=request_id,
            stream=stream,
            status=status,
        ) from None


def _header(headers: Mapping[str, Any], name: str) -> str:
    return next(
        (str(value) for key, value in headers.items() if str(key).lower() == name.lower()),
        "",
    )


def _split_link_values(value: str) -> list[str]:
    parts: list[str] = []
    start = 0
    in_angle = False
    in_quote = False
    escaped = False
    for index, char in enumerate(value):
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_quote:
            escaped = True
        elif char == '"' and not in_angle:
            in_quote = not in_quote
        elif char == "<" and not in_quote:
            in_angle = True
        elif char == ">" and not in_quote:
            in_angle = False
        elif char == "," and not in_angle and not in_quote:
            parts.append(value[start:index].strip())
            start = index + 1
    parts.append(value[start:].strip())
    return [part for part in parts if part]


_LINK_REL = re.compile(r"(?:^|;)\s*rel\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^;\s]+))", re.I)


def _next_link(value: str) -> str | None:
    for part in _split_link_values(value):
        match = re.match(r"^\s*<([^>]*)>(.*)$", part)
        if not match:
            continue
        relation = _LINK_REL.search(match.group(2))
        if not relation:
            continue
        relations = next((item for item in relation.groups() if item is not None), "")
        if "next" in relations.lower().split():
            return match.group(1)
    return None


def _canonical_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ""))


def _pagination_error(message: str, page: Page | None, stream: str) -> PaginationError:
    return PaginationError(
        message,
        request_id=page.request_id if page else "",
        stream=stream,
        url=page.url if page else "",
    )


def paginate(
    requester: HttpRequester,
    url: str,
    *,
    records_path: str | None,
    paginator: PaginatorSpec,
    method: str = "GET",
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    stream: str = "",
    max_pages: int | None = None,
) -> list[Page]:
    """Fetch every page or raise; partial page lists are never returned.

    Offset and page pagination stop on a short page. Cursor and Link-header
    modes are at-least-once across an inclusive page boundary.
    """
    page_limit = max_pages if max_pages is not None else paginator.max_pages
    if page_limit < 1:
        raise ValueError("max_pages must be positive")

    pages: list[Page] = []
    base_params = dict(params or {})
    current_url = url
    current_token: Any = paginator.initial_token
    seen_tokens: set[str] = (
        {str(paginator.initial_token)}
        if paginator.initial_token not in (None, "")
        else set()
    )
    seen_urls: set[str] = set()
    origin = (urlsplit(url).scheme.lower(), urlsplit(url).netloc.lower())
    page_number = paginator.start_index
    offset = paginator.start_offset

    while True:
        if paginator.type == "link_header":
            key = _canonical_url(current_url)
            if key in seen_urls:
                raise _pagination_error(
                    f"pagination repeated URL {redact_url(current_url)}",
                    pages[-1] if pages else None,
                    stream,
                )
            seen_urls.add(key)
            page_params: dict[str, Any] | None = base_params if not pages else None
        else:
            page_params = dict(base_params)
            if paginator.type in {"cursor", "offset", "page"} and paginator.page_size_param:
                page_params[paginator.page_size_param] = paginator.page_size
            if paginator.type == "cursor" and current_token not in (None, ""):
                page_params[paginator.cursor_param] = current_token
            elif paginator.type == "offset":
                page_params[paginator.offset_param] = offset
            elif paginator.type == "page":
                page_params[paginator.page_param] = page_number

        result = requester.request_json(
            method,
            current_url,
            headers=headers,
            params=page_params,
            stream=stream,
        )
        records = _extract_records(
            result.payload,
            records_path,
            request_id=result.request_id,
            stream=stream,
            status=result.response.status_code,
        )
        page = Page(
            records=records,
            headers=dict(result.response.headers),
            request_id=result.request_id,
            url=redact_url(current_url),
        )
        pages.append(page)

        if paginator.type == "none":
            return pages

        if paginator.type in {"offset", "page"}:
            if len(records) < paginator.page_size:
                return pages
            if len(pages) >= page_limit:
                raise _pagination_error(
                    f"pagination reached max_pages={page_limit} with more pages implied",
                    page,
                    stream,
                )
            if paginator.type == "offset":
                offset += paginator.page_size
            else:
                page_number += 1
            continue

        if paginator.type == "cursor":
            if paginator.cursor_header:
                next_token: Any = _header(result.response.headers, paginator.cursor_header)
            else:
                next_token = _dig_path(result.payload, paginator.cursor_path)
            if next_token in (None, ""):
                return pages
            if not records:
                raise _pagination_error(
                    "pagination received an empty page with a next token",
                    page,
                    stream,
                )
            token_key = str(next_token)
            if token_key in seen_tokens:
                raise _pagination_error(
                    "pagination repeated cursor token",
                    page,
                    stream,
                )
            if len(pages) >= page_limit:
                raise _pagination_error(
                    f"pagination reached max_pages={page_limit} with a next token",
                    page,
                    stream,
                )
            seen_tokens.add(token_key)
            current_token = next_token
            continue

        link_value = _header(result.response.headers, "Link")
        next_link = _next_link(link_value) if link_value else None
        if not next_link:
            return pages
        if not records:
            raise _pagination_error(
                "pagination received an empty page with a next URL",
                page,
                stream,
            )
        if len(pages) >= page_limit:
            raise _pagination_error(
                f"pagination reached max_pages={page_limit} with a next URL",
                page,
                stream,
            )
        current_url = urljoin(current_url, next_link)
        resolved = urlsplit(current_url)
        if (
            resolved.scheme.lower() not in {"http", "https"}
            or (resolved.scheme.lower(), resolved.netloc.lower()) != origin
        ):
            raise _pagination_error(
                f"pagination next URL must remain on the source origin: {redact_url(current_url)}",
                page,
                stream,
            )
