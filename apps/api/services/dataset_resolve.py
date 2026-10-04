"""Bind a dataset hint to one schema without silent industry-template swaps.

Exact folded names win. An uploaded file with measured rows wins over an empty
industry template. A keyword such as ``payment`` must not replace
``sample_payments`` with Financial Services. Several different uploads that
only share a keyword stay unbound.
"""

from __future__ import annotations

import re
from typing import Any

_FOLD_RE = re.compile(r"[\s_\-]+")
_FILE_TYPES = ("csv", "tsv", "json", "jsonl", "xlsx", "xls", "parquet", "xml", "yaml", "yml")
# Whole tokens only. "hr" must not match inside another word.
_INDUSTRY_KEYWORDS = (
    ("human", "hr"),
    ("employee", "hr"),
    ("logistics", "logistics"),
    ("shipping", "logistics"),
    ("freight", "logistics"),
    ("finance", "finance"),
    ("payment", "finance"),
    ("transaction", "finance"),
    ("retail", "retail"),
    ("order", "retail"),
    ("customer", "retail"),
    ("healthcare", "healthcare"),
    ("patient", "healthcare"),
    ("medical", "healthcare"),
    ("health", "healthcare"),
    ("hr", "hr"),
)


def _fold_name(value: str) -> str:
    return _FOLD_RE.sub(" ", (value or "").strip().lower()).strip()


def _tokens(value: str) -> list[str]:
    return [part for part in _fold_name(value).split(" ") if part]


def _split_hint(hint: str) -> tuple[str, str]:
    raw = (hint or "").strip()
    file_type = ""
    lower = raw.lower()
    for ext in _FILE_TYPES:
        for sep in (".", " "):
            suffix = sep + ext
            if lower.endswith(suffix) and len(lower) > len(suffix):
                raw = raw[: -len(suffix)]
                file_type = ext
                break
        if file_type:
            break
    return _fold_name(raw), file_type


def _prefer_measured(schemas: list[Any]) -> Any:
    uploads = [s for s in schemas if getattr(s, "source", "") == "upload"]
    pool = uploads or list(schemas)
    return max(pool, key=lambda s: (int(getattr(s, "row_count", 0) or 0), len(getattr(s, "columns", []) or [])))


def _keyword_in_name(keyword: str, name: str) -> bool:
    for token in _tokens(name):
        if token == keyword:
            return True
        if len(keyword) >= 5 and token.startswith(keyword):
            return True
    return False


def pick_dataset(schemas: list[Any], hint: str | None) -> Any | None:
    if not schemas:
        return None
    if not hint or not str(hint).strip():
        uploads = [s for s in schemas if getattr(s, "source", "") == "upload"]
        return uploads[0] if uploads else schemas[0]

    folded, file_type = _split_hint(str(hint))
    if not folded:
        return None

    exact = [s for s in schemas if _fold_name(getattr(s, "name", "")) == folded]
    if file_type:
        typed = [s for s in exact if (getattr(s, "file_type", "") or "").lower() == file_type]
        if typed:
            exact = typed
    if exact:
        return _prefer_measured(exact)

    upload_hits: list[Any] = []
    for schema in schemas:
        if getattr(schema, "source", "") != "upload":
            continue
        name = _fold_name(getattr(schema, "name", ""))
        if len(folded) < 4 or len(name) < 4 or name == folded:
            continue
        if folded in name or name in folded:
            upload_hits.append(schema)
    distinct = {_fold_name(getattr(s, "name", "")) for s in upload_hits}
    if len(distinct) == 1:
        chosen = upload_hits
        if file_type:
            typed = [s for s in chosen if (getattr(s, "file_type", "") or "").lower() == file_type]
            if typed:
                chosen = typed
        return _prefer_measured(chosen)
    if len(distinct) > 1:
        return None

    if " " not in folded and len(folded) >= 3:
        column_hits: list[Any] = []
        for schema in schemas:
            if getattr(schema, "source", "") != "upload":
                continue
            columns = getattr(schema, "columns", []) or []
            if any(_fold_name(col) == folded for col in columns):
                column_hits.append(schema)
        column_names = {_fold_name(getattr(s, "name", "")) for s in column_hits}
        if len(column_names) == 1:
            return _prefer_measured(column_hits)
        if len(column_names) > 1:
            return None

    hint_tokens = set(_tokens(folded))
    for keyword, industry in _INDUSTRY_KEYWORDS:
        if keyword not in hint_tokens:
            continue
        keyword_uploads = [
            s
            for s in schemas
            if getattr(s, "source", "") == "upload"
            and (
                _keyword_in_name(keyword, getattr(s, "name", ""))
                or getattr(s, "industry", None) == industry
            )
        ]
        keyword_names = {_fold_name(getattr(s, "name", "")) for s in keyword_uploads}
        if len(keyword_names) == 1:
            return _prefer_measured(keyword_uploads)
        if len(keyword_names) > 1:
            return None
        for schema in schemas:
            if getattr(schema, "source", "") == "industry" and (
                getattr(schema, "industry", None) == industry
                or industry in _fold_name(getattr(schema, "name", ""))
            ):
                return schema
    return None
