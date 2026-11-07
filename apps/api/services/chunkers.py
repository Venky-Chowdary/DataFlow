from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Protocol


class ChunkerConfigError(ValueError):
    pass


class Tokenizer(Protocol):
    def encode(self, text: str) -> list[int]: ...

    def decode(self, tokens: list[int]) -> str: ...


@dataclass(frozen=True)
class ChunkerConfig:
    strategy: str = "recursive"
    chunk_size: int = 512
    chunk_overlap: int = 50
    unit: str = "chars"
    tokenizer: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.strategy, str) or self.strategy not in {
            "recursive",
            "fixed",
            "token",
            "markdown",
        }:
            raise ChunkerConfigError(f"Unsupported chunk strategy: {self.strategy!r}")
        if not isinstance(self.unit, str) or self.unit not in {"chars", "tokens"}:
            raise ChunkerConfigError(f"Unsupported chunk unit: {self.unit!r}")
        if self.tokenizer is not None and not isinstance(self.tokenizer, str):
            raise ChunkerConfigError("tokenizer must be a string or None")
        if (
            isinstance(self.chunk_size, bool)
            or not isinstance(self.chunk_size, int)
            or self.chunk_size <= 0
        ):
            raise ChunkerConfigError("chunk_size must be a positive integer")
        if (
            isinstance(self.chunk_overlap, bool)
            or not isinstance(self.chunk_overlap, int)
            or self.chunk_overlap < 0
            or self.chunk_overlap >= self.chunk_size
        ):
            raise ChunkerConfigError(
                "chunk_overlap must be non-negative and smaller than chunk_size"
            )
        if self.strategy == "token" and self.unit != "tokens":
            raise ChunkerConfigError("token strategy requires unit='tokens'")


@dataclass(frozen=True)
class Chunk:
    text: str
    index: int
    section: str | None = None


def _recursive_chunks(
    text: str,
    chunk_size: int,
    chunk_overlap: int,
    split_on: Callable[[str], list[str]] | None = None,
) -> list[str]:
    if not text or not text.strip():
        return []

    def _paragraphs(value: str) -> list[str]:
        return [part.strip() for part in value.split("\n\n") if part.strip()]

    def _sentences(value: str) -> list[str]:
        parts = re.split(r"(?<=[.!?])\s+", value)
        return [part.strip() for part in parts if part.strip()]

    def _fixed_chunks(value: str, size: int, overlap: int) -> list[str]:
        step = max(1, size - overlap)
        chunks = []
        start = 0
        while start < len(value):
            chunks.append(value[start : start + size].strip())
            start += step
        return [part for part in chunks if part]

    def _split(value: str) -> list[str]:
        if split_on:
            return [part.strip() for part in split_on(value) if part.strip()]
        paragraphs = _paragraphs(value)
        if all(len(part) <= chunk_size for part in paragraphs):
            return paragraphs
        out: list[str] = []
        for paragraph in paragraphs:
            if len(paragraph) <= chunk_size:
                out.append(paragraph)
                continue
            for sentence in _sentences(paragraph):
                if len(sentence) <= chunk_size:
                    out.append(sentence)
                else:
                    out.extend(
                        _fixed_chunks(sentence, chunk_size, chunk_overlap)
                    )
        return out

    chunks = _split(text)
    merged: list[str] = []
    current = ""
    for part in chunks:
        if len(current) + len(part) + 1 <= chunk_size:
            current = f"{current}\n\n{part}".strip() if current else part
        else:
            if current:
                merged.append(current)
            current = part
    if current:
        merged.append(current)
    return merged


def _get_tokenizer(name: str | None, tokenizer: Tokenizer | None) -> Tokenizer:
    if tokenizer is not None:
        return tokenizer
    try:
        import tiktoken
    except ImportError:
        raise ChunkerConfigError(
            "token chunking requires tiktoken; install it or choose 'recursive'"
        ) from None
    try:
        encoding = tiktoken.get_encoding(name or "cl100k_base")
    except (KeyError, ValueError):
        raise ChunkerConfigError(
            f"Unknown tiktoken encoding {name or 'cl100k_base'!r}"
        ) from None

    class StrictTiktokenAdapter:
        def encode(self, text: str) -> list[int]:
            return encoding.encode(text)

        def decode(self, tokens: list[int]) -> str:
            return encoding.decode(tokens, errors="strict")

    return StrictTiktokenAdapter()


def _token_chunks(
    text: str,
    config: ChunkerConfig,
    tokenizer: Tokenizer | None,
    section: str | None,
    start_index: int,
) -> list[Chunk]:
    codec = _get_tokenizer(config.tokenizer, tokenizer)
    tokens = codec.encode(text)
    chunks: list[Chunk] = []
    start = 0
    while start < len(tokens):
        end = min(start + config.chunk_size, len(tokens))
        decoded = ""
        while end > start:
            try:
                decoded = codec.decode(tokens[start:end])
                break
            except UnicodeDecodeError:
                end -= 1
        if end == start:
            raise ChunkerConfigError(
                "token chunk_size is too small to keep a Unicode codepoint intact"
            )
        if decoded:
            chunks.append(Chunk(decoded, start_index + len(chunks), section))
        if end >= len(tokens):
            break
        next_start = max(start + 1, end - config.chunk_overlap)
        while next_start > start:
            try:
                codec.decode(tokens[next_start:end])
                break
            except UnicodeDecodeError:
                next_start -= 1
        start = next_start if next_start > start else end
    return chunks


_ATX_HEADING = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.+?)[ \t]*#*[ \t]*$")
_FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")


def _markdown_sections(text: str) -> list[tuple[str | None, str]]:
    sections: list[tuple[str | None, str]] = []
    headings: list[tuple[int, str]] = []
    body: list[str] = []
    fenced_char: str | None = None
    fenced_size = 0

    def finish() -> None:
        if body:
            sections.append(
                (
                    " > ".join(title for _, title in headings) or None,
                    "".join(body).strip(),
                )
            )
            body.clear()

    for line in text.splitlines(keepends=True):
        if fenced_char is not None:
            closing = re.match(r"^ {0,3}(`{3,}|~{3,})[ \t]*$", line.rstrip("\r\n"))
            if (
                closing
                and closing.group(1)[0] == fenced_char
                and len(closing.group(1)) >= fenced_size
            ):
                fenced_char, fenced_size = None, 0
            body.append(line)
            continue
        fence = _FENCE.match(line)
        if fence:
            marker = fence.group(1)
            fenced_char, fenced_size = marker[0], len(marker)
            body.append(line)
            continue
        heading = _ATX_HEADING.match(line.rstrip("\r\n"))
        if heading:
            finish()
            level = len(heading.group(1))
            title = heading.group(2).strip()
            headings = [
                (prior_level, prior_title)
                for prior_level, prior_title in headings
                if prior_level < level
            ]
            headings.append((level, title))
            continue
        body.append(line)
    finish()
    return sections


def chunk(
    text: str,
    config: ChunkerConfig,
    *,
    tokenizer: Tokenizer | None = None,
    split_on: Callable[[str], list[str]] | None = None,
) -> list[Chunk]:
    if not text or not text.strip():
        return []
    if config.strategy == "recursive":
        return [
            Chunk(value, index)
            for index, value in enumerate(
                _recursive_chunks(
                    text, config.chunk_size, config.chunk_overlap, split_on
                )
            )
        ]
    if config.strategy == "fixed" and config.unit == "chars":
        step = config.chunk_size - config.chunk_overlap
        chunks: list[Chunk] = []
        start = 0
        while start < len(text):
            value = text[start : start + config.chunk_size]
            if not value:
                break
            chunks.append(Chunk(value, len(chunks)))
            if start + len(value) >= len(text):
                break
            start += step
        return chunks
    if config.strategy == "markdown":
        result: list[Chunk] = []
        for section, body in _markdown_sections(text):
            if not body:
                continue
            if config.unit == "tokens":
                result.extend(
                    _token_chunks(
                        body, config, tokenizer, section, len(result)
                    )
                )
            else:
                section_chunks = _recursive_chunks(
                    body, config.chunk_size, config.chunk_overlap
                )
                first_index = len(result)
                result.extend(
                    Chunk(value, first_index + index, section)
                    for index, value in enumerate(section_chunks)
                )
        return result
    return _token_chunks(text, config, tokenizer, None, 0)
