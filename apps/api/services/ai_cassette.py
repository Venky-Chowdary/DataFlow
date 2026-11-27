"""Recorded model responses ("cassettes") for the ai-benchmarks suites.

A cassette replays a provider's answers at the SDK boundary: the Anthropic
provider keeps its own code path (egress gate, manifest, response parsing,
tool-use parsing) and only ``client.messages.create`` is answered from the
file. Replay needs no key and no network and gives the same answer every run.

Format (``tests/fixtures/ai_cassettes/<suite>.json``)::

    {"suite": "...", "provenance": {"how": "seeded" | "recorded", "provider": "...",
     "model": "...", "at": "..."},
     "cases": {"<case>": {"responses": [<Messages API response>, ...],
                          "prompt_sha256": ["<digest of the gated request>", ...]}}}

A Messages API response is ``{"content": [{"type": "text", "text": "..."} |
{"type": "tool_use", "id": "...", "name": "...", "input": {...}}],
"stop_reason": "end_turn"}``. ``provenance.how`` says whether the answers were
written by hand (``seeded``: an adversarial or reference answer) or captured
from a live provider by ``run_ai_benchmarks.py --mode live --record``.

``prompt_sha256`` is the digest of the request the provider would have sent
(model-independent fields only). When the product's prompt changes, replay
still answers but reports the drift, so a stale cassette is visible rather
than silently passing.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

CASSETTE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "ai_cassettes"


class CassetteExhausted(RuntimeError):
    """The code asked the model more often than the cassette recorded."""


def request_digest(kwargs: dict[str, Any]) -> str:
    """Digest of what crossed the boundary: system, messages and the full tool schemas (not the model id)."""
    tools = [t for t in kwargs.get("tools") or [] if isinstance(t, dict)]
    body = {
        "system": kwargs.get("system") or "",
        "messages": kwargs.get("messages") or [],
        "tools": sorted(tools, key=lambda t: str(t.get("name") or "")),
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _block(raw: dict[str, Any]) -> SimpleNamespace:
    if raw.get("type") == "tool_use":
        return SimpleNamespace(type="tool_use", id=raw.get("id") or "toolu_1", name=raw.get("name") or "",
                               input=raw.get("input") or {})
    return SimpleNamespace(type="text", text=str(raw.get("text") or ""))


def _sdk_response(raw: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        content=[_block(b) for b in raw.get("content") or []],
        stop_reason=raw.get("stop_reason") or "end_turn",
        usage=SimpleNamespace(input_tokens=0, output_tokens=0),
    )


def _raw_response(resp: Any) -> dict[str, Any]:
    blocks = []
    for b in getattr(resp, "content", None) or []:
        if getattr(b, "type", "") == "tool_use":
            blocks.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input})
        elif getattr(b, "type", "") == "text":
            blocks.append({"type": "text", "text": b.text})
    return {"content": blocks, "stop_reason": getattr(resp, "stop_reason", None) or "end_turn"}


@dataclass
class Tape:
    """One case: the responses to play (or recorded), and the prompts seen."""

    responses: list[dict[str, Any]] = field(default_factory=list)
    recorded_prompts: list[str] = field(default_factory=list)
    seen_prompts: list[str] = field(default_factory=list)
    calls: int = 0

    def drift(self) -> list[int]:
        """Indexes of calls whose request differs from the recorded one."""
        return [i for i, (seen, rec) in enumerate(zip(self.seen_prompts, self.recorded_prompts)) if seen != rec]


class _Messages:
    def __init__(self, tape: Tape, live: Any = None) -> None:
        self._tape = tape
        self._live = live

    def create(self, **kwargs: Any) -> Any:
        self._tape.seen_prompts.append(request_digest(kwargs))
        self._tape.calls += 1
        if self._live is not None:
            resp = self._live.messages.create(**kwargs)
            self._tape.responses.append(_raw_response(resp))
            return resp
        index = self._tape.calls - 1
        if index >= len(self._tape.responses):
            raise CassetteExhausted(f"cassette has {len(self._tape.responses)} responses; call {index + 1} asked")
        return _sdk_response(self._tape.responses[index])


class CassetteClient:
    """Stands in for ``anthropic.Anthropic``: replays a tape, or records a live client into one."""

    def __init__(self, tape: Tape, live: Any = None) -> None:
        self.messages = _Messages(tape, live)


def load_cassette(suite: str, directory: Path | None = None) -> dict[str, Any]:
    path = (directory or CASSETTE_DIR) / f"{suite}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def tape_for(cassette: dict[str, Any], case: str) -> Tape:
    raw = (cassette.get("cases") or {}).get(case) or {}
    return Tape(responses=list(raw.get("responses") or []), recorded_prompts=list(raw.get("prompt_sha256") or []))


@contextlib.contextmanager
def anthropic_cassette(tape: Tape, *, live: bool = False) -> Iterator[Tape]:
    """Route every Anthropic provider built inside the block to ``tape``; other providers are off.

    ``live=True`` keeps the real client (a key must be configured) and records
    its answers into the tape instead of replaying.
    """
    from src.ai.llm import provider as prov

    original_init = prov.DataTransferAnthropicProvider._init_client
    originals = {cls: cls.is_available for cls in (prov.DataTransferOpenAIProvider, prov.DataTransferOllamaProvider)}

    def init_client(self: Any) -> None:
        if live:
            original_init(self)
            if self._client is not None:
                self._client = CassetteClient(tape, live=self._client)
        else:
            self._client = CassetteClient(tape)

    prov.DataTransferAnthropicProvider._init_client = init_client  # type: ignore[method-assign]
    if not live:
        for cls in originals:
            cls.is_available = lambda self: False  # type: ignore[method-assign]
    try:
        yield tape
    finally:
        prov.DataTransferAnthropicProvider._init_client = original_init  # type: ignore[method-assign]
        for cls, fn in originals.items():
            cls.is_available = fn  # type: ignore[method-assign]


def tape_record(tape: Tape) -> dict[str, Any]:
    """A tape as a cassette case (what ``--record`` writes)."""
    return {"responses": tape.responses, "prompt_sha256": tape.seen_prompts}
