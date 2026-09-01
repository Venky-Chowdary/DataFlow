"""AI egress manifest + enforced metadata-only mode (N2).

Security review asked what left the customer boundary toward a model, and
whether the mapper ever saw cell values. Without enforcement plus a durable
record, the answer is a promise. N3's hash-chained evidence store is where
this module writes; this module is the gate.

* **Metadata-only** (default on): the mapper may send column names, declared
  types, and aggregate profiles. It may not send cell values. A prompt that
  already interpolated samples is stripped before any provider sees it.
* **Manifest**: one chain record per outbound generate, committing to the
  SHA-256 of the bytes that were actually handed to the provider — never the
  cell values themselves. Cloud providers are flagged as having crossed the
  customer boundary; local / Ollama are recorded and not claimed as "no
  model".

``is_lossy_coercion`` and mapping confidence floors are untouched. ITEM 1
still holds: the LLM does not decide source→target.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator, Mapping

from services.brand_env import getenv_brand
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

AI_EGRESS_ACTION = "ai.egress"
ORIGIN_METADATA_ONLY = "metadata_only"
ORIGIN_CELLS_ALLOWED = "cells_allowed"

CLOUD_PROVIDERS = frozenset({"openai", "anthropic"})

_FALSEY = frozenset({"false", "0", "off", "disabled", "no"})

_job_id: ContextVar[str] = ContextVar("ai_egress_job_id", default="")
_purpose: ContextVar[str] = ContextVar("ai_egress_purpose", default="unspecified")
_column_names: ContextVar[tuple[str, ...]] = ContextVar("ai_egress_columns", default=())
_source_types: ContextVar[tuple[tuple[str, str], ...]] = ContextVar(
    "ai_egress_types", default=()
)

_LAST_MANIFEST: dict[str, Any] | None = None

_SAMPLE_LINE = re.compile(r"(?im)^[ \t]*samples\[[^\]]+\]:.*$")
_SOURCE_SAMPLES = re.compile(
    r"(?is)Source Samples:.*?(?=Retrieved Context:|\Z)"
)
_SAMPLE_VALUES = re.compile(
    r"(?is)Sample Values:.*?(?=Retrieved Context:|\nSteps:|\Z)"
)
_COLUMNS_AND_SAMPLES = re.compile(
    r"(?is)Columns and Samples:.*?(?=For each column:|\Z)"
)


def metadata_only_enabled() -> bool:
    """Fail-closed default: the mapper does not send cell values.

    Opt out explicitly with ``DATAWRAP_AI_METADATA_ONLY=false`` (or
    ``DATAFLOW_AI_METADATA_ONLY``). An unset variable is on — that is the
    security-review answer, not a promise the operator has to remember to flip.
    """
    raw = getenv_brand("AI_METADATA_ONLY", "true")
    return str(raw or "true").strip().lower() not in _FALSEY


def crossed_customer_boundary(provider: str) -> bool:
    """True when the named provider is a cloud model outside the customer VPC."""
    return str(provider or "").strip().lower() in CLOUD_PROVIDERS


def column_profiles_without_cells(
    samples_by_column: Mapping[str, list[str]] | None,
) -> dict[str, dict[str, int | bool]]:
    """Aggregate profiles a mapper may send. Never includes a cell string."""
    out: dict[str, dict[str, int | bool]] = {}
    for name, values in dict(samples_by_column or {}).items():
        col = str(name or "").strip()
        if not col:
            continue
        cells = [str(v) for v in values if v is not None and str(v) != ""]
        out[col] = {
            "n": len(list(values or [])),
            "non_empty": len(cells),
            "max_len": max((len(c) for c in cells), default=0),
            "looks_numeric": bool(cells) and all(_looks_numeric(c) for c in cells),
        }
    return out


def _looks_numeric(value: str) -> bool:
    text = value.strip().replace(",", "")
    if not text:
        return False
    try:
        float(text)
    except ValueError:
        return False
    return True


def contains_cell_values(text: str) -> bool:
    """True when a prompt still carries interpolated sample cells."""
    raw = text or ""
    if _SAMPLE_LINE.search(raw):
        # ``samples[email]: ['a@x.com']`` vs empty ``samples[email]: []``
        for match in _SAMPLE_LINE.finditer(raw):
            line = match.group(0)
            if re.search(r":\s*\[.+\]", line) and not re.search(r":\s*\[\s*\]", line):
                return True
    for pattern, empty in (
        (_SOURCE_SAMPLES, re.compile(r"Source Samples:\s*(\{\}|None|\[withheld:[^\]]+\])\s*$", re.I)),
        (_SAMPLE_VALUES, re.compile(r"Sample Values:\s*(\[\s*\]|None|\[withheld:[^\]]+\])\s*$", re.I)),
        (_COLUMNS_AND_SAMPLES, re.compile(r"Columns and Samples:\s*(\{\}|None|\[withheld:[^\]]+\])\s*$", re.I)),
    ):
        found = pattern.search(raw)
        if not found:
            continue
        block = found.group(0).strip()
        if empty.search(block):
            continue
        if "[withheld:" in block.lower():
            continue
        # A non-empty mapping / list after the label is cell payload.
        if re.search(r"\{.+:.+\}|\[.+'|.+\"", block):
            return True
        if re.search(r":\s+\S+", block.split(":", 1)[-1]):
            remainder = block.split(":", 1)[-1].strip()
            if remainder and remainder.lower() not in {"none", "{}", "[]"}:
                return True
    return False


def strip_cell_sections(text: str) -> tuple[str, bool]:
    """Remove interpolated sample blocks. Schema / type lines stay."""
    if not text:
        return text, False
    out = text
    stripped = False
    replacements = (
        (_SOURCE_SAMPLES, "Source Samples: [withheld: metadata-only]\n"),
        (_SAMPLE_VALUES, "Sample Values: [withheld: metadata-only]\n"),
        (_COLUMNS_AND_SAMPLES, "Columns and Samples: [withheld: metadata-only]\n"),
    )
    for pattern, repl in replacements:
        new = pattern.sub(repl, out)
        if new != out:
            stripped = True
            out = new
    new_lines: list[str] = []
    for line in out.splitlines(keepends=True):
        if _SAMPLE_LINE.match(line.rstrip("\n")):
            stripped = True
            new_lines.append("  samples: [withheld: metadata-only]\n")
            continue
        new_lines.append(line)
    return "".join(new_lines), stripped


@dataclass(frozen=True)
class OutboundPrompt:
    prompt: str
    system: str
    cell_values_included: bool
    cells_withheld: bool
    metadata_only: bool
    payload_sha256: str
    byte_count: int


def _payload_digest(prompt: str, system: str) -> tuple[str, int]:
    blob = (system or "") + "\n" + (prompt or "")
    raw = blob.encode("utf-8")
    return hashlib.sha256(raw).hexdigest(), len(raw)


def gate_outbound_prompt(prompt: str, system: str, *, provider: str) -> OutboundPrompt:
    """Return the prompt a provider may send, and whether cells were withheld.

    Metadata-only strips cell sections then re-checks. A residual cell payload
    after strip is refused by replacing the whole user prompt with a
    metadata-only refusal stub — fail closed, never send the original.
    """
    meta_only = metadata_only_enabled()
    had_cells = contains_cell_values(prompt) or contains_cell_values(system)
    if meta_only:
        out_prompt, stripped_p = strip_cell_sections(prompt)
        out_system, stripped_s = strip_cell_sections(system)
        withheld = bool(had_cells or stripped_p or stripped_s)
        if contains_cell_values(out_prompt) or contains_cell_values(out_system):
            out_prompt = (
                "Metadata-only policy refused this prompt: residual cell values "
                "could not be stripped. Mapper may use column names and types only."
            )
            out_system = ""
            withheld = True
        digest, nbytes = _payload_digest(out_prompt, out_system)
        return OutboundPrompt(
            prompt=out_prompt,
            system=out_system,
            cell_values_included=False,
            cells_withheld=withheld,
            metadata_only=True,
            payload_sha256=digest,
            byte_count=nbytes,
        )
    digest, nbytes = _payload_digest(prompt, system)
    return OutboundPrompt(
        prompt=prompt,
        system=system,
        cell_values_included=had_cells,
        cells_withheld=False,
        metadata_only=False,
        payload_sha256=digest,
        byte_count=nbytes,
    )


def prepare_generate(prompt: str, system: str, *, provider: str) -> tuple[str, str]:
    """Choke point for ``Provider.generate``. Records the manifest, returns gated text."""
    outbound = gate_outbound_prompt(prompt, system, provider=provider)
    record_manifest(
        provider=provider,
        outbound=outbound,
        channel="generate",
    )
    return outbound.prompt, outbound.system


def prepare_messages(
    messages: list[dict[str, Any]],
    system: str,
    *,
    provider: str,
) -> tuple[list[dict[str, Any]], str]:
    """Choke point for ``generate_agent``. Strips string contents; records a manifest."""
    combined_parts: list[str] = [system or ""]
    gated_messages: list[dict[str, Any]] = []
    for item in messages or []:
        row = dict(item)
        content = row.get("content")
        if isinstance(content, str):
            combined_parts.append(content)
        gated_messages.append(row)
    joined = "\n".join(combined_parts)
    outbound = gate_outbound_prompt(joined, "", provider=provider)
    if outbound.cells_withheld or outbound.metadata_only:
        new_messages: list[dict[str, Any]] = []
        for item in gated_messages:
            row = dict(item)
            content = row.get("content")
            if isinstance(content, str):
                stripped, _ = strip_cell_sections(content)
                if outbound.metadata_only and contains_cell_values(stripped):
                    row["content"] = "[withheld: metadata-only]"
                else:
                    row["content"] = stripped
            new_messages.append(row)
        gated_messages = new_messages
        system_out, _ = strip_cell_sections(system)
        if outbound.metadata_only and contains_cell_values(system_out):
            system_out = ""
        outbound = gate_outbound_prompt(
            "\n".join(
                str(m.get("content") or "")
                for m in gated_messages
                if isinstance(m.get("content"), str)
            ),
            system_out,
            provider=provider,
        )
    else:
        system_out = system
    record_manifest(
        provider=provider,
        outbound=outbound,
        channel="generate_agent",
    )
    return gated_messages, system_out


@contextmanager
def egress_scope(
    *,
    job_id: str = "",
    purpose: str = "unspecified",
    column_names: list[str] | None = None,
    source_types: Mapping[str, str] | None = None,
) -> Iterator[None]:
    """Bind job / purpose / schema metadata for the next generate call."""
    t_job = _job_id.set(str(job_id or ""))
    t_purpose = _purpose.set(str(purpose or "unspecified"))
    t_cols = _column_names.set(tuple(str(c) for c in (column_names or []) if str(c)))
    t_types = _source_types.set(
        tuple(
            (str(k), str(v))
            for k, v in dict(source_types or {}).items()
            if k and str(v or "").strip()
        )
    )
    try:
        yield
    finally:
        _job_id.reset(t_job)
        _purpose.reset(t_purpose)
        _column_names.reset(t_cols)
        _source_types.reset(t_types)


def last_manifest() -> dict[str, Any] | None:
    """The most recent manifest written in this process, if any."""
    return dict(_LAST_MANIFEST) if _LAST_MANIFEST else None


def record_manifest(
    *,
    provider: str,
    outbound: OutboundPrompt,
    channel: str,
) -> dict[str, Any]:
    """Append one AI-egress record to the durable chain. Never stores cell values."""
    global _LAST_MANIFEST
    job = _job_id.get() or ""
    purpose = _purpose.get() or "unspecified"
    columns = list(_column_names.get() or ())
    types = {k: v for k, v in (_source_types.get() or ())}
    details = {
        "job_id": job,
        "purpose": purpose,
        "channel": channel,
        "provider": str(provider or ""),
        "crossed_customer_boundary": crossed_customer_boundary(provider),
        "metadata_only": outbound.metadata_only,
        "cell_values_included": outbound.cell_values_included,
        "cells_withheld": outbound.cells_withheld,
        "payload_sha256": outbound.payload_sha256,
        "byte_count": outbound.byte_count,
        "column_names": columns,
        "source_types": types,
        "policy": ORIGIN_METADATA_ONLY if outbound.metadata_only else ORIGIN_CELLS_ALLOWED,
    }
    # Never put prompt text in the chain — the digest is the commitment.
    event: dict[str, Any] = {}
    try:
        from services.audit_log import append_audit_event

        resource = f"job:{job}" if job else "map:unscoped"
        event = append_audit_event(
            action=AI_EGRESS_ACTION,
            resource=resource,
            actor="system",
            level="info",
            details=details,
        )
    except Exception as exc:  # noqa: BLE001 — store failure must not fail mapping
        logger.warning("AI egress manifest could not be written: %s", exc)
        event = {}
    manifest = {
        **details,
        "event_id": event.get("id"),
        "event_hash": event.get("event_hash"),
        "prev_hash": event.get("prev_hash"),
        "sealed_at": event.get("time"),
        "anchored": bool(event.get("event_hash")),
    }
    _LAST_MANIFEST = manifest
    return manifest


def proof_pack_ai_egress(job_id: str = "") -> dict[str, Any]:
    """Auditor-facing slice for a signed proof pack. No cell values."""
    calls = manifests_for_job(job_id) if job_id else []
    last = last_manifest()
    if last and not job_id:
        calls = [last]
    return {
        "metadata_only_policy": metadata_only_enabled(),
        "calls": [_public_call(c) for c in calls],
        "honesty": (
            "Each call commits to payload_sha256 of the bytes handed to the "
            "provider after the metadata-only gate. Cell values are never stored "
            "in this manifest."
        ),
    }


def manifests_for_job(job_id: str) -> list[dict[str, Any]]:
    """Re-read the durable chain for this job — not the in-process last call."""
    wanted = str(job_id or "").strip()
    if not wanted:
        return []
    try:
        from services.evidence_chain import read_chain
    except ImportError:
        return []
    out: list[dict[str, Any]] = []
    for event in read_chain():
        if str(event.get("action") or "") != AI_EGRESS_ACTION:
            continue
        details = event.get("details") if isinstance(event.get("details"), dict) else {}
        resource = str(event.get("resource") or "")
        if str(details.get("job_id") or "") != wanted and resource != f"job:{wanted}":
            continue
        out.append(
            {
                **details,
                "event_id": event.get("id"),
                "event_hash": event.get("event_hash"),
                "prev_hash": event.get("prev_hash"),
                "sealed_at": event.get("time"),
                "anchored": True,
            }
        )
    return out


def _public_call(row: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {
        "job_id",
        "purpose",
        "channel",
        "provider",
        "crossed_customer_boundary",
        "metadata_only",
        "cell_values_included",
        "cells_withheld",
        "payload_sha256",
        "byte_count",
        "column_names",
        "source_types",
        "policy",
        "event_id",
        "event_hash",
        "sealed_at",
        "anchored",
    }
    return {k: row[k] for k in allowed if k in row}


def canonical_manifest_json(row: Mapping[str, Any]) -> str:
    return json.dumps(dict(row), sort_keys=True, separators=(",", ":"), default=json_default)
