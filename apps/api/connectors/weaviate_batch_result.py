"""Per-object results of a Weaviate ``POST /v1/batch/objects`` (D304).

Weaviate answers a batch with one entry per object, in request order, each
carrying the object ``id`` and ``result``; a refused object has
``result.errors.error[].message`` (and, on newer servers,
``result.status == "FAILED"``). A batch is not atomic: the other objects of
the same request are stored. See the Weaviate REST reference, "Batch create
objects" (https://weaviate.io/developers/weaviate/api/rest#tag/batch) and
"Error handling" for batch imports
(https://weaviate.io/developers/weaviate/manage-data/import#error-handling).

The writer used to keep ``str(result.errors)`` (a Python dict repr) as the
reason and the object UUID as the finding's ``row``. The run's quarantine
renumbering then did ``int(<uuid>)`` and the operator saw "invalid literal for
int() with base 10" with 0/0 counts while Weaviate held the accepted objects.
"""

from __future__ import annotations

from typing import Any

#: Object properties the writer adds itself; never part of the source record.
_WRITER_ONLY = frozenset({"content", "chunk_index"})


def weaviate_object_errors(item: Any) -> list[str]:
    """Weaviate's own messages for one batch entry; empty when it was stored."""
    if not isinstance(item, dict):
        return ["Weaviate returned no result for this object"]
    result = item.get("result") if isinstance(item.get("result"), dict) else {}
    errors = result.get("errors")
    messages: list[str] = []
    if isinstance(errors, dict):
        for err in errors.get("error") or []:
            text = str((err or {}).get("message") or "").strip() if isinstance(err, dict) else ""
            if text:
                messages.append(text)
        if not messages:
            messages.append("Weaviate refused the object without a message")
    elif errors:
        messages.append(str(errors).strip())
    if not messages and str(result.get("status") or "").upper() == "FAILED":
        messages.append("Weaviate marked the object FAILED without a message")
    return messages


def weaviate_batch_rejects(
    response_items: list[Any],
    batch: list[dict[str, Any]],
    *,
    class_name: str,
    policy: str,
) -> tuple[int, list[dict[str, Any]]]:
    """``(stored, findings)`` for one acknowledged batch.

    ``stored`` is the number of objects Weaviate kept. Each finding names the
    source record (its mapped properties, a record-valued ``row`` like a Kafka
    poison message, so it is never renumbered as a batch line), the object id,
    the document key and Weaviate's own reason.
    """
    sent = {str(obj.get("id") or ""): obj for obj in batch}
    findings: list[dict[str, Any]] = []
    for index, item in enumerate(response_items):
        messages = weaviate_object_errors(item)
        if not messages:
            continue
        object_id = str(item.get("id") or "") if isinstance(item, dict) else ""
        obj = sent.get(object_id) or (batch[index] if index < len(batch) else {})
        object_id = object_id or str(obj.get("id") or "")
        props = obj.get("properties") if isinstance(obj.get("properties"), dict) else {}
        record = {k: v for k, v in props.items() if k not in _WRITER_ONLY}
        source_id = str(props.get("source_id") or "")
        chunk = props.get("chunk_index")
        where = f"source row {source_id}" if source_id else f"object {object_id}"
        if chunk not in (None, 0, "0"):
            where += f" chunk {chunk}"
        findings.append({
            "row": record or {"object_id": object_id},
            "column": "",
            "target": class_name,
            "value": "",
            "object_id": object_id,
            "source_id": source_id,
            "chunk_index": chunk,
            "reason": f"Weaviate rejected {where}: " + "; ".join(messages)[:480],
            "policy": "write_fail" if policy == "fail" else "write_quarantine",
        })
    return len(batch) - len(findings), findings
