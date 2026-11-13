"""Execute-time guard for document-model routes (Validate ≡ Execute).

Mirrors ``services.multi_stream_plan.approved_recipe_refusal`` for the
document model: the model Execute is handed must be the one Validate
approved, by identity (``services.document_model.model_hash``).

One difference from the transform recipe: a document model that was never
approved is refused too. A recipe with no approval runs today's behaviour; a
model changes what is written (one nested document instead of one
collection per table), so it always goes through Validate first.
"""

from __future__ import annotations

from typing import Any


def approved_model_refusal(model: Any, approved_hash: Any) -> str:
    """``""`` when ``model`` is the model Validate approved, else the reason.

    * no model and no approval: ``""`` (every route without a model);
    * an approval but no model: refused, the approved model went missing;
    * a model that does not parse: refused with the parse error;
    * a model with no approval: refused, it was never validated;
    * a model whose hash differs from the approval: refused.

    This never raises. It is the gate Execute runs, so an unexpected input (a
    stored payload of any shape, a hash that is not text) must come back as
    a refusal to show the user, not as an exception that crashes the run.
    The broad ``except`` below fails closed: whatever cannot be checked is
    refused, never let through.
    """
    try:
        reason = _refusal(model, approved_hash)
    except Exception as exc:  # noqa: BLE001 - fail closed, see the docstring
        reason = (
            "the document model could not be checked against its approval "
            f"({type(exc).__name__}) — re-validate the model before running"
        )
    # A reason quotes the model's own text; keep it printable as UTF-8.
    return reason.encode("utf-8", "backslashreplace").decode("utf-8")


def _refusal(model: Any, approved_hash: Any) -> str:
    from services.document_model import DocumentModelError, model_hash

    approved = str(approved_hash or "").strip()
    try:
        digest = model_hash(model)
    except DocumentModelError as exc:
        return f"the document model cannot be read: {exc}"
    if not digest:
        if approved:
            return (
                f"this run was approved with a document model ({approved}), but no "
                "model was supplied — re-validate before running"
            )
        return ""
    if not approved:
        return (
            f"document model {digest} was never approved at Validate — validate "
            "the model before running"
        )
    if digest != approved:
        return (
            f"document model {digest} is not the one approved at Validate "
            f"({approved}) — re-validate the changed model before running"
        )
    return ""


def request_document_model_refusal(request: Any) -> str:
    """:func:`approved_model_refusal` for a ``TransferRequest``."""
    return approved_model_refusal(
        getattr(request, "document_model", None),
        str(getattr(request, "approved_document_model_hash", "") or ""),
    )


__all__ = ["approved_model_refusal", "request_document_model_refusal"]
