"""One validator for the two transfer policy enums: ``validation_mode`` and ``schema_policy``.

QA round 12 (bugs 6 and 7): an unknown value was queued. ``validation_mode``
silently ran as ``strict`` (``normalize_validation_mode`` fails closed) and an
unknown ``schema_policy`` reached Validate as itself. The caller never learned
that the word they sent means nothing.

The allowed values are not restated here. They come from the canonical sets
the engine already uses:

* ``services.validation_mode_contract.VALIDATION_MODES`` (+ ``VALIDATION_MODE_ALIASES``)
* ``services.schema_drift.SCHEMA_POLICIES`` (+ ``SCHEMA_POLICY_ALIASES``: ``block``,
  ``approve``)

Rules, the same at every entry point (REST, Studio form, preflight, MCP,
schedules, GitOps):

* case and surrounding whitespace are not significant: ``" Strict "`` is ``strict``;
* a documented alias is accepted and stored as its canonical id;
* blank (``""`` or only whitespace) means "not given": the entry point's own
  default applies, exactly as an omitted field;
* anything else is refused, naming the field and every allowed value.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ValidationInfo, field_validator

POLICY_FIELDS = ("validation_mode", "schema_policy")


def _vocabulary(field: str) -> tuple[frozenset[str], dict[str, str]]:
    if field == "validation_mode":
        from services.validation_mode_contract import (
            VALIDATION_MODE_ALIASES,
            VALIDATION_MODES,
        )

        return VALIDATION_MODES, VALIDATION_MODE_ALIASES
    if field == "schema_policy":
        from services.schema_drift import SCHEMA_POLICIES, SCHEMA_POLICY_ALIASES

        return SCHEMA_POLICIES, SCHEMA_POLICY_ALIASES
    raise KeyError(field)


def allowed_values(field: str) -> list[str]:
    """Canonical values for ``field``, sorted (what an error lists)."""
    values, _ = _vocabulary(field)
    return sorted(values)


class PolicyValueError(ValueError):
    """An unknown ``validation_mode`` / ``schema_policy``. ``str()`` is operator text."""

    def __init__(self, field: str, value: Any) -> None:
        values, aliases = _vocabulary(field)
        self.field = field
        self.value = value
        self.allowed = sorted(values)
        self.aliases = dict(sorted(aliases.items()))
        message = (
            f"Invalid {field} {value!r}. Allowed values: {', '.join(self.allowed)}."
        )
        if self.aliases:
            message += " Also accepted: " + ", ".join(
                f"{alias} (= {target})" for alias, target in self.aliases.items()
            ) + "."
        super().__init__(message)


def canonical_policy_value(field: str, value: Any) -> str:
    """Canonical id for ``value``, ``""`` when blank; raise :class:`PolicyValueError`.

    ``None`` and blank both return ``""``: the caller applies its own default.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        raise PolicyValueError(field, value)
    key = value.strip().lower()
    if not key:
        return ""
    values, aliases = _vocabulary(field)
    key = aliases.get(key, key)
    if key not in values:
        raise PolicyValueError(field, value)
    return key


def canonical_validation_mode(value: Any) -> str:
    return canonical_policy_value("validation_mode", value)


def canonical_schema_policy(value: Any) -> str:
    return canonical_policy_value("schema_policy", value)


def canonicalize_policy_fields(
    data: dict[str, Any], *, drop_blank: bool = True
) -> dict[str, Any]:
    """Validate both policy fields in a mapping (MCP arguments, a manifest spec).

    Returns a copy with canonical values. A blank field is dropped when
    ``drop_blank`` so the receiver's default applies. Raises
    :class:`PolicyValueError` on the first unknown value.
    """
    out = dict(data)
    for field in POLICY_FIELDS:
        if field not in out:
            continue
        canonical = canonical_policy_value(field, out[field])
        if canonical:
            out[field] = canonical
        elif drop_blank:
            out.pop(field)
    return out


def form_policy_value(field: str, value: Any) -> str:
    """Studio multipart form field. Blank stays ``""`` (= inherit the plan / default).

    Raises FastAPI's ``RequestValidationError`` so the form route answers the
    same 422 shape a JSON body does.
    """
    try:
        return canonical_policy_value(field, value)
    except PolicyValueError as exc:
        from fastapi.exceptions import RequestValidationError

        raise RequestValidationError([
            {
                "type": "enum",
                "loc": ("body", field),
                "msg": str(exc),
                "input": value,
                "ctx": {"expected": ", ".join(exc.allowed)},
            }
        ]) from exc


class PolicyFieldsModel(BaseModel):
    """Base model: validate ``validation_mode`` / ``schema_policy`` once, at the model.

    A request model that carries either field derives from this instead of
    ``BaseModel``. A blank value takes the field's declared default (``None`` on
    a partial-update model: no change). An unknown value is a 422 naming the
    field and listing the allowed values.
    """

    @field_validator("validation_mode", "schema_policy", mode="before", check_fields=False)
    @classmethod
    def _canonical_policy_value(cls, value: Any, info: ValidationInfo) -> Any:
        field = str(info.field_name)
        if value is None:
            return None
        canonical = canonical_policy_value(field, value)
        if canonical:
            return canonical
        return cls.model_fields[field].get_default(call_default_factory=True)
