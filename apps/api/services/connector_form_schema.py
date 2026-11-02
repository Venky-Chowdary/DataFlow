"""Connector form fields as the UI shows them, read by the API side.

``apps/web/src/lib/connectorFormConfig.ts`` owns the connector form.
``data/connector_form_schema.json`` is exported from it (``npx tsx
scripts/export_connector_form_schema.ts`` in apps/web; a web unit test fails
when the export is stale), so Pilot and any other server-side consumer
describe exactly the fields, labels and setup steps the operator sees.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "data" / "connector_form_schema.json"

_cache: dict[str, "ConnectorForm"] | None = None


@dataclass(frozen=True)
class FormField:
    key: str
    label: str
    type: str = "text"
    optional: bool = False
    sensitive: bool = False
    hint: str = ""
    placeholder: str = ""


@dataclass(frozen=True)
class AuthMode:
    value: str
    label: str
    description: str = ""
    fields: tuple[FormField, ...] = ()


@dataclass(frozen=True)
class ConnectorForm:
    type: str
    label: str
    default_auth_mode: str
    auth_modes: tuple[AuthMode, ...] = ()
    common_fields: tuple[FormField, ...] = ()
    setup_steps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def default_mode(self) -> AuthMode | None:
        for mode in self.auth_modes:
            if mode.value == self.default_auth_mode:
                return mode
        return self.auth_modes[0] if self.auth_modes else None


def _field(raw: dict) -> FormField:
    return FormField(
        key=str(raw.get("key", "")),
        label=str(raw.get("label", "")),
        type=str(raw.get("type") or "text"),
        optional=bool(raw.get("optional", False)),
        sensitive=bool(raw.get("sensitive", False)),
        hint=str(raw.get("hint") or ""),
        placeholder=str(raw.get("placeholder") or ""),
    )


def _load() -> dict[str, ConnectorForm]:
    global _cache
    if _cache is not None:
        return _cache
    forms: dict[str, ConnectorForm] = {}
    if SCHEMA_PATH.exists():
        raw = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        for type_id, entry in raw.items():
            forms[type_id] = ConnectorForm(
                type=type_id,
                label=str(entry.get("label") or type_id),
                default_auth_mode=str(entry.get("default_auth_mode") or ""),
                auth_modes=tuple(
                    AuthMode(
                        value=str(m.get("value", "")),
                        label=str(m.get("label", "")),
                        description=str(m.get("description") or ""),
                        fields=tuple(_field(f) for f in m.get("fields", ())),
                    )
                    for m in entry.get("auth_modes", ())
                ),
                common_fields=tuple(_field(f) for f in entry.get("common_fields", ())),
                setup_steps=tuple(str(s) for s in entry.get("setup_steps", ())),
            )
    _cache = forms
    return forms


def get_connector_form(type_id: str) -> ConnectorForm | None:
    """Form for a resolved driver type (``snowflake``, ``mongodb``), or None."""
    return _load().get((type_id or "").lower().strip())


def list_connector_form_types() -> tuple[str, ...]:
    return tuple(sorted(_load()))


def describe_connector_fields(mode: AuthMode) -> str:
    """Operator steps: required fields, optional ones, then toggles.

    Each step opens with an imperative verb so the answer composer keeps it
    as part of the procedure it opened on.
    """
    toggles = [f for f in mode.fields if f.type == "checkbox"]
    inputs = [f for f in mode.fields if f.type != "checkbox"]
    required = [f for f in inputs if not f.optional]
    optional = [f for f in inputs if f.optional]

    def _name(f: FormField) -> str:
        example = f.placeholder.strip()
        if (
            example
            and not f.sensitive
            and "\n" not in example
            and len(example) <= 48
            and not example.startswith("/")
        ):
            return f"{f.label} (for example {example})"
        return f.label

    def _join(names: list[str]) -> str:
        if len(names) <= 1:
            return "".join(names)
        return ", ".join(names[:-1]) + " and " + names[-1]

    parts: list[str] = []
    if required:
        sentence = "Enter " + ", ".join(_name(f) for f in required)
        if optional:
            verb = "is" if len(optional) == 1 else "are"
            sentence += f"; {_join([f.label for f in optional])} {verb} optional"
        parts.append(sentence + ".")
    elif optional:
        parts.append("Enter " + _join([f.label for f in optional]) + " as needed.")
    for toggle in toggles:
        hint = f" — {toggle.hint.rstrip('.')}" if toggle.hint else ""
        parts.append(f"Enable {toggle.label} when the server requires it{hint}.")
    return " ".join(parts)
