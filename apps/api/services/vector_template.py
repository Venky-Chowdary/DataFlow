from __future__ import annotations

import re
import string
from collections.abc import Iterable, Mapping


class TemplateConfigError(ValueError):
    pass


class TemplateFieldMissingError(ValueError):
    def __init__(self, field: str) -> None:
        self.field = field
        super().__init__(f"Template field {field!r} is missing or null")


class TemplateFieldRenderError(ValueError):
    def __init__(self, field: str) -> None:
        self.field = field
        super().__init__(f"Template field {field!r} could not be formatted")


_FIELD_NAME = re.compile(r"^[^.\[\]{}!:]+$")
_SIMPLE_FORMAT_SPEC = re.compile(
    r"^(?:[<>=^])?[+\- ]?#?0?\d{0,4}(?:\.\d{1,4})?[bcdeEfFgGnosxX%]?$"
)
_FORMATTER = string.Formatter()


def _parse_template(template: str) -> list[tuple[str, str | None, str]]:
    if not isinstance(template, str):
        raise TemplateConfigError("text_template must be a string")
    try:
        parsed = list(_FORMATTER.parse(template))
    except ValueError:
        raise TemplateConfigError("text_template has invalid format braces") from None
    references: list[tuple[str, str | None, str]] = []
    for literal, field_name, format_spec, conversion in parsed:
        if field_name is None:
            references.append((literal, None, ""))
            continue
        if not field_name or not _FIELD_NAME.fullmatch(field_name):
            raise TemplateConfigError(
                "Template fields must be plain field names without attribute or index access"
            )
        if conversion is not None:
            raise TemplateConfigError("Template conversions are not allowed")
        if (
            "{" in format_spec
            or "}" in format_spec
            or not _SIMPLE_FORMAT_SPEC.fullmatch(format_spec)
        ):
            raise TemplateConfigError("Template format spec must be simple")
        references.append((literal, field_name, format_spec))
    return references


def validate_template(
    template: str,
    *,
    available_fields: Iterable[str],
    excluded_fields: Iterable[str],
) -> list[str]:
    fields = {str(field) for field in available_fields}
    excluded = {str(field) for field in excluded_fields}
    excluded_folded = {field.casefold() for field in excluded}
    references: list[str] = []
    for _, field, _ in _parse_template(template):
        if field is None:
            continue
        if field in excluded or field.casefold() in excluded_folded:
            raise TemplateConfigError(
                f"Template field {field!r} is excluded by the PII policy"
            )
        if field not in fields:
            raise TemplateConfigError(f"Template references unknown field {field!r}")
        if field not in references:
            references.append(field)
    return references


def _mapped_template_fields(
    headers: Iterable[str],
    mappings: Iterable[Mapping] | None,
    records: Iterable[Mapping] = (),
) -> set[str]:
    fields = {str(field) for field in headers if field}
    for mapping in mappings or ():
        fields.update(
            str(mapping.get(key) or "").strip()
            for key in ("source", "target")
            if mapping.get(key)
        )
    for record in records:
        fields.update(str(field) for field in record)
    return fields


def _mapped_excluded_fields(
    excluded_fields: Iterable[str],
    mappings: Iterable[Mapping] | None,
) -> set[str]:
    excluded = {str(field) for field in excluded_fields if field}
    for mapping in mappings or ():
        source = str(mapping.get("source") or "").strip()
        target = str(mapping.get("target") or "").strip()
        if source in excluded and target:
            excluded.add(target)
        if target in excluded and source:
            excluded.add(source)
    return excluded


def render_record_template(template: str, record: Mapping) -> str:
    pieces: list[str] = []
    for literal, field, format_spec in _parse_template(template):
        pieces.append(literal)
        if field is None:
            continue
        value = record.get(field)
        if value is None:
            raise TemplateFieldMissingError(field)
        try:
            pieces.append(format(value, format_spec) if format_spec else str(value))
        except Exception:
            raise TemplateFieldRenderError(field) from None
    return "".join(pieces)
