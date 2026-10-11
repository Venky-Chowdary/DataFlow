import pytest


@pytest.mark.parametrize("engine", ["pgvector", "qdrant"])
def test_writer_rejects_template_using_excluded_mapped_field(engine, caplog):
    if engine == "pgvector":
        from connectors.pgvector_writer import write_mapped_rows
    else:
        from connectors.qdrant_writer import write_mapped_rows

    result = write_mapped_rows(
        host="localhost",
        port=1,
        database="",
        username="",
        password="",
        schema="public",
        connection_string="",
        ssl=False,
        table_name="template-test",
        headers=["ssn"],
        data_rows=[],
        mappings=[{"source": "ssn", "target": "person_id"}],
        column_types={},
        exclude_pii_columns=["ssn"],
        text_template="sensitive={person_id}",
    )
    assert result.ok is False
    assert "PII" in (result.error or "")
    assert "sensitive=" not in caplog.text


def test_template_renders_plain_fields_and_simple_format_specs():
    from services.vector_template import render_record_template, validate_template

    template = "{name}: {amount:.2f}"
    assert validate_template(
        template,
        available_fields={"name", "amount"},
        excluded_fields=set(),
    ) == ["name", "amount"]
    assert render_record_template(template, {"name": "Widget", "amount": 3.5}) == (
        "Widget: 3.50"
    )


@pytest.mark.parametrize("record", [{"name": None}, {}])
def test_template_missing_or_none_field_is_typed(record):
    from services.vector_template import (
        TemplateFieldMissingError,
        render_record_template,
    )

    with pytest.raises(TemplateFieldMissingError, match="name"):
        render_record_template("item={name}", record)


def test_template_format_failure_is_typed_without_exposing_value():
    from services.vector_template import TemplateFieldRenderError, render_record_template

    with pytest.raises(TemplateFieldRenderError, match="amount"):
        render_record_template("amount={amount:.2f}", {"amount": "SECRET_VALUE"})


@pytest.mark.parametrize("template", ["{a.b}", "{a[0]}", "{a!r}"])
def test_template_refuses_attribute_index_and_conversion_syntax(template):
    from services.vector_template import TemplateConfigError, validate_template

    with pytest.raises(TemplateConfigError):
        validate_template(
            template,
            available_fields={"a"},
            excluded_fields=set(),
        )


def test_template_refuses_pii_and_unknown_fields():
    from services.vector_template import TemplateConfigError, validate_template

    with pytest.raises(TemplateConfigError, match="PII"):
        validate_template(
            "name={name}",
            available_fields={"name"},
            excluded_fields={"name"},
        )
    with pytest.raises(TemplateConfigError, match="unknown field"):
        validate_template(
            "name={name}",
            available_fields={"title"},
            excluded_fields=set(),
        )


def test_template_refuses_nested_or_compound_format_specs():
    from services.vector_template import TemplateConfigError, validate_template

    with pytest.raises(TemplateConfigError, match="format spec"):
        validate_template(
            "{amount:{width}}",
            available_fields={"amount", "width"},
            excluded_fields=set(),
        )
