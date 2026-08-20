"""Create-new destination types must be a function of the declared source type.

Snowflake → MySQL projected the same ``HIRE_DATE`` column as ``DATE`` on one Map
and ``DATETIME(6)`` on the next: sample profiling re-inferred the warehouse's
declared ``DATE`` whenever a sampled value rendered with a time part, and the
wider projection then carried its own execution policy. A create-new DDL that
changes with the sample draw is not a migration plan.
"""

from services.mapping_pipeline import run_mapping_pipeline

DECLARED_DATE = [
    {"name": "HIRE_DATE", "inferred_type": "DATE", "native_type": "DATE", "nullable": True},
]


def _project(samples: dict[str, list[str]] | None, *, authoritative: bool) -> dict:
    result = run_mapping_pipeline(
        source_columns=["HIRE_DATE"],
        target_columns=[],
        source_schemas=DECLARED_DATE,
        source_samples=samples,
        destination_db_type="mysql",
        source_db_type="snowflake",
        destination_table_exists=False,
        source_types_authoritative=authoritative,
    )
    return result["mappings"][0]


def test_declared_date_projects_date_regardless_of_sample_rendering():
    date_only = _project({"HIRE_DATE": ["2011-08-12", "2012-01-03"]}, authoritative=True)
    with_time = _project({"HIRE_DATE": ["2011-08-12 10:00:00"]}, authoritative=True)
    no_samples = _project(None, authoritative=True)

    for m in (date_only, with_time, no_samples):
        assert m["source_type"].upper() == "DATE", m
        assert m["target_type"].upper() == "DATE", m
        assert m["fidelity"] == "preserve", m
        assert not m.get("requires_risk_contract"), m

    assert date_only["target_type"] == with_time["target_type"] == no_samples["target_type"]


def test_unknown_source_type_without_samples_does_not_invent_a_temporal_target():
    """A name-derived transform is not evidence of a temporal source column.

    Stamping ``DATE`` from the column name alone made the fidelity verdict read
    ``VARCHAR → DATE`` — the pipeline calling its own invention a lossy cast and
    demanding a Risk Contract for it.
    """
    result = run_mapping_pipeline(
        source_columns=["HIRE_DATE"],
        target_columns=[],
        source_schemas=[{"name": "HIRE_DATE", "nullable": True}],
        source_samples=None,
        destination_db_type="mysql",
        source_db_type="snowflake",
        destination_table_exists=False,
    )
    m = result["mappings"][0]
    assert m["fidelity"] != "lossy_cast", m
    assert not m.get("requires_risk_contract"), m
    assert m["target_type"].upper() not in {"DATE", "DATETIME", "DATETIME(6)"}, m


DECLARED_TEXT = [
    {
        "name": "HIRE_DATE",
        "inferred_type": "VARCHAR(16777216)",
        "native_type": "VARCHAR(16777216)",
        "nullable": True,
        "samples": ["2011-08-12", "2012-01-03"],
    },
]


def _project_text(*, target_schemas: list[dict] | None = None, prior: list[dict] | None = None) -> dict:
    result = run_mapping_pipeline(
        source_columns=["HIRE_DATE"],
        target_columns=["HIRE_DATE"] if target_schemas else [],
        source_schemas=DECLARED_TEXT,
        target_schemas=target_schemas,
        source_samples={"HIRE_DATE": ["2011-08-12", "2012-01-03"]},
        destination_db_type="mysql",
        source_db_type="snowflake",
        destination_table_exists=False,
        source_types_authoritative=True,
        prior_mappings=prior,
    )
    return result["mappings"][0]


def test_declared_text_source_creates_a_text_column_not_a_signed_cast():
    """Date-like strings are evidence of content, not of the source's type.

    Narrowing eight sampled values into ``DATETIME(6)`` for a table that does not
    exist yet, then reporting ``VARCHAR(16777216) → DATETIME(6)`` as a fidelity
    loss, charged the operator a Risk Contract for the pipeline's own choice.
    """
    m = _project_text()
    assert m["target_type"].upper() in {"TEXT", "LONGTEXT"}, m
    assert m["fidelity"] == "preserve", m
    assert not m.get("requires_risk_contract"), m
    assert m["transform"] == "none", m


def test_absent_table_target_schema_is_a_proposal_not_a_declared_type():
    """Studio echoes the last projection back as the destination schema.

    Accepting it as catalog truth let a guessed ``DATETIME(6)`` return as the
    destination's declared type, complete with ``destination_catalog``
    provenance for a table the probe proved absent.
    """
    m = _project_text(target_schemas=[{"name": "HIRE_DATE", "inferred_type": "DATETIME(6)"}])
    assert m["target_type"].upper() in {"TEXT", "LONGTEXT"}, m
    assert m["fidelity"] == "preserve", m
    assert not m.get("requires_risk_contract"), m
    assert m.get("target_type_origin") != "destination_catalog", m


def test_operator_chosen_typed_target_survives_and_discloses_its_risk():
    """The refusal is against silent invention, not against an operator decision."""
    m = _project_text(
        target_schemas=[{"name": "HIRE_DATE", "inferred_type": "DATETIME(6)"}],
        prior=[
            {
                "source": "HIRE_DATE",
                "target": "HIRE_DATE",
                "target_type": "DATETIME(6)",
                "user_override": True,
                "transform": "date",
                "confidence": 0.9,
                "reasoning": "operator picked DATETIME(6)",
            }
        ],
    )
    assert m["target_type"].upper() == "DATETIME(6)", m
    assert m["fidelity"] == "lossy_cast", m
    assert m.get("requires_risk_contract"), m
