"""D151: two gates that blocked rerunning a Kafka-source route into its own table.

- A profiled sample is bounded, so its length is not a load volume: a 100-row
  sample of a 6,000-message topic read as "below 50% of the prior median".
- An unkeyed message leaves the connector's own ``_kafka_key`` envelope column
  all NULL; the connector types it, so mapping it onto the column the first
  run created is not weak type evidence.
"""
from __future__ import annotations

from services.data_quality_history import build_load_history_report, profile_batch
from services.mapping_pipeline import run_mapping_pipeline


def _prior(rows: int) -> dict:
    return {"row_count": rows, "profile": {}, "schema_version": 1}


def test_sample_length_is_not_a_load_volume():
    sample = [{"id": str(i)} for i in range(100)]
    report = build_load_history_report(
        current_profile=profile_batch(sample, {"id": "INTEGER"}),
        prior_runs=[_prior(6000), _prior(6000)],
        current_row_count=None,
    )
    assert not [a for a in report["anomalies"] if "prior median" in a]


def test_a_measured_volume_drop_still_flags():
    report = build_load_history_report(
        current_profile=profile_batch([{"id": "1"}], {"id": "INTEGER"}),
        prior_runs=[_prior(6000), _prior(6000)],
        current_row_count=100,
    )
    assert [a for a in report["anomalies"] if "prior median" in a]


def test_all_null_kafka_envelope_key_maps_onto_the_existing_column():
    cols = ["id", "_kafka_key"]
    samples = {"id": ["1", "2"], "_kafka_key": []}
    result = run_mapping_pipeline(
        source_columns=cols,
        target_columns=cols,
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": samples["id"]},
            {"name": "_kafka_key", "inferred_type": "VARCHAR", "samples": []},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "integer"},
            {"name": "_kafka_key", "inferred_type": "text"},
        ],
        file_format="kafka",
        confidence_threshold=0.85,
        source_samples=samples,
        validation_mode="strict",
        use_llm=False,
        schema_policy="manual_review",
        destination_db_type="postgresql",
        source_db_type="kafka",
        destination_table_exists=True,
        source_types_authoritative=False,
        job_id="",
    )
    key = next(m for m in result["mappings"] if m["source"] == "_kafka_key")
    assert key["confidence"] >= 0.85
    assert not key.get("requires_review")
