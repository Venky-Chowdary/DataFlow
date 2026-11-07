"""M15: a Kafka-source rerun does not demand force — the consumer resumes at
the committed group offset, so a retry cannot re-read committed messages."""
from services.execution_engine_contract import ResumeKind, decide_retry_from_start


def test_kafka_source_retry_allowed_from_committed_offset():
    d = decide_retry_from_start(
        status="failed",
        sync_mode="incremental_append",
        rows_committed=5000,
        rows_committed_known=True,
        source_engine="kafka",
        destination_engine="postgresql",
    )
    assert d["allowed"] is True
    assert d["kind"] == ResumeKind.FROM_CURSOR.value
    assert "committed offset" in d["reason"]


def test_kafka_source_retry_even_without_cursor_contract():
    # No declared cursor column — the consumer-group offset IS the cursor.
    d = decide_retry_from_start(
        status="failed",
        sync_mode="full_refresh_append",
        rows_committed=100,
        rows_committed_known=True,
        source_engine="kafka",
        destination_engine="sqlite",
    )
    assert d["allowed"] is True
    assert d["kind"] == ResumeKind.FROM_CURSOR.value


def test_kafka_destination_still_refuses_uncursorable_retry():
    d = decide_retry_from_start(
        status="failed",
        sync_mode="incremental_append",
        rows_committed=100,
        rows_committed_known=True,
        source_engine="postgresql",
        destination_engine="kafka",
        has_cursor_contract=False,
    )
    assert d["allowed"] is False
    assert "cannot be rolled back" in d["reason"]


def test_kafka_to_kafka_retry_allowed_via_source_offset():
    d = decide_retry_from_start(
        status="failed",
        sync_mode="incremental_append",
        rows_committed=100,
        rows_committed_known=True,
        source_engine="kafka",
        destination_engine="kafka",
    )
    assert d["allowed"] is True
    assert d["kind"] == ResumeKind.FROM_CURSOR.value


def test_database_source_append_still_refused():
    d = decide_retry_from_start(
        status="failed",
        sync_mode="incremental_append",
        rows_committed=100,
        rows_committed_known=True,
        source_engine="postgresql",
        destination_engine="sqlite",
        has_cursor_contract=False,
    )
    assert d["allowed"] is False
