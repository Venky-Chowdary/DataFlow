"""D1 — a sampled dest shape is a profile that widens, not a declared catalog.

Run 2 of Postgres → object-store used to refuse what run 1 wrote: the dest
probe measured DECIMAL(2,2) from the landed values and compared it as if the
object had declared that width. Object-store writers also enforced the probed
width, so suppressing the Map verdict alone would fail open.

These cases pin the algorithm. Live proof is ``test_d1_postgres_minio_repeated_run_live``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_schema_authority import (
    CARRIER_DECLARED,
    CARRIER_SAMPLED,
    apply_sampled_profile_to_dest_types,
    authority_from_batch_meta,
    column_carrier_authority,
    default_column_authority,
    destination_schema_is_sampled,
    probe_failed_schema_is_unknown,
    widen_sampled_dest_carrier,
)
from services.mapping_pipeline import run_mapping_pipeline
from services.shape_contract import FIDELITY_DEST_TYPE_UNREAD
from services.type_system import (
    dest_decimal_single_capacity_digits,
    destination_carriers_are_inferred,
    is_lossy_coercion,
)


def _map(
    source: dict[str, str],
    destination: dict[str, str],
    *,
    dest_db: str,
    dest_exists: bool | None = True,
    authority: dict[str, str] | None = None,
) -> list[dict]:
    result = run_mapping_pipeline(
        list(source),
        list(destination),
        source_schemas=[
            {"name": name, "inferred_type": carrier, "samples": ["1"]}
            for name, carrier in source.items()
        ],
        target_schemas=[
            {"name": name, "inferred_type": carrier}
            for name, carrier in destination.items()
        ],
        use_llm=False,
        source_db_type="postgresql",
        destination_db_type=dest_db,
        destination_table_exists=dest_exists,
        source_types_authoritative=True,
        target_type_authority=authority,
    )
    return list(result.get("mappings") or [])


def _by_source(rows: list[dict]) -> dict[str, dict]:
    return {str(r.get("source")): r for r in rows}


# ---------------------------------------------------------------------------
# Engine defaults
# ---------------------------------------------------------------------------


def test_object_store_schema_is_sampled_elasticsearch_is_declared() -> None:
    assert destination_schema_is_sampled("s3") is True
    assert destination_schema_is_sampled("gcs") is True
    assert destination_schema_is_sampled("sftp") is True
    assert destination_schema_is_sampled("minio") is True
    assert destination_schema_is_sampled("redis") is True
    assert destination_schema_is_sampled("mongodb") is True
    assert destination_schema_is_sampled("elasticsearch") is False
    assert destination_schema_is_sampled("postgresql") is False
    assert default_column_authority("s3") == CARRIER_SAMPLED
    assert default_column_authority("elasticsearch") == CARRIER_DECLARED


def test_elasticsearch_is_not_an_inferred_document_store() -> None:
    """D16: an index mapping is a catalog. Unbinding it would regress 0.99."""
    assert destination_carriers_are_inferred("elasticsearch") is False
    assert destination_carriers_are_inferred("mongodb") is True
    assert destination_carriers_are_inferred("s3") is False


def test_document_store_decimal_exemption_unchanged() -> None:
    assert dest_decimal_single_capacity_digits(dest_db="mongodb") == 34
    assert dest_decimal_single_capacity_digits(dest_db="dynamodb") == 38
    assert dest_decimal_single_capacity_digits(dest_db="s3") is None
    assert is_lossy_coercion(
        "NUMERIC(12,2)", "DECIMAL", dest_db="mongodb"
    ) is False


# ---------------------------------------------------------------------------
# Widen algorithm
# ---------------------------------------------------------------------------


def test_sampled_decimal_width_widens_to_the_source_declaration() -> None:
    widened = widen_sampled_dest_carrier(
        "DECIMAL(12,2)", "DECIMAL(2,2)", dest_db="s3"
    )
    assert is_lossy_coercion("DECIMAL(12,2)", widened, dest_db="s3") is False
    assert widened.upper() != "DECIMAL(2,2)"


def test_narrow_first_sample_integer_widens_to_declared_decimal() -> None:
    widened = widen_sampled_dest_carrier("DECIMAL(12,2)", "INTEGER", dest_db="s3")
    assert is_lossy_coercion("DECIMAL(12,2)", widened, dest_db="s3") is False


def test_declared_narrowing_is_not_widened() -> None:
    """is_lossy_coercion itself is not weakened — a declared INTEGER still loses."""
    assert is_lossy_coercion("DECIMAL(12,2)", "INTEGER", dest_db="postgresql") is True
    assert is_lossy_coercion(
        "DECIMAL(12,2)", "DECIMAL(2,2)", dest_db="postgresql"
    ) is True


def test_incompatible_sampled_family_stays_lossy() -> None:
    assert (
        widen_sampled_dest_carrier("DECIMAL(12,2)", "DATE", dest_db="s3") == "DATE"
    )
    assert is_lossy_coercion("DECIMAL(12,2)", "DATE", dest_db="s3") is True


def test_write_types_widen_sampled_and_keep_operator_ceiling() -> None:
    dest = {"amount": "DECIMAL(2,2)", "id": "INTEGER"}
    mappings = [
        {
            "source": "amount",
            "target": "amount",
            "source_type": "DECIMAL(12,2)",
            "target_type": "DECIMAL(2,2)",
        },
        {
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "INTEGER",
        },
    ]
    out = apply_sampled_profile_to_dest_types(dest, mappings, dest_db="s3")
    assert is_lossy_coercion("DECIMAL(12,2)", out["amount"], dest_db="s3") is False
    ceiling = apply_sampled_profile_to_dest_types(
        dest,
        [
            {
                **mappings[0],
                "user_override": True,
                "target_type": "DECIMAL(2,2)",
            },
            mappings[1],
        ],
        dest_db="s3",
    )
    assert ceiling["amount"] == "DECIMAL(2,2)"


def test_declared_authority_is_not_widened_on_write() -> None:
    dest = {"amount": "DECIMAL(2,2)"}
    mappings = [
        {
            "source": "amount",
            "target": "amount",
            "source_type": "DECIMAL(12,2)",
            "target_type": "DECIMAL(2,2)",
        }
    ]
    out = apply_sampled_profile_to_dest_types(
        dest,
        mappings,
        dest_db="elasticsearch",
        authority_map={"amount": CARRIER_DECLARED},
    )
    assert out["amount"] == "DECIMAL(2,2)"


# ---------------------------------------------------------------------------
# Map / fidelity
# ---------------------------------------------------------------------------


def test_s3_sampled_decimal_is_not_a_lossy_refusal() -> None:
    rows = _by_source(
        _map(
            {"id": "INTEGER", "amount": "DECIMAL(12,2)"},
            {"id": "INTEGER", "amount": "DECIMAL(2,2)"},
            dest_db="s3",
        )
    )
    amount = rows["amount"]
    assert amount.get("fidelity") != "lossy_cast"
    assert amount.get("type_narrowing") is not True
    assert amount.get("target_type_origin") == "sampled_profile"
    assert float(amount.get("confidence") or 0) >= 0.85


def test_repeated_run_stability_same_route_no_new_refusal() -> None:
    source = {"id": "INTEGER", "amount": "DECIMAL(12,2)", "note": "VARCHAR(64)"}
    dest = {"id": "INTEGER", "amount": "DECIMAL(2,2)", "note": "VARCHAR"}
    first = _by_source(_map(source, dest, dest_db="s3"))
    second = _by_source(_map(source, dest, dest_db="s3"))
    for col in source:
        assert first[col].get("fidelity") == second[col].get("fidelity")
        assert second[col].get("fidelity") != "lossy_cast"


def test_operator_override_on_sampled_dest_is_a_write_ceiling() -> None:
    """An operator-authored narrowing is not silently undone by the profile."""
    dest = {"amount": "DECIMAL(2,2)"}
    mappings = [
        {
            "source": "amount",
            "target": "amount",
            "source_type": "DECIMAL(12,2)",
            "target_type": "DECIMAL(2,2)",
            "user_override": True,
        }
    ]
    out = apply_sampled_profile_to_dest_types(dest, mappings, dest_db="s3")
    assert out["amount"] == "DECIMAL(2,2)"


def test_elasticsearch_declared_identity_stays_confident() -> None:
    """D16 regression: declared long → BIGINT identity is 0.99, not 0.63."""
    rows = _by_source(
        _map(
            {"id": "INTEGER", "amount": "DECIMAL(12,2)"},
            {"id": "BIGINT", "amount": "DECIMAL(12,2)"},
            dest_db="elasticsearch",
            authority={"id": CARRIER_DECLARED, "amount": CARRIER_DECLARED},
        )
    )
    assert float(rows["id"].get("confidence") or 0) == 0.99
    assert rows["id"].get("target_type_origin") == "destination_catalog"


def test_postgresql_declared_narrowing_still_lossy() -> None:
    rows = _by_source(
        _map(
            {"amount": "DECIMAL(12,2)"},
            {"amount": "DECIMAL(2,2)"},
            dest_db="postgresql",
        )
    )
    assert rows["amount"].get("fidelity") == "lossy_cast"
    assert rows["amount"].get("target_type_origin") == "destination_catalog"


def test_unread_dest_types_are_not_invented_compatible() -> None:
    rows = _by_source(
        _map(
            {"id": "INTEGER", "amount": "DECIMAL(12,2)"},
            {},
            dest_db="s3",
            dest_exists=None,
        )
    )
    # No dest columns + unknown existence → pending / unread, never preserve.
    for row in rows.values():
        fidelity = str(row.get("fidelity") or "")
        assert fidelity != "preserve"
        strategy = str(row.get("assignment_strategy") or "")
        if strategy == "pending_dest_schema" or not row.get("target_type"):
            assert fidelity in {FIDELITY_DEST_TYPE_UNREAD, "cast", ""}


def test_probe_failure_stays_unknown() -> None:
    assert probe_failed_schema_is_unknown(
        schema={}, table_exists=None, probe_error="NoSuchKey"
    ) is True
    assert probe_failed_schema_is_unknown(
        schema={}, table_exists=False, probe_error=""
    ) is False
    assert probe_failed_schema_is_unknown(
        schema={"id": "INTEGER"}, table_exists=True, probe_error=""
    ) is False


def test_batch_meta_authority_does_not_invent_declarations() -> None:
    sampled = authority_from_batch_meta(
        ["id", "amount"],
        {
            "native_types": {"id": "INTEGER", "amount": "DECIMAL(2,2)"},
            "native_types_authority": {
                "id": CARRIER_SAMPLED,
                "amount": CARRIER_SAMPLED,
            },
        },
        "s3",
    )
    assert sampled["amount"] == CARRIER_SAMPLED
    declared = authority_from_batch_meta(
        ["id", "amount", "dynamic"],
        {
            "native_types": {"id": "BIGINT", "amount": "DECIMAL(12,2)"},
            "native_types_authority": {
                "id": CARRIER_DECLARED,
                "amount": CARRIER_DECLARED,
            },
        },
        "elasticsearch",
    )
    assert declared["id"] == CARRIER_DECLARED
    assert declared["amount"] == CARRIER_DECLARED
    assert declared["dynamic"] != CARRIER_DECLARED


def test_column_authority_folds_unstamped_to_engine_default() -> None:
    assert column_carrier_authority("s3", "amount") == CARRIER_SAMPLED
    assert column_carrier_authority("postgresql", "amount") == CARRIER_DECLARED
    assert (
        column_carrier_authority(
            "elasticsearch", "amount", authority_map={"amount": CARRIER_DECLARED}
        )
        == CARRIER_DECLARED
    )


def test_schema_from_batch_keeps_declared_native_types() -> None:
    from src.transfer.endpoint_intelligence import (
        _authority_from_batch,
        _schema_from_batch,
    )

    batch = MagicMock()
    batch.headers = ["id", "amount"]
    batch.meta = {
        "native_types": {"id": "BIGINT", "amount": "DECIMAL(12,2)"},
        "native_types_authority": {
            "id": CARRIER_DECLARED,
            "amount": CARRIER_DECLARED,
        },
    }
    assert _schema_from_batch(batch) == {
        "id": "BIGINT",
        "amount": "DECIMAL(12,2)",
    }
    assert _authority_from_batch(batch, "elasticsearch")["amount"] == CARRIER_DECLARED


def test_s3_nosuchkey_probe_does_not_invent_a_schema() -> None:
    from src.transfer.endpoint_intelligence import _attach_db_sample
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(
        kind="database",
        format="s3",
        host="localhost",
        port=9000,
        database="bucket",
        table="missing.json",
        username="dataflow",
        password="secret",
    )
    out: dict = {
        "kind": "database",
        "format": "s3",
        "connected": True,
        "objects": [],
        "columns": [],
        "schema": {},
        "message": "connected",
    }
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={
            "type": "s3",
            "database": "bucket",
            "host": "localhost",
            "port": 9000,
            "username": "dataflow",
            "password": "secret",
        },
    ), patch(
        "connectors.s3_reader.read_object",
        side_effect=Exception("An error occurred (NoSuchKey) when calling the GetObject"),
    ):
        _attach_db_sample(out, dest)
    assert out.get("schema") in ({}, None) or not out.get("schema")
    assert out.get("table_exists") is None
    assert "NoSuchKey" in str(out.get("sample_error") or out.get("message") or "")


def test_json_get_stream_dest_sample_is_not_unreadable() -> None:
    """Gate-8 dest sample of a JSON array GET must not raise json_unreadable.

    Object-store dest sample walks a one-shot body, not ``bytes``. Treating
    that handle as unreadable (the ijson-absent fallback) failed Execute after
    a correct Postgres → MinIO write.
    """
    import io

    from services.dest_precount import sample_artifact_records

    class _ForwardOnly(io.RawIOBase):
        def __init__(self, payload: bytes) -> None:
            super().__init__()
            self._buf = io.BytesIO(payload)

        def readable(self) -> bool:
            return True

        def seekable(self) -> bool:
            return False

        def read(self, size: int = -1) -> bytes:  # type: ignore[override]
            return self._buf.read(size)

    payload = (
        b'[{"id":1,"amount":12.50,"note":"alpha","created_on":"2026-08-30"},'
        b'{"id":2,"amount":100.00,"note":"beta","created_on":"2026-08-31"}]'
    )
    rows = sample_artifact_records(
        _ForwardOnly(payload), name="exports/d1_src.json", limit=50
    )
    assert len(rows) == 2
    assert rows[0]["id"] in {1, "1"}
    assert str(rows[0]["amount"]) in {"12.50", "12.5"}


def test_sftp_probe_failure_does_not_invent_a_schema() -> None:
    from src.transfer.endpoint_intelligence import _attach_db_sample
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(
        kind="database",
        format="sftp",
        host="localhost",
        port=22,
        database="/exports",
        table="missing.json",
        username="dataflow",
        password="secret",
    )
    out: dict = {
        "kind": "database",
        "format": "sftp",
        "connected": True,
        "objects": [],
        "columns": [],
        "schema": {},
        "message": "connected",
    }
    with patch(
        "src.transfer.endpoint_intelligence.resolve_connector_config",
        return_value={"type": "sftp", "database": "/exports", "host": "localhost"},
    ), patch(
        "connectors.sftp_reader.read_object",
        side_effect=Exception("No such file"),
    ):
        _attach_db_sample(out, dest)
    assert out.get("schema") in ({}, None) or not out.get("schema")
    assert out.get("table_exists") is None
    assert "No such file" in str(out.get("sample_error") or out.get("message") or "")


def test_object_store_write_types_widen_sampled_studio() -> None:
    from connectors.object_store_common import resolve_object_store_write_dest_types

    dest, err = resolve_object_store_write_dest_types(
        ["id", "amount"],
        [
            {
                "source": "id",
                "target": "id",
                "source_type": "INTEGER",
                "target_type": "INTEGER",
            },
            {
                "source": "amount",
                "target": "amount",
                "source_type": "DECIMAL(12,2)",
                "target_type": "DECIMAL(2,2)",
            },
        ],
        {"id": "INTEGER", "amount": "DECIMAL(12,2)"},
        destination_column_types={"id": "INTEGER", "amount": "DECIMAL(2,2)"},
    )
    assert err is None
    assert is_lossy_coercion("DECIMAL(12,2)", dest["amount"], dest_db="s3") is False
